import {
  SandboxProviderAccountId,
  SandboxProviderKind,
  type SandboxProviderCapabilities,
  ThreadId,
} from "@t3tools/contracts";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as SecretResolver from "@t3tools/secret-provider-core/server/SecretResolver";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";

import * as ServerConfig from "../config.ts";
import * as ServerSettingsModule from "../serverSettings.ts";
import * as CloudThreadStore from "./CloudThreadStore.ts";
import * as CloudThreads from "./CloudThreads.ts";
import * as SandboxAccounts from "./SandboxAccounts.ts";
import { SandboxProviderError, type SandboxProviderDriver } from "./SandboxProvider.ts";
import * as SandboxLifecycle from "./SandboxLifecycle.ts";
import * as SandboxStore from "./SandboxStore.ts";

const E2B = SandboxProviderKind.make("e2b");
const accountId = SandboxProviderAccountId.make("e2b-work");
const threadId = ThreadId.make("thread-cloud-1");

const capabilities: SandboxProviderCapabilities = {
  create: true,
  exec: true,
  pauseResume: true,
  delete: true,
  snapshots: true,
  privatePorts: true,
  sizes: [{ id: "small", label: "Small", cpuCount: 2, memoryMB: 1024 }],
  regions: [{ id: "us", label: "United States" }],
};

interface FakeState {
  readonly calls: Ref.Ref<ReadonlyArray<string>>;
  readonly execs: Ref.Ref<ReadonlyArray<string>>;
  readonly writes: Ref.Ref<ReadonlyArray<string>>;
  failClone: boolean;
  created: number;
}

const fakeDriver = (state: FakeState): SandboxProviderDriver => ({
  kind: E2B,
  label: "E2B",
  capabilities,
  validateCredential: () => Effect.void,
  create: () =>
    Effect.sync(() => {
      state.created += 1;
      return { providerSandboxId: `e2b-${state.created}` };
    }),
  findCreated: () => Effect.succeed([]),
  get: () => Effect.succeed({ status: "running" as const }),
  pause: () => Effect.void,
  resume: () => Effect.void,
  delete: () => Ref.update(state.calls, (calls) => [...calls, "delete"]).pipe(Effect.asVoid),
  exec: (input) =>
    Effect.gen(function* () {
      yield* Ref.update(state.execs, (execs) => [...execs, input.command]);
      if (input.command.includes("git clone")) {
        return state.failClone
          ? {
              exitCode: 128,
              stdout: "",
              stderr: "fatal: repository not found",
            }
          : { exitCode: 0, stdout: "", stderr: "" };
      }
      if (input.command.includes("$HOME")) {
        return { exitCode: 0, stdout: "/home/t3\n", stderr: "" };
      }
      return { exitCode: 0, stdout: "", stderr: "" };
    }),
  writeFile: (input) =>
    Ref.update(state.writes, (writes) => [...writes, String(input.path)]).pipe(Effect.asVoid),
  startProcess: () =>
    Effect.succeed({
      pid: 1,
      sendStdin: () => Effect.void,
      kill: Effect.void,
      wait: Effect.succeed({ exitCode: 0 }),
    }),
});

const testLayer = (driver: SandboxProviderDriver) => {
  const accounts = SandboxAccounts.layerFromDrivers([driver]);
  const store = SandboxStore.layer;
  const cloudStore = CloudThreadStore.layer;
  const lifecycle = SandboxLifecycle.layer.pipe(Layer.provide(accounts), Layer.provide(store));
  const cloud = CloudThreads.layer.pipe(
    Layer.provide(accounts),
    Layer.provide(store),
    Layer.provide(cloudStore),
    Layer.provide(lifecycle),
  );
  return Layer.mergeAll(accounts, store, cloudStore, lifecycle, cloud).pipe(
    Layer.provideMerge(ServerSettingsModule.layerTest({})),
    Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "t3-cloud-threads-test-" })),
    Layer.provide(NodeServices.layer),
    Layer.provide(NodeCrypto.layer),
    Layer.provide(Layer.mock(SecretResolver.SecretResolver)({})),
  );
};

const connect = SandboxAccounts.SandboxAccounts.pipe(
  Effect.flatMap((accounts) =>
    accounts.connect({
      id: accountId,
      config: {
        kind: E2B,
        label: "E2B",
        apiKey: "valid-key",
        template: "base",
        enabled: true,
      },
    }),
  ),
);

const makeState = (overrides: Partial<FakeState> = {}) =>
  Effect.gen(function* () {
    return {
      calls: yield* Ref.make<ReadonlyArray<string>>([]),
      execs: yield* Ref.make<ReadonlyArray<string>>([]),
      writes: yield* Ref.make<ReadonlyArray<string>>([]),
      failClone: false,
      created: 0,
      ...overrides,
    } satisfies FakeState;
  });

describe("cloud threads", () => {
  it.effect("provisions one sandbox, writes the host, and clones the selected branch", () => {
    const state = Effect.gen(function* () {
      const fake = yield* makeState();
      const driver = fakeDriver(fake);
      return yield* Effect.gen(function* () {
        yield* connect;
        const cloud = yield* CloudThreads.CloudThreads;
        const sandbox = yield* cloud.prepare({
          threadId,
          accountId,
          cloneUrl: "https://github.com/example/public.git",
          baseRef: "main",
          branch: "main",
        });
        expect(sandbox.threadId).toBe(threadId);
        expect(sandbox.status).toBe("running");
        expect(fake.created).toBe(1);
        const execs = yield* Ref.get(fake.execs);
        expect(execs.some((command) => command.includes("git clone --branch 'main'"))).toBe(true);
        expect(
          execs.some((command) => command.includes("https://github.com/example/public.git")),
        ).toBe(true);
        const writes = yield* Ref.get(fake.writes);
        expect(writes.some((path) => path.endsWith("/.t3/thread-host/thread-host.py"))).toBe(true);
        const lifecycle = yield* SandboxLifecycle.SandboxLifecycle;
        const listed = yield* lifecycle.listTestSandboxes;
        expect(listed.sandboxes).toEqual([]);
        const record = yield* cloud.getRecord(threadId);
        expect(record._tag).toBe("Some");
        if (record._tag === "Some") {
          expect(record.value.workspacePath).toBe(`/home/t3/.t3/threads/${threadId}`);
        }
      }).pipe(Effect.provide(testLayer(driver)));
    });
    return state;
  });

  it.effect("returns the same sandbox when preparation retries after ready", () => {
    const state = Effect.gen(function* () {
      const fake = yield* makeState();
      const driver = fakeDriver(fake);
      return yield* Effect.gen(function* () {
        yield* connect;
        const cloud = yield* CloudThreads.CloudThreads;
        const input = {
          threadId,
          accountId,
          cloneUrl: "https://github.com/example/public.git",
          baseRef: "main",
          branch: "main",
        };
        const first = yield* cloud.prepare(input);
        const second = yield* cloud.prepare(input);
        expect(second.id).toBe(first.id);
        expect(fake.created).toBe(1);
      }).pipe(Effect.provide(testLayer(driver)));
    });
    return state;
  });

  it.effect("deletes the sandbox when the clone fails", () => {
    const state = Effect.gen(function* () {
      const fake = yield* makeState({ failClone: true });
      const driver = fakeDriver(fake);
      return yield* Effect.gen(function* () {
        yield* connect;
        const cloud = yield* CloudThreads.CloudThreads;
        const result = yield* Effect.result(
          cloud.prepare({
            threadId,
            accountId,
            cloneUrl: "https://github.com/example/private.git",
            baseRef: "main",
            branch: "main",
          }),
        );
        expect(result._tag).toBe("Failure");
        if (result._tag === "Failure") {
          expect(result.failure.operation).toBe("prepare");
          expect(result.failure.detail).toContain("Could not clone");
        }
        const calls = yield* Ref.get(fake.calls);
        expect(calls).toContain("delete");
        const record = yield* cloud.getRecord(threadId);
        expect(record._tag).toBe("None");
        const sandbox = yield* cloud.getSandbox(threadId);
        expect(sandbox._tag).toBe("None");
      }).pipe(Effect.provide(testLayer(driver)));
    });
    return state;
  });

  it("reports a provider failure without leaking an error detail", () => {
    const failure = new SandboxProviderError({
      providerKind: E2B,
      operation: "create",
      detail: "The provider has no capacity right now.",
    });
    expect(failure.message).toBe("The provider has no capacity right now.");
  });
});
