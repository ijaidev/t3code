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
import * as CloudRepositoryCredential from "./CloudRepositoryCredential.ts";
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
  readonly execEnvs: Ref.Ref<ReadonlyArray<Readonly<Record<string, string>> | undefined>>;
  readonly writes: Ref.Ref<ReadonlyArray<string>>;
  failClone: boolean;
  failWrite: boolean;
  hostProtocolVersion: string;
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
      yield* Ref.update(state.execEnvs, (envs) => [...envs, input.envs]);
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
    Ref.update(state.writes, (writes) => [...writes, String(input.path)]).pipe(
      Effect.andThen(
        state.failWrite
          ? Effect.fail(
              new SandboxProviderError({
                providerKind: E2B,
                operation: "exec",
                detail: "The sandbox filesystem rejected the write.",
              }),
            )
          : Effect.void,
      ),
    ),
  startProcess: (input) =>
    Effect.sync(() => {
      input.onStdout(
        `${JSON.stringify({
          type: "ready",
          protocolVersion: state.hostProtocolVersion,
          pid: 1,
        })}\n`,
      );
      return {
        pid: 1,
        sendStdin: () => Effect.void,
        kill: Effect.void,
        wait: Effect.succeed({ exitCode: 0 }),
      };
    }),
});

const testLayer = (
  driver: SandboxProviderDriver,
  credential: CloudRepositoryCredential.CloudRepositoryCredentialResolver["Service"] = {
    resolve: () => Effect.succeedNone,
  },
) => {
  const accounts = SandboxAccounts.layerFromDrivers([driver]);
  const store = SandboxStore.layer;
  const cloudStore = CloudThreadStore.layer;
  const lifecycle = SandboxLifecycle.layer.pipe(Layer.provide(accounts), Layer.provide(store));
  const cloud = CloudThreads.layer.pipe(
    Layer.provide(accounts),
    Layer.provide(store),
    Layer.provide(cloudStore),
    Layer.provide(lifecycle),
    Layer.provide(
      Layer.succeed(CloudRepositoryCredential.CloudRepositoryCredentialResolver, credential),
    ),
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
      execEnvs: yield* Ref.make<ReadonlyArray<Readonly<Record<string, string>> | undefined>>([]),
      writes: yield* Ref.make<ReadonlyArray<string>>([]),
      failClone: false,
      failWrite: false,
      hostProtocolVersion: "1",
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

  it.effect("reports a failed attachment write instead of claiming it was staged", () => {
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
        fake.failWrite = true;
        const written = yield* cloud.writeAttachment(
          threadId,
          `${sandbox.threadId}/attachment.txt`,
          new TextEncoder().encode("hello"),
        );
        expect(written).toBe(false);
      }).pipe(Effect.provide(testLayer(driver)));
    });
    return state;
  });

  it.effect("passes a repository credential only in the clone process environment", () => {
    const state = Effect.gen(function* () {
      const fake = yield* makeState();
      const driver = fakeDriver(fake);
      const credential = {
        resolve: () =>
          Effect.succeedSome({
            username: "x-access-token",
            token: "repo-secret",
            url: "https://github.com/example/private.git",
          }),
      };
      return yield* Effect.gen(function* () {
        yield* connect;
        const cloud = yield* CloudThreads.CloudThreads;
        yield* cloud.prepare({
          threadId,
          accountId,
          cloneUrl: "https://github.com/example/private.git",
          baseRef: "main",
          branch: "main",
        });
        const execs = yield* Ref.get(fake.execs);
        expect(execs.some((command) => command.includes("repo-secret"))).toBe(false);
        const writes = yield* Ref.get(fake.writes);
        expect(writes.some((path) => path.includes("repo-secret"))).toBe(false);
        const envs = yield* Ref.get(fake.execEnvs);
        const cloneEnvs = envs.filter((entry) => entry !== undefined);
        expect(cloneEnvs.length).toBe(1);
        expect(cloneEnvs[0]?.["GIT_CONFIG_VALUE_0"]).toBe(
          `AUTHORIZATION: basic ${Buffer.from("x-access-token:repo-secret").toString("base64")}`,
        );
        const record = yield* cloud.getRecord(threadId);
        if (record._tag === "Some") {
          expect(JSON.stringify(record.value)).not.toContain("repo-secret");
        }
      }).pipe(Effect.provide(testLayer(driver, credential)));
    });
    return state;
  });

  it.effect("fails closed when the sandbox host speaks another protocol", () => {
    const state = Effect.gen(function* () {
      const fake = yield* makeState({ hostProtocolVersion: "999" });
      const driver = fakeDriver(fake);
      return yield* Effect.gen(function* () {
        yield* connect;
        const cloud = yield* CloudThreads.CloudThreads;
        yield* cloud.prepare({
          threadId,
          accountId,
          cloneUrl: "https://github.com/example/public.git",
          baseRef: "main",
          branch: "main",
        });
        const opened = yield* Effect.result(cloud.openHost(threadId));
        expect(opened._tag).toBe("Failure");
        if (opened._tag === "Failure") {
          expect(opened.failure.detail).toContain("999");
        }
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
