import {
  type CloudSandboxState,
  ModelSelection,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  SandboxInstanceId,
  SandboxProviderAccountId,
  SandboxProviderKind,
  type SandboxProviderCapabilities,
  ThreadId,
} from "@t3tools/contracts";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as SecretResolver from "@t3tools/secret-provider-core/server/SecretResolver";
import * as CloudRepositoryCredential from "./CloudRepositoryCredential.ts";
import { describe, expect, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import { HttpClient } from "effect/http";

import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as CloudThreadPorts from "./CloudThreadPorts.ts";

import * as ServerConfig from "../config.ts";
import * as PreviewManager from "../preview/Manager.ts";
import * as ServerSettingsModule from "../serverSettings.ts";
import * as CloudSandboxPreviewBridge from "./CloudSandboxPreviewBridge.ts";
import * as CloudThreadRuntime from "./CloudThreadRuntime.ts";
import * as CloudThreadStore from "./CloudThreadStore.ts";
import * as CloudThreads from "./CloudThreads.ts";
import * as SandboxAccounts from "./SandboxAccounts.ts";
import { type SandboxProviderDriver } from "./SandboxProvider.ts";
import * as SandboxLifecycle from "./SandboxLifecycle.ts";
import * as SandboxStore from "./SandboxStore.ts";

const E2B = SandboxProviderKind.make("e2b");
const accountId = SandboxProviderAccountId.make("e2b-work");
const threadId = ThreadId.make("thread-cloud-lifecycle");

const defaultCapabilities: SandboxProviderCapabilities = {
  create: true,
  exec: true,
  pauseResume: true,
  delete: true,
  snapshots: true,
  privatePorts: true,
  sizes: [{ id: "small", label: "Small", cpuCount: 2, memoryMB: 1024 }],
  regions: [{ id: "us", label: "United States" }],
};

interface FakeCloud {
  readonly calls: Ref.Ref<ReadonlyArray<string>>;
  capabilities: SandboxProviderCapabilities;
  created: number;
  paused: boolean;
  dirtyStatus: string;
  aheadCount: string;
  resumeGate: Deferred.Deferred<void> | null;
  hostStartGate: Deferred.Deferred<void> | null;
  hostStartEntered: Deferred.Deferred<void> | null;
}

const fakeDriver = (state: FakeCloud): SandboxProviderDriver => ({
  kind: E2B,
  label: "E2B",
  capabilities: state.capabilities,
  validateCredential: () => Effect.void,
  create: () =>
    Effect.sync(() => {
      state.created += 1;
      state.paused = false;
      return { providerSandboxId: `e2b-${state.created}` };
    }),
  findCreated: () => Effect.succeed([]),
  get: () => Effect.succeed({ status: state.paused ? ("paused" as const) : ("running" as const) }),
  pause: () =>
    Effect.sync(() => {
      state.paused = true;
    }).pipe(Effect.andThen(Ref.update(state.calls, (calls) => [...calls, "pause"])), Effect.asVoid),
  resume: () =>
    (state.resumeGate === null ? Effect.void : Deferred.await(state.resumeGate)).pipe(
      Effect.andThen(
        Effect.sync(() => {
          state.paused = false;
        }),
      ),
      Effect.andThen(Ref.update(state.calls, (calls) => [...calls, "resume"])),
      Effect.asVoid,
    ),
  delete: () => Ref.update(state.calls, (calls) => [...calls, "delete"]).pipe(Effect.asVoid),
  exec: (input) =>
    Effect.gen(function* () {
      if (input.command === "git status --porcelain") {
        return { exitCode: 0, stdout: state.dirtyStatus, stderr: "" };
      }
      if (input.command.includes("git rev-list --count")) {
        return { exitCode: 0, stdout: `${state.aheadCount}\n`, stderr: "" };
      }
      if (input.command.includes("$HOME")) {
        return { exitCode: 0, stdout: "/home/t3\n", stderr: "" };
      }
      return { exitCode: 0, stdout: "", stderr: "" };
    }),
  writeFile: () => Effect.void,
  startProcess: (input) =>
    Effect.gen(function* () {
      const entered = state.hostStartEntered;
      if (entered !== null) yield* Deferred.succeed(entered, undefined);
      const gate = state.hostStartGate;
      if (gate !== null) yield* Deferred.await(gate);
      input.onStdout(`${JSON.stringify({ type: "ready", protocolVersion: "2", pid: 1 })}\n`);
      return {
        pid: 1,
        sendStdin: (data: string) =>
          Effect.sync(() => {
            const message = JSON.parse(data) as { readonly type?: string };
            if (message.type === "turn") {
              input.onStdout(`${JSON.stringify({ type: "turn_started", runId: "run-1" })}\n`);
            }
          }),
        kill: Effect.void,
        wait: Effect.never,
      };
    }),
});

const makeLayer = (
  driver: SandboxProviderDriver,
  idleMinutes = 10,
  ports?: CloudThreadPorts.CloudThreadPorts["Service"],
) => {
  const accounts = SandboxAccounts.layerFromDrivers([driver]);
  const store = SandboxStore.layer;
  const cloudStore = CloudThreadStore.layer;
  const lifecycle = SandboxLifecycle.layer.pipe(Layer.provide(accounts), Layer.provide(store));
  const cloudWithPorts =
    ports === undefined
      ? CloudThreads.layer
      : CloudThreads.layer.pipe(
          Layer.provide(Layer.succeed(CloudThreadPorts.CloudThreadPorts, ports)),
        );
  const cloud = cloudWithPorts.pipe(
    Layer.provide(accounts),
    Layer.provide(store),
    Layer.provide(cloudStore),
    Layer.provide(lifecycle),
    Layer.provide(
      Layer.succeed(CloudRepositoryCredential.CloudRepositoryCredentialResolver, {
        resolve: () => Effect.succeedNone,
      }),
    ),
  );
  return Layer.mergeAll(accounts, store, cloudStore, lifecycle, cloud).pipe(
    Layer.provideMerge(ServerSettingsModule.layerTest({ cloudSandboxIdleMinutes: idleMinutes })),
    Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "t3-cloud-lifecycle-test-" })),
    Layer.provide(NodeServices.layer),
    Layer.provide(NodeCrypto.layer),
    Layer.provide(Layer.mock(SecretResolver.SecretResolver)({})),
  );
};

const makePreviewLayer = (driver: SandboxProviderDriver) => {
  const sandbox = makeLayer(driver);
  const preview = PreviewManager.layer.pipe(
    Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "t3-cloud-preview-test-" })),
    Layer.provide(NodeCrypto.layer),
    Layer.provide(NodeServices.layer),
  );
  const bridge = CloudSandboxPreviewBridge.layer.pipe(
    Layer.provide(sandbox),
    Layer.provide(preview),
  );
  return Layer.mergeAll(sandbox, preview, bridge);
};

const makeRuntimeLayer = (driver: SandboxProviderDriver) => {
  const sandbox = makeLayer(driver);
  const cloudThreadPorts = Layer.succeed(CloudThreadPorts.CloudThreadPorts, {
    setPorts: () => Effect.void,
    clear: () => Effect.void,
    snapshot: Effect.succeed({ servers: [], unavailable: [] }),
    subscribe: () => Effect.void,
  });
  return Layer.merge(
    sandbox,
    CloudThreadRuntime.layer.pipe(
      Layer.provide(sandbox),
      Layer.provide(cloudThreadPorts),
      Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "t3-cloud-runtime-test-" })),
      Layer.provide(NodeServices.layer),
      Layer.provide(
        Layer.succeed(
          HttpClient.HttpClient,
          HttpClient.make(() => Effect.die("the test never calls the model")),
        ),
      ),
    ),
  );
};

const runtimeInstanceId = ProviderInstanceId.make("claudeAgent");
const runtimeModelSelection: ModelSelection = {
  instanceId: runtimeInstanceId,
  model: "claude-sonnet-4-5",
};

const runtimeBaseAdapter = {
  instanceId: runtimeInstanceId,
  driver: ProviderDriverKind.make("claudeAgent"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.die("unused"),
  openSession: () => Effect.die("unused"),
} as unknown as ProviderAdapter.ProviderAdapterV2["Service"];

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

const prepare = CloudThreads.CloudThreads.pipe(
  Effect.flatMap((cloud) =>
    cloud.prepare({
      threadId,
      accountId,
      cloneUrl: "https://github.com/example/public.git",
      baseRef: "main",
      branch: "main",
    }),
  ),
);

const makeState = Effect.fn("test.makeState")(function* (
  capabilities: SandboxProviderCapabilities = defaultCapabilities,
) {
  const calls = yield* Ref.make<ReadonlyArray<string>>([]);
  const state: FakeCloud = {
    calls,
    capabilities,
    created: 0,
    paused: false,
    dirtyStatus: "",
    aheadCount: "0",
    resumeGate: null,
    hostStartGate: null,
    hostStartEntered: null,
  };
  return state;
});

const oldActivity = (store: SandboxStore.SandboxStore["Service"], sandboxId: SandboxInstanceId) =>
  Effect.gen(function* () {
    const record = yield* store.get(sandboxId);
    if (Option.isNone(record)) return;
    const now = yield* DateTime.now;
    yield* store.upsert({
      ...record.value,
      lastActivityAt: DateTime.formatIso(DateTime.subtract(now, { minutes: 20 })),
    });
  });

describe("cloud sandbox lifecycle", () => {
  it.effect("reports provider, size, region, and age for a running sandbox", () => {
    const test = Effect.gen(function* () {
      const fake = yield* makeState();
      return yield* Effect.gen(function* () {
        yield* connect;
        const sandbox = yield* prepare;
        const cloud = yield* CloudThreads.CloudThreads;
        const state = yield* cloud.getCloudSandboxState(threadId);
        expect(Option.isSome(state)).toBe(true);
        if (Option.isSome(state)) {
          expect(state.value.status).toBe("running");
          expect(state.value.providerLabel).toBe("E2B");
          expect(state.value.size?.id).toBe("small");
          expect(state.value.region?.id).toBe("us");
          expect(state.value.canSleep).toBe(true);
          expect(state.value.canDelete).toBe(true);
          expect(state.value.createdAt).toBe(sandbox.createdAt);
        }
      }).pipe(Effect.provide(makeLayer(fakeDriver(fake))));
    });
    return test;
  });

  it.effect("sleeps an idle sandbox and wakes it before continuing", () => {
    const test = Effect.gen(function* () {
      const fake = yield* makeState();
      return yield* Effect.gen(function* () {
        yield* connect;
        yield* prepare;
        const cloud = yield* CloudThreads.CloudThreads;
        const sleeping = yield* cloud.sleepCloudSandbox(threadId);
        expect(sleeping.status).toBe("sleeping");
        expect(fake.paused).toBe(true);
        expect(yield* Ref.get(fake.calls)).toContain("pause");

        const states = yield* Queue.unbounded<CloudSandboxState>();
        yield* Effect.forkScoped(
          cloud
            .subscribeCloudSandbox(threadId)
            .pipe(Stream.runForEach((state) => Queue.offer(states, state))),
        );
        // The snapshot is the sleeping state.
        expect((yield* Queue.take(states)).status).toBe("sleeping");

        const gate = yield* Deferred.make<void>();
        fake.resumeGate = gate;
        const waking = yield* Effect.forkScoped(cloud.wakeCloudSandbox(threadId));
        expect((yield* Queue.take(states)).status).toBe("waking");
        yield* Deferred.succeed(gate, undefined);
        const resumed = yield* Fiber.join(waking);
        expect(resumed.status).toBe("running");
        expect((yield* Queue.take(states)).status).toBe("running");
        expect(fake.paused).toBe(false);
      }).pipe(Effect.provide(makeLayer(fakeDriver(fake))));
    });
    return test;
  });

  it.effect("keeps a sandbox awake during a turn and an open preview", () => {
    const test = Effect.gen(function* () {
      const fake = yield* makeState();
      return yield* Effect.gen(function* () {
        yield* connect;
        yield* prepare;
        const cloud = yield* CloudThreads.CloudThreads;

        yield* cloud.beginTurn(threadId);
        const duringTurn = yield* Effect.result(cloud.sleepCloudSandbox(threadId));
        expect(duringTurn._tag).toBe("Failure");
        if (duringTurn._tag === "Failure") {
          expect(duringTurn.failure.detail).toContain("active turn");
        }
        yield* cloud.endTurn(threadId);

        yield* cloud.holdPreview(threadId, "tab-1");
        const duringPreview = yield* Effect.result(cloud.sleepCloudSandbox(threadId));
        expect(duringPreview._tag).toBe("Failure");
        if (duringPreview._tag === "Failure") {
          expect(duringPreview.failure.detail).toContain("preview");
        }
        yield* cloud.releasePreview(threadId, "tab-1");

        const inUse = yield* cloud.getCloudSandboxState(threadId);
        expect(Option.isSome(inUse) && inUse.value.canSleep).toBe(true);
        const slept = yield* cloud.sleepCloudSandbox(threadId);
        expect(slept.status).toBe("sleeping");

        // Opening a preview wakes the sandbox and holds it awake until the tab closes.
        yield* cloud.holdPreview(threadId, "preview-1");
        const awake = yield* cloud.getCloudSandboxState(threadId);
        expect(Option.isSome(awake) && awake.value.status).toBe("running");
        expect(Option.isSome(awake) && awake.value.sleepDisabledReason).toBe("open-preview");
        yield* cloud.releasePreview(threadId, "preview-1");
        const released = yield* cloud.getCloudSandboxState(threadId);
        expect(Option.isSome(released) && released.value.canSleep).toBe(true);
      }).pipe(Effect.provide(makeLayer(fakeDriver(fake))));
    });
    return test;
  });

  it.effect("holds the sandbox before the host starts", () => {
    const test = Effect.gen(function* () {
      const fake = yield* makeState();
      return yield* Effect.gen(function* () {
        yield* connect;
        yield* prepare;
        const cloud = yield* CloudThreads.CloudThreads;
        const runtimeService = yield* CloudThreadRuntime.CloudThreadRuntime;
        const runtime = yield* runtimeService.openSession(
          {
            threadId,
            providerSessionId: ProviderSessionId.make("session-cloud-hold"),
            modelSelection: runtimeModelSelection,
            runtimePolicy: { runtimeMode: "full-access", interactionMode: "default", cwd: null },
            environment: [{ name: "ANTHROPIC_API_KEY", value: "test-key" }],
          },
          runtimeBaseAdapter,
        );
        const providerThread = yield* runtime.ensureThread({
          threadId,
          modelSelection: runtimeModelSelection,
          runtimePolicy: { runtimeMode: "full-access", interactionMode: "default", cwd: null },
        });
        const entered = yield* Deferred.make<void>();
        const gate = yield* Deferred.make<void>();
        fake.hostStartEntered = entered;
        fake.hostStartGate = gate;
        const started = yield* runtime
          .startTurn({
            appThread: {} as never,
            threadId,
            runId: "run-hold" as never,
            runOrdinal: 1,
            providerTurnOrdinal: 1,
            attemptId: "attempt-hold" as never,
            rootNodeId: "node-hold" as never,
            providerThread,
            message: {
              messageId: "message-hold" as never,
              text: "stay awake",
              attachments: [],
              createdBy: "user",
              creationSource: "web",
            },
            modelSelection: runtimeModelSelection,
            runtimePolicy: { runtimeMode: "full-access", interactionMode: "default", cwd: null },
          })
          .pipe(Effect.forkChild);

        yield* Deferred.await(entered);
        const refused = yield* Effect.result(cloud.sleepCloudSandbox(threadId));
        expect(refused._tag).toBe("Failure");
        if (refused._tag === "Failure") {
          expect(refused.failure.detail).toContain("active turn");
        }

        yield* Deferred.succeed(gate, undefined);
        yield* Fiber.join(started);
      }).pipe(Effect.provide(makeRuntimeLayer(fakeDriver(fake))));
    });
    return test;
  });

  it.effect("holds a cloud sandbox awake while a preview is open", () => {
    const test = Effect.gen(function* () {
      const fake = yield* makeState();
      return yield* Effect.gen(function* () {
        yield* connect;
        yield* prepare;
        const cloud = yield* CloudThreads.CloudThreads;
        const previews = yield* PreviewManager.PreviewManager;
        const states = yield* Queue.unbounded<CloudSandboxState>();
        yield* Effect.forkScoped(
          cloud
            .subscribeCloudSandbox(threadId)
            .pipe(Stream.runForEach((state) => Queue.offer(states, state))),
        );
        expect((yield* Queue.take(states)).canSleep).toBe(true);

        const session = yield* previews.open({ threadId, url: "localhost:5173" });
        const held = yield* Queue.take(states);
        expect(held.status).toBe("running");
        expect(held.sleepDisabledReason).toBe("open-preview");
        const refused = yield* Effect.result(cloud.sleepCloudSandbox(threadId));
        expect(refused._tag).toBe("Failure");

        yield* previews.close({ threadId, tabId: session.tabId });
        expect((yield* Queue.take(states)).canSleep).toBe(true);
      }).pipe(Effect.provide(makePreviewLayer(fakeDriver(fake))));
    });
    return test;
  });

  it.effect("sweeps a sandbox idle past the configured timeout", () => {
    const test = Effect.gen(function* () {
      const fake = yield* makeState();
      return yield* Effect.gen(function* () {
        yield* connect;
        const sandbox = yield* prepare;
        const cloud = yield* CloudThreads.CloudThreads;
        const store = yield* SandboxStore.SandboxStore;
        yield* oldActivity(store, sandbox.id);

        const slept = yield* cloud.sweepIdle;
        expect(slept).toBe(1);
        const state = yield* cloud.getCloudSandboxState(threadId);
        expect(Option.isSome(state) && state.value.status).toBe("sleeping");
        expect(yield* Ref.get(fake.calls)).toContain("pause");
      }).pipe(Effect.provide(makeLayer(fakeDriver(fake), 1)));
    });
    return test;
  });

  it.effect("warns about unpushed work and keeps the transcript record after delete", () => {
    const test = Effect.gen(function* () {
      const fake = yield* makeState();
      fake.dirtyStatus = " M src/file.ts\n?? notes.md\n";
      fake.aheadCount = "2";
      const cleared: ThreadId[] = [];
      const ports = {
        setPorts: () => Effect.void,
        clear: (clearedThreadId: ThreadId) =>
          Effect.sync(() => {
            cleared.push(clearedThreadId);
          }),
        snapshot: Effect.succeed({ servers: [], unavailable: [] }),
        subscribe: () => Effect.void,
      } satisfies CloudThreadPorts.CloudThreadPorts["Service"];
      return yield* Effect.gen(function* () {
        yield* connect;
        yield* prepare;
        const cloud = yield* CloudThreads.CloudThreads;
        const preview = yield* cloud.previewCloudSandboxDelete(threadId);
        expect(preview.risk).toEqual({
          hasUnpushedChanges: true,
          changedFiles: 2,
          unpushedCommits: 2,
        });

        const refused = yield* Effect.result(
          cloud.deleteCloudSandbox({ threadId, acknowledgedUnpushedChanges: false }),
        );
        expect(refused._tag).toBe("Failure");
        if (refused._tag === "Failure") {
          expect(refused.failure.detail).toContain("2 changed file");
          expect(refused.failure.detail).toContain("2 unpushed commit");
        }

        const deleted = yield* cloud.deleteCloudSandbox({
          threadId,
          acknowledgedUnpushedChanges: true,
        });
        expect(deleted.status).toBe("lost");
        expect(deleted.sandboxId).toBeNull();
        expect(deleted.providerLabel).toBe("E2B");
        expect(deleted.size?.id).toBe("small");
        expect(yield* Ref.get(fake.calls)).toContain("delete");

        const record = yield* cloud.getRecord(threadId);
        expect(Option.isSome(record) && record.value.status).toBe("lost");
        const sandbox = yield* cloud.getSandbox(threadId);
        expect(Option.isNone(sandbox)).toBe(true);
        expect(cleared).toEqual([threadId]);

        // The explicit-delete path is idempotent and also clears ports when the sandbox is gone.
        yield* cloud.deleteCloudSandbox({ threadId, acknowledgedUnpushedChanges: true });
        expect(cleared).toEqual([threadId, threadId]);
      }).pipe(Effect.provide(makeLayer(fakeDriver(fake), 10, ports)));
    });
    return test;
  });

  it.effect("hides sleep with a reason on a provider without pause, but still deletes", () => {
    const test = Effect.gen(function* () {
      const fake = yield* makeState({ ...defaultCapabilities, pauseResume: false });
      return yield* Effect.gen(function* () {
        yield* connect;
        yield* prepare;
        const cloud = yield* CloudThreads.CloudThreads;
        const state = yield* cloud.getCloudSandboxState(threadId);
        expect(Option.isSome(state)).toBe(true);
        if (Option.isSome(state)) {
          expect(state.value.canSleep).toBe(false);
          expect(state.value.sleepDisabledReason).toBe("provider-no-sleep");
          expect(state.value.canDelete).toBe(true);
        }
        const refused = yield* Effect.result(cloud.sleepCloudSandbox(threadId));
        expect(refused._tag).toBe("Failure");

        const deleted = yield* cloud.deleteCloudSandbox({
          threadId,
          acknowledgedUnpushedChanges: false,
        });
        expect(deleted.status).toBe("lost");
        expect(yield* Ref.get(fake.calls)).toContain("delete");
      }).pipe(Effect.provide(makeLayer(fakeDriver(fake))));
    });
    return test;
  });
});
