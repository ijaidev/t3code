import {
  DEFAULT_SERVER_SETTINGS,
  MessageId,
  type ModelSelection,
  NodeId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";

import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import type { ProviderAdapterHostPaths } from "@t3tools/provider-core/server/ProviderAdapterHostProtocol";

import * as ServerConfig from "../config.ts";
import * as CloudProviderRuntime from "./CloudProviderRuntime.ts";
import * as CloudThreads from "./CloudThreads.ts";
import type { CloudThreadRecord } from "./CloudThreadStore.ts";

const threadId = ThreadId.make("thread-cloud-opencode");
const instanceId = ProviderInstanceId.make("opencode");
const driver = ProviderDriverKind.make("opencode");
const modelSelection: ModelSelection = { instanceId, model: "opencode-go/kimi" };
const now = "2026-11-27T00:00:00.000Z";
const paths: ProviderAdapterHostPaths = {
  cwd: "/home/t3/.t3/threads/thread-cloud-opencode",
  baseDir: "/home/t3/.t3/provider-host",
  stateDir: "/home/t3/.t3/provider-host/state",
  providerStatusCacheDir: "/home/t3/.t3/provider-host/cache",
  attachmentsDir: "/home/t3/.t3/attachments",
};

const record: CloudThreadRecord = {
  threadId,
  sandboxId: "sandbox-1" as CloudThreadRecord["sandboxId"],
  accountId: "e2b-work" as CloudThreadRecord["accountId"],
  providerKind: "e2b" as CloudThreadRecord["providerKind"],
  repositoryUrl: "https://github.com/example/public.git",
  branch: "main",
  baseRef: "main",
  workspacePath: paths.cwd,
  homePath: "/home/t3",
  status: "ready",
  createdAt: now,
  updatedAt: now,
};

const makeTurnInput = (attachmentId: string): ProviderAdapter.ProviderAdapterV2TurnInput => ({
  appThread: {} as never,
  threadId,
  runId: RunId.make("run-1"),
  runOrdinal: 1,
  providerTurnOrdinal: 1,
  attemptId: RunAttemptId.make("attempt-1"),
  rootNodeId: NodeId.make("node-1"),
  providerThread: {
    id: ProviderThreadId.make("provider-thread-1"),
  } as ProviderAdapter.ProviderAdapterV2TurnInput["providerThread"],
  message: {
    messageId: MessageId.make("message-1"),
    text: "look at this",
    attachments: [
      {
        type: "file",
        id: attachmentId,
        name: "notes.txt",
        mimeType: "text/plain",
        sizeBytes: 5,
      } as ProviderAdapter.ProviderAdapterV2TurnInput["message"]["attachments"][number],
    ],
    createdBy: "user",
    creationSource: "web",
  },
  modelSelection,
  runtimePolicy: { runtimeMode: "full-access", interactionMode: "default", cwd: paths.cwd },
});

const testLayer = (cloud: CloudThreads.CloudThreads["Service"]) => {
  const config = ServerConfig.layerTest(process.cwd(), {
    prefix: "t3-cloud-opencode-",
  }).pipe(Layer.provide(NodeServices.layer));
  return Layer.mergeAll(
    CloudProviderRuntime.layer.pipe(
      Layer.provide(Layer.succeed(CloudThreads.CloudThreads, cloud)),
      Layer.provide(Layer.mergeAll(config, NodeServices.layer)),
    ),
    config,
    NodeServices.layer,
  );
};

const makeFakeSession = (events: Queue.Queue<ProviderAdapter.ProviderAdapterV2Event>) =>
  ({
    instanceId,
    driver,
    providerSessionId: ProviderSessionId.make("session-1"),
    providerSession: {
      id: ProviderSessionId.make("session-1"),
      driver,
      providerInstanceId: instanceId,
      status: "ready",
      cwd: paths.cwd,
      model: modelSelection.model,
      capabilities: {},
      extensionStatuses: [],
      createdAt: DateTime.makeUnsafe(now),
      updatedAt: DateTime.makeUnsafe(now),
      lastError: null,
    },
    events: Stream.fromQueue(events),
    ensureThread: () => Effect.die("unused ensureThread"),
    resumeThread: (
      input: Parameters<ProviderAdapter.ProviderAdapterV2SessionRuntime["resumeThread"]>[0],
    ) => Effect.succeed(input.providerThread),
    startTurn: () => Effect.void,
    steerTurn: () => Effect.void,
    interruptTurn: () => Effect.void,
    respondToRuntimeRequest: () => Effect.void,
    readThreadSnapshot: () => Effect.die("unused readThreadSnapshot"),
    rollbackThread: () => Effect.die("unused rollbackThread"),
    forkThread: () => Effect.die("unused forkThread"),
  }) as unknown as ProviderAdapter.ProviderAdapterV2SessionRuntime;

const makeFakeAdapter = (
  events: Queue.Queue<ProviderAdapter.ProviderAdapterV2Event>,
): ProviderAdapter.ProviderAdapterV2["Service"] => {
  const session = makeFakeSession(events);
  return {
    instanceId,
    driver,
    getCapabilities: () => Effect.succeed({}),
    planSelectionTransition: () => Effect.die("unused planSelectionTransition"),
    openSession: () => Effect.succeed(session),
  } as unknown as ProviderAdapter.ProviderAdapterV2["Service"];
};

describe("cloud provider runtime", () => {
  it.effect("opens the sandbox adapter and stages attachments for each turn", () =>
    Effect.gen(function* () {
      const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
      const written: Array<string> = [];
      let begins = 0;
      let ends = 0;
      let opened: CloudThreads.CloudProviderHostInput | null = null;
      let startTurns = 0;
      let compactTurns = 0;
      let failWrite = false;
      const session = makeFakeSession(events);
      const trackedSession: ProviderAdapter.ProviderAdapterV2SessionRuntime = {
        ...session,
        startTurn: () => {
          startTurns += 1;
          return Effect.void;
        },
        compactThread: () => {
          compactTurns += 1;
          return Effect.void;
        },
      };
      const adapter = {
        ...makeFakeAdapter(events),
        openSession: () => Effect.succeed(trackedSession),
      } as unknown as ProviderAdapter.ProviderAdapterV2["Service"];
      const cloud = {
        getRecord: () => Effect.succeedSome(record),
        openProviderHost: (_threadId: ThreadId, input: CloudThreads.CloudProviderHostInput) => {
          opened = input;
          return Effect.succeed({
            adapter,
            paths,
            protocolVersion: "1",
            hostVersion: "t3-provider-adapter-host-v1",
            close: Effect.void,
          });
        },
        writeAttachment: (_threadId: ThreadId, path: string) => {
          written.push(path);
          return Effect.succeed(!failWrite);
        },
        beginTurn: () => {
          begins += 1;
          return Effect.void;
        },
        endTurn: () => {
          ends += 1;
          return Effect.void;
        },
      } as unknown as CloudThreads.CloudThreads["Service"];

      return yield* Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const serverConfig = yield* ServerConfig.ServerConfig;
        const attachmentId = "thread-cloud-opencode-attachment";
        yield* fileSystem.makeDirectory(serverConfig.attachmentsDir, { recursive: true });
        yield* fileSystem.writeFileString(
          `${serverConfig.attachmentsDir}/${attachmentId}.txt`,
          "hello",
        );

        const service = yield* CloudProviderRuntime.CloudProviderRuntime;
        const runtime = yield* service.openSession(
          {
            threadId,
            providerSessionId: ProviderSessionId.make("session-1"),
            modelSelection,
            runtimePolicy: {
              runtimeMode: "full-access",
              interactionMode: "default",
              cwd: paths.cwd,
            },
            environment: [{ name: "OPENAI_API_KEY", value: "sk-test" }],
            config: { binaryPath: "/usr/local/bin/opencode", customModels: [] },
            settings: {
              ...DEFAULT_SERVER_SETTINGS,
              sandboxProviders: {
                "e2b-work": {
                  kind: "e2b",
                  label: "E2B",
                  apiKey: "e2b_super_secret",
                  template: "base",
                  enabled: true,
                },
              },
              cloudSpeechCredentials: {
                provider: "openai",
                apiKey: "sk-speech-secret",
              },
              providerInstances: {
                opencode: {
                  driver: "opencode",
                  config: { serverPassword: "sk-provider-secret" },
                },
              },
            } as never,
          },
          adapter,
        );

        expect(opened?.config).toEqual({
          binaryPath: "/home/t3/.t3/provider-host/runtime/bin/opencode",
          serverUrl: "",
          serverPassword: "",
          customModels: [],
        });
        const hello = JSON.stringify(opened?.settings);
        expect(hello).not.toContain("e2b_super_secret");
        expect(hello).not.toContain("sk-speech-secret");
        expect(hello).not.toContain("sk-provider-secret");
        expect(opened?.settings.sandboxProviders).toEqual({});

        yield* runtime.startTurn(makeTurnInput(attachmentId));
        expect(startTurns).toBe(1);
        expect(begins).toBe(1);
        expect(written).toEqual([`/home/t3/.t3/attachments/${attachmentId}.txt`]);

        yield* Queue.offer(events, {
          type: "turn.terminal",
          driver,
          providerTurnId: ProviderTurnId.make("turn-1"),
          status: "completed",
        } as ProviderAdapter.ProviderAdapterV2Event);
        yield* runtime.events.pipe(Stream.take(1), Stream.runDrain);
        expect(ends).toBe(1);

        const compact = runtime.compactThread;
        expect(compact).toBeDefined();
        if (compact === undefined) return;
        yield* compact(makeTurnInput(attachmentId));
        expect(compactTurns).toBe(1);
        expect(startTurns).toBe(1);

        failWrite = true;
        const failed = yield* runtime.startTurn(makeTurnInput(attachmentId)).pipe(Effect.flip);
        expect(failed._tag).toBe("ProviderAdapterProtocolError");
        expect(ends).toBe(2);
      }).pipe(Effect.scoped, Effect.provide(testLayer(cloud)));
    }),
  );

  it.effect("refuses to resume a retired cloud-host thread natively", () =>
    Effect.gen(function* () {
      const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
      const adapter = makeFakeAdapter(events);
      const cloud = {
        getRecord: () => Effect.succeedSome(record),
        openProviderHost: () => Effect.die("a legacy thread must not start the native host"),
      } as unknown as CloudThreads.CloudThreads["Service"];

      return yield* Effect.gen(function* () {
        const service = yield* CloudProviderRuntime.CloudProviderRuntime;
        const failure = yield* service
          .openSession(
            {
              threadId,
              providerSessionId: ProviderSessionId.make("session-1"),
              modelSelection,
              runtimePolicy: {
                runtimeMode: "full-access",
                interactionMode: "default",
                cwd: "/Users/me/project",
              },
              environment: [],
              config: { binaryPath: "/usr/local/bin/opencode", customModels: [] },
              settings: DEFAULT_SERVER_SETTINGS,
              initialNativeThreadId: `cloud:${threadId}`,
            },
            adapter,
          )
          .pipe(Effect.flip);
        expect(failure._tag).toBe("ProviderAdapterProtocolError");
        expect((failure as { readonly detail?: string }).detail).toContain(
          "Start a new cloud thread",
        );
      }).pipe(Effect.scoped, Effect.provide(testLayer(cloud)));
    }),
  );

  it.effect("rewrites controller workspace paths to the sandbox workspace", () =>
    Effect.gen(function* () {
      const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
      const controllerCwd = "/Users/me/project";
      let openedPolicy: ProviderAdapter.ProviderAdapterV2RuntimePolicy | null = null;
      let turnPolicy: ProviderAdapter.ProviderAdapterV2RuntimePolicy | null = null;
      const session = makeFakeSession(events);
      const trackedSession: ProviderAdapter.ProviderAdapterV2SessionRuntime = {
        ...session,
        startTurn: (input) => {
          turnPolicy = input.runtimePolicy;
          return Effect.void;
        },
      };
      const adapter = {
        ...makeFakeAdapter(events),
        openSession: (input: {
          readonly runtimePolicy: ProviderAdapter.ProviderAdapterV2RuntimePolicy;
        }) => {
          openedPolicy = input.runtimePolicy;
          return Effect.succeed(trackedSession);
        },
      } as unknown as ProviderAdapter.ProviderAdapterV2["Service"];
      const cloud = {
        getRecord: () => Effect.succeedSome(record),
        openProviderHost: () =>
          Effect.succeed({
            adapter,
            paths,
            protocolVersion: "1",
            hostVersion: "t3-provider-adapter-host-v1",
            close: Effect.void,
          }),
        beginTurn: () => Effect.void,
        endTurn: () => Effect.void,
      } as unknown as CloudThreads.CloudThreads["Service"];
      const workspace = {
        folders: [{ name: "project", path: controllerCwd, isGitRepository: true }],
        extraRoots: [`${controllerCwd}/extra`],
      };

      return yield* Effect.gen(function* () {
        const service = yield* CloudProviderRuntime.CloudProviderRuntime;
        const runtime = yield* service.openSession(
          {
            threadId,
            providerSessionId: ProviderSessionId.make("session-1"),
            modelSelection,
            runtimePolicy: {
              runtimeMode: "full-access",
              interactionMode: "default",
              cwd: controllerCwd,
              workspace,
            },
            environment: [],
            config: { binaryPath: "/usr/local/bin/opencode", customModels: [] },
            settings: DEFAULT_SERVER_SETTINGS,
          },
          adapter,
        );
        expect(openedPolicy?.cwd).toBe(paths.cwd);
        expect(openedPolicy?.workspace?.folders[0]?.path).toBe(paths.cwd);
        expect(openedPolicy?.workspace?.extraRoots).toEqual([]);

        yield* runtime.startTurn({
          ...makeTurnInput("unused"),
          runtimePolicy: {
            runtimeMode: "full-access",
            interactionMode: "default",
            cwd: controllerCwd,
            workspace,
          },
        });
        expect(turnPolicy?.cwd).toBe(paths.cwd);
        expect(turnPolicy?.workspace?.folders[0]?.path).toBe(paths.cwd);
      }).pipe(Effect.scoped, Effect.provide(testLayer(cloud)));
    }),
  );

  it.effect("binds Claude settings to the sandbox runtime", () =>
    Effect.gen(function* () {
      const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
      let opened: CloudThreads.CloudProviderHostInput | null = null;
      const claudeInstanceId = ProviderInstanceId.make("claude-agent");
      const claudeSelection: ModelSelection = {
        instanceId: claudeInstanceId,
        model: "claude-agent/sonnet",
      };
      const claudeDriver = ProviderDriverKind.make("claudeAgent");
      const adapter = {
        ...makeFakeAdapter(events),
        driver: claudeDriver,
      } as unknown as ProviderAdapter.ProviderAdapterV2["Service"];
      const cloud = {
        getRecord: () => Effect.succeedSome(record),
        openProviderHost: (_threadId: ThreadId, input: CloudThreads.CloudProviderHostInput) => {
          opened = input;
          return Effect.succeed({
            adapter,
            paths,
            protocolVersion: "1",
            hostVersion: "t3-provider-adapter-host-v1",
            close: Effect.void,
          });
        },
        beginTurn: () => Effect.void,
        endTurn: () => Effect.void,
      } as unknown as CloudThreads.CloudThreads["Service"];

      return yield* Effect.gen(function* () {
        const service = yield* CloudProviderRuntime.CloudProviderRuntime;
        expect(service.supports(claudeDriver)).toBe(true);
        yield* service.openSession(
          {
            threadId,
            providerSessionId: ProviderSessionId.make("session-1"),
            modelSelection: claudeSelection,
            runtimePolicy: {
              runtimeMode: "full-access",
              interactionMode: "default",
              cwd: paths.cwd,
            },
            environment: [{ name: "ANTHROPIC_API_KEY", value: "sk-ant-test" }],
            config: { binaryPath: "/usr/local/bin/claude", homePath: "/home/me/.claude" },
            settings: DEFAULT_SERVER_SETTINGS,
          },
          adapter,
        );

        expect(opened?.driver).toBe(claudeDriver);
        expect(opened?.config).toEqual({
          binaryPath: "/home/t3/.t3/provider-host/runtime/bin/claude",
          homePath: "",
        });
        expect(opened?.environment).toEqual([{ name: "ANTHROPIC_API_KEY", value: "sk-ant-test" }]);
      }).pipe(Effect.scoped, Effect.provide(testLayer(cloud)));
    }),
  );

  it.effect("refuses providers without a sandbox host implementation", () =>
    Effect.gen(function* () {
      const service = yield* CloudProviderRuntime.CloudProviderRuntime;
      expect(service.supports(driver)).toBe(true);
      expect(service.supports(ProviderDriverKind.make("claudeAgent"))).toBe(true);
      expect(service.supports(ProviderDriverKind.make("codex"))).toBe(false);
    }).pipe(Effect.provide(testLayer({} as CloudThreads.CloudThreads["Service"]))),
  );
});
