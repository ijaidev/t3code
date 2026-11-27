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
import type { CloudThreadStore } from "./CloudThreadStore.ts";

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

const record: CloudThreadStore.CloudThreadRecord = {
  threadId,
  sandboxId: "sandbox-1" as CloudThreadStore.CloudThreadRecord["sandboxId"],
  accountId: "e2b-work" as CloudThreadStore.CloudThreadRecord["accountId"],
  providerKind: "e2b" as CloudThreadStore.CloudThreadRecord["providerKind"],
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
        size: 5,
      } as ProviderAdapter.ProviderAdapterV2TurnInput["message"]["attachments"][number],
    ],
    createdBy: "user",
    creationSource: "client",
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
    resumeThread: ({ providerThread }) => Effect.succeed(providerThread),
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
      const session = makeFakeSession(events);
      const trackedSession: ProviderAdapter.ProviderAdapterV2SessionRuntime = {
        ...session,
        startTurn: () => {
          startTurns += 1;
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
          return Effect.succeed(true);
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
            settings: DEFAULT_SERVER_SETTINGS,
          },
          adapter,
        );

        expect(opened?.config).toEqual({
          binaryPath: "/home/t3/.t3/provider-host/runtime/bin/opencode",
          serverUrl: "",
          serverPassword: "",
          customModels: [],
        });

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
      }).pipe(Effect.scoped, Effect.provide(testLayer(cloud)));
    }),
  );

  it.effect("refuses providers without a sandbox host implementation", () =>
    Effect.gen(function* () {
      const service = yield* CloudProviderRuntime.CloudProviderRuntime;
      expect(service.supports(driver)).toBe(true);
      expect(service.supports(ProviderDriverKind.make("claudeAgent"))).toBe(false);
    }).pipe(Effect.provide(testLayer({} as CloudThreads.CloudThreads["Service"]))),
  );
});
