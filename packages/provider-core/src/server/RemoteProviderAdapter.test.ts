import {
  DEFAULT_SERVER_SETTINGS,
  ModelSelection,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  OrchestrationV2AppThread,
  OrchestrationV2ProviderThread as OrchestrationV2ProviderThreadSchema,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RuntimeRequestId,
  ThreadId,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSocketServer from "@effect/platform-node/NodeSocketServer";
import * as NodeWS from "@effect/platform-node/NodeWS";
import { describe, expect, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import type * as Socket from "effect/socket/Socket";
import * as Stream from "effect/Stream";

import * as ProviderAdapter from "./ProviderAdapter.ts";
import { ProviderAdapterHostRefusal, serveProviderAdapterHost } from "./ProviderAdapterHost.ts";
import {
  PROVIDER_ADAPTER_HOST_PROTOCOL_VERSION,
  type ProviderAdapterHostPaths,
} from "./ProviderAdapterHostProtocol.ts";
import * as RemoteProviderAdapter from "./RemoteProviderAdapter.ts";
import * as SandboxProviderHost from "./SandboxProviderHost.ts";

const driver = ProviderDriverKind.make("opencode");
const instanceId = ProviderInstanceId.make("opencode-work");
const threadId = ThreadId.make("thread-cloud-host");
const providerSessionId = ProviderSessionId.make("session-cloud-host");
const providerThreadId = ProviderThreadId.make("provider-thread-cloud-host");
const token = "host-token-1234";
const secret = "sk-live-credential-value";
const modelSelection: ModelSelection = { instanceId, model: "opencode-go/kimi" };
const runtimePolicy = {
  runtimeMode: "full-access" as const,
  interactionMode: "default" as const,
  cwd: "/workspace",
};

const capabilities = {
  sessions: {
    supportsMultipleProviderThreadsPerSession: true,
    supportsModelSwitchInSession: true,
    supportsProviderSwitchingViaHandoff: true,
    supportsRuntimeModeSwitchInSession: true,
    pendingRequestsSurviveRestart: false,
  },
  threads: {
    canCreateEmptyThread: true,
    canReadThreadSnapshot: true,
    canRollbackThread: true,
    canForkThread: true,
    canForkFromTurn: true,
    canForkWhileSourceRunning: true,
    canForkFromSubagentThread: true,
    exposesNativeThreadId: true,
  },
  turns: {
    exposesNativeTurnId: true,
    emitsTurnStarted: true,
    emitsTurnCompleted: true,
    supportsInterrupt: true,
    supportsNativeResume: true,
    supportsActiveSteering: true,
    activeSteeringDuringCompaction: true,
    supportsSteeringByInterruptRestart: true,
    supportsQueuedMessages: true,
    terminalStatusQuality: "strong",
  },
  streaming: {
    streamsAssistantText: true,
    streamsReasoning: true,
    streamsToolOutput: true,
    streamsPlanText: true,
    emitsMessageCompleted: true,
  },
  tools: {
    exposesToolItemIds: true,
    emitsToolStarted: true,
    emitsToolCompleted: true,
    emitsToolOutput: true,
    supportsMcpTools: true,
    supportsDynamicToolCallbacks: true,
  },
  approvals: {
    supportsCommandApproval: true,
    supportsFileReadApproval: true,
    supportsFileChangeApproval: true,
    supportsApplyPatchApproval: true,
    approvalsHaveNativeRequestIds: true,
    approvalCallbacksAreLiveOnly: true,
    approvalsCanOriginateFromSubagents: true,
  },
  planning: {
    emitsPlanUpdated: true,
    emitsTodoList: true,
    emitsProposedPlan: true,
    supportsStructuredQuestions: true,
    planDeltasHaveItemIds: true,
  },
  subagents: {
    supportsSubagents: true,
    exposesSubagentThreadIds: true,
    emitsSubagentLifecycle: true,
    canWaitForSubagents: true,
    canCloseSubagents: true,
    canForkSubagentThread: true,
  },
  context: {
    acceptsSystemContext: true,
    acceptsDeveloperContext: true,
    acceptsSyntheticUserContext: true,
    canGenerateSummaries: true,
    canConsumeHandoffSummaries: true,
    supportsDeltaHandoff: true,
    supportsFullThreadHandoff: true,
    maxRecommendedHandoffChars: null,
  },
  checkpointing: {
    appCanCheckpointFilesystem: true,
    supportsNestedCheckpointScopes: true,
    providerCanRollbackConversation: true,
    providerRollbackReturnsSnapshot: true,
    providerCanReadConversationSnapshot: true,
  },
  identity: {
    nativeThreadIds: "strong",
    nativeTurnIds: "strong",
    nativeItemIds: "strong",
    nativeRequestIds: "strong",
  },
  runtimePolicy: {
    enforcement: "native",
  },
} satisfies OrchestrationV2ProviderCapabilities;

const createdAt = DateTime.makeUnsafe("2026-11-27T00:00:00.000Z");

const providerSession: OrchestrationV2ProviderSession = {
  id: providerSessionId,
  driver,
  providerInstanceId: instanceId,
  status: "ready",
  cwd: "/workspace",
  model: modelSelection.model,
  capabilities,
  extensionStatuses: [],
  createdAt,
  updatedAt: createdAt,
  lastError: null,
};

const providerThread: OrchestrationV2ProviderThread = Schema.decodeSync(
  OrchestrationV2ProviderThreadSchema,
)({
  id: providerThreadId,
  driver,
  providerInstanceId: instanceId,
  providerSessionId,
  appThreadId: threadId,
  ownerNodeId: null,
  nativeThreadRef: null,
  nativeConversationHeadRef: null,
  status: "idle",
  firstRunOrdinal: null,
  lastRunOrdinal: null,
  handoffIds: [],
  forkedFrom: null,
  pendingBackgroundTasks: [],
  contextUsage: null,
  nativeMetadata: null,
  goal: null,
  createdAt,
  updatedAt: createdAt,
});

const appThread = Schema.decodeSync(OrchestrationV2AppThread)({
  createdBy: "user",
  creationSource: "server",
  id: threadId,
  projectId: "project-cloud-host",
  title: "Cloud host thread",
  providerInstanceId: instanceId,
  modelSelection,
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  activeProviderThreadId: null,
  lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
  forkedFrom: null,
  createdAt,
  updatedAt: createdAt,
  archivedAt: null,
  deletedAt: null,
});

const makeFakeAdapter = (input: {
  readonly calls: string[];
  readonly events: Queue.Queue<ProviderAdapter.ProviderAdapterV2Event>;
  readonly failure?: ProviderAdapter.ProviderAdapterV2Error;
}): ProviderAdapter.ProviderAdapterV2["Service"] => ({
  instanceId,
  driver,
  checksVersionAtHandshake: true,
  getCapabilities: () =>
    input.failure === undefined ? Effect.succeed(capabilities) : Effect.fail(input.failure),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
  openSession: (sessionInput) =>
    Effect.sync(() => {
      input.calls.push("openSession");
      if (sessionInput.resumeFromSession !== undefined) {
        input.calls.push(
          `resume:${DateTime.toEpochMillis(sessionInput.resumeFromSession.createdAt)}`,
        );
      }
      return {
        instanceId,
        driver,
        providerSessionId,
        providerSession,
        events: Stream.fromQueue(input.events),
        ensureThread: (threadInput) => {
          input.calls.push(`ensureThread:${threadInput.threadId}`);
          return Effect.succeed(providerThread);
        },
        resumeThread: () => {
          input.calls.push("resumeThread");
          return Effect.succeed(providerThread);
        },
        startTurn: (turnInput) =>
          Effect.sync(() => {
            input.calls.push(`startTurn:${turnInput.runId}`);
            Queue.offerUnsafe(input.events, {
              type: "thread_model_selection.updated",
              driver,
              threadId,
              instanceId,
              changes: [{ id: "model", from: modelSelection.model, to: "opencode-go/other" }],
            });
          }),
        steerTurn: (steerInput) =>
          Effect.sync(() => {
            input.calls.push(`steerTurn:${steerInput.providerTurnId}`);
          }),
        interruptTurn: (interruptInput) =>
          Effect.sync(() => {
            input.calls.push(`interruptTurn:${interruptInput.providerTurnId}`);
          }),
        respondToRuntimeRequest: (responseInput) =>
          Effect.sync(() => {
            input.calls.push(`respond:${responseInput.requestId}:${responseInput.decision}`);
          }),
        readThreadSnapshot: () => {
          input.calls.push("readThreadSnapshot");
          return Effect.succeed({
            providerThread,
            messages: [],
            runtimeRequests: [],
          });
        },
        rollbackThread: () => {
          input.calls.push("rollbackThread");
          return Effect.succeed({
            providerThread,
            messages: [],
            runtimeRequests: [],
          });
        },
        forkThread: () => {
          input.calls.push("forkThread");
          return Effect.succeed(providerThread);
        },
        hasPendingBackgroundWork: Effect.sync(() => {
          input.calls.push("hasPendingBackgroundWork");
          return false;
        }),
        hasPendingBackgroundWorkForThread: () => Effect.succeed(false),
      };
    }),
});

interface HostHandle {
  readonly url: string;
}

const startHost = (input: {
  readonly calls: string[];
  readonly events: Queue.Queue<ProviderAdapter.ProviderAdapterV2Event>;
  readonly hostRef: Ref.Ref<SandboxProviderHost.SandboxProviderHost | null>;
  readonly stopped: Deferred.Deferred<void>;
  readonly expectedDriver?: ProviderDriverKind;
  readonly acceptConfig?: (config: unknown) => boolean;
  readonly failure?: ProviderAdapter.ProviderAdapterV2Error;
}): Effect.Effect<HostHandle, never, NodeServices.NodeServices | Scope.Scope> =>
  Effect.gen(function* () {
    const fake = makeFakeAdapter({
      calls: input.calls,
      events: input.events,
      ...(input.failure === undefined ? {} : { failure: input.failure }),
    });
    const server = yield* NodeSocketServer.makeWebSocket({
      host: "127.0.0.1",
      port: 0,
    }).pipe(Effect.orDie);
    const address = server.address;
    if (address._tag !== "InetAddressV4") return yield* Effect.die("unexpected host address");
    yield* server
      .run((socket) =>
        serveProviderAdapterHost(socket, {
          token,
          hostVersion: "test-build",
          createAdapter: (hello) =>
            Effect.gen(function* () {
              const host = yield* SandboxProviderHost.make({
                paths: hello.paths,
                settings: hello.settings,
              });
              yield* Ref.set(input.hostRef, host);
              if (hello.driver !== (input.expectedDriver ?? driver)) {
                return yield* new ProviderAdapterHostRefusal({
                  code: "unsupported_driver",
                  reason: `This build cannot run ${hello.driver} in a sandbox.`,
                });
              }
              if (input.acceptConfig !== undefined && !input.acceptConfig(hello.config)) {
                return yield* new ProviderAdapterHostRefusal({
                  code: "invalid_config",
                  reason: "The sandbox host rejected the provider settings.",
                });
              }
              return {
                adapter: fake,
                onStop: Effect.gen(function* () {
                  yield* host.clearCredentials;
                  yield* Deferred.succeed(input.stopped, undefined);
                }),
              };
            }),
        }).pipe(Effect.scoped),
      )
      .pipe(Effect.forkScoped);
    return { url: `ws://127.0.0.1:${address.port}` };
  });

const connect = (input: {
  readonly url: string;
  readonly paths: ProviderAdapterHostPaths;
  readonly authToken?: string;
  readonly connectDriver?: ProviderDriverKind;
  readonly config?: unknown;
}): Effect.Effect<
  RemoteProviderAdapter.RemoteProviderAdapterConnection,
  RemoteProviderAdapter.RemoteProviderAdapterError,
  Scope.Scope | Socket.WebSocketConstructor
> =>
  RemoteProviderAdapter.connect({
    url: input.url,
    token: input.authToken ?? token,
    driver: input.connectDriver ?? driver,
    instanceId,
    config: input.config ?? { enabled: true },
    settings: DEFAULT_SERVER_SETTINGS,
    paths: input.paths,
    expectedHostVersion: "test-build",
  });

const makePaths = (root: string): ProviderAdapterHostPaths => ({
  cwd: root,
  baseDir: `${root}/.t3/host`,
  stateDir: `${root}/.t3/host/state`,
  providerStatusCacheDir: `${root}/.t3/host/caches`,
  attachmentsDir: `${root}/.t3/host/attachments`,
});

const testLayer = Layer.mergeAll(NodeServices.layer, NodeWS.layerWebSocketConstructor);

describe("remote provider adapter", () => {
  it.effect("forwards a session and its event stream across the host boundary", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const calls: string[] = [];
        const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
        const hostRef = yield* Ref.make<SandboxProviderHost.SandboxProviderHost | null>(null);
        const stopped = yield* Deferred.make<void>();
        const fileSystem = yield* FileSystem.FileSystem;
        const root = yield* fileSystem.makeTempDirectory({ prefix: "t3-remote-adapter-" });
        const paths = makePaths(root);
        const host = yield* startHost({ calls, events, hostRef, stopped });

        const connection = yield* connect({ url: host.url, paths });
        expect(connection.protocolVersion).toBe(PROVIDER_ADAPTER_HOST_PROTOCOL_VERSION);
        expect(connection.hostVersion).toBe("test-build");

        const sandboxHost = yield* Ref.get(hostRef);
        expect(sandboxHost).not.toBeNull();
        if (sandboxHost === null) return;
        expect(sandboxHost.host.paths).toEqual(paths);
        const attachmentPath = sandboxHost.host.resolveAttachmentPath({
          id: "staged.txt",
        } as never);
        expect(attachmentPath).toBe(`${paths.attachmentsDir}/staged.txt`);

        const capabilitiesResult = yield* connection.adapter.getCapabilities();
        expect(capabilitiesResult.turns.supportsActiveSteering).toBe(true);

        yield* Effect.scoped(
          Effect.gen(function* () {
            const session = yield* connection.adapter.openSession({
              threadId,
              providerSessionId,
              modelSelection,
              runtimePolicy,
              resumeFromSession: providerSession,
            });
            expect(session.providerSession.id).toBe(providerSessionId);
            expect(calls).toContain(`resume:${DateTime.toEpochMillis(providerSession.createdAt)}`);
            const pendingProbe = session.hasPendingBackgroundWork;
            expect(pendingProbe).toBeDefined();
            if (pendingProbe !== undefined) {
              expect(yield* pendingProbe).toBe(false);
            }
            const ensured = yield* session.ensureThread({
              threadId,
              title: "cloud thread",
              modelSelection,
              runtimePolicy,
            });
            expect(ensured.id).toBe(providerThreadId);

            const eventFiber = yield* session.events.pipe(Stream.runHead, Effect.forkChild);
            yield* Effect.yieldNow;
            yield* session.startTurn({
              appThread,
              threadId,
              runId: "run-1",
              runOrdinal: 1,
              providerTurnOrdinal: 1,
              attemptId: "attempt-1",
              rootNodeId: "node-1",
              providerThread,
              message: {
                messageId: "message-1",
                text: "hello",
                attachments: [],
                createdBy: "user",
                creationSource: "server",
              },
              modelSelection,
              runtimePolicy,
            } as never);
            const event = yield* Fiber.join(eventFiber);
            expect(Option.isSome(event) ? event.value.type : null).toBe(
              "thread_model_selection.updated",
            );
            yield* session.steerTurn({
              threadId,
              runId: "run-1",
              providerThread,
              providerTurnId: ProviderTurnId.make("turn-1"),
              message: {
                messageId: "message-2",
                text: "steer",
                attachments: [],
                createdBy: "user",
                creationSource: "server",
              },
            } as never);
            yield* session.interruptTurn({
              providerThread,
              providerTurnId: ProviderTurnId.make("turn-1"),
            });
            yield* session.respondToRuntimeRequest({
              requestId: RuntimeRequestId.make("request-1"),
              decision: "accept",
            });
            const rolledBack = yield* session.rollbackThread({
              providerThread,
              target: { type: "thread_start", checkpointId: "checkpoint-1", appRunOrdinal: 0 },
              providerThreadTurns: [],
            } as never);
            expect(rolledBack.providerThread.id).toBe(providerThreadId);
            const forked = yield* session.forkThread({
              sourceProviderThread: providerThread,
              targetThreadId: ThreadId.make("thread-fork"),
            } as never);
            expect(forked.id).toBe(providerThreadId);
            const snapshot = yield* session.readThreadSnapshot({ providerThread });
            expect(snapshot.messages).toEqual([]);
          }),
        );

        expect(calls).toContain("ensureThread:thread-cloud-host");
        expect(calls).toContain("startTurn:run-1");
        expect(calls).toContain("steerTurn:turn-1");
        expect(calls).toContain("interruptTurn:turn-1");
        expect(calls).toContain("respond:request-1:accept");
        expect(calls).toContain("rollbackThread");
        expect(calls).toContain("forkThread");
        expect(calls).toContain("hasPendingBackgroundWork");
        yield* connection.close;
        const stoppedResult = yield* Deferred.await(stopped).pipe(
          Effect.timeoutOption("5 seconds"),
        );
        expect(Option.isSome(stoppedResult)).toBe(true);
      }),
    ).pipe(Effect.provide(testLayer)),
  );

  it.effect("returns structured host errors to the controller", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const calls: string[] = [];
        const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
        const hostRef = yield* Ref.make<SandboxProviderHost.SandboxProviderHost | null>(null);
        const stopped = yield* Deferred.make<void>();
        const fileSystem = yield* FileSystem.FileSystem;
        const root = yield* fileSystem.makeTempDirectory({ prefix: "t3-remote-adapter-" });
        const paths = makePaths(root);
        const failure = new ProviderAdapter.ProviderAdapterProtocolError({
          driver,
          detail: "the host adapter refused",
          cause: new Error("inner failure"),
        });
        const host = yield* startHost({ calls, events, hostRef, stopped, failure });

        const connection = yield* connect({ url: host.url, paths });
        const result = yield* connection.adapter
          .getCapabilities()
          .pipe(Effect.match({ onFailure: (error) => error, onSuccess: () => null }));
        expect(result === null ? null : result._tag).toBe("ProviderAdapterProtocolError");
        expect(result === null ? null : result.message).toBe(
          "opencode provider protocol error: the host adapter refused.",
        );
        yield* connection.close;
      }),
    ).pipe(Effect.provide(testLayer)),
  );

  it.effect("fails calls after the controller closes instead of hanging", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const calls: string[] = [];
        const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
        const hostRef = yield* Ref.make<SandboxProviderHost.SandboxProviderHost | null>(null);
        const stopped = yield* Deferred.make<void>();
        const fileSystem = yield* FileSystem.FileSystem;
        const root = yield* fileSystem.makeTempDirectory({ prefix: "t3-remote-adapter-" });
        const paths = makePaths(root);
        const host = yield* startHost({ calls, events, hostRef, stopped });

        const connection = yield* connect({ url: host.url, paths });
        yield* connection.close;
        const stoppedResult = yield* Deferred.await(stopped).pipe(
          Effect.timeoutOption("5 seconds"),
        );
        expect(Option.isSome(stoppedResult)).toBe(true);

        const closedCall = yield* connection.adapter
          .getCapabilities()
          .pipe(Effect.match({ onFailure: (error) => error, onSuccess: () => null }));
        expect(closedCall === null ? null : closedCall._tag).toBe("ProviderAdapterProtocolError");
      }),
    ).pipe(Effect.provide(testLayer)),
  );

  it.effect("refuses a wrong token before any session opens", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const calls: string[] = [];
        const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
        const hostRef = yield* Ref.make<SandboxProviderHost.SandboxProviderHost | null>(null);
        const stopped = yield* Deferred.make<void>();
        const fileSystem = yield* FileSystem.FileSystem;
        const root = yield* fileSystem.makeTempDirectory({ prefix: "t3-remote-adapter-" });
        const paths = makePaths(root);
        const host = yield* startHost({ calls, events, hostRef, stopped });

        const exit = yield* connect({ url: host.url, paths, authToken: "wrong-token" }).pipe(
          Effect.exit,
        );
        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isFailure(exit)) {
          expect(JSON.stringify(exit.cause)).toContain("rejected the controller token");
        }
        expect(calls).toHaveLength(0);
      }),
    ).pipe(Effect.provide(testLayer)),
  );

  it.effect("refuses an unsupported driver before a session starts", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const calls: string[] = [];
        const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
        const hostRef = yield* Ref.make<SandboxProviderHost.SandboxProviderHost | null>(null);
        const stopped = yield* Deferred.make<void>();
        const fileSystem = yield* FileSystem.FileSystem;
        const root = yield* fileSystem.makeTempDirectory({ prefix: "t3-remote-adapter-" });
        const paths = makePaths(root);
        const host = yield* startHost({
          calls,
          events,
          hostRef,
          stopped,
          expectedDriver: ProviderDriverKind.make("claudeAgent"),
        });

        const exit = yield* connect({ url: host.url, paths }).pipe(Effect.exit);
        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isFailure(exit)) {
          expect(JSON.stringify(exit.cause)).toContain("cannot run opencode");
        }
        expect(calls).toHaveLength(0);
      }),
    ).pipe(Effect.provide(testLayer)),
  );

  it.effect("refuses an invalid config before a session starts", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const calls: string[] = [];
        const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
        const hostRef = yield* Ref.make<SandboxProviderHost.SandboxProviderHost | null>(null);
        const stopped = yield* Deferred.make<void>();
        const fileSystem = yield* FileSystem.FileSystem;
        const root = yield* fileSystem.makeTempDirectory({ prefix: "t3-remote-adapter-" });
        const paths = makePaths(root);
        const host = yield* startHost({
          calls,
          events,
          hostRef,
          stopped,
          acceptConfig: (config) =>
            typeof config === "object" && config !== null && "enabled" in config,
        });

        const exit = yield* connect({ url: host.url, paths, config: { model: 1 } }).pipe(
          Effect.exit,
        );
        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isFailure(exit)) {
          expect(JSON.stringify(exit.cause)).toContain("rejected the provider settings");
        }
        expect(calls).toHaveLength(0);
      }),
    ).pipe(Effect.provide(testLayer)),
  );

  it.effect("keeps credentials in host memory and clears them on stop", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const calls: string[] = [];
        const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
        const hostRef = yield* Ref.make<SandboxProviderHost.SandboxProviderHost | null>(null);
        const stopped = yield* Deferred.make<void>();
        const fileSystem = yield* FileSystem.FileSystem;
        const root = yield* fileSystem.makeTempDirectory({ prefix: "t3-remote-adapter-" });
        const paths = makePaths(root);
        const host = yield* startHost({ calls, events, hostRef, stopped });

        const connection = yield* connect({ url: host.url, paths });
        const sandboxHost = yield* Ref.get(hostRef);
        expect(sandboxHost).not.toBeNull();
        if (sandboxHost === null) return;

        const credential = yield* sandboxHost.host.credentials("opencode", "binding-1");
        yield* credential.set(new TextEncoder().encode(secret));
        expect(Option.isSome(yield* credential.get)).toBe(true);

        const files = yield* fileSystem
          .readDirectory(paths.baseDir, { recursive: true })
          .pipe(Effect.orElseSucceed(() => [] as ReadonlyArray<string>));
        for (const file of files) {
          const contents = yield* fileSystem
            .readFileString(`${paths.baseDir}/${file}`)
            .pipe(Effect.orElseSucceed(() => ""));
          expect(contents).not.toContain(secret);
        }

        yield* connection.close;
        const stoppedResult = yield* Deferred.await(stopped).pipe(
          Effect.timeoutOption("5 seconds"),
        );
        expect(Option.isSome(stoppedResult)).toBe(true);
        expect(Option.isNone(yield* credential.get)).toBe(true);
      }),
    ).pipe(Effect.provide(testLayer)),
  );
});
