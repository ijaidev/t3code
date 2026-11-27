import {
  type OrchestrationV2ThreadProjection,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  MessageId,
  NodeId,
  RunAttemptId,
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import { expect, vi } from "vite-plus/test";

import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

import * as ServerConfig from "../config.ts";
import * as CloudProviderRuntime from "../sandbox/CloudProviderRuntime.ts";
import * as ContextHandoffService from "./ContextHandoffService.ts";
import * as EventSink from "./EventSink.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderAuthService from "../provider/ProviderAuthService.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ProviderTurnStart from "./ProviderTurnStartService.ts";
import * as ProviderUsageAnalytics from "../telemetry/ProviderUsageAnalytics.ts";
import * as RunExecutionService from "./RunExecutionService.ts";
import * as RuntimePolicy from "./RuntimePolicy.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as TurnItemPositionStore from "./TurnItemPositionStore.ts";
import * as GitWorkflow from "../git/GitWorkflowService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as TaskFolders from "../workspace/TaskFolders.ts";

const threadId = ThreadId.make("thread-cloud-resume-native");
const runId = RunId.make("run-cloud-resume-native");
const attemptId = RunAttemptId.make("attempt-cloud-resume-native");
const rootNodeId = NodeId.make("node-cloud-resume-native");
const messageId = MessageId.make("message-cloud-resume-native");
const providerThreadId = ProviderThreadId.make("provider-thread-cloud-resume-native");
const providerTurnId = ProviderTurnId.make("provider-turn-cloud-resume-native");
const providerSessionId = ProviderSessionId.make("provider-session-cloud-resume-native");
const providerInstanceId = ProviderInstanceId.make("opencode");
const driver = ProviderDriverKind.make("opencode");

const layerFor = (input: {
  readonly projection: () => OrchestrationV2ThreadProjection;
  readonly open: (openInput: {
    readonly resumeCloudTurn?: { readonly providerThreadId: ProviderThreadId };
  }) => Effect.Effect<ProviderAdapter.ProviderAdapterV2SessionRuntime, unknown>;
  readonly startRootRun: (
    runInput: RunExecutionService.RunExecutionServiceV2StartRootRunInput,
  ) => Effect.Effect<void>;
}) =>
  ProviderTurnStart.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ContextHandoffService.ContextHandoffServiceV2)({}),
        Layer.mock(EventSink.EventSinkV2)({}),
        IdAllocator.layer,
        Layer.mock(TurnItemPositionStore.TurnItemPositionStoreV2)({}),
        Layer.mock(TaskFolders.TaskFolders)({}),
        Layer.mock(GitWorkflow.GitWorkflowService)({}),
        Layer.mock(ProjectService.ProjectService)({}),
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getTurnStartContext: () => Effect.succeed(input.projection()),
          getRun: (_threadId, id) =>
            Effect.succeed(input.projection().runs.find((run) => run.id === id)),
        }),
        Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
          open: input.open as never,
          close: () => Effect.void,
        }),
        Layer.mock(ProviderAuthService.ProviderAuthService)({}),
        Layer.mock(RunExecutionService.RunExecutionServiceV2)({
          startRootRun: input.startRootRun as never,
        }),
        Layer.mock(RuntimePolicy.RuntimePolicyV2)({}),
        Layer.mock(ProviderUsageAnalytics.ProviderTurnAnalytics)({}),
        Layer.succeed(
          CloudProviderRuntime.CloudProviderRuntime,
          CloudProviderRuntime.CloudProviderRuntime.of({
            supports: () => true,
            openSession: () => Effect.die("unused openSession"),
          }),
        ),
        ServerConfig.layerTest(process.cwd(), { prefix: "t3-cloud-resume-" }).pipe(
          Layer.provide(NodeServices.layer),
        ),
        ServerSettings.layerTest(),
        NodeServices.layer,
      ),
    ),
  );

const makeProjection = (now: DateTime.Utc): OrchestrationV2ThreadProjection =>
  ({
    thread: {
      id: threadId,
      runtimeMode: "full-access",
      interactionMode: "default",
      worktreePath: null,
      cloudWorkspace: {
        accountId: "e2b-work",
        providerKind: "e2b",
        workspacePath: "/home/t3/.t3/threads/thread-cloud-resume-native",
      },
      createdAt: now,
      updatedAt: now,
    },
    runs: [
      {
        id: runId,
        threadId,
        ordinal: 1,
        status: "running",
        activeAttemptId: attemptId,
        rootNodeId,
        providerThreadId,
        userMessageId: messageId,
        providerInstanceId,
        modelSelection: { instanceId: providerInstanceId, model: "opencode-go/kimi" },
        startedAt: now,
      },
    ],
    attempts: [{ id: attemptId, runId, status: "running", reason: "initial" }],
    nodes: [{ id: rootNodeId, runId, status: "running", checkpointScopeId: "scope-cloud-resume" }],
    providerThreads: [
      {
        id: providerThreadId,
        driver,
        providerInstanceId,
        providerSessionId,
        nativeThreadRef: { driver, nativeId: "native-cloud-resume", strength: "strong" },
        status: "active",
      },
    ],
    providerTurns: [
      {
        id: providerTurnId,
        providerThreadId,
        nodeId: rootNodeId,
        runAttemptId: attemptId,
        ordinal: 1,
        status: "running",
        startedAt: now,
      },
    ],
    messages: [
      {
        id: messageId,
        threadId,
        runId,
        role: "user",
        text: "Keep going",
        attachments: [],
        createdBy: "user",
        creationSource: "web",
        streaming: false,
        createdAt: now,
        updatedAt: now,
      },
    ],
    checkpointScopes: [
      {
        id: "scope-cloud-resume",
        threadId,
        runId,
        nodeId: rootNodeId,
        providerThreadId,
        kind: "root_run",
        cwd: "/home/t3/.t3/threads/thread-cloud-resume-native",
        createdAt: now,
      },
    ],
    providerSessions: [
      {
        id: providerSessionId,
        driver,
        providerInstanceId,
        status: "ready",
        cwd: "/home/t3/.t3/threads/thread-cloud-resume-native",
        model: "opencode-go/kimi",
        capabilities: {},
        extensionStatuses: [],
        createdAt: now,
        updatedAt: now,
        lastError: null,
      },
    ],
    turnItems: [
      {
        id: TurnItemId.make("turn-item-cloud-resume-native"),
        threadId,
        runId,
        type: "approval_request",
        status: "waiting",
        ordinal: 1,
      },
    ],
  }) as unknown as OrchestrationV2ThreadProjection;

const fakeSession = (resumeTurn: () => Effect.Effect<void>) =>
  ({
    instanceId: providerInstanceId,
    driver,
    providerSessionId,
    providerSession: {},
    events: Stream.never,
    ensureThread: () => Effect.die("unused ensureThread"),
    resumeThread: vi.fn(() => Effect.succeed({} as never)),
    startTurn: vi.fn(() => Effect.die("a resume must not start a turn")),
    resumeTurn: vi.fn(resumeTurn),
    steerTurn: () => Effect.die("unused steerTurn"),
    interruptTurn: () => Effect.void,
    respondToRuntimeRequest: () => Effect.void,
    readThreadSnapshot: () => Effect.die("unused readThreadSnapshot"),
    rollbackThread: () => Effect.die("unused rollbackThread"),
    forkThread: () => Effect.die("unused forkThread"),
  }) as unknown as ProviderAdapter.ProviderAdapterV2SessionRuntime;

it.effect("resumes one native cloud turn and keeps its pending approval", () => {
  const opened: Array<{
    readonly resumeCloudTurn?: { readonly providerThreadId: ProviderThreadId };
  }> = [];
  const started: Array<RunExecutionService.RunExecutionServiceV2StartRootRunInput> = [];
  return Effect.gen(function* () {
    const now = yield* DateTime.now;
    const projection = makeProjection(now);
    const session = fakeSession(() => Effect.void);
    const layer = layerFor({
      projection: () => projection,
      open: (input) => {
        opened.push(input);
        return Effect.succeed(session);
      },
      startRootRun: (input) => {
        started.push(input);
        return Effect.void;
      },
    });

    yield* Effect.gen(function* () {
      yield* (yield* ProviderTurnStart.ProviderTurnStartServiceV2).resumeCloudTurn({
        threadId,
        runId,
      });
    }).pipe(Effect.provide(layer));

    expect(opened).toHaveLength(1);
    expect(opened[0]?.resumeCloudTurn?.providerThreadId).toBe(providerThreadId);
    const resume = started[0];
    expect(resume?.resume).toBe(true);
    expect(resume?.recovered).toBe(true);
    expect(resume?.providerTurnOrdinal).toBe(1);
    expect(resume?.session).toBe(session);
    // The pending approval in the projection was not expired or re-issued.
    expect(projection.turnItems.find((item) => item.type === "approval_request")?.status).toBe(
      "waiting",
    );
    // Run execution reattaches the conversation after subscribing, before it resumes the turn.
    yield* resume!.prepareProviderTurn!();
    expect(session.resumeThread).toHaveBeenCalledTimes(1);
    expect(session.resumeThread).toHaveBeenCalledWith(
      expect.objectContaining({ providerThread: expect.anything(), preservePendingRequests: true }),
    );
    expect(session.startTurn).not.toHaveBeenCalled();
  });
});

it.effect("settles the turn through run execution when the native session cannot open", () => {
  const started: Array<RunExecutionService.RunExecutionServiceV2StartRootRunInput> = [];
  return Effect.gen(function* () {
    const now = yield* DateTime.now;
    const projection = makeProjection(now);
    const failure = new ProviderSessionManager.ProviderSessionOpenError({
      instanceId: providerInstanceId,
      providerSessionId,
      cause: "The sandbox is gone.",
    });
    const layer = layerFor({
      projection: () => projection,
      open: () => Effect.fail(failure),
      startRootRun: (input) => {
        started.push(input);
        return Effect.void;
      },
    });

    yield* Effect.gen(function* () {
      yield* (yield* ProviderTurnStart.ProviderTurnStartServiceV2).resumeCloudTurn({
        threadId,
        runId,
      });
    }).pipe(Effect.provide(layer));

    const resume = started[0];
    expect(resume?.resume).toBe(true);
    expect(resume?.recovered).toBe(true);
    expect(resume?.startFailureMessage).toContain("could not reconnect after the server restart");
    const reconnect = yield* resume!.session.resumeTurn!({} as never).pipe(Effect.flip);
    expect(reconnect.message).toContain("could not reconnect after the server restart");
    expect(resume!.session.startTurn).toBeDefined();
  });
});
