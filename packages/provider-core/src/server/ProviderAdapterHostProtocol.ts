import {
  AgentFeedbackMode,
  ChatAttachment,
  CheckpointId,
  MessageId,
  ModelSelection,
  NodeId,
  OrchestrationV2AppThread,
  OrchestrationV2ConversationMessage,
  OrchestrationV2HistoricalMessage,
  OrchestrationV2ProviderCapabilities,
  OrchestrationV2ProviderSession,
  OrchestrationV2ProviderThread,
  OrchestrationV2ProviderTurn,
  OrchestrationV2RuntimeRequest,
  ProviderApprovalDecision,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderTurnId,
  ProviderUserInputAnswers,
  RunAttemptId,
  RunId,
  RuntimeRequestId,
  ServerSettings,
  ThreadId,
} from "@t3tools/contracts";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  ProviderAdapterV2Event,
  ProviderAdapterV2Error,
  ProviderAdapterV2RuntimePolicy,
  ProviderAdapterV2TurnMessage,
  type ProviderAdapterV2,
} from "./ProviderAdapter.ts";
import type { ProviderHostPaths } from "./ProviderHost.ts";

/**
 * Bumped with any change to the request or event envelope. A controller and a
 * sandbox host whose versions differ are refused rather than silently degraded.
 */
export const PROVIDER_ADAPTER_HOST_PROTOCOL_VERSION = "2";

export const ProviderAdapterHostPaths = Schema.Struct({
  cwd: Schema.String,
  baseDir: Schema.String,
  stateDir: Schema.String,
  providerStatusCacheDir: Schema.String,
  attachmentsDir: Schema.String,
});
export type ProviderAdapterHostPaths = typeof ProviderAdapterHostPaths.Type;

export const ProviderAdapterHostHello = Schema.Struct({
  type: Schema.Literal("hello"),
  protocolVersion: Schema.String,
  token: Schema.String,
  driver: ProviderDriverKind,
  instanceId: ProviderInstanceId,
  displayName: Schema.optional(Schema.String),
  accentColor: Schema.optional(Schema.String),
  config: Schema.Unknown,
  settings: ServerSettings,
  paths: ProviderAdapterHostPaths,
});
export type ProviderAdapterHostHello = typeof ProviderAdapterHostHello.Type;

export const PROVIDER_ADAPTER_HOST_SESSION_FEATURES = [
  "injectHistory",
  "resumeTurn",
  "compactThread",
  "applyModelSelection",
  "applyThreadTitle",
  "stopTask",
  "awaitBackgroundTasks",
  "stopSubagent",
  "unloadThread",
  "uploadFeedback",
  "hasPendingBackgroundWork",
  "hasPendingBackgroundWorkForThread",
] as const;

export const ProviderAdapterHostSessionFeature = Schema.Literals(
  PROVIDER_ADAPTER_HOST_SESSION_FEATURES,
);
export type ProviderAdapterHostSessionFeature = typeof ProviderAdapterHostSessionFeature.Type;

export const ProviderAdapterHostReady = Schema.Struct({
  type: Schema.Literal("ready"),
  protocolVersion: Schema.String,
  driver: ProviderDriverKind,
  instanceId: ProviderInstanceId,
  hostVersion: Schema.optional(Schema.String),
  checksVersionAtHandshake: Schema.optional(Schema.Boolean),
});
export type ProviderAdapterHostReady = typeof ProviderAdapterHostReady.Type;

export const ProviderAdapterHostRefusalCode = Schema.Literals([
  "protocol_mismatch",
  "unauthorized",
  "unsupported_driver",
  "invalid_config",
  "adapter_unavailable",
]);
export type ProviderAdapterHostRefusalCode = typeof ProviderAdapterHostRefusalCode.Type;

export const ProviderAdapterHostRefused = Schema.Struct({
  type: Schema.Literal("refused"),
  code: ProviderAdapterHostRefusalCode,
  reason: Schema.String,
});
export type ProviderAdapterHostRefused = typeof ProviderAdapterHostRefused.Type;

export const ProviderAdapterHostSessionDescriptor = Schema.Struct({
  providerSession: OrchestrationV2ProviderSession,
  features: Schema.Array(ProviderAdapterHostSessionFeature),
  canResumeAfterRestart: Schema.optional(Schema.Boolean),
  backgroundTasksEndWithProcess: Schema.optional(Schema.Boolean),
});
export type ProviderAdapterHostSessionDescriptor = typeof ProviderAdapterHostSessionDescriptor.Type;

export const ProviderAdapterHostSubagentOutcome = Schema.Struct({
  status: Schema.Literals(["completed", "failed", "cancelled"]),
  completedAt: Schema.DateTimeUtc,
  result: Schema.Unknown,
});
export type ProviderAdapterHostSubagentOutcome = typeof ProviderAdapterHostSubagentOutcome.Type;

export const ProviderAdapterHostThreadSnapshot = Schema.Struct({
  providerThread: OrchestrationV2ProviderThread,
  messages: Schema.Array(OrchestrationV2ConversationMessage),
  reasoningMessageIds: Schema.optional(Schema.Array(MessageId)),
  runtimeRequests: Schema.Array(OrchestrationV2RuntimeRequest),
  providerPayload: Schema.optional(Schema.Unknown),
});
export type ProviderAdapterHostThreadSnapshot = typeof ProviderAdapterHostThreadSnapshot.Type;

const AppContext = Schema.Array(Schema.Struct({ key: Schema.String, text: Schema.String }));

export const ProviderAdapterHostTurnInput = Schema.Struct({
  agentFeedback: Schema.optional(AgentFeedbackMode),
  appThread: OrchestrationV2AppThread,
  threadId: ThreadId,
  runId: RunId,
  runOrdinal: Schema.Number,
  nativeThreadHasTurns: Schema.optional(Schema.Boolean),
  providerTurnOrdinal: Schema.Number,
  restartContinuationOfRunId: Schema.optional(RunId),
  resume: Schema.optional(Schema.Literal(true)),
  attemptId: RunAttemptId,
  rootNodeId: NodeId,
  providerThread: OrchestrationV2ProviderThread,
  message: ProviderAdapterV2TurnMessage,
  inlineContextLength: Schema.optional(Schema.Number),
  modelSelection: ModelSelection,
  runtimePolicy: ProviderAdapterV2RuntimePolicy,
  appContext: Schema.optional(AppContext),
});

export const ProviderAdapterHostRollbackTarget = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("thread_start"),
    checkpointId: CheckpointId,
    appRunOrdinal: Schema.Literal(0),
  }),
  Schema.Struct({
    type: Schema.Literal("provider_turn"),
    checkpointId: CheckpointId,
    appRunOrdinal: Schema.Number,
    providerTurn: OrchestrationV2ProviderTurn,
    message: Schema.optional(
      Schema.Struct({
        role: OrchestrationV2ConversationMessage.fields.role,
        text: Schema.String,
        attachments: Schema.Array(ChatAttachment),
      }),
    ),
  }),
]);

export const ProviderAdapterHostParams = {
  planSelectionTransition: Schema.Struct({
    current: ModelSelection,
    target: ModelSelection,
    sessionCapabilities: OrchestrationV2ProviderCapabilities,
  }),
  openSession: Schema.Struct({
    threadId: ThreadId,
    providerSessionId: ProviderSessionId,
    modelSelection: ModelSelection,
    runtimePolicy: ProviderAdapterV2RuntimePolicy,
    /** Evaluated by the controller; the host recreates the live effect. */
    promptSuggestions: Schema.optional(Schema.Boolean),
    resumeFromSession: Schema.optional(OrchestrationV2ProviderSession),
    initialNativeThreadId: Schema.optional(Schema.String),
    initialProviderItemIdentityVersion: Schema.optional(Schema.Literal(2)),
  }),
  ensureThread: Schema.Struct({
    threadId: ThreadId,
    title: Schema.optional(Schema.String),
    modelSelection: ModelSelection,
    runtimePolicy: ProviderAdapterV2RuntimePolicy,
    providerSessionId: Schema.optional(ProviderSessionId),
    existingProviderThread: Schema.optional(OrchestrationV2ProviderThread),
    forceNewNativeThread: Schema.optional(Schema.Boolean),
  }),
  resumeThread: Schema.Struct({
    providerThread: OrchestrationV2ProviderThread,
    threadId: Schema.optional(ThreadId),
    modelSelection: Schema.optional(ModelSelection),
    runtimePolicy: Schema.optional(ProviderAdapterV2RuntimePolicy),
    importedNativeSession: Schema.optional(Schema.Boolean),
    preservePendingRequests: Schema.optional(Schema.Boolean),
  }),
  injectHistory: Schema.Struct({
    providerThread: OrchestrationV2ProviderThread,
    messages: Schema.Array(OrchestrationV2HistoricalMessage),
    context: Schema.String,
  }),
  startTurn: ProviderAdapterHostTurnInput,
  resumeTurn: ProviderAdapterHostTurnInput.mapFields(
    ({ message: _message, resume: _resume, ...fields }) => ({
      ...fields,
      messageId: MessageId,
    }),
  ),
  compactThread: ProviderAdapterHostTurnInput,
  steerTurn: Schema.Struct({
    threadId: ThreadId,
    runId: RunId,
    providerThread: OrchestrationV2ProviderThread,
    providerTurnId: ProviderTurnId,
    message: ProviderAdapterV2TurnMessage,
    modelSelection: Schema.optional(ModelSelection),
    appContext: Schema.optional(AppContext),
  }),
  applyModelSelection: Schema.Struct({
    providerThread: OrchestrationV2ProviderThread,
    modelSelection: ModelSelection,
  }),
  applyThreadTitle: Schema.Struct({
    providerThread: OrchestrationV2ProviderThread,
    title: Schema.String,
  }),
  stopTask: Schema.Struct({
    providerThread: OrchestrationV2ProviderThread,
    taskId: Schema.String,
  }),
  awaitBackgroundTasks: Schema.Struct({
    providerThread: OrchestrationV2ProviderThread,
    taskIds: Schema.Array(Schema.String),
    awaited: Schema.Boolean,
  }),
  interruptTurn: Schema.Struct({
    providerThread: OrchestrationV2ProviderThread,
    providerTurnId: ProviderTurnId,
    requestRuntimeRestart: Schema.optional(Schema.Boolean),
  }),
  stopSubagent: Schema.Struct({
    providerThread: OrchestrationV2ProviderThread,
    nativeTaskId: Schema.String,
    requestedAt: Schema.optional(Schema.DateTimeUtc),
  }),
  unloadThread: Schema.Struct({
    providerThread: OrchestrationV2ProviderThread,
    archive: Schema.optional(Schema.Boolean),
  }),
  respondToRuntimeRequest: Schema.Struct({
    requestId: RuntimeRequestId,
    decision: Schema.optional(ProviderApprovalDecision),
    feedback: Schema.optional(Schema.String),
    answers: Schema.optional(ProviderUserInputAnswers),
  }),
  readThreadSnapshot: Schema.Struct({
    providerThread: OrchestrationV2ProviderThread,
  }),
  uploadFeedback: Schema.Struct({
    providerThread: OrchestrationV2ProviderThread,
    reason: Schema.optional(Schema.String),
  }),
  rollbackThread: Schema.Struct({
    providerThread: OrchestrationV2ProviderThread,
    target: ProviderAdapterHostRollbackTarget,
    providerThreadTurns: Schema.Array(OrchestrationV2ProviderTurn),
  }),
  forkThread: Schema.Struct({
    sourceProviderThread: OrchestrationV2ProviderThread,
    sourceProviderTurns: Schema.optional(Schema.Array(OrchestrationV2ProviderTurn)),
    providerTurnId: Schema.optional(ProviderTurnId),
    targetThreadId: ThreadId,
    ownerNodeId: Schema.optional(NodeId),
    modelSelection: Schema.optional(ModelSelection),
    runtimePolicy: Schema.optional(ProviderAdapterV2RuntimePolicy),
  }),
  hasPendingBackgroundWorkForThread: Schema.Struct({
    providerThread: OrchestrationV2ProviderThread,
    options: Schema.optional(Schema.Struct({ includeSubagents: Schema.optional(Schema.Boolean) })),
  }),
} as const;

export type ProviderAdapterHostParams = typeof ProviderAdapterHostParams;
export type ProviderAdapterHostMethod = keyof ProviderAdapterHostParams;

export const ProviderAdapterHostCall = Schema.Struct({
  type: Schema.Literal("call"),
  id: Schema.String,
  method: Schema.String,
  sessionId: Schema.optional(Schema.String),
  params: Schema.optional(Schema.Unknown),
});
export type ProviderAdapterHostCall = typeof ProviderAdapterHostCall.Type;

export const ProviderAdapterHostClientMessage = ProviderAdapterHostCall;
export type ProviderAdapterHostClientMessage = typeof ProviderAdapterHostClientMessage.Type;

export const ProviderAdapterHostResponse = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("response"),
    id: Schema.String,
    ok: Schema.Literal(true),
    result: Schema.optional(Schema.Unknown),
  }),
  Schema.Struct({
    type: Schema.Literal("response"),
    id: Schema.String,
    ok: Schema.Literal(false),
    error: ProviderAdapterV2Error,
  }),
]);
export type ProviderAdapterHostResponse = typeof ProviderAdapterHostResponse.Type;

export const ProviderAdapterHostEvent = Schema.Struct({
  type: Schema.Literal("event"),
  sessionId: Schema.String,
  event: ProviderAdapterV2Event,
});
export type ProviderAdapterHostEvent = typeof ProviderAdapterHostEvent.Type;

export const ProviderAdapterHostSessionError = Schema.Struct({
  type: Schema.Literal("session_error"),
  sessionId: Schema.String,
  error: ProviderAdapterV2Error,
});
export type ProviderAdapterHostSessionError = typeof ProviderAdapterHostSessionError.Type;

export const ProviderAdapterHostSessionClosed = Schema.Struct({
  type: Schema.Literal("session_closed"),
  sessionId: Schema.String,
});
export type ProviderAdapterHostSessionClosed = typeof ProviderAdapterHostSessionClosed.Type;

export const ProviderAdapterHostServerMessage = Schema.Union([
  ProviderAdapterHostReady,
  ProviderAdapterHostRefused,
  ProviderAdapterHostResponse,
  ProviderAdapterHostEvent,
  ProviderAdapterHostSessionError,
  ProviderAdapterHostSessionClosed,
]);
export type ProviderAdapterHostServerMessage = typeof ProviderAdapterHostServerMessage.Type;

export interface ProviderAdapterHostAdapterCreateInput {
  readonly driver: ProviderDriverKind;
  readonly instanceId: ProviderInstanceId;
  readonly displayName: string | undefined;
  readonly accentColor: string | undefined;
  readonly config: unknown;
  readonly settings: ServerSettings;
  readonly paths: ProviderHostPaths;
}

export interface ProviderAdapterHostCreatedAdapter {
  readonly adapter: ProviderAdapterV2["Service"];
  /** Runs when the controller connection ends, including credential clearance. */
  readonly onStop?: Effect.Effect<void>;
}
