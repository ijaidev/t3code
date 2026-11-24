// @effect-diagnostics-next-line nodeBuiltinImport:off -- ids must be available synchronously inside adapter callbacks.
import * as NodeCrypto from "node:crypto";

import {
  type ChatAttachment,
  MessageId,
  type ModelSelection,
  type NodeId,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2TurnItem,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RuntimeRequestId as RuntimeRequestIdSchema,
  type RunId,
  type ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { HttpClient, HttpClientRequest } from "effect/http";

import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";

import { resolveAttachmentPath } from "../attachmentStore.ts";
import * as ServerConfig from "../config.ts";
import * as CloudThreadPorts from "./CloudThreadPorts.ts";
import * as CloudThreadStore from "./CloudThreadStore.ts";
import * as CloudThreads from "./CloudThreads.ts";

const MODEL_REQUEST_TIMEOUT_MS = 5 * 60_000;
const MAX_MODEL_REQUEST_TOKENS = 8192;
const CLOUD_THREAD_DRIVERS: ReadonlySet<string> = new Set(["claudeAgent"]);
const decodeJson = Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Unknown));

/** The sandbox may ask, but the thread's selection and a fixed ceiling decide. */
export function boundModelRequest(
  request: unknown,
  model: string,
): Readonly<Record<string, unknown>> {
  const source =
    typeof request === "object" && request !== null
      ? (request as Readonly<Record<string, unknown>>)
      : {};
  const requested = Number(source["max_tokens"]);
  const maxTokens =
    Number.isFinite(requested) && requested > 0
      ? Math.min(Math.trunc(requested), MAX_MODEL_REQUEST_TOKENS)
      : MAX_MODEL_REQUEST_TOKENS;
  return { ...source, model, max_tokens: maxTokens };
}

interface AnthropicAccess {
  readonly apiKey?: string;
  readonly authToken?: string;
  readonly baseUrl: string;
}

export interface CloudThreadRuntimeOpenInput {
  readonly threadId: ThreadId;
  readonly providerSessionId: ProviderSessionId;
  readonly modelSelection: ModelSelection;
  readonly runtimePolicy: ProviderAdapter.ProviderAdapterV2RuntimePolicy;
  readonly environment: ReadonlyArray<{ readonly name: string; readonly value: string }>;
}

const accessFromEnvironment = (
  environment: ReadonlyArray<{ readonly name: string; readonly value: string }>,
): AnthropicAccess => {
  const value = (name: string) => environment.find((entry) => entry.name === name)?.value;
  const apiKey = value("ANTHROPIC_API_KEY");
  const authToken = value("ANTHROPIC_AUTH_TOKEN") ?? value("CLAUDE_CODE_OAUTH_TOKEN");
  return {
    ...(apiKey === undefined || apiKey === "" ? {} : { apiKey }),
    ...(authToken === undefined || authToken === "" ? {} : { authToken }),
    baseUrl: (value("ANTHROPIC_BASE_URL") ?? "https://api.anthropic.com").replace(/\/$/, ""),
  };
};

const cloudCapabilities = (
  base: OrchestrationV2ProviderCapabilities,
): OrchestrationV2ProviderCapabilities => ({
  ...base,
  sessions: {
    ...base.sessions,
    supportsMultipleProviderThreadsPerSession: true,
    supportsRuntimeModeSwitchInSession: true,
  },
  threads: {
    ...base.threads,
    canReadThreadSnapshot: true,
    canRollbackThread: false,
    canForkThread: false,
    canForkFromTurn: false,
    canForkFromSubagentThread: false,
  },
  turns: {
    ...base.turns,
    supportsActiveSteering: false,
    supportsSteeringByInterruptRestart: false,
    supportsQueuedMessages: true,
  },
  streaming: {
    ...base.streaming,
    streamsAssistantText: false,
    streamsReasoning: false,
    streamsToolOutput: true,
    streamsPlanText: false,
  },
  approvals: {
    ...base.approvals,
    supportsCommandApproval: true,
    supportsFileChangeApproval: true,
    supportsFileReadApproval: false,
    supportsApplyPatchApproval: false,
    approvalsHaveNativeRequestIds: false,
    approvalCallbacksAreLiveOnly: false,
  },
  checkpointing: {
    appCanCheckpointFilesystem: false,
    supportsNestedCheckpointScopes: false,
    providerCanRollbackConversation: false,
    providerRollbackReturnsSnapshot: false,
    providerCanReadConversationSnapshot: false,
  },
  context: {
    ...base.context,
    supportsFullThreadHandoff: false,
    supportsDeltaHandoff: false,
    canConsumeHandoffSummaries: false,
    canGenerateSummaries: false,
  },
  subagents: {
    ...base.subagents,
    supportsSubagents: false,
    exposesSubagentThreadIds: false,
    emitsSubagentLifecycle: false,
    canWaitForSubagents: false,
    canCloseSubagents: false,
    canForkSubagentThread: false,
  },
  runtimePolicy: { enforcement: "client-boundary" },
});

interface ActiveTurn {
  readonly providerTurnId: ProviderTurnId;
  readonly runId: RunId;
  readonly nodeId: NodeId;
  readonly providerThreadId: ProviderThreadId;
  readonly providerTurnOrdinal: number;
  readonly runOrdinal: number;
  readonly startedAt: DateTime.Utc;
  readonly text: string;
  readonly toolOrdinals: ReadonlyMap<string, number>;
  readonly nextOrdinal: number;
}

const randomId = () => NodeCrypto.randomUUID();

export class CloudThreadRuntime extends Context.Service<
  CloudThreadRuntime,
  {
    readonly openSession: (
      input: CloudThreadRuntimeOpenInput,
      base: ProviderAdapter.ProviderAdapterV2["Service"],
    ) => Effect.Effect<
      ProviderAdapter.ProviderAdapterV2SessionRuntime,
      ProviderAdapter.ProviderAdapterV2Error,
      Scope.Scope
    >;
    readonly hasThread: (threadId: ThreadId) => Effect.Effect<boolean>;
  }
>()("t3/sandbox/CloudThreadRuntime") {}

const make = Effect.gen(function* () {
  const cloudThreads = yield* CloudThreads.CloudThreads;
  const cloudThreadPorts = yield* CloudThreadPorts.CloudThreadPorts;
  const fileSystem = yield* FileSystem.FileSystem;
  const http = yield* HttpClient.HttpClient;
  const { attachmentsDir } = yield* ServerConfig.ServerConfig;

  const hasThread: CloudThreadRuntime["Service"]["hasThread"] = Effect.fn(
    "CloudThreadRuntime.hasThread",
  )(function* (threadId) {
    const record = yield* cloudThreads.getRecord(threadId);
    return Option.isSome(record);
  });

  const openSession: CloudThreadRuntime["Service"]["openSession"] = Effect.fn(
    "CloudThreadRuntime.openSession",
  )(function* (input, base) {
    if (!CLOUD_THREAD_DRIVERS.has(base.driver)) {
      return yield* new ProviderAdapter.ProviderAdapterProtocolError({
        driver: base.driver,
        detail:
          "Cloud threads currently run only the Claude provider. Switch this thread to Claude to continue.",
      });
    }
    const record = yield* cloudThreads.getRecord(input.threadId);
    if (Option.isNone(record)) {
      return yield* new ProviderAdapter.ProviderAdapterProtocolError({
        driver: base.driver,
        detail: "No cloud sandbox is recorded for this thread",
      });
    }
    const threadRecord = record.value;
    const access = accessFromEnvironment(input.environment);
    const capabilities = cloudCapabilities(yield* base.getCapabilities());
    const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
    const connection = yield* Ref.make<CloudThreads.CloudHostConnection | null>(null);
    const activeTurn = yield* Ref.make<ActiveTurn | null>(null);
    const pendingApprovals = yield* Ref.make<
      ReadonlyMap<
        string,
        {
          readonly request: OrchestrationV2RuntimeRequest;
          readonly item: Extract<OrchestrationV2TurnItem, { readonly type: "approval_request" }>;
        }
      >
    >(new Map());
    const waitingForTurnStart = yield* Ref.make<Deferred.Deferred<void> | null>(null);
    yield* Effect.addFinalizer(() =>
      Ref.get(connection).pipe(Effect.flatMap((host) => host?.close ?? Effect.void)),
    );

    const emit = (event: ProviderAdapter.ProviderAdapterV2Event) =>
      Queue.offer(events, event).pipe(Effect.asVoid);

    const createdAt = yield* DateTime.now;
    const session: OrchestrationV2ProviderSession = {
      id: input.providerSessionId,
      driver: base.driver,
      providerInstanceId: input.modelSelection.instanceId,
      status: "ready",
      cwd: threadRecord.workspacePath,
      model: input.modelSelection.model,
      capabilities,
      extensionStatuses: [],
      createdAt,
      updatedAt: createdAt,
      lastError: null,
    };

    const allocateOrdinal = (turn: ActiveTurn) =>
      Ref.updateAndGet(activeTurn, (current) =>
        current === null ? null : { ...current, nextOrdinal: current.nextOrdinal + 1 },
      ).pipe(Effect.map((current) => current?.nextOrdinal ?? turn.nextOrdinal + 1));

    const toolOrdinal = (turn: ActiveTurn, toolId: string) =>
      Ref.updateAndGet(activeTurn, (current) => {
        if (current === null) return null;
        if (current.toolOrdinals.has(toolId)) return current;
        const toolOrdinals = new Map(current.toolOrdinals);
        toolOrdinals.set(toolId, current.nextOrdinal + 1);
        return { ...current, toolOrdinals, nextOrdinal: current.nextOrdinal + 1 };
      }).pipe(Effect.map((current) => current?.toolOrdinals.get(toolId) ?? turn.nextOrdinal + 1));

    const replyToModelRequest = Effect.fn("CloudThreadRuntime.replyToModelRequest")(function* (
      host: CloudThreads.CloudHostConnection,
      message: CloudThreads.CloudHostMessage,
    ) {
      const requestId = typeof message["id"] === "string" ? message["id"] : "";
      if (access.apiKey === undefined && access.authToken === undefined) {
        yield* host.send({
          type: "model_response",
          id: requestId,
          ok: false,
          error:
            "Cloud threads need Anthropic credentials. Set ANTHROPIC_API_KEY in this provider's environment, then retry.",
        });
        return;
      }
      const headers: Record<string, string> = {
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
      };
      if (access.apiKey !== undefined) headers["x-api-key"] = access.apiKey;
      if (access.authToken !== undefined) headers.authorization = `Bearer ${access.authToken}`;
      const request = HttpClientRequest.post(`${access.baseUrl}/v1/messages`).pipe(
        HttpClientRequest.setHeaders(headers),
        HttpClientRequest.bodyJsonUnsafe(
          boundModelRequest(message["request"], input.modelSelection.model),
        ),
      );
      const response = yield* http
        .execute(request)
        .pipe(Effect.timeout(MODEL_REQUEST_TIMEOUT_MS), Effect.result);
      if (response._tag === "Failure") {
        yield* host.send({
          type: "model_response",
          id: requestId,
          ok: false,
          error: "The model request could not reach Anthropic.",
        });
        return;
      }
      const text = yield* response.success.text.pipe(Effect.result);
      if (text._tag === "Failure") {
        yield* host.send({
          type: "model_response",
          id: requestId,
          ok: false,
          error: "Could not read the model response.",
        });
        return;
      }
      if (response.success.status < 200 || response.success.status >= 300) {
        yield* host.send({
          type: "model_response",
          id: requestId,
          ok: false,
          error: `The model request failed with ${response.success.status}: ${text.success.slice(0, 500)}`,
        });
        return;
      }
      const parsed = decodeJson(text.success);
      if (Option.isNone(parsed)) {
        yield* host.send({
          type: "model_response",
          id: requestId,
          ok: false,
          error: "The model response was not JSON.",
        });
        return;
      }
      const content =
        typeof parsed.value === "object" && parsed.value !== null
          ? (parsed.value as { readonly content?: unknown }).content
          : undefined;
      yield* host.send({
        type: "model_response",
        id: requestId,
        ok: true,
        content: Array.isArray(content) ? content : [],
      });
    });

    const failActiveTurn = Effect.fn("CloudThreadRuntime.failActiveTurn")(function* (
      message: string,
    ) {
      const turn = yield* Ref.get(activeTurn);
      if (turn === null) return;
      const failure = makeProviderFailure({
        message,
        class: "provider_error",
        retryable: true,
      });
      const ordinal = yield* allocateOrdinal(turn);
      yield* emit({
        type: "turn.terminal",
        driver: base.driver,
        providerThreadId: turn.providerThreadId,
        providerTurnId: turn.providerTurnId,
        runOrdinal: turn.runOrdinal,
        failureItemOrdinal: ordinal,
        status: "failed",
        failure,
        threadDisposition: "reusable",
      });
      yield* Ref.set(activeTurn, null);
    });

    const handleHostMessage = Effect.fn("CloudThreadRuntime.handleHostMessage")(function* (
      host: CloudThreads.CloudHostConnection,
      message: CloudThreads.CloudHostMessage,
    ) {
      switch (message.type) {
        case "turn_started": {
          const waiting = yield* Ref.get(waitingForTurnStart);
          if (waiting !== null) yield* Deferred.succeed(waiting, undefined);
          return;
        }
        case "ports": {
          yield* cloudThreadPorts.setPorts(
            input.threadId,
            Array.isArray(message["ports"]) ? message["ports"] : [],
          );
          return;
        }
        case "model_request":
          yield* Effect.forkDetach(replyToModelRequest(host, message));
          return;
        case "text": {
          const turn = yield* Ref.get(activeTurn);
          const text = typeof message["text"] === "string" ? message["text"] : "";
          if (turn === null || text === "") return;
          const occurredAt = yield* DateTime.now;
          const ordinal = yield* allocateOrdinal(turn);
          const messageId = MessageId.make(randomId());
          const item: OrchestrationV2TurnItem = {
            id: TurnItemId.make(randomId()),
            threadId: input.threadId,
            runId: turn.runId,
            nodeId: turn.nodeId,
            providerThreadId: turn.providerThreadId,
            providerTurnId: turn.providerTurnId,
            nativeItemRef: null,
            parentItemId: null,
            ordinal,
            status: "completed",
            title: null,
            startedAt: occurredAt,
            completedAt: occurredAt,
            updatedAt: occurredAt,
            type: "assistant_message",
            messageId,
            text,
            streaming: false,
          };
          yield* emit({
            type: "message.updated",
            driver: base.driver,
            message: {
              createdBy: "agent",
              creationSource: "provider",
              id: messageId,
              threadId: input.threadId,
              runId: turn.runId,
              nodeId: turn.nodeId,
              role: "assistant",
              text,
              attachments: [],
              streaming: false,
              createdAt: occurredAt,
              updatedAt: occurredAt,
            },
          });
          yield* emit({ type: "turn_item.updated", driver: base.driver, turnItem: item });
          return;
        }
        case "tool_start":
        case "tool_end": {
          const turn = yield* Ref.get(activeTurn);
          if (turn === null) return;
          const toolId =
            typeof message["id"] === "string" && message["id"] !== "" ? message["id"] : randomId();
          const name = typeof message["name"] === "string" ? message["name"] : "tool";
          const ended = message.type === "tool_end";
          const occurredAt = yield* DateTime.now;
          const ordinal = yield* toolOrdinal(turn, toolId);
          const itemId = TurnItemId.make(toolItemId(turn.providerTurnId, toolId));
          const inputValue = message["input"];
          const output = message["output"];
          const item: OrchestrationV2TurnItem =
            name === "bash"
              ? {
                  id: itemId,
                  threadId: input.threadId,
                  runId: turn.runId,
                  nodeId: turn.nodeId,
                  providerThreadId: turn.providerThreadId,
                  providerTurnId: turn.providerTurnId,
                  nativeItemRef: null,
                  parentItemId: null,
                  ordinal,
                  status: ended ? "completed" : "running",
                  title: "Command",
                  startedAt: occurredAt,
                  completedAt: ended ? occurredAt : null,
                  updatedAt: occurredAt,
                  type: "command_execution",
                  input:
                    typeof inputValue === "object" && inputValue !== null
                      ? String((inputValue as { readonly command?: unknown }).command ?? "")
                      : "",
                  ...(ended && typeof output === "string" ? { output } : {}),
                  ...(ended && typeof message["exitCode"] === "number"
                    ? { exitCode: message["exitCode"] }
                    : {}),
                }
              : {
                  id: itemId,
                  threadId: input.threadId,
                  runId: turn.runId,
                  nodeId: turn.nodeId,
                  providerThreadId: turn.providerThreadId,
                  providerTurnId: turn.providerTurnId,
                  nativeItemRef: null,
                  parentItemId: null,
                  ordinal,
                  status: ended ? "completed" : "running",
                  title: name,
                  startedAt: occurredAt,
                  completedAt: ended ? occurredAt : null,
                  updatedAt: occurredAt,
                  type: "dynamic_tool",
                  toolName: name,
                  input: inputValue,
                  ...(ended && output !== undefined ? { output } : {}),
                };
          yield* emit({ type: "turn_item.updated", driver: base.driver, turnItem: item });
          return;
        }
        case "approval_request": {
          const turn = yield* Ref.get(activeTurn);
          if (turn === null) return;
          const requestId = typeof message["id"] === "string" ? message["id"] : randomId();
          const kind = message["kind"] === "command" ? "command" : "file-change";
          const createdAt = yield* DateTime.now;
          const request: OrchestrationV2RuntimeRequest = {
            id: RuntimeRequestIdSchema.make(requestId),
            nodeId: turn.nodeId,
            providerTurnId: turn.providerTurnId,
            nativeRequestRef: null,
            kind,
            status: "pending",
            responseCapability: {
              type: "live",
              providerSessionId: input.providerSessionId,
            },
            createdAt,
            resolvedAt: null,
          };
          const item: Extract<OrchestrationV2TurnItem, { readonly type: "approval_request" }> = {
            id: TurnItemId.make(randomId()),
            threadId: input.threadId,
            runId: turn.runId,
            nodeId: turn.nodeId,
            providerThreadId: turn.providerThreadId,
            providerTurnId: turn.providerTurnId,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: yield* allocateOrdinal(turn),
            status: "waiting",
            title: typeof message["title"] === "string" ? message["title"] : null,
            startedAt: createdAt,
            completedAt: null,
            updatedAt: createdAt,
            type: "approval_request",
            requestId: request.id,
            requestKind: kind,
            ...(typeof message["prompt"] === "string" ? { prompt: message["prompt"] } : {}),
          };
          yield* Ref.update(pendingApprovals, (current) =>
            new Map(current).set(requestId, { request, item }),
          );
          yield* emit({
            type: "runtime_request.updated",
            driver: base.driver,
            runtimeRequest: request,
          });
          yield* emit({ type: "turn_item.updated", driver: base.driver, turnItem: item });
          return;
        }
        case "turn_end": {
          const turn = yield* Ref.get(activeTurn);
          if (turn === null) return;
          const completedAt = yield* DateTime.now;
          const status =
            message["status"] === "failed"
              ? "failed"
              : message["status"] === "cancelled"
                ? "cancelled"
                : "completed";
          const providerTurn: OrchestrationV2ProviderTurn = {
            id: turn.providerTurnId,
            providerThreadId: turn.providerThreadId,
            nodeId: turn.nodeId,
            runAttemptId: null,
            nativeTurnRef: null,
            ordinal: turn.providerTurnOrdinal,
            status,
            startedAt: turn.startedAt,
            completedAt,
          };
          yield* emit({ type: "provider_turn.updated", driver: base.driver, providerTurn });
          if (status === "failed") {
            const failure = makeProviderFailure({
              message:
                typeof message["error"] === "string" ? message["error"] : "The cloud turn failed.",
              class: "provider_error",
              retryable: true,
            });
            const ordinal = yield* allocateOrdinal(turn);
            yield* emit({
              type: "turn.terminal",
              driver: base.driver,
              providerThreadId: turn.providerThreadId,
              providerTurnId: turn.providerTurnId,
              runOrdinal: turn.runOrdinal,
              failureItemOrdinal: ordinal,
              status: "failed",
              failure,
              threadDisposition: "reusable",
            });
          } else {
            yield* emit({
              type: "turn.terminal",
              driver: base.driver,
              providerThreadId: turn.providerThreadId,
              providerTurnId: turn.providerTurnId,
              runOrdinal: turn.runOrdinal,
              status,
              failure: null,
              threadDisposition: "reusable",
            });
          }
          yield* Ref.set(activeTurn, null);
          return;
        }
        case "process_exit": {
          yield* failActiveTurn("The sandbox thread host exited before the turn finished.");
          yield* Ref.set(connection, null);
          return;
        }
        default:
          return;
      }
    });

    const ensureConnection = Effect.fn("CloudThreadRuntime.ensureConnection")(function* () {
      const existing = yield* Ref.get(connection);
      if (existing !== null) return existing;
      const opened = yield* cloudThreads.openHost(input.threadId).pipe(
        Effect.mapError(
          (error) =>
            new ProviderAdapter.ProviderAdapterProtocolError({
              driver: base.driver,
              detail: error.detail,
              cause: error,
            }),
        ),
      );
      yield* Ref.set(connection, opened);
      yield* Effect.forkDetach(
        opened.messages.pipe(
          Stream.runForEach((message) => handleHostMessage(opened, message)),
          Effect.catchCause((cause) =>
            Effect.logWarning("Cloud thread host stream stopped", {
              threadId: input.threadId,
              cause,
            }),
          ),
        ),
      );
      return opened;
    });

    const runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime = {
      instanceId: input.modelSelection.instanceId,
      driver: base.driver,
      providerSessionId: input.providerSessionId,
      providerSession: session,
      canResumeAfterRestart: true,
      events: Stream.fromQueue(events),
      ensureThread: Effect.fn("CloudThreadRuntime.ensureThread")(function* (ensureInput) {
        const createdAt = yield* DateTime.now;
        const existing = ensureInput.existingProviderThread;
        return {
          id: existing?.id ?? ProviderThreadId.make(randomId()),
          driver: base.driver,
          providerInstanceId: input.modelSelection.instanceId,
          providerSessionId: input.providerSessionId,
          appThreadId: input.threadId,
          ownerNodeId: existing?.ownerNodeId ?? null,
          nativeThreadRef: existing?.nativeThreadRef ?? {
            driver: base.driver,
            nativeId: `cloud:${input.threadId}`,
            strength: "strong" as const,
          },
          nativeConversationHeadRef: existing?.nativeConversationHeadRef ?? null,
          status: "active" as const,
          firstRunOrdinal: existing?.firstRunOrdinal ?? null,
          lastRunOrdinal: existing?.lastRunOrdinal ?? null,
          handoffIds: existing?.handoffIds ?? [],
          forkedFrom: null,
          pendingBackgroundTasks: [],
          contextUsage: null,
          nativeMetadata: null,
          goal: null,
          createdAt: existing?.createdAt ?? createdAt,
          updatedAt: createdAt,
        };
      }),
      resumeThread: ({ providerThread }) => Effect.succeed(providerThread),
      startTurn: Effect.fn("CloudThreadRuntime.startTurn")(function* (turnInput) {
        if (access.apiKey === undefined && access.authToken === undefined) {
          return yield* new ProviderAdapter.ProviderAdapterProtocolError({
            driver: base.driver,
            detail:
              "Cloud threads need Anthropic credentials. Set ANTHROPIC_API_KEY in this provider's environment, then retry.",
          });
        }
        const host = yield* ensureConnection();
        const startedAt = yield* DateTime.now;
        const started = yield* Deferred.make<void>();
        yield* Ref.set(waitingForTurnStart, started);
        const providerTurnId = ProviderTurnId.make(randomId());
        const providerThreadId = turnInput.providerThread.id;
        yield* Ref.set(activeTurn, {
          providerTurnId,
          runId: turnInput.runId,
          nodeId: turnInput.rootNodeId,
          providerThreadId,
          providerTurnOrdinal: turnInput.providerTurnOrdinal,
          runOrdinal: turnInput.runOrdinal,
          startedAt,
          text: "",
          toolOrdinals: new Map(),
          nextOrdinal: 0,
        });
        yield* emit({
          type: "provider_turn.updated",
          driver: base.driver,
          providerTurn: {
            id: providerTurnId,
            providerThreadId,
            nodeId: turnInput.rootNodeId,
            runAttemptId: turnInput.attemptId,
            nativeTurnRef: null,
            ordinal: turnInput.providerTurnOrdinal,
            status: "running",
            startedAt,
            completedAt: null,
          },
        });
        const attachments = yield* stageAttachments(threadRecord, turnInput.message.attachments);
        const approvalMode =
          turnInput.runtimePolicy.runtimeMode === "full-access"
            ? "none"
            : turnInput.runtimePolicy.runtimeMode === "auto-accept-edits"
              ? "edits"
              : "ask";
        yield* host
          .send({
            type: "turn",
            runId: turnInput.runId,
            model: turnInput.modelSelection.model,
            maxTokens: 8192,
            system: systemPrompt(threadRecord),
            text: turnInput.message.text,
            attachments,
            history: [],
            approvalMode,
          })
          .pipe(
            Effect.mapError(
              (error) =>
                new ProviderAdapter.ProviderAdapterProtocolError({
                  driver: base.driver,
                  detail: error.detail,
                  cause: error,
                }),
            ),
          );
        yield* Deferred.await(started).pipe(Effect.timeout("20 seconds"), Effect.ignore);
      }),
      steerTurn: () =>
        Effect.fail(
          new ProviderAdapter.ProviderAdapterSteerRunUnsupportedError({
            driver: base.driver,
            providerThreadId: ProviderThreadId.make("cloud"),
          }),
        ),
      interruptTurn: Effect.fn("CloudThreadRuntime.interruptTurn")(function* () {
        const host = yield* Ref.get(connection);
        if (host !== null) yield* host.send({ type: "interrupt" }).pipe(Effect.ignore);
      }),
      respondToRuntimeRequest: Effect.fn("CloudThreadRuntime.respondToRuntimeRequest")(
        function* (response) {
          const pending = yield* Ref.get(pendingApprovals);
          const key = String(response.requestId);
          const pendingApproval = pending.get(key);
          if (pendingApproval === undefined) return;
          const host = yield* Ref.get(connection);
          if (host !== null) {
            yield* host
              .send({
                type: "approval_response",
                id: key,
                decision: response.decision ?? "cancel",
              })
              .pipe(Effect.ignore);
          }
          const resolvedAt = yield* DateTime.now;
          const resolved: OrchestrationV2RuntimeRequest = {
            ...pendingApproval.request,
            status: response.decision === "cancel" ? "cancelled" : "resolved",
            resolvedAt,
            ...(response.decision === undefined ? {} : { decision: response.decision }),
          };
          const resolvedItem: OrchestrationV2TurnItem = {
            ...pendingApproval.item,
            status: response.decision === "cancel" ? "cancelled" : "completed",
            completedAt: resolvedAt,
            updatedAt: resolvedAt,
          };
          yield* Ref.update(pendingApprovals, (current) => {
            const next = new Map(current);
            next.delete(key);
            return next;
          });
          yield* emit({
            type: "runtime_request.updated",
            driver: base.driver,
            runtimeRequest: resolved,
          });
          yield* emit({ type: "turn_item.updated", driver: base.driver, turnItem: resolvedItem });
        },
      ),
      readThreadSnapshot: ({ providerThread }) =>
        Effect.succeed({ providerThread, messages: [], runtimeRequests: [] }),
      rollbackThread: () =>
        Effect.fail(
          new ProviderAdapter.ProviderAdapterRollbackThreadError({
            driver: base.driver,
            providerThreadId: ProviderThreadId.make("cloud"),
          }),
        ),
      forkThread: () =>
        Effect.fail(
          new ProviderAdapter.ProviderAdapterForkThreadError({
            driver: base.driver,
            providerThreadId: ProviderThreadId.make("cloud"),
          }),
        ),
    };
    return runtime;
  });

  const stageAttachments = Effect.fn("CloudThreadRuntime.stageAttachments")(function* (
    record: CloudThreadStore.CloudThreadRecord,
    attachments: ReadonlyArray<ChatAttachment>,
  ) {
    if (attachments.length === 0) return [];
    const staged: Array<{ readonly path: string; readonly name: string }> = [];
    for (const attachment of attachments) {
      const localPath = resolveAttachmentPath({ attachmentsDir, attachment });
      if (localPath === null) continue;
      const bytes = yield* fileSystem
        .readFile(localPath)
        .pipe(Effect.catchCause(() => Effect.succeed(new Uint8Array())));
      if (bytes.length === 0) continue;
      const safeName = attachment.name.replaceAll(/[^a-zA-Z0-9._-]/g, "_");
      const remotePath = `${record.homePath}/.t3/attachments/${attachment.id}-${safeName}`;
      const written = yield* cloudThreads.writeAttachment(record.threadId, remotePath, bytes);
      if (written) staged.push({ path: remotePath, name: attachment.name });
    }
    return staged;
  });

  return CloudThreadRuntime.of({ openSession, hasThread });
});

const systemPrompt = (record: CloudThreadStore.CloudThreadRecord) =>
  [
    "You are T3 Code's cloud agent, running inside an isolated sandbox.",
    `Work only in the checkout at ${record.workspacePath} on branch ${record.branch}.`,
    "The controlling machine cannot see or use your files; make every edit here.",
    "Attachments are staged outside the checkout under the sandbox home; read them by their absolute paths.",
    "Use the provided bash, read_file, write_file, edit_file, and list_directory tools.",
    "There is no T3 server, T3 sign-in, or T3 MCP tool inside this sandbox.",
  ].join("\n");

const toolItemId = (providerTurnId: ProviderTurnId, toolId: string) =>
  `cloud-${providerTurnId}-${toolId}`.replaceAll(/[^a-zA-Z0-9_-]/g, "_");

export const layer = Layer.effect(CloudThreadRuntime, make);
