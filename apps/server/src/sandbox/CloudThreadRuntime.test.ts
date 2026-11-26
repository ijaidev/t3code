import {
  ModelSelection,
  NodeId,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2TurnItem,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunId,
  RuntimeRequestId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
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

import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as ServerConfig from "../config.ts";
import * as CloudThreadPorts from "./CloudThreadPorts.ts";
import * as CloudThreadStore from "./CloudThreadStore.ts";
import * as CloudThreadRuntime from "./CloudThreadRuntime.ts";
import * as CloudThreads from "./CloudThreads.ts";

const threadId = ThreadId.make("thread-cloud-runtime");
const instanceId = ProviderInstanceId.make("claudeAgent");
const driver = ProviderDriverKind.make("claudeAgent");
const modelSelection: ModelSelection = { instanceId, model: "claude-sonnet-4-5" };
const now = "2026-11-24T00:00:00.000Z";
const resumeRunId = RunId.make("run-resume");
const resumeProviderTurnId = ProviderTurnId.make("turn-resume");
const resumeProviderThreadId = ProviderThreadId.make("provider-thread-resume");
const resumeNodeId = NodeId.make("node-resume");

const approvalRequest = (
  id: string,
  status: OrchestrationV2RuntimeRequest["status"],
  now: DateTime.Utc,
  decision?: OrchestrationV2RuntimeRequest["decision"],
): OrchestrationV2RuntimeRequest => ({
  id: RuntimeRequestId.make(id),
  nodeId: resumeNodeId,
  providerTurnId: resumeProviderTurnId,
  nativeRequestRef: null,
  kind: "command",
  status,
  responseCapability: { type: "live", providerSessionId: input.providerSessionId },
  createdAt: now,
  resolvedAt: status === "pending" ? null : now,
  ...(decision === undefined ? {} : { decision }),
});

const approvalItem = (
  id: string,
  requestId: string,
  now: DateTime.Utc,
): Extract<OrchestrationV2TurnItem, { readonly type: "approval_request" }> => ({
  id: TurnItemId.make(`approval-item-${id}`),
  threadId,
  runId: resumeRunId,
  nodeId: resumeNodeId,
  providerThreadId: resumeProviderThreadId,
  providerTurnId: resumeProviderTurnId,
  nativeItemRef: null,
  parentItemId: null,
  ordinal: 7,
  status: "waiting",
  title: "Run command",
  startedAt: now,
  completedAt: null,
  updatedAt: now,
  type: "approval_request",
  requestId: RuntimeRequestId.make(requestId),
  requestKind: "command",
  prompt: "pwd",
});

const record: CloudThreadStore.CloudThreadRecord = {
  threadId,
  sandboxId: "sandbox-1" as CloudThreadStore.CloudThreadRecord["sandboxId"],
  accountId: "e2b-work" as CloudThreadStore.CloudThreadRecord["accountId"],
  providerKind: "e2b" as CloudThreadStore.CloudThreadRecord["providerKind"],
  repositoryUrl: "https://github.com/example/public.git",
  branch: "main",
  baseRef: "main",
  workspacePath: "/home/t3/.t3/threads/thread-cloud-runtime",
  homePath: "/home/t3",
  status: "ready",
  createdAt: now,
  updatedAt: now,
};

const sandbox = {
  id: record.sandboxId,
  accountId: record.accountId,
  providerKind: record.providerKind,
  providerSandboxId: "e2b-1",
  threadId,
  status: "running" as const,
  template: "base",
  createdAt: now,
  updatedAt: now,
};

const baseAdapter = {
  instanceId,
  driver,
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.die("unused"),
  openSession: () => Effect.die("unused"),
} as unknown as ProviderAdapter.ProviderAdapterV2["Service"];

const input = {
  threadId,
  providerSessionId: ProviderSessionId.make("session-cloud-runtime"),
  modelSelection,
  runtimePolicy: {
    runtimeMode: "full-access" as const,
    interactionMode: "default" as const,
    cwd: null,
  },
  environment: [{ name: "ANTHROPIC_API_KEY", value: "test-key" }],
};

const testLayer = (
  cloud: CloudThreads.CloudThreads["Service"],
  ports: CloudThreadPorts.CloudThreadPorts["Service"] = {
    setPorts: () => Effect.void,
    clear: () => Effect.void,
    snapshot: Effect.succeed({ servers: [], unavailable: [] }),
    subscribe: () => Effect.void,
  },
) =>
  CloudThreadRuntime.layer.pipe(
    Layer.provide(Layer.succeed(CloudThreads.CloudThreads, cloud)),
    Layer.provide(Layer.succeed(CloudThreadPorts.CloudThreadPorts, ports)),
    Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "t3-cloud-runtime-test-" })),
    Layer.provide(NodeServices.layer),
    Layer.provide(
      Layer.succeed(
        HttpClient.HttpClient,
        HttpClient.make(() => Effect.die("the test never calls the model")),
      ),
    ),
  );

describe("cloud thread model requests", () => {
  it("keeps the selected model and a fixed token ceiling", () => {
    expect(
      CloudThreadRuntime.boundModelRequest(
        { model: "attacker-model", max_tokens: 999_999, messages: [] },
        "claude-sonnet-4-5",
      ),
    ).toEqual({
      model: "claude-sonnet-4-5",
      max_tokens: 8192,
      messages: [],
    });
    expect(CloudThreadRuntime.boundModelRequest({ max_tokens: -1 }, "claude-sonnet-4-5")).toEqual({
      model: "claude-sonnet-4-5",
      max_tokens: 8192,
    });
  });
});

describe("cloud thread runtime", () => {
  it.effect("turns host messages into persisted provider events", () => {
    const events = Effect.gen(function* () {
      const outbound = yield* Queue.unbounded<CloudThreads.CloudHostMessage>();
      const sent: Array<unknown> = [];
      const cloud = CloudThreads.CloudThreads.of({
        prepare: () => Effect.die("unused"),
        getSandbox: () => Effect.succeedSome(sandbox),
        getRecord: () => Effect.succeedSome(record),
        forWorkspace: () => Effect.succeedNone,
        exec: () => Effect.die("unused"),
        writeAttachment: () => Effect.succeed(false),
        openHost: () =>
          Effect.succeed({
            sandbox,
            messages: Stream.fromQueue(outbound),
            send: (message) => Effect.sync(() => sent.push(message)),
            close: Effect.void,
            activeRunId: null,
            latestEventType: null,
            latestRunId: null,
            pendingApprovalIds: [],
          }),
        deleteForThread: () => Effect.void,
        getCloudSandboxState: () => Effect.succeedNone,
        verifyCloudSandbox: () => Effect.succeedNone,
        markCloudSandboxLost: () => Effect.void,
        recreateCloudSandbox: () => Effect.die("unused"),
        subscribeCloudSandbox: () => Stream.empty,
        sleepCloudSandbox: () => Effect.die("unused"),
        wakeCloudSandbox: () => Effect.die("unused"),
        previewCloudSandboxDelete: () => Effect.die("unused"),
        deleteCloudSandbox: () => Effect.die("unused"),
        beginTurn: () => Effect.void,
        endTurn: () => Effect.void,
        holdPreview: () => Effect.void,
        releasePreview: () => Effect.void,
        sweepIdle: Effect.succeed(0),
      });
      return yield* Effect.gen(function* () {
        const runtimeService = yield* CloudThreadRuntime.CloudThreadRuntime;
        const runtime = yield* runtimeService.openSession(input, baseAdapter);
        const providerThread = yield* runtime.ensureThread({
          threadId,
          modelSelection,
          runtimePolicy: input.runtimePolicy,
        });
        const started = yield* runtime
          .startTurn({
            appThread: {} as never,
            threadId,
            runId: "run-1" as never,
            runOrdinal: 1,
            providerTurnOrdinal: 1,
            attemptId: "attempt-1" as never,
            rootNodeId: "node-1" as never,
            providerThread,
            message: {
              messageId: "message-1" as never,
              text: "show me the working directory",
              attachments: [],
              createdBy: "user",
              creationSource: "web",
            },
            modelSelection,
            runtimePolicy: input.runtimePolicy,
          })
          .pipe(Effect.forkChild);
        yield* Queue.offer(outbound, { type: "turn_started", runId: "run-1" });
        yield* Fiber.join(started);
        yield* Queue.offer(outbound, { type: "text", runId: "run-1", text: "Running pwd." });
        yield* Queue.offer(outbound, {
          type: "approval_request",
          runId: "run-1",
          id: "approval-1",
          kind: "command",
          title: "Run command",
          prompt: "pwd",
        });
        yield* Queue.offer(outbound, {
          type: "tool_start",
          runId: "run-1",
          id: "tool-1",
          name: "bash",
          input: { command: "pwd" },
        });
        yield* Queue.offer(outbound, {
          type: "tool_end",
          runId: "run-1",
          id: "tool-1",
          name: "bash",
          input: { command: "pwd" },
          output: "/home/t3/.t3/threads/thread-cloud-runtime\n",
          exitCode: 0,
          isError: false,
        });
        yield* Queue.offer(outbound, { type: "turn_end", runId: "run-1", status: "completed" });
        const collected = yield* runtime.events.pipe(
          Stream.takeUntil((event) => event.type === "turn.terminal"),
          Stream.runCollect,
        );
        const eventList = [...collected];
        expect(sent.some((message) => (message as { type?: string }).type === "turn")).toBe(true);
        expect(
          eventList.some(
            (event) => event.type === "message.updated" && event.message.text === "Running pwd.",
          ),
        ).toBe(true);
        const toolStart = eventList.find(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "command_execution" &&
            event.turnItem.status === "running",
        );
        const toolEnd = eventList.find(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "command_execution" &&
            event.turnItem.status === "completed",
        );
        expect(toolStart?.type).toBe("turn_item.updated");
        expect(toolEnd?.type).toBe("turn_item.updated");
        if (
          toolEnd?.type === "turn_item.updated" &&
          toolEnd.turnItem.type === "command_execution"
        ) {
          expect(toolEnd.turnItem.input).toBe("pwd");
          expect(toolEnd.turnItem.exitCode).toBe(0);
        }
        expect(
          eventList.some(
            (event) =>
              event.type === "runtime_request.updated" && event.runtimeRequest.id === "approval-1",
          ),
        ).toBe(true);
        const approvalCard = eventList.find(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "approval_request" &&
            event.turnItem.requestId === "approval-1",
        );
        expect(approvalCard?.type).toBe("turn_item.updated");
        if (
          approvalCard?.type === "turn_item.updated" &&
          approvalCard.turnItem.type === "approval_request"
        ) {
          expect(approvalCard.turnItem.prompt).toBe("pwd");
        }
        const terminal = eventList.at(-1);
        expect(terminal?.type).toBe("turn.terminal");
        if (terminal?.type === "turn.terminal") {
          expect(terminal.status).toBe("completed");
          expect(terminal.providerTurnId).toBeTruthy();
        }
      }).pipe(Effect.provide(testLayer(cloud)));
    });
    return events;
  });

  it.effect("reports the sandbox host's listening ports to the thread", () => {
    const result = Effect.gen(function* () {
      const outbound = yield* Queue.unbounded<CloudThreads.CloudHostMessage>();
      const ports = yield* Deferred.make<ReadonlyArray<unknown>>();
      const cloud = CloudThreads.CloudThreads.of({
        prepare: () => Effect.die("unused"),
        getSandbox: () => Effect.succeedSome(sandbox),
        getRecord: () => Effect.succeedSome(record),
        forWorkspace: () => Effect.succeedNone,
        exec: () => Effect.die("unused"),
        writeAttachment: () => Effect.succeed(false),
        openHost: () =>
          Effect.succeed({
            sandbox,
            messages: Stream.fromQueue(outbound),
            send: () => Effect.void,
            close: Effect.void,
            activeRunId: null,
            latestEventType: null,
            latestRunId: null,
            pendingApprovalIds: [],
          }),
        deleteForThread: () => Effect.void,
        getCloudSandboxState: () => Effect.succeedNone,
        verifyCloudSandbox: () => Effect.succeedNone,
        markCloudSandboxLost: () => Effect.void,
        recreateCloudSandbox: () => Effect.die("unused"),
        subscribeCloudSandbox: () => Stream.empty,
        sleepCloudSandbox: () => Effect.die("unused"),
        wakeCloudSandbox: () => Effect.die("unused"),
        previewCloudSandboxDelete: () => Effect.die("unused"),
        deleteCloudSandbox: () => Effect.die("unused"),
        beginTurn: () => Effect.void,
        endTurn: () => Effect.void,
        holdPreview: () => Effect.void,
        releasePreview: () => Effect.void,
        sweepIdle: Effect.succeed(0),
      });
      return yield* Effect.gen(function* () {
        const runtimeService = yield* CloudThreadRuntime.CloudThreadRuntime;
        const runtime = yield* runtimeService.openSession(input, baseAdapter);
        const providerThread = yield* runtime.ensureThread({
          threadId,
          modelSelection,
          runtimePolicy: input.runtimePolicy,
        });
        const started = yield* runtime
          .startTurn({
            appThread: {} as never,
            threadId,
            runId: "run-ports" as never,
            runOrdinal: 1,
            providerTurnOrdinal: 1,
            attemptId: "attempt-ports" as never,
            rootNodeId: "node-ports" as never,
            providerThread,
            message: {
              messageId: "message-ports" as never,
              text: "start the dev server",
              attachments: [],
              createdBy: "user",
              creationSource: "web",
            },
            modelSelection,
            runtimePolicy: input.runtimePolicy,
          })
          .pipe(Effect.forkChild);
        yield* Queue.offer(outbound, { type: "turn_started", runId: "run-ports" });
        yield* Fiber.join(started);
        yield* Queue.offer(outbound, {
          type: "ports",
          ports: [{ port: 5173 }],
        });
        expect(yield* Deferred.await(ports)).toEqual([{ port: 5173 }]);
      }).pipe(
        Effect.provide(
          testLayer(cloud, {
            setPorts: (_threadId, reported) =>
              Deferred.succeed(ports, reported).pipe(Effect.asVoid),
            clear: () => Effect.void,
            snapshot: Effect.succeed({ servers: [], unavailable: [] }),
            subscribe: () => Effect.void,
          }),
        ),
      );
    });
    return result;
  });

  it.effect("resolves an approval request and tells the host", () => {
    const result = Effect.gen(function* () {
      const outbound = yield* Queue.unbounded<CloudThreads.CloudHostMessage>();
      const sent: Array<unknown> = [];
      const cloud = CloudThreads.CloudThreads.of({
        prepare: () => Effect.die("unused"),
        getSandbox: () => Effect.succeedSome(sandbox),
        getRecord: () => Effect.succeedSome(record),
        forWorkspace: () => Effect.succeedNone,
        exec: () => Effect.die("unused"),
        writeAttachment: () => Effect.succeed(false),
        openHost: () =>
          Effect.succeed({
            sandbox,
            messages: Stream.fromQueue(outbound),
            send: (message) => Effect.sync(() => sent.push(message)),
            close: Effect.void,
            activeRunId: null,
            latestEventType: null,
            latestRunId: null,
            pendingApprovalIds: [],
          }),
        deleteForThread: () => Effect.void,
        getCloudSandboxState: () => Effect.succeedNone,
        verifyCloudSandbox: () => Effect.succeedNone,
        markCloudSandboxLost: () => Effect.void,
        recreateCloudSandbox: () => Effect.die("unused"),
        subscribeCloudSandbox: () => Stream.empty,
        sleepCloudSandbox: () => Effect.die("unused"),
        wakeCloudSandbox: () => Effect.die("unused"),
        previewCloudSandboxDelete: () => Effect.die("unused"),
        deleteCloudSandbox: () => Effect.die("unused"),
        beginTurn: () => Effect.void,
        endTurn: () => Effect.void,
        holdPreview: () => Effect.void,
        releasePreview: () => Effect.void,
        sweepIdle: Effect.succeed(0),
      });
      return yield* Effect.gen(function* () {
        const runtimeService = yield* CloudThreadRuntime.CloudThreadRuntime;
        const runtime = yield* runtimeService.openSession(input, baseAdapter);
        const providerThread = yield* runtime.ensureThread({
          threadId,
          modelSelection,
          runtimePolicy: input.runtimePolicy,
        });
        const started = yield* runtime
          .startTurn({
            appThread: {} as never,
            threadId,
            runId: "run-2" as never,
            runOrdinal: 1,
            providerTurnOrdinal: 1,
            attemptId: "attempt-2" as never,
            rootNodeId: "node-2" as never,
            providerThread,
            message: {
              messageId: "message-2" as never,
              text: "run it",
              attachments: [],
              createdBy: "user",
              creationSource: "web",
            },
            modelSelection,
            runtimePolicy: input.runtimePolicy,
          })
          .pipe(Effect.forkChild);
        yield* Queue.offer(outbound, { type: "turn_started", runId: "run-2" });
        yield* Fiber.join(started);
        yield* Queue.offer(outbound, {
          type: "approval_request",
          runId: "run-2",
          id: "approval-2",
          kind: "command",
          title: "Run command",
          prompt: "pwd",
        });
        const pending = yield* runtime.events.pipe(
          Stream.filter(
            (event) =>
              event.type === "runtime_request.updated" &&
              event.runtimeRequest.id === "approval-2" &&
              event.runtimeRequest.status === "pending",
          ),
          Stream.runHead,
        );
        expect(Option.isSome(pending)).toBe(true);
        yield* runtime.respondToRuntimeRequest({
          requestId: "approval-2" as never,
          decision: "accept",
        });
        expect(
          sent.some(
            (message) =>
              (message as { type?: string; id?: string; decision?: string }).type ===
                "approval_response" &&
              (message as { id?: string }).id === "approval-2" &&
              (message as { decision?: string }).decision === "accept",
          ),
        ).toBe(true);
        yield* Queue.offer(outbound, { type: "turn_end", runId: "run-2", status: "cancelled" });
        const terminal = yield* runtime.events.pipe(
          Stream.filter((event) => event.type === "turn.terminal"),
          Stream.runHead,
        );
        expect(Option.isSome(terminal)).toBe(true);
      }).pipe(Effect.provide(testLayer(cloud)));
    });
    return result;
  });

  it.effect("reattaches a running cloud turn and replays its missed events", () => {
    const result = Effect.gen(function* () {
      const outbound = yield* Queue.unbounded<CloudThreads.CloudHostMessage>();
      const begins = yield* Ref.make(0);
      const ends = yield* Ref.make(0);
      const sent: Array<unknown> = [];
      const cloud = CloudThreads.CloudThreads.of({
        prepare: () => Effect.die("unused"),
        getSandbox: () => Effect.succeedSome(sandbox),
        getRecord: () => Effect.succeedSome(record),
        forWorkspace: () => Effect.succeedNone,
        exec: () => Effect.die("unused"),
        writeAttachment: () => Effect.succeed(false),
        openHost: () =>
          Effect.succeed({
            sandbox,
            messages: Stream.fromQueue(outbound),
            send: (message) => Effect.sync(() => sent.push(message)),
            close: Effect.void,
            activeRunId: String(resumeRunId),
            latestEventType: "turn_started",
            latestRunId: null,
            pendingApprovalIds: [],
          }),
        deleteForThread: () => Effect.void,
        getCloudSandboxState: () => Effect.succeedNone,
        subscribeCloudSandbox: () => Stream.empty,
        sleepCloudSandbox: () => Effect.die("unused"),
        wakeCloudSandbox: () => Effect.die("unused"),
        previewCloudSandboxDelete: () => Effect.die("unused"),
        deleteCloudSandbox: () => Effect.die("unused"),
        verifyCloudSandbox: () => Effect.succeedNone,
        markCloudSandboxLost: () => Effect.void,
        recreateCloudSandbox: () => Effect.die("unused"),
        beginTurn: () => Ref.update(begins, (count) => count + 1),
        endTurn: () => Ref.update(ends, (count) => count + 1),
        holdPreview: () => Effect.void,
        releasePreview: () => Effect.void,
        sweepIdle: Effect.succeed(0),
      });
      return yield* Effect.gen(function* () {
        const runtimeService = yield* CloudThreadRuntime.CloudThreadRuntime;
        const runtime = yield* runtimeService.openSession(
          {
            ...input,
            resumeTurn: {
              runId: resumeRunId,
              runOrdinal: 1,
              providerTurnId: resumeProviderTurnId,
              providerTurnOrdinal: 1,
              providerThreadId: resumeProviderThreadId,
              nodeId: resumeNodeId,
              startedAt: yield* DateTime.now,
              nextItemOrdinal: 5,
              approvals: [],
            },
          },
          baseAdapter,
        );
        expect(yield* Ref.get(begins)).toBe(1);
        yield* Queue.offer(outbound, {
          type: "text",
          runId: String(RunId.make("run-old")),
          seq: 6,
          text: "Stale turn text.",
        });
        yield* Queue.offer(outbound, {
          type: "text",
          runId: String(resumeRunId),
          seq: 7,
          text: "Replayed after restart.",
        });
        yield* Queue.offer(outbound, {
          type: "turn_end",
          runId: String(resumeRunId),
          seq: 8,
          status: "completed",
        });
        const collected = yield* runtime.events.pipe(
          Stream.takeUntil((event) => event.type === "turn.terminal"),
          Stream.runCollect,
        );
        const eventList = [...collected];
        expect(
          eventList.some(
            (event) =>
              event.type === "message.updated" && event.message.text === "Stale turn text.",
          ),
        ).toBe(false);
        const message = eventList.find((event) => event.type === "message.updated");
        expect(message?.type).toBe("message.updated");
        if (message?.type === "message.updated") {
          expect(message.message.id).toBe(`cloud-message-${resumeProviderTurnId}-7`);
          expect(message.message.text).toBe("Replayed after restart.");
        }
        const terminal = eventList.at(-1);
        expect(terminal?.type).toBe("turn.terminal");
        expect(sent.some((value) => (value as { type?: string }).type === "turn")).toBe(false);
        expect(yield* Ref.get(ends)).toBe(1);
      }).pipe(Effect.provide(testLayer(cloud)));
    });
    return result;
  });

  it.effect("fails a reconnect when the daemon restarted without the turn", () => {
    const result = Effect.gen(function* () {
      const firstEvents = yield* Queue.unbounded<CloudThreads.CloudHostMessage>();
      const secondEvents = yield* Queue.unbounded<CloudThreads.CloudHostMessage>();
      const opens = yield* Ref.make(0);
      const reconnectedRunId = "run-reconnect";
      const cloud = CloudThreads.CloudThreads.of({
        prepare: () => Effect.die("unused"),
        getSandbox: () => Effect.succeedSome(sandbox),
        getRecord: () => Effect.succeedSome(record),
        forWorkspace: () => Effect.succeedNone,
        exec: () => Effect.die("unused"),
        writeAttachment: () => Effect.succeed(false),
        openHost: () =>
          Ref.updateAndGet(opens, (count) => count + 1).pipe(
            Effect.map((count) => ({
              sandbox,
              messages:
                count === 1 ? Stream.fromQueue(firstEvents) : Stream.fromQueue(secondEvents),
              send: () => Effect.void,
              close: Effect.void,
              activeRunId: count === 1 ? reconnectedRunId : null,
              latestEventType: count === 1 ? "turn_started" : "text",
              latestRunId: reconnectedRunId,
              pendingApprovalIds: [],
            })),
          ),
        deleteForThread: () => Effect.void,
        getCloudSandboxState: () => Effect.succeedNone,
        subscribeCloudSandbox: () => Stream.empty,
        sleepCloudSandbox: () => Effect.die("unused"),
        wakeCloudSandbox: () => Effect.die("unused"),
        previewCloudSandboxDelete: () => Effect.die("unused"),
        deleteCloudSandbox: () => Effect.die("unused"),
        verifyCloudSandbox: () => Effect.succeedNone,
        markCloudSandboxLost: () => Effect.void,
        recreateCloudSandbox: () => Effect.die("unused"),
        beginTurn: () => Effect.void,
        endTurn: () => Effect.void,
        holdPreview: () => Effect.void,
        releasePreview: () => Effect.void,
        sweepIdle: Effect.succeed(0),
      });
      return yield* Effect.gen(function* () {
        const runtimeService = yield* CloudThreadRuntime.CloudThreadRuntime;
        const runtime = yield* runtimeService.openSession(input, baseAdapter);
        const providerThread = yield* runtime.ensureThread({
          threadId,
          modelSelection,
          runtimePolicy: input.runtimePolicy,
        });
        const started = yield* runtime
          .startTurn({
            appThread: {} as never,
            threadId,
            runId: reconnectedRunId as never,
            runOrdinal: 1,
            providerTurnOrdinal: 1,
            attemptId: "attempt-reconnect" as never,
            rootNodeId: "node-reconnect" as never,
            providerThread,
            message: {
              messageId: "message-reconnect" as never,
              text: "keep working",
              attachments: [],
              createdBy: "user",
              creationSource: "web",
            },
            modelSelection,
            runtimePolicy: input.runtimePolicy,
          })
          .pipe(Effect.forkChild);
        yield* Queue.offer(firstEvents, {
          type: "turn_started",
          runId: reconnectedRunId,
        });
        yield* Fiber.join(started);
        yield* Queue.offer(firstEvents, { type: "process_exit", exitCode: 1 });
        const collected = yield* runtime.events.pipe(
          Stream.takeUntil((event) => event.type === "turn.terminal"),
          Stream.runCollect,
        );
        const terminal = [...collected].at(-1);
        expect(terminal?.type).toBe("turn.terminal");
        if (terminal?.type === "turn.terminal") {
          expect(terminal.status).toBe("failed");
          expect(terminal.failure?.message).toContain("restarted");
        }
        expect(yield* Ref.get(opens)).toBe(2);
      }).pipe(Effect.provide(testLayer(cloud)));
    });
    return result;
  });

  it.effect("refuses a stale turn_end from an older run", () => {
    const result = Effect.gen(function* () {
      const cloud = CloudThreads.CloudThreads.of({
        prepare: () => Effect.die("unused"),
        getSandbox: () => Effect.succeedSome(sandbox),
        getRecord: () => Effect.succeedSome(record),
        forWorkspace: () => Effect.succeedNone,
        exec: () => Effect.die("unused"),
        writeAttachment: () => Effect.succeed(false),
        openHost: () =>
          Effect.succeed({
            sandbox,
            messages: Stream.empty,
            send: () => Effect.void,
            close: Effect.void,
            activeRunId: null,
            latestEventType: "turn_end",
            latestRunId: String(RunId.make("run-old")),
            pendingApprovalIds: [],
          }),
        deleteForThread: () => Effect.void,
        getCloudSandboxState: () => Effect.succeedNone,
        subscribeCloudSandbox: () => Stream.empty,
        sleepCloudSandbox: () => Effect.die("unused"),
        wakeCloudSandbox: () => Effect.die("unused"),
        previewCloudSandboxDelete: () => Effect.die("unused"),
        deleteCloudSandbox: () => Effect.die("unused"),
        verifyCloudSandbox: () => Effect.succeedNone,
        markCloudSandboxLost: () => Effect.void,
        recreateCloudSandbox: () => Effect.die("unused"),
        beginTurn: () => Effect.void,
        endTurn: () => Effect.void,
        holdPreview: () => Effect.void,
        releasePreview: () => Effect.void,
        sweepIdle: Effect.succeed(0),
      });
      return yield* Effect.gen(function* () {
        const runtimeService = yield* CloudThreadRuntime.CloudThreadRuntime;
        const runtime = yield* runtimeService.openSession(
          {
            ...input,
            resumeTurn: {
              runId: resumeRunId,
              runOrdinal: 1,
              providerTurnId: resumeProviderTurnId,
              providerTurnOrdinal: 1,
              providerThreadId: resumeProviderThreadId,
              nodeId: resumeNodeId,
              startedAt: yield* DateTime.now,
              nextItemOrdinal: 1,
              approvals: [],
            },
          },
          baseAdapter,
        );
        const collected = yield* runtime.events.pipe(
          Stream.takeUntil((event) => event.type === "turn.terminal"),
          Stream.runCollect,
        );
        const eventList = [...collected];
        expect(
          eventList.some(
            (event) => event.type === "message.updated" || event.type === "turn_item.updated",
          ),
        ).toBe(false);
        const terminal = eventList.at(-1);
        expect(terminal?.type).toBe("turn.terminal");
        if (terminal?.type === "turn.terminal") {
          expect(terminal.status).toBe("failed");
          expect(terminal.failure?.message).toContain("restarted");
        }
      }).pipe(Effect.provide(testLayer(cloud)));
    });
    return result;
  });

  it.effect("keeps a resolved approval and sends the stored decision on resume", () => {
    const result = Effect.gen(function* () {
      const now = yield* DateTime.now;
      const requestId = "approval-resolved";
      const request = approvalRequest(requestId, "resolved", now, "accept");
      const item = approvalItem(requestId, requestId, now);
      const outbound = yield* Queue.unbounded<CloudThreads.CloudHostMessage>();
      const sent: Array<unknown> = [];
      const cloud = CloudThreads.CloudThreads.of({
        prepare: () => Effect.die("unused"),
        getSandbox: () => Effect.succeedSome(sandbox),
        getRecord: () => Effect.succeedSome(record),
        forWorkspace: () => Effect.succeedNone,
        exec: () => Effect.die("unused"),
        writeAttachment: () => Effect.succeed(false),
        openHost: () =>
          Effect.succeed({
            sandbox,
            messages: Stream.fromQueue(outbound),
            send: (message) => Effect.sync(() => sent.push(message)),
            close: Effect.void,
            activeRunId: String(resumeRunId),
            latestEventType: "turn_started",
            latestRunId: String(resumeRunId),
            pendingApprovalIds: [requestId],
          }),
        deleteForThread: () => Effect.void,
        getCloudSandboxState: () => Effect.succeedNone,
        subscribeCloudSandbox: () => Stream.empty,
        sleepCloudSandbox: () => Effect.die("unused"),
        wakeCloudSandbox: () => Effect.die("unused"),
        previewCloudSandboxDelete: () => Effect.die("unused"),
        deleteCloudSandbox: () => Effect.die("unused"),
        verifyCloudSandbox: () => Effect.succeedNone,
        markCloudSandboxLost: () => Effect.void,
        recreateCloudSandbox: () => Effect.die("unused"),
        beginTurn: () => Effect.void,
        endTurn: () => Effect.void,
        holdPreview: () => Effect.void,
        releasePreview: () => Effect.void,
        sweepIdle: Effect.succeed(0),
      });
      return yield* Effect.gen(function* () {
        const runtimeService = yield* CloudThreadRuntime.CloudThreadRuntime;
        const runtime = yield* runtimeService.openSession(
          {
            ...input,
            resumeTurn: {
              runId: resumeRunId,
              runOrdinal: 1,
              providerTurnId: resumeProviderTurnId,
              providerTurnOrdinal: 1,
              providerThreadId: resumeProviderThreadId,
              nodeId: resumeNodeId,
              startedAt: now,
              nextItemOrdinal: 8,
              approvals: [{ request, item }],
            },
          },
          baseAdapter,
        );
        expect(
          sent.some(
            (value) =>
              (value as { type?: string; id?: string; decision?: string }).type ===
                "approval_response" &&
              (value as { id?: string }).id === requestId &&
              (value as { decision?: string }).decision === "accept",
          ),
        ).toBe(true);
        yield* Queue.offer(outbound, {
          type: "approval_request",
          runId: String(resumeRunId),
          id: requestId,
          kind: "command",
          title: "Run command",
          prompt: "pwd",
        });
        yield* Queue.offer(outbound, {
          type: "turn_end",
          runId: String(resumeRunId),
          status: "completed",
        });
        const collected = yield* runtime.events.pipe(
          Stream.takeUntil((event) => event.type === "turn.terminal"),
          Stream.runCollect,
        );
        expect(
          [...collected].some(
            (event) =>
              event.type === "runtime_request.updated" &&
              event.runtimeRequest.id === requestId &&
              event.runtimeRequest.status === "pending",
          ),
        ).toBe(false);
      }).pipe(Effect.provide(testLayer(cloud)));
    });
    return result;
  });

  it.effect("cancels a persisted pending approval the host already moved past", () => {
    const result = Effect.gen(function* () {
      const now = yield* DateTime.now;
      const requestId = "approval-orphaned";
      const request = approvalRequest(requestId, "pending", now);
      const item = approvalItem(requestId, requestId, now);
      const cloud = CloudThreads.CloudThreads.of({
        prepare: () => Effect.die("unused"),
        getSandbox: () => Effect.succeedSome(sandbox),
        getRecord: () => Effect.succeedSome(record),
        forWorkspace: () => Effect.succeedNone,
        exec: () => Effect.die("unused"),
        writeAttachment: () => Effect.succeed(false),
        openHost: () =>
          Effect.succeed({
            sandbox,
            messages: Stream.empty,
            send: () => Effect.void,
            close: Effect.void,
            activeRunId: String(resumeRunId),
            latestEventType: "turn_started",
            latestRunId: String(resumeRunId),
            pendingApprovalIds: [],
          }),
        deleteForThread: () => Effect.void,
        getCloudSandboxState: () => Effect.succeedNone,
        subscribeCloudSandbox: () => Stream.empty,
        sleepCloudSandbox: () => Effect.die("unused"),
        wakeCloudSandbox: () => Effect.die("unused"),
        previewCloudSandboxDelete: () => Effect.die("unused"),
        deleteCloudSandbox: () => Effect.die("unused"),
        verifyCloudSandbox: () => Effect.succeedNone,
        markCloudSandboxLost: () => Effect.void,
        recreateCloudSandbox: () => Effect.die("unused"),
        beginTurn: () => Effect.void,
        endTurn: () => Effect.void,
        holdPreview: () => Effect.void,
        releasePreview: () => Effect.void,
        sweepIdle: Effect.succeed(0),
      });
      return yield* Effect.gen(function* () {
        const runtimeService = yield* CloudThreadRuntime.CloudThreadRuntime;
        const runtime = yield* runtimeService.openSession(
          {
            ...input,
            resumeTurn: {
              runId: resumeRunId,
              runOrdinal: 1,
              providerTurnId: resumeProviderTurnId,
              providerTurnOrdinal: 1,
              providerThreadId: resumeProviderThreadId,
              nodeId: resumeNodeId,
              startedAt: now,
              nextItemOrdinal: 8,
              approvals: [{ request, item }],
            },
          },
          baseAdapter,
        );
        const collected = yield* runtime.events.pipe(Stream.take(2), Stream.runCollect);
        const events = [...collected];
        expect(
          events.some(
            (event) =>
              event.type === "runtime_request.updated" &&
              event.runtimeRequest.id === requestId &&
              event.runtimeRequest.status === "cancelled",
          ),
        ).toBe(true);
        expect(
          events.some(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.type === "approval_request" &&
              event.turnItem.requestId === requestId &&
              event.turnItem.status === "cancelled",
          ),
        ).toBe(true);
      }).pipe(Effect.provide(testLayer(cloud)));
    });
    return result;
  });

  it.effect("fails a rollback when the host cannot persist the reset", () => {
    const result = Effect.gen(function* () {
      const outbound = yield* Queue.unbounded<CloudThreads.CloudHostMessage>();
      const turnEnded = yield* Ref.make(false);
      const resetBeforeTurnEnd = yield* Ref.make(false);
      const cloud = CloudThreads.CloudThreads.of({
        prepare: () => Effect.die("unused"),
        getSandbox: () => Effect.succeedSome(sandbox),
        getRecord: () => Effect.succeedSome(record),
        forWorkspace: () => Effect.succeedNone,
        exec: () => Effect.die("unused"),
        writeAttachment: () => Effect.succeed(false),
        openHost: () =>
          Effect.succeed({
            sandbox,
            messages: Stream.fromQueue(outbound),
            send: (message) =>
              Effect.suspend(() => {
                const type =
                  typeof message === "object" && message !== null && "type" in message
                    ? (message as { readonly type?: unknown }).type
                    : undefined;
                if (type === "turn") {
                  Queue.offerUnsafe(outbound, { type: "turn_started", runId: "run-1" });
                  return Effect.void;
                }
                if (type === "interrupt") {
                  return Effect.void;
                }
                if (type === "reset_history") {
                  return Ref.get(turnEnded).pipe(
                    Effect.flatMap((ended) =>
                      Ref.set(resetBeforeTurnEnd, !ended).pipe(
                        Effect.andThen(
                          Effect.sync(() => {
                            Queue.offerUnsafe(outbound, {
                              type: "history_reset",
                              ok: false,
                              error: "Could not persist the reset conversation history.",
                            });
                          }),
                        ),
                      ),
                    ),
                  );
                }
                return Effect.void;
              }),
            close: Effect.void,
            activeRunId: null,
            latestEventType: null,
            latestRunId: null,
            pendingApprovalIds: [],
          }),
        deleteForThread: () => Effect.void,
        getCloudSandboxState: () => Effect.succeedNone,
        verifyCloudSandbox: () => Effect.succeedNone,
        markCloudSandboxLost: () => Effect.void,
        recreateCloudSandbox: () => Effect.die("unused"),
        subscribeCloudSandbox: () => Stream.empty,
        sleepCloudSandbox: () => Effect.die("unused"),
        wakeCloudSandbox: () => Effect.die("unused"),
        previewCloudSandboxDelete: () => Effect.die("unused"),
        deleteCloudSandbox: () => Effect.die("unused"),
        beginTurn: () => Effect.void,
        endTurn: () => Effect.void,
        holdPreview: () => Effect.void,
        releasePreview: () => Effect.void,
        sweepIdle: Effect.succeed(0),
      });
      return yield* Effect.gen(function* () {
        const runtimeService = yield* CloudThreadRuntime.CloudThreadRuntime;
        const runtime = yield* runtimeService.openSession(input, baseAdapter);
        const providerThread = yield* runtime.ensureThread({
          threadId,
          modelSelection,
          runtimePolicy: input.runtimePolicy,
        });
        yield* runtime.startTurn({
          appThread: {} as never,
          threadId,
          runId: "run-1" as never,
          runOrdinal: 1,
          providerTurnOrdinal: 1,
          attemptId: "attempt-1" as never,
          rootNodeId: "node-1" as never,
          providerThread,
          message: {
            messageId: "message-1" as never,
            text: "hello",
            attachments: [],
            createdBy: "user",
            creationSource: "web",
          },
          modelSelection,
          runtimePolicy: input.runtimePolicy,
        });
        const pending = yield* Effect.forkChild(
          Effect.flip(
            runtime.rollbackThread({
              providerThread,
              target: {
                type: "thread_start",
                checkpointId: "checkpoint-1",
                appRunOrdinal: 0,
              } as never,
              providerThreadTurns: [],
            }),
          ),
        );
        yield* Effect.yieldNow;
        expect(yield* Ref.get(resetBeforeTurnEnd)).toBe(false);
        yield* Ref.set(turnEnded, true);
        Queue.offerUnsafe(outbound, { type: "turn_end", runId: "run-1", status: "cancelled" });
        const failure = yield* Fiber.join(pending);
        expect(failure._tag).toBe("ProviderAdapterRollbackThreadError");
      }).pipe(Effect.provide(testLayer(cloud)));
    });
    return result;
  });
});
