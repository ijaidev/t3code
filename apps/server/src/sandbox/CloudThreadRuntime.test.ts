import {
  ModelSelection,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ThreadId,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
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
    snapshot: Effect.succeed([]),
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
        writeAttachment: () => Effect.succeed(false),
        openHost: () =>
          Effect.succeed({
            sandbox,
            messages: Stream.fromQueue(outbound),
            send: (message) => Effect.sync(() => sent.push(message)),
            close: Effect.void,
          }),
        deleteForThread: () => Effect.void,
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
        writeAttachment: () => Effect.succeed(false),
        openHost: () =>
          Effect.succeed({
            sandbox,
            messages: Stream.fromQueue(outbound),
            send: () => Effect.void,
            close: Effect.void,
          }),
        deleteForThread: () => Effect.void,
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
            snapshot: Effect.succeed([]),
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
        writeAttachment: () => Effect.succeed(false),
        openHost: () =>
          Effect.succeed({
            sandbox,
            messages: Stream.fromQueue(outbound),
            send: (message) => Effect.sync(() => sent.push(message)),
            close: Effect.void,
          }),
        deleteForThread: () => Effect.void,
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
});
