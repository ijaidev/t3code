// @effect-diagnostics globalJsonInEffect:off -- The host envelope is already schema-encoded; JSON is only the frame transport.
// @effect-diagnostics nodeBuiltinImport:off -- Constant-time token comparison needs the Node crypto primitive.
import * as NodeCrypto from "node:crypto";

import { OrchestrationV2ProviderThread, type ProviderDriverKind } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import type * as Duration from "effect/Duration";
import type * as Socket from "effect/socket/Socket";

import {
  ProviderAdapterEventStreamError,
  ProviderAdapterProtocolError,
  type ProviderAdapterV2Error,
  type ProviderAdapterV2OpenSessionInput,
  type ProviderAdapterV2SessionRuntime,
} from "./ProviderAdapter.ts";
import {
  PROVIDER_ADAPTER_HOST_PROTOCOL_VERSION,
  PROVIDER_ADAPTER_HOST_SESSION_FEATURES,
  ProviderAdapterHostClientMessage,
  ProviderAdapterHostHello,
  ProviderAdapterHostParams,
  ProviderAdapterHostServerMessage,
  ProviderAdapterHostSessionDescriptor,
  ProviderAdapterHostSubagentOutcome,
  ProviderAdapterHostThreadSnapshot,
  type ProviderAdapterHostAdapterCreateInput,
  type ProviderAdapterHostCreatedAdapter,
  type ProviderAdapterHostMethod,
  type ProviderAdapterHostRefusalCode,
  type ProviderAdapterHostServerMessage as HostServerMessage,
} from "./ProviderAdapterHostProtocol.ts";

const HANDSHAKE_TIMEOUT = "10 seconds";
const encodeThread = Schema.encodeSync(Schema.toCodecJson(OrchestrationV2ProviderThread));
const encodeDescriptor = Schema.encodeSync(
  Schema.toCodecJson(ProviderAdapterHostSessionDescriptor),
);
const encodeSnapshot = Schema.encodeSync(Schema.toCodecJson(ProviderAdapterHostThreadSnapshot));
const encodeSubagentOutcome = Schema.encodeSync(
  Schema.toCodecJson(ProviderAdapterHostSubagentOutcome),
);

export class ProviderAdapterHostRefusal extends Schema.TaggedError<ProviderAdapterHostRefusal>()(
  "ProviderAdapterHostRefusal",
  {
    code: Schema.Literals([
      "protocol_mismatch",
      "unauthorized",
      "unsupported_driver",
      "invalid_config",
      "adapter_unavailable",
    ]),
    reason: Schema.String,
  },
) {
  override get message(): string {
    return this.reason;
  }
}

export interface ServeProviderAdapterHostOptions<R = never> {
  /** The controller's one-time secret. It never appears in a URL, log, or payload. */
  readonly token: string;
  readonly hostVersion?: string;
  readonly handshakeTimeout?: Duration.Input;
  readonly createAdapter: (
    input: ProviderAdapterHostAdapterCreateInput,
  ) => Effect.Effect<
    ProviderAdapterHostCreatedAdapter,
    ProviderAdapterHostRefusal,
    Scope.Scope | R
  >;
}

const ADAPTER_METHODS = new Set(["getCapabilities", "planSelectionTransition", "openSession"]);
const SESSION_METHODS = new Set<string>([
  "ensureThread",
  "resumeThread",
  "injectHistory",
  "startTurn",
  "resumeTurn",
  "compactThread",
  "steerTurn",
  "applyModelSelection",
  "applyThreadTitle",
  "stopTask",
  "awaitBackgroundTasks",
  "interruptTurn",
  "stopSubagent",
  "unloadThread",
  "respondToRuntimeRequest",
  "readThreadSnapshot",
  "uploadFeedback",
  "rollbackThread",
  "forkThread",
  "hasPendingBackgroundWork",
  "hasPendingBackgroundWorkForThread",
]);

const encodeServerMessage = Schema.encodeSync(
  Schema.fromJsonString(Schema.toCodecJson(ProviderAdapterHostServerMessage)),
);
const decodeHello = Schema.decodeUnknownOption(
  Schema.fromJsonString(Schema.toCodecJson(ProviderAdapterHostHello)),
);
const decodeClientMessage = Schema.decodeUnknownOption(
  Schema.fromJsonString(Schema.toCodecJson(ProviderAdapterHostClientMessage)),
);

const safeEqual = (left: string, right: string): boolean => {
  const expected = Buffer.from(left, "utf8");
  const actual = Buffer.from(right, "utf8");
  return expected.length === actual.length && NodeCrypto.timingSafeEqual(expected, actual);
};

const protocolError = (driver: ProviderDriverKind, detail: string) =>
  new ProviderAdapterProtocolError({ driver, detail });

const decodeParamByMethod = new Map<ProviderAdapterHostMethod, (value: unknown) => unknown>(
  Object.entries(ProviderAdapterHostParams).map(([method, schema]) => [
    method as ProviderAdapterHostMethod,
    Schema.decodeUnknownSync(Schema.toCodecJson(schema)) as (value: unknown) => unknown,
  ]),
);

const decodeParams = (method: string, params: unknown): unknown => {
  const decode = decodeParamByMethod.get(method as ProviderAdapterHostMethod);
  return decode === undefined ? params : decode(params);
};

const protocolErrorFromCause = (driver: ProviderDriverKind, cause: Cause.Cause<unknown>) =>
  new ProviderAdapterProtocolError({
    driver,
    detail: "The sandbox provider adapter failed inside its host.",
    cause: Cause.squash(cause),
  });

interface SessionEntry {
  readonly runtime: ProviderAdapterV2SessionRuntime;
  readonly scope: Scope.Closeable;
}

// oxlint-disable-next-line t3code/prefer-effect-fn -- Generic over the host factory's environment; Effect.fn cannot carry R.
export const serveProviderAdapterHost = <R = never>(
  socket: Socket.Socket,
  options: ServeProviderAdapterHostOptions<R>,
): Effect.Effect<void, never, Scope.Scope | R> =>
  Effect.gen(function* () {
    const connectionScope = yield* Effect.scope;
    const reader = yield* socket.reader;
    const writer = yield* socket.writer;
    const decoder = new TextDecoder();
    const frames = yield* Queue.unbounded<string>();
    const outbox = yield* Queue.unbounded<string>();

    yield* Effect.gen(function* () {
      while (true) {
        const batch = yield* reader.pull;
        yield* Effect.forEach(
          batch,
          (frame) => Queue.offer(frames, typeof frame === "string" ? frame : decoder.decode(frame)),
          { discard: true },
        );
      }
    }).pipe(
      Effect.catchCause(() => Queue.shutdown(frames)),
      Effect.forkScoped,
    );

    yield* Effect.forever(
      Effect.gen(function* () {
        const line = yield* Queue.take(outbox);
        yield* writer.write(line);
      }),
    ).pipe(Effect.ignoreCause, Effect.forkScoped);

    const send = (message: HostServerMessage) =>
      Queue.offer(outbox, encodeServerMessage(message)).pipe(Effect.asVoid);
    const writeNow = (message: HostServerMessage) => writer.write(encodeServerMessage(message));
    const refuse = (code: ProviderAdapterHostRefusalCode, reason: string) =>
      writeNow({ type: "refused", code, reason }).pipe(Effect.ignore);

    const first = Option.flatten(
      yield* Stream.fromQueue(frames).pipe(
        Stream.runHead,
        Effect.timeoutOption(options.handshakeTimeout ?? HANDSHAKE_TIMEOUT),
      ),
    );
    if (Option.isNone(first)) {
      yield* refuse("protocol_mismatch", "The controller did not complete the host handshake.");
      return;
    }
    const hello = decodeHello(first.value);
    if (Option.isNone(hello)) {
      yield* refuse("protocol_mismatch", "The controller sent an unreadable host handshake.");
      return;
    }
    const handshake = hello.value;
    if (!safeEqual(options.token, handshake.token)) {
      yield* refuse("unauthorized", "The sandbox host rejected the controller token.");
      return;
    }
    if (handshake.protocolVersion !== PROVIDER_ADAPTER_HOST_PROTOCOL_VERSION) {
      yield* refuse(
        "protocol_mismatch",
        `The sandbox host speaks protocol ${PROVIDER_ADAPTER_HOST_PROTOCOL_VERSION}; the controller sent ${handshake.protocolVersion}.`,
      );
      return;
    }

    const created = yield* options
      .createAdapter({
        driver: handshake.driver,
        instanceId: handshake.instanceId,
        displayName: handshake.displayName,
        accentColor: handshake.accentColor,
        config: handshake.config,
        settings: handshake.settings,
        paths: handshake.paths,
      })
      .pipe(
        Effect.map((created) => ({ _tag: "Created" as const, created })),
        Effect.catch((refusal) => Effect.succeed({ _tag: "Refused" as const, refusal })),
        Effect.catchCause(() =>
          Effect.succeed({
            _tag: "Refused" as const,
            refusal: new ProviderAdapterHostRefusal({
              code: "adapter_unavailable",
              reason: "The sandbox could not start the requested provider adapter.",
            }),
          }),
        ),
      );
    if (created._tag === "Refused") {
      yield* refuse(created.refusal.code, created.refusal.reason);
      return;
    }

    yield* Effect.addFinalizer(() => created.created.onStop ?? Effect.void);
    yield* writeNow({
      type: "ready",
      protocolVersion: PROVIDER_ADAPTER_HOST_PROTOCOL_VERSION,
      driver: created.created.adapter.driver,
      instanceId: created.created.adapter.instanceId,
      ...(options.hostVersion === undefined ? {} : { hostVersion: options.hostVersion }),
      ...(created.created.adapter.checksVersionAtHandshake === undefined
        ? {}
        : {
            checksVersionAtHandshake: created.created.adapter.checksVersionAtHandshake,
          }),
    });

    const adapter = created.created.adapter;
    const sessions = new Map<string, SessionEntry>();

    const finishSession = Effect.fn("ProviderAdapterHost.finishSession")(function* (
      sessionId: string,
    ) {
      const entry = sessions.get(sessionId);
      if (entry === undefined) return;
      sessions.delete(sessionId);
      yield* send({ type: "session_closed", sessionId });
      yield* Scope.close(entry.scope, Exit.void).pipe(Effect.forkIn(connectionScope));
    });

    const openSession = Effect.fn("ProviderAdapterHost.openSession")(function* (
      sessionId: string,
      params: unknown,
    ): Effect.fn.Return<ProviderAdapterHostSessionDescriptor, ProviderAdapterV2Error> {
      if (sessions.has(sessionId)) {
        return yield* protocolError(adapter.driver, "That host session is already open.");
      }
      const scope = yield* Scope.make();
      const decoded = params as ProviderAdapterV2OpenSessionInput & {
        readonly promptSuggestions?: boolean;
      };
      const runtime = yield* adapter
        .openSession({
          ...decoded,
          ...(decoded.promptSuggestions === undefined
            ? {}
            : { promptSuggestions: Effect.succeed(decoded.promptSuggestions) }),
        })
        .pipe(
          Scope.provide(scope),
          Effect.onExit((exit) =>
            Exit.isSuccess(exit) ? Effect.void : Scope.close(scope, Exit.void),
          ),
        );
      const features = PROVIDER_ADAPTER_HOST_SESSION_FEATURES.filter((feature) =>
        feature === "hasPendingBackgroundWork"
          ? runtime.hasPendingBackgroundWork !== undefined
          : typeof (runtime as unknown as Record<string, unknown>)[feature] === "function",
      );
      const descriptor: ProviderAdapterHostSessionDescriptor = {
        providerSession: runtime.providerSession,
        features: [...features],
        ...(runtime.canResumeAfterRestart === undefined
          ? {}
          : { canResumeAfterRestart: runtime.canResumeAfterRestart }),
        ...(runtime.backgroundTasksEndWithProcess === undefined
          ? {}
          : { backgroundTasksEndWithProcess: runtime.backgroundTasksEndWithProcess }),
      };
      sessions.set(sessionId, { runtime, scope });
      yield* runtime.events.pipe(
        Stream.runForEach((event) => send({ type: "event", sessionId, event })),
        Effect.catchCause((cause) =>
          send({
            type: "session_error",
            sessionId,
            error: new ProviderAdapterEventStreamError({
              driver: adapter.driver,
              providerSessionId: runtime.providerSessionId,
              cause: Cause.squash(cause),
            }),
          }),
        ),
        Effect.ensuring(finishSession(sessionId)),
        Effect.forkIn(scope),
      );
      return descriptor;
    });

    const closeSession = Effect.fn("ProviderAdapterHost.closeSession")(function* (
      sessionId: string,
    ) {
      const entry = sessions.get(sessionId);
      if (entry === undefined) return;
      sessions.delete(sessionId);
      yield* send({ type: "session_closed", sessionId });
      yield* Scope.close(entry.scope, Exit.void);
    });

    const callSession = (
      runtime: ProviderAdapterV2SessionRuntime,
      method: string,
      params: unknown,
    ): Effect.Effect<unknown, ProviderAdapterV2Error> => {
      if (!SESSION_METHODS.has(method)) {
        return Effect.fail(protocolError(adapter.driver, `The host does not know ${method}.`));
      }
      if (method === "hasPendingBackgroundWork") {
        return runtime.hasPendingBackgroundWork === undefined
          ? Effect.fail(
              protocolError(
                adapter.driver,
                `The ${adapter.driver} adapter does not support ${method}.`,
              ),
            )
          : runtime.hasPendingBackgroundWork;
      }
      const handler = (runtime as unknown as Record<string, unknown>)[method];
      if (typeof handler !== "function") {
        return Effect.fail(
          protocolError(
            adapter.driver,
            `The ${adapter.driver} adapter does not support ${method}.`,
          ),
        );
      }
      return Effect.suspend(() =>
        (handler as (input: unknown) => Effect.Effect<unknown, ProviderAdapterV2Error>).call(
          runtime,
          params,
        ),
      );
    };

    const encodeResult = (method: string, result: unknown): unknown => {
      switch (method) {
        case "openSession":
          return encodeDescriptor(result as ProviderAdapterHostSessionDescriptor);
        case "ensureThread":
        case "resumeThread":
        case "forkThread":
          return encodeThread(result as OrchestrationV2ProviderThread);
        case "readThreadSnapshot":
        case "rollbackThread":
          return encodeSnapshot(result as ProviderAdapterHostThreadSnapshot);
        case "stopSubagent":
          return result === undefined
            ? undefined
            : encodeSubagentOutcome(result as ProviderAdapterHostSubagentOutcome);
        default:
          return result;
      }
    };

    const dispatch = (
      method: string,
      sessionId: string | undefined,
      params: unknown,
    ): Effect.Effect<unknown, ProviderAdapterV2Error> => {
      const decodedParams = decodeParams(method, params);
      if (method === "getCapabilities") return adapter.getCapabilities();
      if (method === "planSelectionTransition") {
        return adapter.planSelectionTransition(
          decodedParams as import("./selectionTransition.ts").ProviderSelectionTransitionInput,
        );
      }
      if (method === "openSession") {
        if (sessionId === undefined) {
          return Effect.fail(protocolError(adapter.driver, "A host session needs an id."));
        }
        return openSession(sessionId, decodedParams);
      }
      if (sessionId !== undefined && method === "closeSession") return closeSession(sessionId);
      if (!ADAPTER_METHODS.has(method) && sessionId === undefined) {
        return Effect.fail(protocolError(adapter.driver, `The host does not know ${method}.`));
      }
      if (sessionId === undefined) {
        return Effect.fail(protocolError(adapter.driver, `${method} needs an open host session.`));
      }
      const entry = sessions.get(sessionId);
      if (entry === undefined) {
        return Effect.fail(protocolError(adapter.driver, "That host session is not open."));
      }
      return callSession(entry.runtime, method, decodedParams);
    };

    return yield* Effect.forever(
      Effect.gen(function* () {
        const frame = yield* Queue.take(frames);
        const message = decodeClientMessage(frame);
        if (Option.isNone(message)) return;
        const call = message.value;
        yield* Effect.gen(function* () {
          const result = yield* dispatch(call.method, call.sessionId, call.params);
          yield* send({
            type: "response",
            id: call.id,
            ok: true,
            result: encodeResult(call.method, result),
          });
        }).pipe(
          Effect.catch((error) => send({ type: "response", id: call.id, ok: false, error })),
          Effect.catchCause((cause) =>
            send({
              type: "response",
              id: call.id,
              ok: false,
              error: protocolErrorFromCause(adapter.driver, cause),
            }),
          ),
          Effect.forkScoped,
        );
      }),
    ).pipe(
      Effect.ensuring(
        Effect.suspend(() =>
          Effect.forEach([...sessions.keys()], (sessionId) => closeSession(sessionId), {
            discard: true,
          }),
        ),
      ),
    );
  }).pipe(Effect.ignoreCause);
