// @effect-diagnostics globalJsonInEffect:off -- The host envelope is schema-encoded; JSON is only the frame transport.
import {
  OrchestrationV2ProviderCapabilities,
  OrchestrationV2ProviderThread,
  type ProviderDriverKind,
  type ProviderInstanceId,
  type ProviderSessionId,
  type ServerSettings,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import type * as Duration from "effect/Duration";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import type * as Scope from "effect/Scope";
import * as Socket from "effect/socket/Socket";

import {
  ProviderAdapterProtocolError,
  ProviderAdapterV2,
  ProviderAdapterV2Error,
  type ProviderAdapterV2Event,
  type ProviderAdapterV2OpenSessionInput,
  type ProviderAdapterV2SessionRuntime,
  type ProviderAdapterV2SubagentOutcome,
  type ProviderAdapterV2ThreadSnapshot,
} from "./ProviderAdapter.ts";
import type { ProviderSelectionTransitionPlan } from "./selectionTransition.ts";
import {
  PROVIDER_ADAPTER_HOST_PROTOCOL_VERSION,
  ProviderAdapterHostClientMessage,
  ProviderAdapterHostHello,
  ProviderAdapterHostParams,
  ProviderAdapterHostServerMessage,
  ProviderAdapterHostSessionDescriptor,
  ProviderAdapterHostSubagentOutcome,
  ProviderAdapterHostThreadSnapshot,
  type ProviderAdapterHostCall,
  type ProviderAdapterHostMethod,
  type ProviderAdapterHostPaths,
} from "./ProviderAdapterHostProtocol.ts";
import { randomUuidV4 } from "./randomUuid.ts";

export class RemoteProviderAdapterError extends Schema.TaggedError<RemoteProviderAdapterError>()(
  "RemoteProviderAdapterError",
  {
    phase: Schema.Literals(["connect", "handshake", "protocol", "closed"]),
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return this.detail;
  }
}

export interface RemoteProviderAdapterConnectOptions {
  readonly url: string;
  readonly token: string;
  readonly driver: ProviderDriverKind;
  readonly instanceId: ProviderInstanceId;
  readonly displayName?: string;
  readonly accentColor?: string;
  /** Driver settings. Secrets belong in the host process environment, not here. */
  readonly config: unknown;
  readonly settings: ServerSettings;
  readonly paths: ProviderAdapterHostPaths;
  readonly headers?: Readonly<Record<string, string>>;
  readonly expectedProtocolVersion?: string;
  readonly expectedHostVersion?: string;
  readonly openTimeout?: Duration.Input;
  readonly handshakeTimeout?: Duration.Input;
}

export interface RemoteProviderAdapterConnection {
  readonly adapter: ProviderAdapterV2["Service"];
  readonly protocolVersion: string;
  readonly hostVersion: string | undefined;
  /** Closes the controller side; the sandbox host then clears its credentials and sessions. */
  readonly close: Effect.Effect<void>;
}

interface RemoteSession {
  readonly providerSessionId: ProviderSessionId;
  readonly hub: PubSub.PubSub<ProviderAdapterV2Event>;
  readonly failure: Ref.Ref<ProviderAdapterV2Error | null>;
}

const HANDSHAKE_TIMEOUT = "10 seconds";

const encodeHello = Schema.encodeSync(
  Schema.fromJsonString(Schema.toCodecJson(ProviderAdapterHostHello)),
);
const encodeClientMessage = Schema.encodeSync(
  Schema.fromJsonString(Schema.toCodecJson(ProviderAdapterHostClientMessage)),
);
const decodeServerMessage = Schema.decodeUnknownOption(
  Schema.fromJsonString(Schema.toCodecJson(ProviderAdapterHostServerMessage)),
);
const isRemoteProviderAdapterError = Schema.is(RemoteProviderAdapterError);
const decodeSessionDescriptor = Schema.decodeUnknownSync(
  Schema.toCodecJson(ProviderAdapterHostSessionDescriptor),
);

const decodeSnapshotValue = Schema.decodeUnknownSync(
  Schema.toCodecJson(ProviderAdapterHostThreadSnapshot),
);
const decodeProviderThread = Schema.decodeUnknownSync(
  Schema.toCodecJson(OrchestrationV2ProviderThread),
);
const decodeProviderCapabilities = Schema.decodeUnknownSync(
  Schema.toCodecJson(OrchestrationV2ProviderCapabilities),
);
const decodeSubagentOutcome = Schema.decodeUnknownSync(
  Schema.toCodecJson(ProviderAdapterHostSubagentOutcome),
);

const decodeSnapshot = (value: unknown): ProviderAdapterV2ThreadSnapshot => {
  const decoded = decodeSnapshotValue(value);
  return {
    providerThread: decoded.providerThread,
    messages: decoded.messages,
    ...(decoded.reasoningMessageIds === undefined
      ? {}
      : { reasoningMessageIds: decoded.reasoningMessageIds }),
    runtimeRequests: decoded.runtimeRequests,
    ...(decoded.providerPayload === undefined ? {} : { providerPayload: decoded.providerPayload }),
  };
};

const protocolError = (driver: ProviderDriverKind, detail: string) =>
  new ProviderAdapterProtocolError({ driver, detail });

const encodeParamByMethod = new Map<ProviderAdapterHostMethod, (value: unknown) => unknown>(
  Object.entries(ProviderAdapterHostParams).map(([method, schema]) => [
    method as ProviderAdapterHostMethod,
    Schema.encodeSync(Schema.toCodecJson(schema)) as (value: unknown) => unknown,
  ]),
);

const encodeParams = (method: string, params: unknown): unknown => {
  const encode = encodeParamByMethod.get(method as ProviderAdapterHostMethod);
  return encode === undefined ? params : encode(params);
};

// oxlint-disable-next-line t3code/prefer-effect-fn -- Socket acquisition errors are mapped at the connection boundary.
export const connect = (
  options: RemoteProviderAdapterConnectOptions,
): Effect.Effect<
  RemoteProviderAdapterConnection,
  RemoteProviderAdapterError,
  Socket.WebSocketConstructor | Scope.Scope
> =>
  Effect.gen(function* () {
    const socket = yield* Socket.makeWebSocket(options.url, {
      ...(options.headers === undefined ? {} : { headers: options.headers }),
      ...(options.openTimeout === undefined ? {} : { openTimeout: options.openTimeout }),
    });
    const reader = yield* socket.reader;
    const writer = yield* socket.writer;
    const decoder = new TextDecoder();
    const frames = yield* Queue.unbounded<string>();
    const outbox = yield* Queue.unbounded<string | Socket.CloseEvent>();
    const pending = new Map<string, Deferred.Deferred<unknown, ProviderAdapterV2Error>>();
    const sessions = new Map<string, RemoteSession>();
    const closed = yield* Ref.make<ProviderAdapterV2Error | null>(null);

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
        const frame = yield* Queue.take(outbox);
        yield* writer.write(frame);
      }),
    ).pipe(Effect.ignoreCause, Effect.forkScoped);

    const send = Effect.fn("RemoteProviderAdapter.send")(function (
      message: ProviderAdapterHostCall,
    ) {
      return Queue.offer(outbox, encodeClientMessage(message));
    });

    const failAll = Effect.fn("RemoteProviderAdapter.failAll")(function* (
      error: ProviderAdapterV2Error,
    ) {
      if ((yield* Ref.get(closed)) !== null) return;
      yield* Ref.set(closed, error);
      for (const deferred of pending.values()) {
        yield* Deferred.fail(deferred, error);
      }
      pending.clear();
      for (const session of sessions.values()) {
        yield* Ref.set(session.failure, error);
        yield* PubSub.shutdown(session.hub);
      }
      sessions.clear();
      yield* Queue.shutdown(outbox);
    });

    const hello = encodeHello({
      type: "hello",
      protocolVersion: PROVIDER_ADAPTER_HOST_PROTOCOL_VERSION,
      token: options.token,
      driver: options.driver,
      instanceId: options.instanceId,
      ...(options.displayName === undefined ? {} : { displayName: options.displayName }),
      ...(options.accentColor === undefined ? {} : { accentColor: options.accentColor }),
      config: options.config,
      settings: options.settings,
      paths: options.paths,
    });
    yield* writer.write(hello);

    const first = Option.flatten(
      yield* Stream.fromQueue(frames).pipe(
        Stream.runHead,
        Effect.timeoutOption(options.handshakeTimeout ?? HANDSHAKE_TIMEOUT),
      ),
    );
    if (Option.isNone(first)) {
      return yield* new RemoteProviderAdapterError({
        phase: "handshake",
        detail: "The sandbox provider host did not answer the handshake.",
      });
    }
    const firstMessage = decodeServerMessage(first.value);
    if (Option.isNone(firstMessage)) {
      return yield* new RemoteProviderAdapterError({
        phase: "handshake",
        detail: "The sandbox provider host sent an unreadable handshake answer.",
      });
    }
    const message = firstMessage.value;
    if (message.type === "refused") {
      return yield* new RemoteProviderAdapterError({
        phase: "handshake",
        detail: message.reason,
      });
    }
    if (message.type !== "ready") {
      return yield* new RemoteProviderAdapterError({
        phase: "handshake",
        detail: "The sandbox provider host answered before it was ready.",
      });
    }
    if (
      message.protocolVersion !==
      (options.expectedProtocolVersion ?? PROVIDER_ADAPTER_HOST_PROTOCOL_VERSION)
    ) {
      return yield* new RemoteProviderAdapterError({
        phase: "handshake",
        detail: `The sandbox provider host speaks protocol ${message.protocolVersion}; this controller needs ${options.expectedProtocolVersion ?? PROVIDER_ADAPTER_HOST_PROTOCOL_VERSION}.`,
      });
    }
    if (message.driver !== options.driver || message.instanceId !== options.instanceId) {
      return yield* new RemoteProviderAdapterError({
        phase: "handshake",
        detail: "The sandbox provider host answered for a different provider instance.",
      });
    }
    if (
      options.expectedHostVersion !== undefined &&
      message.hostVersion !== options.expectedHostVersion
    ) {
      return yield* new RemoteProviderAdapterError({
        phase: "handshake",
        detail: `The sandbox provider host build ${message.hostVersion ?? "unknown"} does not match this controller.`,
      });
    }

    // oxlint-disable-next-line t3code/prefer-effect-fn -- The result decoder is generic; Effect.fn cannot carry A.
    const call = <A>(
      method: string,
      params: unknown,
      sessionId: string | undefined,
      decode: (value: unknown) => A,
    ): Effect.Effect<A, ProviderAdapterV2Error> =>
      Effect.gen(function* () {
        const id = yield* randomUuidV4;
        const deferred = yield* Deferred.make<unknown, ProviderAdapterV2Error>();
        const closedError = yield* Ref.get(closed);
        if (closedError !== null) {
          return yield* closedError;
        }
        pending.set(id, deferred);
        const offered = yield* send({
          type: "call",
          id,
          method,
          ...(sessionId === undefined ? {} : { sessionId }),
          ...(params === undefined ? {} : { params: encodeParams(method, params) }),
        });
        if (!offered) {
          pending.delete(id);
          const closeError =
            (yield* Ref.get(closed)) ??
            protocolError(options.driver, "The sandbox provider host connection is closed.");
          return yield* closeError;
        }
        const result = yield* Deferred.await(deferred).pipe(
          Effect.ensuring(Effect.sync(() => pending.delete(id))),
        );
        return decode(result);
      });

    const eventsFor = (
      session: RemoteSession,
    ): Stream.Stream<ProviderAdapterV2Event, ProviderAdapterV2Error> =>
      Stream.fromPubSub(session.hub).pipe(
        Stream.concat(
          Stream.fromEffect(
            Ref.get(session.failure).pipe(
              Effect.flatMap((failure) => (failure === null ? Effect.void : Effect.fail(failure))),
            ),
          ).pipe(Stream.drain),
        ),
      );

    const makeSession = (
      sessionId: string,
      session: RemoteSession,
      descriptor: ProviderAdapterHostSessionDescriptor,
    ): ProviderAdapterV2SessionRuntime => {
      const has = (feature: (typeof descriptor.features)[number]) =>
        descriptor.features.includes(feature);
      const invoke = (method: string, params: unknown) =>
        call(method, params, sessionId, (value) => value);
      return {
        instanceId: options.instanceId,
        driver: options.driver,
        providerSessionId: session.providerSessionId,
        providerSession: descriptor.providerSession,
        events: eventsFor(session),
        subscribeEvents: Effect.succeed({
          events: eventsFor(session),
          close: Effect.void,
        }),
        ...(descriptor.canResumeAfterRestart === undefined
          ? {}
          : { canResumeAfterRestart: descriptor.canResumeAfterRestart }),
        ...(descriptor.backgroundTasksEndWithProcess === true
          ? { backgroundTasksEndWithProcess: true as const }
          : {}),
        ...(has("hasPendingBackgroundWork")
          ? {
              hasPendingBackgroundWork: call(
                "hasPendingBackgroundWork",
                undefined,
                sessionId,
                (value) => value as boolean,
              ).pipe(Effect.orElseSucceed(() => false)),
            }
          : {}),
        ...(has("hasPendingBackgroundWorkForThread")
          ? {
              hasPendingBackgroundWorkForThread: (
                providerThread: import("@t3tools/contracts").OrchestrationV2ProviderThread,
                threadOptions?: { readonly includeSubagents?: boolean },
              ) =>
                call(
                  "hasPendingBackgroundWorkForThread",
                  { providerThread, options: threadOptions },
                  sessionId,
                  (value) => value as boolean,
                ).pipe(Effect.orElseSucceed(() => false)),
            }
          : {}),
        ensureThread: (input) =>
          call("ensureThread", input, sessionId, (value) => decodeProviderThread(value)),
        resumeThread: (input) =>
          call("resumeThread", input, sessionId, (value) => decodeProviderThread(value)),
        ...(has("injectHistory")
          ? {
              injectHistory: (
                input: Parameters<NonNullable<ProviderAdapterV2SessionRuntime["injectHistory"]>>[0],
              ) => call("injectHistory", input, sessionId, (value) => value as boolean),
            }
          : {}),
        startTurn: (input) => invoke("startTurn", input).pipe(Effect.asVoid),
        ...(has("resumeTurn")
          ? {
              resumeTurn: (
                input: Parameters<NonNullable<ProviderAdapterV2SessionRuntime["resumeTurn"]>>[0],
              ) => invoke("resumeTurn", input).pipe(Effect.asVoid),
            }
          : {}),
        ...(has("compactThread")
          ? {
              compactThread: (
                input: Parameters<NonNullable<ProviderAdapterV2SessionRuntime["compactThread"]>>[0],
              ) => invoke("compactThread", input).pipe(Effect.asVoid),
            }
          : {}),
        steerTurn: (input) => invoke("steerTurn", input).pipe(Effect.asVoid),
        ...(has("applyModelSelection")
          ? {
              applyModelSelection: (
                input: Parameters<
                  NonNullable<ProviderAdapterV2SessionRuntime["applyModelSelection"]>
                >[0],
              ) => invoke("applyModelSelection", input).pipe(Effect.asVoid),
            }
          : {}),
        ...(has("applyThreadTitle")
          ? {
              applyThreadTitle: (
                input: Parameters<
                  NonNullable<ProviderAdapterV2SessionRuntime["applyThreadTitle"]>
                >[0],
              ) => invoke("applyThreadTitle", input).pipe(Effect.asVoid),
            }
          : {}),
        ...(has("stopTask")
          ? {
              stopTask: (
                input: Parameters<NonNullable<ProviderAdapterV2SessionRuntime["stopTask"]>>[0],
              ) => invoke("stopTask", input).pipe(Effect.asVoid),
            }
          : {}),
        ...(has("awaitBackgroundTasks")
          ? {
              awaitBackgroundTasks: (
                input: Parameters<
                  NonNullable<ProviderAdapterV2SessionRuntime["awaitBackgroundTasks"]>
                >[0],
              ) =>
                call(
                  "awaitBackgroundTasks",
                  input,
                  sessionId,
                  (value) => value as { readonly matched: ReadonlyArray<string> },
                ),
            }
          : {}),
        interruptTurn: (input) => invoke("interruptTurn", input).pipe(Effect.asVoid),
        ...(has("stopSubagent")
          ? {
              stopSubagent: (
                input: Parameters<NonNullable<ProviderAdapterV2SessionRuntime["stopSubagent"]>>[0],
              ) =>
                call("stopSubagent", input, sessionId, (value) =>
                  value === undefined
                    ? undefined
                    : (decodeSubagentOutcome(value) as ProviderAdapterV2SubagentOutcome),
                ),
            }
          : {}),
        ...(has("unloadThread")
          ? {
              unloadThread: (
                input: Parameters<NonNullable<ProviderAdapterV2SessionRuntime["unloadThread"]>>[0],
              ) => invoke("unloadThread", input).pipe(Effect.asVoid),
            }
          : {}),
        respondToRuntimeRequest: (input) =>
          invoke("respondToRuntimeRequest", input).pipe(Effect.asVoid),
        readThreadSnapshot: (input) => call("readThreadSnapshot", input, sessionId, decodeSnapshot),
        ...(has("uploadFeedback")
          ? {
              uploadFeedback: (
                input: Parameters<
                  NonNullable<ProviderAdapterV2SessionRuntime["uploadFeedback"]>
                >[0],
              ) =>
                call(
                  "uploadFeedback",
                  input,
                  sessionId,
                  (value) => value as { readonly feedbackId: string },
                ),
            }
          : {}),
        rollbackThread: (input) => call("rollbackThread", input, sessionId, decodeSnapshot),
        forkThread: (input) =>
          call("forkThread", input, sessionId, (value) => decodeProviderThread(value)),
      } satisfies ProviderAdapterV2SessionRuntime;
    };

    const dispatch = (value: string): Effect.Effect<void> => {
      const decoded = decodeServerMessage(value);
      if (Option.isNone(decoded)) return Effect.void;
      const message = decoded.value;
      switch (message.type) {
        case "ready":
        case "refused":
          return Effect.void;
        case "response": {
          const deferred = pending.get(message.id);
          if (deferred === undefined) return Effect.void;
          pending.delete(message.id);
          return message.ok
            ? Deferred.succeed(deferred, message.result)
            : Deferred.fail(deferred, message.error);
        }
        case "event": {
          const session = sessions.get(message.sessionId);
          if (session === undefined) return Effect.void;
          return PubSub.publish(session.hub, message.event).pipe(Effect.asVoid);
        }
        case "session_error": {
          const session = sessions.get(message.sessionId);
          if (session === undefined) return Effect.void;
          sessions.delete(message.sessionId);
          return Ref.set(session.failure, message.error).pipe(
            Effect.andThen(PubSub.shutdown(session.hub)),
          );
        }
        case "session_closed": {
          const session = sessions.get(message.sessionId);
          if (session === undefined) return Effect.void;
          sessions.delete(message.sessionId);
          return PubSub.shutdown(session.hub);
        }
      }
    };

    const adapter: ProviderAdapterV2["Service"] = {
      instanceId: options.instanceId,
      driver: options.driver,
      ...(message.checksVersionAtHandshake === undefined
        ? {}
        : { checksVersionAtHandshake: message.checksVersionAtHandshake }),
      getCapabilities: () =>
        call("getCapabilities", undefined, undefined, (value) => decodeProviderCapabilities(value)),
      planSelectionTransition: (input) =>
        call(
          "planSelectionTransition",
          input,
          undefined,
          (value) => value as ProviderSelectionTransitionPlan,
        ),
      openSession: Effect.fn("RemoteProviderAdapter.openSession")(function* (
        input: ProviderAdapterV2OpenSessionInput,
      ) {
        const promptSuggestions =
          input.promptSuggestions === undefined ? undefined : yield* input.promptSuggestions;
        const { promptSuggestions: _promptSuggestions, ...openInput } = input;
        const sessionId = yield* randomUuidV4;
        const hub = yield* PubSub.unbounded<ProviderAdapterV2Event>();
        const failure = yield* Ref.make<ProviderAdapterV2Error | null>(null);
        const session: RemoteSession = {
          providerSessionId: input.providerSessionId,
          hub,
          failure,
        };
        sessions.set(sessionId, session);
        const opened = yield* call(
          "openSession",
          promptSuggestions === undefined ? openInput : { ...openInput, promptSuggestions },
          sessionId,
          (value) => decodeSessionDescriptor(value),
        ).pipe(
          Effect.onError(() =>
            Effect.sync(() => sessions.delete(sessionId)).pipe(
              Effect.andThen(PubSub.shutdown(hub)),
            ),
          ),
        );
        yield* Effect.addFinalizer(() =>
          call("closeSession", undefined, sessionId, (value) => value).pipe(
            Effect.ignore,
            Effect.timeoutOption("2 seconds"),
          ),
        );
        return makeSession(sessionId, session, opened);
      }),
    };

    yield* Effect.forever(
      Effect.gen(function* () {
        const frame = yield* Queue.take(frames);
        yield* dispatch(frame);
      }),
    ).pipe(
      Effect.ensuring(
        failAll(
          new ProviderAdapterProtocolError({
            driver: options.driver,
            detail: "The sandbox provider host connection closed.",
          }),
        ),
      ),
      Effect.forkScoped,
    );

    return {
      adapter,
      protocolVersion: message.protocolVersion,
      hostVersion: message.hostVersion,
      close: Effect.gen(function* () {
        if ((yield* Ref.get(closed)) === null) {
          yield* writer.write(new Socket.CloseEvent(1000, "controller closed")).pipe(Effect.ignore);
        }
        yield* failAll(
          new ProviderAdapterProtocolError({
            driver: options.driver,
            detail: "The controller closed the sandbox provider host connection.",
          }),
        );
      }).pipe(Effect.ignore),
    };
  }).pipe(
    Effect.catchIf(
      (cause) => !isRemoteProviderAdapterError(cause),
      (cause) =>
        Effect.fail(
          new RemoteProviderAdapterError({
            phase: "connect",
            detail: "The sandbox provider host could not be reached.",
            cause,
          }),
        ),
    ),
  );
