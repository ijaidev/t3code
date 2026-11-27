// @effect-diagnostics nodeBuiltinImport:off globalDate:off -- cloud terminal paths are POSIX sandbox paths and timestamps come from process callbacks outside Effect.
import * as NodePath from "node:path";

import {
  DEFAULT_MAX_TERMINAL_BUFFER_BYTES,
  retainedTerminalHistory,
  TerminalCwdNotFoundError,
  TerminalCwdStatError,
  TerminalNotRunningError,
  TerminalSessionLookupError,
  TerminalWriteError,
  type TerminalAttachInput,
  type TerminalAttachStreamEvent,
  type TerminalClearInput,
  type TerminalCloseInput,
  type TerminalError,
  type TerminalOpenInput,
  type TerminalResizeInput,
  type TerminalMetadataStreamEvent,
  type TerminalSessionSnapshot,
  type TerminalSessionStatus,
  type TerminalSummary,
  type TerminalWriteInput,
  type ThreadId,
} from "@t3tools/contracts";
import { getTerminalLabel } from "@t3tools/shared/terminalLabels";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";

import type { SandboxProviderProcess } from "./SandboxProvider.ts";
import * as CloudThreads from "./CloudThreads.ts";

const DEFAULT_OPEN_COLS = 120;
const DEFAULT_OPEN_ROWS = 30;

const shellQuote = (value: string) => `'${value.replaceAll("'", `'\\''`)}'`;

const isWithin = (root: string, target: string) => {
  const normalized = NodePath.posix.normalize(target);
  return normalized === root || normalized.startsWith(root.endsWith("/") ? root : `${root}/`);
};

const hasTraversal = (value: string) =>
  value
    .replaceAll("\\", "/")
    .split("/")
    .some((segment) => segment === "..");

const isoTimestamp = () => new Date().toISOString();

const terminalLabel = (terminalId: string) => {
  const label = getTerminalLabel(terminalId);
  return label.length <= 128 ? label : label.slice(0, 128);
};

interface CloudTerminalSession {
  readonly threadId: string;
  readonly terminalId: string;
  cwd: string;
  worktreePath: null;
  status: TerminalSessionStatus;
  pid: number | null;
  history: string;
  exitCode: number | null;
  exitSignal: null;
  updatedAt: string;
  sequence: number;
  cols: number;
  rows: number;
  process: SandboxProviderProcess | null;
  listeners: Set<(event: TerminalAttachStreamEvent) => void>;
  stopOnSettle: boolean;
}

const sessionKey = (threadId: string, terminalId: string) => `${threadId}:${terminalId}`;

const summaryFor = (session: CloudTerminalSession): TerminalSummary => ({
  threadId: session.threadId,
  terminalId: session.terminalId,
  cwd: session.cwd,
  worktreePath: null,
  status: session.status,
  pid: session.pid,
  exitCode: session.exitCode,
  exitSignal: null,
  hasRunningSubprocess: false,
  label: terminalLabel(session.terminalId),
  updatedAt: session.updatedAt,
});

const snapshotFor = (
  session: CloudTerminalSession,
  maxHistoryBytes?: number,
): TerminalSessionSnapshot => ({
  threadId: session.threadId,
  terminalId: session.terminalId,
  cwd: session.cwd,
  worktreePath: null,
  status: session.status,
  pid: session.pid,
  history:
    maxHistoryBytes === undefined
      ? session.history
      : retainedTerminalHistory(session.history, maxHistoryBytes),
  exitCode: session.exitCode,
  exitSignal: null,
  label: terminalLabel(session.terminalId),
  updatedAt: session.updatedAt,
  sequence: session.sequence,
  cols: session.cols,
  rows: session.rows,
});

const notifyListeners = (session: CloudTerminalSession, event: TerminalAttachStreamEvent): void => {
  for (const listener of session.listeners) {
    try {
      listener(event);
    } catch {
      // A disconnected viewer must not stop output for the others.
    }
  }
};

type MetadataEvent = TerminalMetadataStreamEvent;

export class CloudTerminal extends Context.Service<
  CloudTerminal,
  {
    readonly hasThread: (threadId: string) => Effect.Effect<boolean>;
    readonly open: (
      input: TerminalOpenInput,
    ) => Effect.Effect<TerminalSessionSnapshot, TerminalError>;
    readonly attachOutputStream: (
      input: TerminalAttachInput,
    ) => Stream.Stream<TerminalAttachStreamEvent, TerminalError>;
    readonly observeOutputStream: (input: {
      readonly threadId: string;
      readonly terminalId: string;
    }) => Stream.Stream<TerminalAttachStreamEvent, TerminalError>;
    readonly write: (input: TerminalWriteInput) => Effect.Effect<void, TerminalError>;
    readonly resize: (input: TerminalResizeInput) => Effect.Effect<void, TerminalError>;
    readonly clear: (input: TerminalClearInput) => Effect.Effect<void, TerminalError>;
    readonly close: (input: TerminalCloseInput) => Effect.Effect<void, TerminalError>;
    readonly listMetadata: Effect.Effect<ReadonlyArray<TerminalSummary>>;
    readonly subscribeMetadata: (
      listener: (event: MetadataEvent) => Effect.Effect<void>,
    ) => Effect.Effect<() => void>;
  }
>()("t3/sandbox/CloudTerminal") {}

const make = Effect.gen(function* () {
  const cloudThreads = yield* CloudThreads.CloudThreads;
  const scope = yield* Effect.scope;
  const sessions = new Map<string, CloudTerminalSession>();
  const metadataListeners = new Set<Queue.Queue<MetadataEvent>>();

  const recordFor = (threadId: string) =>
    cloudThreads.getRecord(threadId as ThreadId).pipe(Effect.orElseSucceed(() => Option.none()));

  const emitMetadata = (event: MetadataEvent) => {
    for (const queue of metadataListeners) {
      Queue.offerUnsafe(queue, event);
    }
  };

  const upsertMetadata = (session: CloudTerminalSession) =>
    emitMetadata({ type: "upsert", terminal: summaryFor(session) });

  const removeMetadata = (session: CloudTerminalSession) =>
    emitMetadata({
      type: "remove",
      threadId: session.threadId,
      terminalId: session.terminalId,
    });

  const appendOutput = (session: CloudTerminalSession, data: string) => {
    if (data === "") return;
    session.history += data;
    if (Buffer.byteLength(session.history) > DEFAULT_MAX_TERMINAL_BUFFER_BYTES) {
      session.history = retainedTerminalHistory(session.history, DEFAULT_MAX_TERMINAL_BUFFER_BYTES);
    }
    session.sequence += 1;
    session.updatedAt = isoTimestamp();
    notifyListeners(session, {
      type: "output",
      threadId: session.threadId,
      terminalId: session.terminalId,
      sequence: session.sequence,
      data,
    });
  };

  const finish = (
    session: CloudTerminalSession,
    exitCode: number | null,
    exitSignal: null = null,
  ) => {
    if (sessions.get(sessionKey(session.threadId, session.terminalId)) !== session) return;
    session.status = "exited";
    session.pid = null;
    session.exitCode = exitCode;
    session.exitSignal = exitSignal;
    session.process = null;
    session.updatedAt = isoTimestamp();
    session.sequence += 1;
    notifyListeners(session, {
      type: "exited",
      threadId: session.threadId,
      terminalId: session.terminalId,
      sequence: session.sequence,
      exitCode,
      exitSignal,
    });
    upsertMetadata(session);
  };

  const validateCwd = (
    record: { readonly workspacePath: string },
    cwd: string,
  ): Effect.Effect<string, TerminalError> => {
    if (hasTraversal(cwd) || !isWithin(record.workspacePath, cwd)) {
      return Effect.fail(new TerminalCwdNotFoundError({ cwd }));
    }
    return Effect.succeed(NodePath.posix.normalize(cwd));
  };

  const startSession = Effect.fn("CloudTerminal.startSession")(function* (
    input: TerminalOpenInput,
    record: { readonly workspacePath: string },
  ) {
    const cwd = yield* validateCwd(record, input.cwd);
    const terminalId = input.terminalId;
    const cols = input.cols ?? DEFAULT_OPEN_COLS;
    const rows = input.rows ?? DEFAULT_OPEN_ROWS;
    const session: CloudTerminalSession = {
      threadId: input.threadId,
      terminalId,
      cwd,
      worktreePath: null,
      status: "starting",
      pid: null,
      history: "",
      exitCode: null,
      exitSignal: null,
      updatedAt: isoTimestamp(),
      sequence: 0,
      cols,
      rows,
      process: null,
      listeners: new Set(),
      stopOnSettle: input.stopOnSettle ?? false,
    };
    sessions.set(sessionKey(session.threadId, session.terminalId), session);
    upsertMetadata(session);

    const startProcess = cloudThreads.startProcess;
    if (startProcess === undefined) {
      sessions.delete(sessionKey(session.threadId, session.terminalId));
      removeMetadata(session);
      return yield* new TerminalCwdStatError({
        cwd,
        cause: new Error("This sandbox cannot start an interactive terminal."),
      });
    }

    const shellCommand = [
      "if command -v script >/dev/null 2>&1; then",
      `  exec script -qefc ${shellQuote(
        `stty cols ${cols} rows ${rows} 2>/dev/null; exec /bin/sh`,
      )} /dev/null`,
      "else",
      "  exec /bin/sh -i",
      "fi",
    ].join("\n");
    const environment = input.env ?? {};
    const process = yield* startProcess({
      threadId: input.threadId as ThreadId,
      command: shellCommand,
      cwd,
      // The controller project path must never reach the sandbox; scripts read this as their root.
      envs: { TERM: "xterm-256color", ...environment, T3CODE_PROJECT_ROOT: cwd },
      onStdout: (data) => appendOutput(session, data),
      onStderr: (data) => appendOutput(session, data),
    }).pipe(
      Effect.mapError(
        (error) =>
          new TerminalCwdStatError({
            cwd,
            cause: error,
          }),
      ),
      Effect.tapError(() =>
        Effect.sync(() => {
          sessions.delete(sessionKey(session.threadId, session.terminalId));
          removeMetadata(session);
        }),
      ),
    );
    session.process = process;
    session.pid = process.pid;
    session.status = "running";
    session.updatedAt = isoTimestamp();
    upsertMetadata(session);
    yield* process.wait.pipe(
      Effect.flatMap(({ exitCode }) =>
        Effect.sync(() => {
          finish(session, exitCode);
        }),
      ),
      Effect.catchCause(() =>
        Effect.sync(() => {
          finish(session, null);
        }),
      ),
      Effect.forkIn(scope),
    );
    return session;
  });

  const hasThread: CloudTerminal["Service"]["hasThread"] = (threadId) =>
    recordFor(threadId).pipe(Effect.map(Option.isSome));

  const open: CloudTerminal["Service"]["open"] = Effect.fn("CloudTerminal.open")(function* (input) {
    const record = yield* recordFor(input.threadId);
    if (Option.isNone(record)) {
      return yield* new TerminalCwdNotFoundError({ cwd: input.cwd });
    }
    const existing = sessions.get(sessionKey(input.threadId, input.terminalId));
    if (existing !== undefined) {
      const nextCwd = yield* validateCwd(record.value, input.cwd);
      if (existing.cwd !== nextCwd || existing.status === "exited") {
        yield* closeSession(existing, false);
        return snapshotFor(yield* startSession(input, record.value));
      }
      existing.stopOnSettle = input.stopOnSettle ?? existing.stopOnSettle;
      if (input.cols !== undefined) existing.cols = input.cols;
      if (input.rows !== undefined) existing.rows = input.rows;
      upsertMetadata(existing);
      return snapshotFor(existing);
    }
    const session = yield* startSession(input, record.value);
    return snapshotFor(session);
  });

  const closeSession = Effect.fn("CloudTerminal.closeSession")(function* (
    session: CloudTerminalSession,
    deleteHistory: boolean,
  ) {
    const key = sessionKey(session.threadId, session.terminalId);
    if (sessions.get(key) !== session) return;
    sessions.delete(key);
    session.listeners.clear();
    session.updatedAt = isoTimestamp();
    const process = session.process;
    session.process = null;
    if (process !== null) {
      yield* process.kill.pipe(Effect.ignore);
    }
    notifyListeners(session, {
      type: "closed",
      threadId: session.threadId,
      terminalId: session.terminalId,
      sequence: session.sequence + 1,
    });
    removeMetadata(session);
    if (deleteHistory) {
      session.history = "";
    }
  });

  const getOrOpen = Effect.fn("CloudTerminal.getOrOpen")(function* (input: TerminalAttachInput) {
    const existing = sessions.get(sessionKey(input.threadId, input.terminalId));
    if (existing !== undefined) {
      if (existing.status === "running" || existing.status === "starting") return existing;
      if (input.resume === true || input.cwd === undefined) return existing;
      yield* closeSession(existing, false);
    } else if (input.resume === true) {
      return yield* new TerminalSessionLookupError(input);
    }
    if (input.cwd === undefined) {
      return yield* new TerminalSessionLookupError(input);
    }
    const record = yield* recordFor(input.threadId);
    if (Option.isNone(record)) {
      return yield* new TerminalCwdNotFoundError({ cwd: input.cwd });
    }
    return yield* startSession(
      {
        threadId: input.threadId,
        terminalId: input.terminalId,
        cwd: input.cwd,
        ...(input.worktreePath !== undefined ? { worktreePath: input.worktreePath } : {}),
        ...(input.cols === undefined ? {} : { cols: input.cols }),
        ...(input.rows === undefined ? {} : { rows: input.rows }),
        ...(input.env === undefined ? {} : { env: input.env }),
        ...(input.providerInstanceId === undefined
          ? {}
          : { providerInstanceId: input.providerInstanceId }),
        ...(input.stopOnSettle === undefined ? {} : { stopOnSettle: input.stopOnSettle }),
      },
      record.value,
    );
  });

  const attachOutputStream: CloudTerminal["Service"]["attachOutputStream"] = (input) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const session = yield* getOrOpen(input);
        const queue = yield* Queue.unbounded<TerminalAttachStreamEvent>();
        const live = (event: TerminalAttachStreamEvent) => {
          Queue.offerUnsafe(queue, event);
        };
        yield* Effect.sync(() => {
          Queue.offerUnsafe(queue, {
            type: "snapshot",
            snapshot: snapshotFor(session, input.maxHistoryBytes),
          });
          session.listeners.add(live);
        });
        return Stream.fromQueue(queue).pipe(
          Stream.ensuring(Effect.sync(() => session.listeners.delete(live))),
        );
      }),
    );

  const observeOutputStream: CloudTerminal["Service"]["observeOutputStream"] = (input) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const session = sessions.get(sessionKey(input.threadId, input.terminalId));
        if (session === undefined) {
          return yield* new TerminalSessionLookupError(input);
        }
        const queue = yield* Queue.unbounded<TerminalAttachStreamEvent>();
        const live = (event: TerminalAttachStreamEvent) => {
          Queue.offerUnsafe(queue, event);
        };
        yield* Effect.sync(() => {
          Queue.offerUnsafe(queue, { type: "snapshot", snapshot: snapshotFor(session) });
          session.listeners.add(live);
        });
        return Stream.fromQueue(queue).pipe(
          Stream.ensuring(Effect.sync(() => session.listeners.delete(live))),
        );
      }),
    );

  const write: CloudTerminal["Service"]["write"] = Effect.fn("CloudTerminal.write")(
    function* (input) {
      const session = sessions.get(sessionKey(input.threadId, input.terminalId));
      if (session === undefined) {
        return yield* new TerminalSessionLookupError(input);
      }
      if (session.status === "exited") return;
      const process = session.process;
      if (process === null || session.status !== "running") {
        return yield* new TerminalNotRunningError(input);
      }
      yield* process.sendStdin(input.data).pipe(
        Effect.mapError(
          (cause) =>
            new TerminalWriteError({
              threadId: input.threadId,
              terminalId: input.terminalId,
              terminalPid: process.pid,
              cause,
            }),
        ),
      );
    },
  );

  const resize: CloudTerminal["Service"]["resize"] = Effect.fn("CloudTerminal.resize")(
    function* (input) {
      const session = sessions.get(sessionKey(input.threadId, input.terminalId));
      if (session === undefined) {
        return yield* new TerminalSessionLookupError(input);
      }
      session.cols = input.cols;
      session.rows = input.rows;
      session.updatedAt = isoTimestamp();
      notifyListeners(session, {
        type: "activity",
        threadId: session.threadId,
        terminalId: session.terminalId,
        sequence: session.sequence,
        hasRunningSubprocess: false,
        label: terminalLabel(session.terminalId),
        cols: input.cols,
        rows: input.rows,
      });
    },
  );

  const clear: CloudTerminal["Service"]["clear"] = Effect.fn("CloudTerminal.clear")(
    function* (input) {
      yield* Effect.sync(() => {
        const session = sessions.get(sessionKey(input.threadId, input.terminalId));
        if (session === undefined) return;
        session.history = "";
        session.sequence += 1;
        session.updatedAt = isoTimestamp();
        notifyListeners(session, {
          type: "cleared",
          threadId: session.threadId,
          terminalId: session.terminalId,
          sequence: session.sequence,
        });
        upsertMetadata(session);
      });
    },
  );

  const close: CloudTerminal["Service"]["close"] = Effect.fn("CloudTerminal.close")(
    function* (input) {
      if (input.terminalId !== undefined) {
        const session = sessions.get(sessionKey(input.threadId, input.terminalId));
        if (session !== undefined) {
          yield* closeSession(session, input.deleteHistory === true);
        }
        return;
      }
      const threadSessions = [...sessions.values()].filter(
        (session) => session.threadId === input.threadId,
      );
      yield* Effect.forEach(threadSessions, (session) => closeSession(session, false), {
        discard: true,
      });
    },
  );

  const listMetadata: CloudTerminal["Service"]["listMetadata"] = Effect.sync(() =>
    [...sessions.values()].map(summaryFor),
  );

  const subscribeMetadata: CloudTerminal["Service"]["subscribeMetadata"] = Effect.fn(
    "CloudTerminal.subscribeMetadata",
  )(function* (listener) {
    const queue = yield* Queue.unbounded<MetadataEvent>();
    metadataListeners.add(queue);
    yield* Stream.fromQueue(queue).pipe(Stream.runForEach(listener), Effect.forkIn(scope));
    return () => {
      metadataListeners.delete(queue);
      Queue.shutdownUnsafe(queue);
    };
  });

  yield* Effect.addFinalizer(() =>
    Effect.forEach([...sessions.values()], (session) => closeSession(session, false), {
      discard: true,
    }),
  );

  return CloudTerminal.of({
    hasThread,
    open,
    attachOutputStream,
    observeOutputStream,
    write,
    resize,
    clear,
    close,
    listMetadata,
    subscribeMetadata,
  });
});

export const layer = Layer.effect(CloudTerminal, make);
