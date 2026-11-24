import { SandboxProviderAccountId, type SandboxTestSandbox, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import * as CloudRepositoryCredential from "./CloudRepositoryCredential.ts";
import * as CloudThreadStore from "./CloudThreadStore.ts";
import * as SandboxAccounts from "./SandboxAccounts.ts";
import * as SandboxLifecycle from "./SandboxLifecycle.ts";
import type { SandboxProviderDriver } from "./SandboxProvider.ts";
import * as SandboxStore from "./SandboxStore.ts";
import { CLOUD_THREAD_HOST_SCRIPT } from "./cloudThreadHostScript.ts";
import { THREAD_HOST_PROTOCOL_VERSION } from "./ThreadHost.ts";

const HOST_SCRIPT_PATH = ".t3/thread-host/thread-host.py";
const THREADS_DIR = ".t3/threads";
const READY_TIMEOUT = "30 seconds";
const CLONE_TIMEOUT_MS = 15 * 60_000;

export class CloudThreadError extends Schema.TaggedError<
  CloudThreadError,
  { readonly brand: unique symbol }
>()("CloudThreadError", {
  operation: Schema.Literals([
    "prepare",
    "clone",
    "start-host",
    "send",
    "exec",
    "delete",
    "resolve",
  ]),
  threadId: Schema.optional(ThreadId),
  detail: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message(): string {
    return this.detail;
  }
}

export interface CloudThreadExecInput {
  readonly threadId: ThreadId;
  readonly command: string;
  readonly cwd?: string;
  readonly envs?: Readonly<Record<string, string>>;
  readonly timeoutMs?: number;
}

export interface CloudThreadExecResult {
  readonly exitCode: number;
  readonly stdout: string;
  readonly stderr: string;
}

export interface CloudHostMessage {
  readonly type: string;
  readonly [key: string]: unknown;
}

export interface CloudHostConnection {
  readonly sandbox: SandboxTestSandbox;
  readonly messages: Stream.Stream<CloudHostMessage>;
  readonly send: (message: unknown) => Effect.Effect<void, CloudThreadError>;
  readonly close: Effect.Effect<void>;
}

export interface CloudThreadPrepareInput {
  readonly threadId: ThreadId;
  readonly accountId: SandboxProviderAccountId;
  readonly cloneUrl: string;
  readonly baseRef: string;
  readonly branch: string;
}

const shellQuote = (value: string) => `'${value.replaceAll("'", `'\\''`)}'`;

/**
 * Git reads this header from the process environment. Keeping it out of the command line and
 * config means neither the sandbox filesystem nor a snapshot can retain it.
 */
export const gitCredentialEnvironment = (credential: {
  readonly username: string;
  readonly token: string;
  readonly url: string;
}): Readonly<Record<string, string>> => ({
  GIT_TERMINAL_PROMPT: "0",
  GIT_CONFIG_COUNT: "1",
  GIT_CONFIG_KEY_0: `http.${credential.url}.extraheader`,
  GIT_CONFIG_VALUE_0: `AUTHORIZATION: basic ${Buffer.from(
    `${credential.username}:${credential.token}`,
    "utf8",
  ).toString("base64")}`,
});

/** `git clone --branch` takes a branch name, not a remote-tracking ref. */
const cloudBranchName = (baseRef: string) => {
  const withoutRefPrefix = baseRef
    .replace(/^refs\/remotes\/[^/]+\//, "")
    .replace(/^refs\/heads\//, "");
  return withoutRefPrefix.startsWith("origin/")
    ? withoutRefPrefix.slice("origin/".length)
    : withoutRefPrefix;
};

const firstLine = (value: string) => value.trim().split("\n")[0] ?? value.trim();

const asRecord = (value: unknown): Record<string, unknown> =>
  typeof value === "object" && value !== null ? (value as Record<string, unknown>) : {};

const parseLine = (line: string): CloudHostMessage | null => {
  try {
    const parsed = JSON.parse(line) as unknown;
    const record = asRecord(parsed);
    const type = record["type"];
    return typeof type === "string" ? (parsed as CloudHostMessage) : null;
  } catch {
    return null;
  }
};

export class CloudThreads extends Context.Service<
  CloudThreads,
  {
    readonly prepare: (
      input: CloudThreadPrepareInput,
    ) => Effect.Effect<SandboxTestSandbox, CloudThreadError>;
    readonly getSandbox: (threadId: ThreadId) => Effect.Effect<Option.Option<SandboxTestSandbox>>;
    readonly getRecord: (
      threadId: ThreadId,
    ) => Effect.Effect<Option.Option<CloudThreadStore.CloudThreadRecord>>;
    readonly forWorkspace: (
      workspacePath: string,
    ) => Effect.Effect<Option.Option<CloudThreadStore.CloudThreadRecord>>;
    readonly exec: (
      input: CloudThreadExecInput,
    ) => Effect.Effect<CloudThreadExecResult, CloudThreadError>;
    readonly writeAttachment: (
      threadId: ThreadId,
      path: string,
      data: Uint8Array,
    ) => Effect.Effect<boolean>;
    readonly openHost: (threadId: ThreadId) => Effect.Effect<CloudHostConnection, CloudThreadError>;
    readonly deleteForThread: (threadId: ThreadId) => Effect.Effect<void, CloudThreadError>;
  }
>()("t3/sandbox/CloudThreads") {}

const make = Effect.gen(function* () {
  const accounts = yield* SandboxAccounts.SandboxAccounts;
  const credentials = yield* CloudRepositoryCredential.CloudRepositoryCredentialResolver;
  const lifecycle = yield* SandboxLifecycle.SandboxLifecycle;
  const store = yield* SandboxStore.SandboxStore;
  const cloudThreads = yield* CloudThreadStore.CloudThreadStore;

  const getRecord: CloudThreads["Service"]["getRecord"] = Effect.fn("CloudThreads.getRecord")(
    function* (threadId) {
      return yield* cloudThreads.get(threadId).pipe(Effect.orElseSucceed(() => Option.none()));
    },
  );

  const forWorkspace: CloudThreads["Service"]["forWorkspace"] = Effect.fn(
    "CloudThreads.forWorkspace",
  )(function* (workspacePath) {
    const records = yield* cloudThreads.list.pipe(Effect.orElseSucceed(() => []));
    return Option.fromUndefinedOr(records.find((record) => record.workspacePath === workspacePath));
  });

  const getSandbox: CloudThreads["Service"]["getSandbox"] = Effect.fn("CloudThreads.getSandbox")(
    function* (threadId) {
      return yield* store.list.pipe(
        Effect.map((records) =>
          Option.fromUndefinedOr(records.find((record) => record.threadId === threadId)),
        ),
        Effect.orElseSucceed(() => Option.none<SandboxTestSandbox>()),
      );
    },
  );

  const requireSandbox = Effect.fn("CloudThreads.requireSandbox")(function* (threadId: ThreadId) {
    const sandbox = yield* getSandbox(threadId);
    if (Option.isNone(sandbox)) {
      return yield* new CloudThreadError({
        operation: "resolve",
        threadId,
        detail: "This cloud thread's sandbox record is missing.",
      });
    }
    return sandbox.value;
  });

  const resolveSandbox = Effect.fn("CloudThreads.resolveSandbox")(function* (
    sandbox: SandboxTestSandbox,
    operation: CloudThreadError["operation"],
  ) {
    const resolved = yield* accounts.resolve(sandbox.accountId).pipe(
      Effect.mapError(
        (error) =>
          new CloudThreadError({
            operation,
            threadId: sandbox.threadId,
            detail: error.detail,
          }),
      ),
    );
    return resolved;
  });

  const run = Effect.fn("CloudThreads.run")(function* (
    sandbox: SandboxTestSandbox,
    driver: SandboxProviderDriver,
    account: SandboxAccounts.ResolvedSandboxAccount["account"],
    command: string,
    operation: CloudThreadError["operation"],
    cwd?: string,
    envs?: Readonly<Record<string, string>>,
  ) {
    if (sandbox.providerSandboxId === null) {
      return yield* new CloudThreadError({
        operation,
        threadId: sandbox.threadId,
        detail: "The sandbox is still provisioning. Try again in a moment.",
      });
    }
    const result = yield* driver
      .exec({
        account,
        providerSandboxId: sandbox.providerSandboxId,
        command,
        ...(cwd === undefined ? {} : { cwd }),
        ...(envs === undefined ? {} : { envs }),
        timeoutMs: CLONE_TIMEOUT_MS,
      })
      .pipe(
        Effect.mapError(
          (error) =>
            new CloudThreadError({
              operation,
              threadId: sandbox.threadId,
              detail: error.detail,
            }),
        ),
      );
    if (result.exitCode !== 0) {
      return yield* new CloudThreadError({
        operation,
        threadId: sandbox.threadId,
        detail:
          firstLine(result.stderr || result.stdout) ||
          `The command exited with ${result.exitCode}.`,
      });
    }
    return result;
  });

  const exec: CloudThreads["Service"]["exec"] = Effect.fn("CloudThreads.exec")(function* (input) {
    const sandbox = yield* requireSandbox(input.threadId);
    const { account, driver } = yield* resolveSandbox(sandbox, "start-host");
    if (sandbox.providerSandboxId === null) {
      return yield* new CloudThreadError({
        operation: "start-host",
        threadId: input.threadId,
        detail: "The sandbox is still provisioning. Try again in a moment.",
      });
    }
    const result = yield* driver
      .exec({
        account,
        providerSandboxId: sandbox.providerSandboxId,
        command: input.command,
        ...(input.cwd === undefined ? {} : { cwd: input.cwd }),
        ...(input.envs === undefined ? {} : { envs: input.envs }),
        ...(input.timeoutMs === undefined ? {} : { timeoutMs: input.timeoutMs }),
      })
      .pipe(
        Effect.mapError(
          (error) =>
            new CloudThreadError({
              operation: "exec",
              threadId: input.threadId,
              detail: error.detail,
            }),
        ),
      );
    return result;
  });

  const markSandbox = Effect.fn("CloudThreads.markSandbox")(function* (
    sandbox: SandboxTestSandbox,
    update: Partial<SandboxTestSandbox>,
  ) {
    const record: SandboxTestSandbox = {
      ...sandbox,
      ...update,
      updatedAt: DateTime.formatIso(yield* DateTime.now),
    };
    yield* store.upsert(record).pipe(
      Effect.mapError(
        (error) =>
          new CloudThreadError({
            operation: "prepare",
            threadId: sandbox.threadId,
            detail: error.message,
            cause: error,
          }),
      ),
    );
    return record;
  });

  const deleteForThread: CloudThreads["Service"]["deleteForThread"] = Effect.fn(
    "CloudThreads.deleteForThread",
  )(function* (threadId) {
    const sandbox = yield* getSandbox(threadId);
    if (Option.isNone(sandbox)) return;
    yield* lifecycle.deleteThreadSandbox({ sandboxId: sandbox.value.id }).pipe(
      Effect.mapError(
        (error) =>
          new CloudThreadError({
            operation: "delete",
            threadId,
            detail: error.detail,
          }),
      ),
    );
    yield* cloudThreads.remove(threadId).pipe(Effect.ignore);
  });

  const prepare: CloudThreads["Service"]["prepare"] = Effect.fn("CloudThreads.prepare")(
    function* (input) {
      const existingRecord = yield* cloudThreads
        .get(input.threadId)
        .pipe(Effect.orElseSucceed(() => Option.none()));
      if (Option.isSome(existingRecord) && existingRecord.value.status === "ready") {
        const existingSandbox = yield* getSandbox(input.threadId);
        if (Option.isSome(existingSandbox)) return existingSandbox.value;
      }
      // A crash before the cloud record committed leaves a sandbox with no usable checkout.
      const stale = yield* getSandbox(input.threadId);
      if (Option.isSome(stale)) {
        yield* lifecycle
          .deleteThreadSandbox({ sandboxId: stale.value.id })
          .pipe(Effect.orElseSucceed(() => undefined));
        yield* cloudThreads.remove(input.threadId).pipe(Effect.ignore);
      }
      const sandbox = yield* lifecycle
        .createThreadSandbox({
          threadId: input.threadId,
          accountId: input.accountId,
          timeoutMinutes: 60,
        })
        .pipe(
          Effect.mapError(
            (error) =>
              new CloudThreadError({
                operation: "prepare",
                threadId: input.threadId,
                detail: error.detail,
              }),
          ),
        );
      const cleanup = Effect.fn("CloudThreads.cleanup")(function* (
        detail: string,
        cause?: unknown,
      ) {
        yield* lifecycle.deleteThreadSandbox({ sandboxId: sandbox.id }).pipe(Effect.ignore);
        return yield* new CloudThreadError({
          operation: "prepare",
          threadId: input.threadId,
          detail,
          ...(cause === undefined ? {} : { cause }),
        });
      });
      return yield* Effect.gen(function* () {
        const { account, driver } = yield* resolveSandbox(sandbox, "prepare");
        const writeFile = driver.writeFile;
        if (writeFile === undefined || driver.startProcess === undefined) {
          return yield* cleanup("This sandbox provider cannot run a cloud thread host.");
        }
        const home = yield* run(sandbox, driver, account, 'printf %s "$HOME"', "prepare");
        const homePath = home.stdout.trim();
        if (homePath === "") return yield* cleanup("The sandbox did not report a home directory.");
        const hostPath = `${homePath}/${HOST_SCRIPT_PATH}`;
        const workspacePath = `${homePath}/${THREADS_DIR}/${input.threadId}`;
        yield* run(
          sandbox,
          driver,
          account,
          `mkdir -p ${shellQuote(`${homePath}/.t3/thread-host`)} ${shellQuote(`${homePath}/${THREADS_DIR}`)}`,
          "prepare",
        );
        yield* writeFile({
          account,
          providerSandboxId: sandbox.providerSandboxId!,
          path: hostPath,
          data: CLOUD_THREAD_HOST_SCRIPT,
        }).pipe(
          Effect.mapError(
            (error) =>
              new CloudThreadError({
                operation: "prepare",
                threadId: input.threadId,
                detail: error.detail,
              }),
          ),
          Effect.catchTags({
            CloudThreadError: (error) => cleanup(error.detail, error),
          }),
        );
        const credential = yield* credentials.resolve(input.cloneUrl);
        const gitEnv = Option.isSome(credential)
          ? gitCredentialEnvironment(credential.value)
          : undefined;
        const clone = [
          "set -eu",
          `git clone --branch ${shellQuote(cloudBranchName(input.baseRef))} --single-branch --no-tags ${shellQuote(input.cloneUrl)} ${shellQuote(workspacePath)}`,
        ].join("\n");
        yield* run(sandbox, driver, account, clone, "clone", undefined, gitEnv).pipe(
          Effect.catchTags({
            CloudThreadError: (error) =>
              cleanup(`Could not clone the project's public branch: ${error.detail}`, error),
          }),
        );
        const ready = yield* markSandbox(sandbox, { status: "running" }).pipe(
          Effect.catchTags({
            CloudThreadError: (error) => cleanup(error.detail, error),
          }),
        );
        const now = DateTime.formatIso(yield* DateTime.now);
        yield* cloudThreads
          .upsert({
            threadId: input.threadId,
            sandboxId: ready.id,
            accountId: input.accountId,
            providerKind: ready.providerKind,
            repositoryUrl: input.cloneUrl,
            branch: input.branch,
            baseRef: input.baseRef,
            workspacePath,
            homePath,
            status: "ready",
            createdAt: now,
            updatedAt: now,
          })
          .pipe(
            Effect.catchTags({
              CloudThreadStoreError: (error) =>
                cleanup(`Could not save the cloud thread state: ${error.message}`, error),
            }),
          );
        return ready;
      });
    },
  );

  const writeAttachment: CloudThreads["Service"]["writeAttachment"] = Effect.fn(
    "CloudThreads.writeAttachment",
  )(function* (threadId, path, data) {
    const sandbox = yield* getSandbox(threadId);
    if (Option.isNone(sandbox) || sandbox.value.providerSandboxId === null) return false;
    const resolved = yield* resolveSandbox(sandbox.value, "prepare").pipe(
      Effect.orElseSucceed(() => null),
    );
    if (resolved === null || resolved.driver.writeFile === undefined) return false;
    const written = yield* resolved.driver
      .writeFile({
        account: resolved.account,
        providerSandboxId: sandbox.value.providerSandboxId,
        path,
        data,
      })
      .pipe(Effect.result);
    return written._tag === "Success";
  });

  const openHost: CloudThreads["Service"]["openHost"] = Effect.fn("CloudThreads.openHost")(
    function* (threadId) {
      const sandbox = yield* requireSandbox(threadId);
      const { account, driver } = yield* resolveSandbox(sandbox, "start-host");
      if (driver.startProcess === undefined) {
        return yield* new CloudThreadError({
          operation: "start-host",
          threadId,
          detail: "This sandbox provider cannot run a cloud thread host.",
        });
      }
      if (sandbox.providerSandboxId === null) {
        return yield* new CloudThreadError({
          operation: "start-host",
          threadId,
          detail: "The sandbox is still provisioning. Try again in a moment.",
        });
      }
      const info = yield* driver.get(account, sandbox.providerSandboxId).pipe(
        Effect.mapError(
          (error) =>
            new CloudThreadError({
              operation: "start-host",
              threadId,
              detail: error.detail,
            }),
        ),
      );
      if (info.status === "paused") {
        yield* driver.resume(account, sandbox.providerSandboxId).pipe(
          Effect.mapError(
            (error) =>
              new CloudThreadError({
                operation: "start-host",
                threadId,
                detail: error.detail,
              }),
          ),
        );
      }
      const home = yield* run(sandbox, driver, account, 'printf %s "$HOME"', "start-host");
      const hostPath = `${home.stdout.trim()}/${HOST_SCRIPT_PATH}`;
      const writeFile = driver.writeFile;
      if (writeFile === undefined) {
        return yield* new CloudThreadError({
          operation: "start-host",
          threadId,
          detail: "This sandbox provider cannot upload the thread host.",
        });
      }
      // Re-upload on every start: the agent can edit anything it can read.
      yield* writeFile({
        account,
        providerSandboxId: sandbox.providerSandboxId,
        path: hostPath,
        data: CLOUD_THREAD_HOST_SCRIPT,
      }).pipe(
        Effect.mapError(
          (error) =>
            new CloudThreadError({
              operation: "start-host",
              threadId,
              detail: error.detail,
            }),
        ),
      );
      const messages = yield* Queue.unbounded<CloudHostMessage>();
      let pending = "";
      const onOutput = (data: string) => {
        pending += data;
        const lines = pending.split("\n");
        pending = lines.pop() ?? "";
        for (const line of lines) {
          const message = parseLine(line.trim());
          if (message !== null) void Queue.offerUnsafe(messages, message);
        }
      };
      const process = yield* driver
        .startProcess({
          account,
          providerSandboxId: sandbox.providerSandboxId,
          command: `python3 -u ${shellQuote(hostPath)}`,
          ...(sandbox.threadId === undefined
            ? {}
            : { cwd: `${home.stdout.trim()}/${THREADS_DIR}/${sandbox.threadId}` }),
          onStdout: onOutput,
          onStderr: (data) => {
            for (const line of data.split("\n")) {
              if (line.trim() !== "") {
                Queue.offerUnsafe(messages, { type: "stderr", line: line.trim() });
              }
            }
          },
        })
        .pipe(
          Effect.mapError(
            (error) =>
              new CloudThreadError({
                operation: "start-host",
                threadId,
                detail: error.detail,
              }),
          ),
        );
      yield* Effect.forkDetach(
        process.wait.pipe(
          Effect.orElseSucceed(() => ({ exitCode: 1 })),
          Effect.flatMap(({ exitCode }) =>
            Queue.offer(messages, { type: "process_exit", exitCode }),
          ),
        ),
      );
      const connection: CloudHostConnection = {
        sandbox,
        messages: Stream.fromQueue(messages),
        send: (message) =>
          process.sendStdin(`${JSON.stringify(message)}\n`).pipe(
            Effect.mapError(
              (error) =>
                new CloudThreadError({
                  operation: "send",
                  threadId,
                  detail: error.detail,
                }),
            ),
          ),
        close: process.kill.pipe(Effect.ignore, Effect.andThen(Queue.shutdown(messages))),
      };
      // The host announces itself before accepting a turn.
      const ready = yield* connection.messages.pipe(
        Stream.filter((message) => message.type === "ready" || message.type === "process_exit"),
        Stream.runHead,
        Effect.timeoutOption(READY_TIMEOUT),
      );
      const handshake = Option.isSome(ready) ? ready.value : Option.none<CloudHostMessage>();
      if (Option.isNone(handshake) || handshake.value.type !== "ready") {
        yield* connection.close;
        return yield* new CloudThreadError({
          operation: "start-host",
          threadId,
          detail: "The sandbox thread host did not start.",
        });
      }
      if (handshake.value["protocolVersion"] !== THREAD_HOST_PROTOCOL_VERSION) {
        yield* connection.close;
        return yield* new CloudThreadError({
          operation: "start-host",
          threadId,
          detail: `The sandbox thread host speaks protocol ${String(handshake.value["protocolVersion"] ?? "unknown")}; this server needs ${THREAD_HOST_PROTOCOL_VERSION}.`,
        });
      }
      return connection;
    },
  );

  return CloudThreads.of({
    prepare,
    getSandbox,
    getRecord,
    forWorkspace,
    exec,
    writeAttachment,
    openHost,
    deleteForThread,
  });
});

export const layer = Layer.effect(CloudThreads, make);
