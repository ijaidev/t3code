import {
  type CloudSandboxDeleteRisk,
  type CloudSandboxState,
  type CloudSandboxStatus,
  type CloudSandboxSleepDisabledReason,
  type SandboxProviderAccountId,
  type SandboxTestSandbox,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";

import * as ServerSettings from "../serverSettings.ts";
import * as CloudRepositoryCredential from "./CloudRepositoryCredential.ts";
import * as CloudThreadPorts from "./CloudThreadPorts.ts";
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
const DEFAULT_IDLE_MINUTES = 10;
// Idle sleep, not the provider's lifetime, should stop a cloud sandbox.
const CLOUD_SANDBOX_LIFETIME_MINUTES = 24 * 60;

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
    "pause",
    "resume",
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
export const credentialTransportAllowed = (url: string): boolean => {
  try {
    return new URL(url).protocol === "https:";
  } catch {
    return false;
  }
};

export const gitCredentialEnvironment = (credential: {
  readonly username: string;
  readonly token: string;
  readonly url: string;
}): Readonly<Record<string, string>> => ({
  GIT_TERMINAL_PROMPT: "0",
  GIT_CONFIG_COUNT: "3",
  GIT_CONFIG_KEY_0: `http.${credential.url}.extraheader`,
  GIT_CONFIG_VALUE_0: `AUTHORIZATION: basic ${Buffer.from(
    `${credential.username}:${credential.token}`,
    "utf8",
  ).toString("base64")}`,
  // Never follow a redirect that could downgrade the credentialed request, and never run a
  // hook or helper the workspace can replace while the credential is in the environment.
  GIT_CONFIG_KEY_1: "http.followRedirects",
  GIT_CONFIG_VALUE_1: "false",
  GIT_CONFIG_KEY_2: "core.hooksPath",
  GIT_CONFIG_VALUE_2: "/dev/null",
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
    readonly getCloudSandboxState: (
      threadId: ThreadId,
    ) => Effect.Effect<Option.Option<CloudSandboxState>>;
    readonly subscribeCloudSandbox: (
      threadId: ThreadId,
    ) => Stream.Stream<CloudSandboxState, never, never>;
    readonly sleepCloudSandbox: (
      threadId: ThreadId,
    ) => Effect.Effect<CloudSandboxState, CloudThreadError>;
    readonly wakeCloudSandbox: (
      threadId: ThreadId,
    ) => Effect.Effect<CloudSandboxState, CloudThreadError>;
    readonly previewCloudSandboxDelete: (
      threadId: ThreadId,
    ) => Effect.Effect<
      { readonly state: CloudSandboxState; readonly risk: CloudSandboxDeleteRisk },
      CloudThreadError
    >;
    readonly deleteCloudSandbox: (input: {
      readonly threadId: ThreadId;
      readonly acknowledgedUnpushedChanges: boolean;
    }) => Effect.Effect<CloudSandboxState, CloudThreadError>;
    readonly beginTurn: (threadId: ThreadId) => Effect.Effect<void>;
    readonly endTurn: (threadId: ThreadId) => Effect.Effect<void>;
    readonly holdPreview: (threadId: ThreadId, tabId: string) => Effect.Effect<void>;
    readonly releasePreview: (threadId: ThreadId, tabId: string) => Effect.Effect<void>;
    /** Pauses every cloud sandbox idle past the configured timeout; returns how many slept. */
    readonly sweepIdle: Effect.Effect<number>;
  }
>()("t3/sandbox/CloudThreads") {}

const make = Effect.gen(function* () {
  const accounts = yield* SandboxAccounts.SandboxAccounts;
  const credentials = yield* CloudRepositoryCredential.CloudRepositoryCredentialResolver;
  const lifecycle = yield* SandboxLifecycle.SandboxLifecycle;
  const store = yield* SandboxStore.SandboxStore;
  const cloudThreads = yield* CloudThreadStore.CloudThreadStore;
  const cloudThreadPorts = yield* Effect.serviceOption(CloudThreadPorts.CloudThreadPorts);

  const clearPorts = (threadId: ThreadId) =>
    Option.match(cloudThreadPorts, {
      onNone: () => Effect.void,
      onSome: (ports) => ports.clear(threadId),
    });

  const settingsService = yield* ServerSettings.ServerSettingsService;

  const lifecycleLock = yield* Semaphore.make(1);
  const notifications = yield* PubSub.unbounded<ThreadId>();
  const transitions = yield* Ref.make<ReadonlyMap<ThreadId, "waking">>(new Map());
  const activity = yield* Ref.make<
    ReadonlyMap<ThreadId, { readonly turns: number; readonly previews: ReadonlySet<string> }>
  >(new Map());

  const activityFor = (threadId: ThreadId) =>
    Ref.get(activity).pipe(
      Effect.map((current) => current.get(threadId) ?? { turns: 0, previews: new Set<string>() }),
    );

  const updateActivity = (
    threadId: ThreadId,
    update: (current: { readonly turns: number; readonly previews: ReadonlySet<string> }) => {
      readonly turns: number;
      readonly previews: ReadonlySet<string>;
    },
  ) =>
    Ref.update(activity, (current) => {
      const next = update(current.get(threadId) ?? { turns: 0, previews: new Set<string>() });
      const map = new Map(current);
      if (next.turns === 0 && next.previews.size === 0) map.delete(threadId);
      else map.set(threadId, next);
      return map;
    });

  const notify = (threadId: ThreadId) =>
    PubSub.publish(notifications, threadId).pipe(Effect.asVoid);

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
      const record = yield* getRecord(threadId);
      return yield* new CloudThreadError({
        operation: "resolve",
        threadId,
        detail:
          Option.isSome(record) && record.value.status === "lost"
            ? "This cloud thread's sandbox was deleted. The transcript is still here."
            : "This cloud thread's sandbox record is missing.",
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

  const touchSandbox = Effect.fnUntraced(function* (sandbox: SandboxTestSandbox) {
    return yield* markSandbox(sandbox, { lastActivityAt: DateTime.formatIso(yield* DateTime.now) });
  });

  const getCloudSandboxState: CloudThreads["Service"]["getCloudSandboxState"] = Effect.fn(
    "CloudThreads.getCloudSandboxState",
  )(function* (threadId) {
    const [recordOption, sandboxOption, currentActivity, waking] = yield* Effect.all([
      getRecord(threadId),
      getSandbox(threadId),
      activityFor(threadId),
      Ref.get(transitions),
    ]);
    const record = Option.getOrNull(recordOption);
    const sandbox = Option.getOrNull(sandboxOption);
    if (record === null && sandbox === null) return Option.none<CloudSandboxState>();

    const accountId = sandbox?.accountId ?? record?.accountId;
    const providerKind = sandbox?.providerKind ?? record?.providerKind;
    const resolved =
      accountId === undefined
        ? null
        : yield* accounts.resolve(accountId).pipe(Effect.orElseSucceed(() => null));
    const now = DateTime.formatIso(yield* DateTime.now);
    const createdAt = sandbox?.createdAt ?? record?.createdAt ?? now;
    const lastActiveAt = sandbox?.lastActivityAt ?? sandbox?.updatedAt ?? record?.updatedAt ?? now;

    let status: CloudSandboxStatus;
    if (record?.status === "lost") status = "lost";
    else if (waking.get(threadId) === "waking") status = "waking";
    else if (sandbox === null) {
      status =
        record?.status === "provisioning"
          ? "provisioning"
          : record?.status === "failed"
            ? "failed"
            : "lost";
    } else if (sandbox.status === "provisioning") status = "provisioning";
    else if (sandbox.status === "paused") status = "sleeping";
    else if (sandbox.status === "deleting") status = "deleting";
    else if (record?.status === "failed") status = "failed";
    else status = "running";

    const pauseResume = resolved?.driver.capabilities.pauseResume === true;
    const providerDelete = resolved?.driver.capabilities.delete === true;
    let sleepDisabledReason: CloudSandboxSleepDisabledReason | undefined;
    if (status === "running" && sandbox !== null) {
      if (resolved === null) sleepDisabledReason = "provider-unavailable";
      else if (!pauseResume) sleepDisabledReason = "provider-no-sleep";
      else if (currentActivity.turns > 0) sleepDisabledReason = "active-turn";
      else if (currentActivity.previews.size > 0) sleepDisabledReason = "open-preview";
    } else if (status === "provisioning" || status === "waking") {
      sleepDisabledReason = "not-running";
    }
    const canSleep = status === "running" && sleepDisabledReason === undefined;
    const canWake = status === "sleeping" && pauseResume;
    const canDelete =
      sandbox !== null &&
      providerDelete &&
      status !== "provisioning" &&
      status !== "deleting" &&
      status !== "lost";

    return Option.some<CloudSandboxState>({
      threadId,
      accountId: accountId!,
      providerKind: providerKind!,
      providerLabel: resolved?.driver.label ?? providerKind!,
      status,
      sandboxId: sandbox?.id ?? null,
      template: sandbox?.template ?? record?.template ?? "base",
      size: sandbox?.size ?? record?.size ?? null,
      region: sandbox?.region ?? record?.region ?? null,
      createdAt,
      lastActiveAt,
      sleepingSince: sandbox?.sleepingSince ?? (status === "sleeping" ? lastActiveAt : null),
      canSleep,
      canWake,
      canDelete,
      ...(sleepDisabledReason === undefined ? {} : { sleepDisabledReason }),
      ...(sandbox?.lastError !== undefined
        ? { lastError: sandbox.lastError }
        : record?.lastError === undefined
          ? {}
          : { lastError: record.lastError }),
    });
  });

  const requireCloudSandboxState = Effect.fn("CloudThreads.requireCloudSandboxState")(function* (
    threadId: ThreadId,
  ) {
    const state = yield* getCloudSandboxState(threadId);
    if (Option.isNone(state)) {
      return yield* new CloudThreadError({
        operation: "resolve",
        threadId,
        detail: "This thread has no cloud sandbox.",
      });
    }
    return state.value;
  });

  const readUnpushedRisk = Effect.fn("CloudThreads.readUnpushedRisk")(function* (
    sandbox: SandboxTestSandbox,
    record: CloudThreadStore.CloudThreadRecord,
  ): Effect.fn.Return<CloudSandboxDeleteRisk, CloudThreadError> {
    const { account, driver } = yield* resolveSandbox(sandbox, "delete");
    const status = yield* run(
      sandbox,
      driver,
      account,
      "git status --porcelain",
      "delete",
      record.workspacePath,
    );
    const ahead = yield* run(
      sandbox,
      driver,
      account,
      `git rev-list --count HEAD --not --remotes`,
      "delete",
      record.workspacePath,
    );
    const changedFiles = status.stdout
      .split("\n")
      .map((line) => line.trim())
      .filter((line) => line !== "").length;
    const unpushedCommits = Number.parseInt(ahead.stdout.trim(), 10);
    return {
      hasUnpushedChanges: changedFiles > 0 || unpushedCommits > 0,
      changedFiles,
      unpushedCommits: Number.isFinite(unpushedCommits) ? unpushedCommits : 0,
    };
  });

  const sleepCloudSandbox: CloudThreads["Service"]["sleepCloudSandbox"] = Effect.fn(
    "CloudThreads.sleepCloudSandbox",
  )(function* (threadId) {
    return yield* lifecycleLock.withPermits(1)(
      Effect.gen(function* () {
        const state = yield* requireCloudSandboxState(threadId);
        if (state.status === "sleeping") return state;
        if (state.status !== "running") {
          return yield* new CloudThreadError({
            operation: "pause",
            threadId,
            detail: "Only a running cloud sandbox can sleep.",
          });
        }
        const currentActivity = yield* activityFor(threadId);
        if (currentActivity.turns > 0) {
          return yield* new CloudThreadError({
            operation: "pause",
            threadId,
            detail: "The sandbox has an active turn, so it stays awake.",
          });
        }
        if (currentActivity.previews.size > 0) {
          return yield* new CloudThreadError({
            operation: "pause",
            threadId,
            detail: "A preview is open on the sandbox, so it stays awake.",
          });
        }
        const sandbox = yield* requireSandbox(threadId);
        const { driver } = yield* resolveSandbox(sandbox, "pause");
        if (!driver.capabilities.pauseResume) {
          return yield* new CloudThreadError({
            operation: "pause",
            threadId,
            detail: `${driver.label} sandboxes cannot sleep.`,
          });
        }
        const sleeping = yield* lifecycle.pauseTestSandbox({ sandboxId: sandbox.id }).pipe(
          Effect.mapError(
            (error) =>
              new CloudThreadError({
                operation: "pause",
                threadId,
                detail: error.detail,
              }),
          ),
        );
        yield* markSandbox(sleeping.sandbox, {
          sleepingSince: DateTime.formatIso(yield* DateTime.now),
        });
        yield* notify(threadId);
        return yield* requireCloudSandboxState(threadId);
      }),
    );
  });

  const wakeCloudSandboxUnlocked = Effect.fn("CloudThreads.wakeCloudSandboxUnlocked")(function* (
    threadId: ThreadId,
  ) {
    const state = yield* requireCloudSandboxState(threadId);
    if (state.status === "running") {
      const sandbox = yield* getSandbox(threadId);
      if (Option.isSome(sandbox)) yield* touchSandbox(sandbox.value);
      return yield* requireCloudSandboxState(threadId);
    }
    if (state.status !== "sleeping") {
      return yield* new CloudThreadError({
        operation: "resume",
        threadId,
        detail:
          state.status === "lost"
            ? "This cloud thread's workspace was deleted."
            : "The cloud sandbox is not ready to wake.",
      });
    }
    const sandbox = yield* requireSandbox(threadId);
    const { driver } = yield* resolveSandbox(sandbox, "resume");
    if (!driver.capabilities.pauseResume) {
      return yield* new CloudThreadError({
        operation: "resume",
        threadId,
        detail: `${driver.label} sandboxes cannot be woken after sleeping.`,
      });
    }
    yield* Ref.update(transitions, (current) => new Map(current).set(threadId, "waking"));
    yield* notify(threadId);
    const resumed = yield* lifecycle
      .resumeTestSandbox({ sandboxId: sandbox.id })
      .pipe(Effect.result);
    yield* Ref.update(transitions, (current) => {
      const next = new Map(current);
      next.delete(threadId);
      return next;
    });
    if (resumed._tag === "Failure") {
      yield* notify(threadId);
      return yield* new CloudThreadError({
        operation: "resume",
        threadId,
        detail: resumed.failure.detail,
      });
    }
    const { sleepingSince: _sleepingSince, ...awake } = resumed.success.sandbox;
    yield* touchSandbox(awake);
    yield* notify(threadId);
    return yield* requireCloudSandboxState(threadId);
  });

  const wakeCloudSandbox: CloudThreads["Service"]["wakeCloudSandbox"] = (threadId) =>
    lifecycleLock.withPermits(1)(wakeCloudSandboxUnlocked(threadId));

  const previewCloudSandboxDelete: CloudThreads["Service"]["previewCloudSandboxDelete"] = Effect.fn(
    "CloudThreads.previewCloudSandboxDelete",
  )(function* (threadId) {
    return yield* lifecycleLock.withPermits(1)(
      Effect.gen(function* () {
        const sandbox = yield* getSandbox(threadId);
        if (Option.isNone(sandbox)) {
          return {
            state: yield* requireCloudSandboxState(threadId),
            risk: { hasUnpushedChanges: false, changedFiles: 0, unpushedCommits: 0 },
          };
        }
        if (sandbox.value.status === "provisioning") {
          return yield* new CloudThreadError({
            operation: "delete",
            threadId,
            detail: "Wait for the sandbox to finish provisioning before deleting it.",
          });
        }
        if (sandbox.value.status === "paused") {
          yield* wakeCloudSandboxUnlocked(threadId);
        }
        const record = yield* getRecord(threadId);
        if (Option.isNone(record)) {
          return yield* new CloudThreadError({
            operation: "delete",
            threadId,
            detail: "This cloud thread's record is missing, so its changes cannot be checked.",
          });
        }
        const current = yield* requireSandbox(threadId);
        return {
          state: yield* requireCloudSandboxState(threadId),
          risk: yield* readUnpushedRisk(current, record.value),
        };
      }),
    );
  });

  const markCloudLost = Effect.fnUntraced(function* (threadId: ThreadId) {
    const record = yield* getRecord(threadId);
    if (Option.isNone(record) || record.value.status === "lost") return;
    const { lastError: _lastError, ...rest } = record.value;
    yield* cloudThreads
      .upsert({
        ...rest,
        status: "lost",
        updatedAt: DateTime.formatIso(yield* DateTime.now),
      })
      .pipe(Effect.ignore);
  });

  const deleteCloudSandbox: CloudThreads["Service"]["deleteCloudSandbox"] = Effect.fn(
    "CloudThreads.deleteCloudSandbox",
  )(function* (input) {
    return yield* lifecycleLock.withPermits(1)(
      Effect.gen(function* () {
        const sandbox = yield* getSandbox(input.threadId);
        if (Option.isNone(sandbox)) {
          yield* markCloudLost(input.threadId);
          yield* clearPorts(input.threadId);
          yield* notify(input.threadId);
          return yield* requireCloudSandboxState(input.threadId);
        }
        if (sandbox.value.status === "provisioning") {
          return yield* new CloudThreadError({
            operation: "delete",
            threadId: input.threadId,
            detail: "Wait for the sandbox to finish provisioning before deleting it.",
          });
        }
        const record = yield* getRecord(input.threadId);
        if (Option.isNone(record)) {
          return yield* new CloudThreadError({
            operation: "delete",
            threadId: input.threadId,
            detail: "This cloud thread's record is missing, so its sandbox cannot be deleted.",
          });
        }
        const { driver } = yield* resolveSandbox(sandbox.value, "delete");
        if (!driver.capabilities.delete) {
          return yield* new CloudThreadError({
            operation: "delete",
            threadId: input.threadId,
            detail: `${driver.label} sandboxes cannot be deleted from T3 Code.`,
          });
        }
        if (sandbox.value.status === "paused") {
          yield* wakeCloudSandboxUnlocked(input.threadId);
        }
        const current = yield* requireSandbox(input.threadId);
        const risk = yield* readUnpushedRisk(current, record.value);
        if (risk.hasUnpushedChanges && !input.acknowledgedUnpushedChanges) {
          return yield* new CloudThreadError({
            operation: "delete",
            threadId: input.threadId,
            detail: `This sandbox has ${risk.changedFiles} changed file(s) and ${risk.unpushedCommits} unpushed commit(s). Confirm that they can be lost before deleting it.`,
          });
        }
        yield* markSandbox(current, { status: "deleting" });
        yield* notify(input.threadId);
        const deleted = yield* lifecycle
          .deleteThreadSandbox({ sandboxId: current.id })
          .pipe(Effect.result);
        if (deleted._tag === "Failure") {
          yield* markSandbox(current, { status: "running" });
          yield* notify(input.threadId);
          return yield* new CloudThreadError({
            operation: "delete",
            threadId: input.threadId,
            detail: deleted.failure.detail,
          });
        }
        yield* Ref.update(activity, (currentActivity) => {
          const next = new Map(currentActivity);
          next.delete(input.threadId);
          return next;
        });
        yield* markCloudLost(input.threadId);
        yield* clearPorts(input.threadId);
        yield* notify(input.threadId);
        return yield* requireCloudSandboxState(input.threadId);
      }),
    );
  });

  const deleteForThread: CloudThreads["Service"]["deleteForThread"] = Effect.fn(
    "CloudThreads.deleteForThread",
  )(function* (threadId) {
    const sandbox = yield* getSandbox(threadId);
    if (Option.isSome(sandbox)) {
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
    }
    yield* Ref.update(activity, (current) => {
      const next = new Map(current);
      next.delete(threadId);
      return next;
    });
    yield* cloudThreads.remove(threadId).pipe(Effect.ignore);
    yield* clearPorts(threadId);
    yield* notify(threadId);
  });

  const beginTurn: CloudThreads["Service"]["beginTurn"] = Effect.fn("CloudThreads.beginTurn")(
    function* (threadId) {
      yield* updateActivity(threadId, (current) => ({ ...current, turns: current.turns + 1 }));
      const sandbox = yield* getSandbox(threadId);
      if (Option.isSome(sandbox)) yield* touchSandbox(sandbox.value).pipe(Effect.ignore);
      yield* notify(threadId);
    },
  );

  const endTurn: CloudThreads["Service"]["endTurn"] = Effect.fn("CloudThreads.endTurn")(
    function* (threadId) {
      yield* updateActivity(threadId, (current) => ({
        ...current,
        turns: Math.max(0, current.turns - 1),
      }));
      const sandbox = yield* getSandbox(threadId);
      if (Option.isSome(sandbox)) yield* touchSandbox(sandbox.value).pipe(Effect.ignore);
      yield* notify(threadId);
    },
  );

  const holdPreview: CloudThreads["Service"]["holdPreview"] = Effect.fn("CloudThreads.holdPreview")(
    function* (threadId, tabId) {
      let sandbox = yield* getSandbox(threadId);
      if (Option.isNone(sandbox)) return;
      // A preview needs the sandbox running; opening one wakes a sleeping sandbox first.
      if (sandbox.value.status === "paused") {
        yield* wakeCloudSandbox(threadId).pipe(Effect.ignore);
        sandbox = yield* getSandbox(threadId);
        if (Option.isNone(sandbox)) return;
      }
      yield* updateActivity(threadId, (current) => ({
        ...current,
        previews: new Set(current.previews).add(tabId),
      }));
      yield* touchSandbox(sandbox.value).pipe(Effect.ignore);
      yield* notify(threadId);
    },
  );

  const releasePreview: CloudThreads["Service"]["releasePreview"] = Effect.fn(
    "CloudThreads.releasePreview",
  )(function* (threadId, tabId) {
    const sandbox = yield* getSandbox(threadId);
    if (Option.isNone(sandbox)) return;
    yield* updateActivity(threadId, (current) => {
      const previews = new Set(current.previews);
      previews.delete(tabId);
      return { ...current, previews };
    });
    yield* touchSandbox(sandbox.value).pipe(Effect.ignore);
    yield* notify(threadId);
  });

  const presentState = (threadId: ThreadId) =>
    getCloudSandboxState(threadId).pipe(
      Effect.map((state): ReadonlyArray<CloudSandboxState> =>
        Option.isSome(state) ? [state.value] : [],
      ),
    );

  const subscribeCloudSandbox: CloudThreads["Service"]["subscribeCloudSandbox"] = (threadId) =>
    Stream.unwrap(
      PubSub.subscribe(notifications).pipe(
        Effect.map((subscription) =>
          Stream.concat(
            Stream.fromEffect(presentState(threadId)).pipe(Stream.flatMap(Stream.fromIterable)),
            Stream.fromSubscription(subscription).pipe(
              Stream.filter((changed) => changed === threadId),
              Stream.mapEffect(() => presentState(threadId)),
              Stream.flatMap(Stream.fromIterable),
            ),
          ),
        ),
      ),
    );

  const sweepIdle: CloudThreads["Service"]["sweepIdle"] = Effect.suspend(() =>
    Effect.gen(function* () {
      const settings = yield* settingsService.getSettings.pipe(Effect.orElseSucceed(() => null));
      if (settings === null) return 0;
      const idleMs = (settings.cloudSandboxIdleMinutes ?? DEFAULT_IDLE_MINUTES) * 60_000;
      const now = yield* DateTime.now;
      const records = yield* store.list.pipe(Effect.orElseSucceed(() => []));
      let slept = 0;
      for (const sandbox of records) {
        if (sandbox.threadId === undefined || sandbox.status !== "running") continue;
        const lastActive = Date.parse(sandbox.lastActivityAt ?? sandbox.updatedAt);
        if (!Number.isFinite(lastActive) || DateTime.toEpochMillis(now) - lastActive < idleMs) {
          continue;
        }
        const currentActivity = yield* activityFor(sandbox.threadId);
        if (currentActivity.turns > 0 || currentActivity.previews.size > 0) continue;
        const result = yield* sleepCloudSandbox(sandbox.threadId).pipe(Effect.result);
        if (result._tag === "Success") slept += 1;
      }
      return slept;
    }),
  );

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
        yield* clearPorts(input.threadId);
      }
      const sandbox = yield* lifecycle
        .createThreadSandbox({
          threadId: input.threadId,
          accountId: input.accountId,
          timeoutMinutes: CLOUD_SANDBOX_LIFETIME_MINUTES,
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
      yield* notify(input.threadId);
      const cleanup = Effect.fn("CloudThreads.cleanup")(function* (
        detail: string,
        cause?: unknown,
      ) {
        yield* lifecycle.deleteThreadSandbox({ sandboxId: sandbox.id }).pipe(Effect.ignore);
        yield* notify(input.threadId);
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
        // A public clone must not receive a credential at all. Probe first, then resolve only for
        // a remote that asks for authentication; a private clone retries with the app credential.
        const probe = yield* driver
          .exec({
            account,
            providerSandboxId: sandbox.providerSandboxId!,
            command: `git ls-remote --heads ${shellQuote(input.cloneUrl)} >/dev/null 2>&1`,
            envs: { GIT_TERMINAL_PROMPT: "0", GIT_ASKPASS: "" },
            timeoutMs: 60_000,
          })
          .pipe(Effect.orElseSucceed(() => ({ exitCode: 1, stdout: "", stderr: "" })));
        const credential =
          probe.exitCode === 0
            ? Option.none<CloudRepositoryCredential.CloudRepositoryCredential>()
            : yield* credentials.resolve(input.cloneUrl);
        if (Option.isSome(credential) && !credentialTransportAllowed(input.cloneUrl)) {
          return yield* cleanup(
            "HTTP Git remotes cannot receive a sandbox credential. Use an HTTPS or SSH remote.",
          );
        }
        const credentialHint =
          probe.exitCode !== 0 && Option.isNone(credential)
            ? " If this repository is private, connect its GitHub App installation in Settings → Source control & issues."
            : "";
        const gitEnv = Option.isSome(credential)
          ? gitCredentialEnvironment(credential.value)
          : undefined;
        const clone = [
          "set -eu",
          `git clone --branch ${shellQuote(cloudBranchName(input.baseRef))} --no-tags ${shellQuote(input.cloneUrl)} ${shellQuote(workspacePath)}`,
        ].join("\n");
        yield* run(sandbox, driver, account, clone, "clone", undefined, gitEnv).pipe(
          Effect.catchTags({
            CloudThreadError: (error) =>
              cleanup(
                `Could not clone the project's branch: ${error.detail}${credentialHint}`,
                error,
              ),
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
            template: ready.template,
            ...(ready.size === undefined ? {} : { size: ready.size }),
            ...(ready.region === undefined ? {} : { region: ready.region }),
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
        yield* notify(input.threadId);
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
      // Waking blocks here, so a turn starts only after the sandbox is running again.
      yield* wakeCloudSandbox(threadId);
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
    getCloudSandboxState,
    subscribeCloudSandbox,
    sleepCloudSandbox,
    wakeCloudSandbox,
    previewCloudSandboxDelete,
    deleteCloudSandbox,
    beginTurn,
    endTurn,
    holdPreview,
    releasePreview,
    sweepIdle,
  });
});

export const layer = Layer.effect(CloudThreads, make);
