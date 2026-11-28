import {
  SandboxError,
  SandboxInstanceId,
  type SandboxAccountInput,
  type SandboxCreateTestSandboxInput,
  type SandboxCreateTestSandboxResult,
  type SandboxHealth,
  type SandboxHealthCheckResult,
  type SandboxInstanceStatus,
  type SandboxOperation,
  type SandboxTestSandbox,
  type SandboxTestSandboxInput,
  type SandboxTestSandboxListResult,
  type SandboxTestSandboxResult,
  type ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Semaphore from "effect/Semaphore";

import * as SandboxAccounts from "./SandboxAccounts.ts";
import type {
  SandboxProviderDriver,
  SandboxProviderError,
  SandboxProviderFoundSandbox,
} from "./SandboxProvider.ts";
import * as SandboxStore from "./SandboxStore.ts";
import {
  parseThreadHostHandshake,
  THREAD_HOST_BOOTSTRAP_COMMAND,
  type ThreadHostHandshakeError,
} from "./ThreadHost.ts";

const DEFAULT_TEST_TIMEOUT_MINUTES = 30;

const toSandboxError = (operation: SandboxOperation, error: SandboxProviderError) =>
  new SandboxError({
    operation,
    detail: error.detail,
    ...(error.statusCode === undefined ? {} : { statusCode: error.statusCode }),
  });

const threadHostError = (error: ThreadHostHandshakeError) =>
  new SandboxError({ operation: "check-sandbox", detail: error.message });

const isNotFound = (error: SandboxProviderError) => error.statusCode === 404;

const withoutLastError = (record: SandboxTestSandbox) => {
  const { lastError: _lastError, ...rest } = record;
  return rest;
};

const runningStatus = (record: SandboxTestSandbox): "running" | "paused" =>
  record.status === "paused" ? "paused" : "running";

export class SandboxLifecycle extends Context.Service<
  SandboxLifecycle,
  {
    readonly listTestSandboxes: Effect.Effect<SandboxTestSandboxListResult>;
    readonly createTestSandbox: (
      input: SandboxCreateTestSandboxInput,
    ) => Effect.Effect<SandboxCreateTestSandboxResult, SandboxError>;
    /** A cloud thread's sandbox, kept out of the settings test list. */
    readonly createThreadSandbox: (input: {
      readonly threadId: ThreadId;
      readonly accountId: SandboxCreateTestSandboxInput["accountId"];
      readonly timeoutMinutes?: number;
    }) => Effect.Effect<SandboxTestSandbox, SandboxError>;
    readonly deleteThreadSandbox: (
      input: SandboxTestSandboxInput,
    ) => Effect.Effect<void, SandboxError>;
    readonly getSandbox: (
      sandboxId: SandboxInstanceId,
    ) => Effect.Effect<Option.Option<SandboxTestSandbox>>;
    readonly pauseTestSandbox: (
      input: SandboxTestSandboxInput,
    ) => Effect.Effect<SandboxTestSandboxResult, SandboxError>;
    readonly resumeTestSandbox: (
      input: SandboxTestSandboxInput,
    ) => Effect.Effect<SandboxTestSandboxResult, SandboxError>;
    readonly deleteTestSandbox: (
      input: SandboxTestSandboxInput,
    ) => Effect.Effect<void, SandboxError>;
    readonly checkTestSandbox: (
      input: SandboxTestSandboxInput,
    ) => Effect.Effect<SandboxHealthCheckResult, SandboxError>;
    readonly disconnectAccount: (input: SandboxAccountInput) => Effect.Effect<void, SandboxError>;
    readonly reconcile: Effect.Effect<void>;
  }
>()("t3/sandbox/SandboxLifecycle") {}

const make = Effect.gen(function* () {
  const accounts = yield* SandboxAccounts.SandboxAccounts;
  const store = yield* SandboxStore.SandboxStore;
  const random = yield* SecureRandom.SecureRandom;
  // Creates, deletes, and account disconnects change billable resources; serialize them.
  const lifecycleLock = yield* Semaphore.make(1);

  const getRecord = Effect.fn("SandboxLifecycle.getRecord")(function* (
    sandboxId: SandboxInstanceId,
  ) {
    const record = yield* store.get(sandboxId);
    if (Option.isNone(record)) {
      return yield* new SandboxError({
        operation: "check-sandbox",
        detail: "That test sandbox no longer exists.",
      });
    }
    return record.value;
  });

  const save = Effect.fnUntraced(function* (
    record: SandboxTestSandbox,
    operation: SandboxOperation,
  ) {
    yield* store.upsert(record).pipe(
      Effect.mapError(
        () =>
          new SandboxError({
            operation,
            detail: "Could not save the sandbox state. Check the server's state directory.",
          }),
      ),
    );
  });

  const markFailed = Effect.fnUntraced(function* (record: SandboxTestSandbox, detail: string) {
    const failed: SandboxTestSandbox = {
      ...record,
      lastError: detail,
      updatedAt: DateTime.formatIso(yield* DateTime.now),
    };
    yield* store.upsert(failed).pipe(Effect.ignore);
  });

  const adopt = Effect.fnUntraced(function* (
    record: SandboxTestSandbox,
    found: SandboxProviderFoundSandbox,
  ) {
    return {
      ...withoutLastError(record),
      providerSandboxId: found.providerSandboxId,
      // A failed sandbox is kept only so its cleanup record stays deletable. Reconcile never
      // adopts one as healthy.
      status: found.status === "failed" ? "running" : found.status,
      updatedAt: DateTime.formatIso(yield* DateTime.now),
    } satisfies SandboxTestSandbox;
  });

  const deleteFound = (
    driver: SandboxProviderDriver,
    account: SandboxAccounts.ResolvedSandboxAccount["account"],
    found: ReadonlyArray<SandboxProviderFoundSandbox>,
  ) =>
    Effect.forEach(
      found,
      (sandbox) =>
        driver.delete(account, sandbox.providerSandboxId).pipe(
          Effect.catchIf(isNotFound, () => Effect.void),
          Effect.result,
        ),
      { concurrency: 1 },
    );

  const deleteAllOrFail = Effect.fnUntraced(function* (
    operation: SandboxOperation,
    driver: SandboxProviderDriver,
    account: SandboxAccounts.ResolvedSandboxAccount["account"],
    found: ReadonlyArray<SandboxProviderFoundSandbox>,
    record: SandboxTestSandbox,
  ) {
    const outcomes = yield* deleteFound(driver, account, found);
    const failure = outcomes.find(Result.isFailure);
    if (failure === undefined) {
      yield* store.remove(record.id).pipe(Effect.ignore);
      return;
    }
    const detail = `The sandbox could not be deleted: ${failure.failure.detail}`;
    yield* store
      .upsert({ ...record, status: runningStatus(record), lastError: detail })
      .pipe(Effect.ignore);
    return yield* new SandboxError({ operation, detail });
  });

  const handleCreateFailure = Effect.fnUntraced(function* (
    record: SandboxTestSandbox,
    account: SandboxAccounts.ResolvedSandboxAccount["account"],
    driver: SandboxProviderDriver,
    createError: SandboxError,
  ) {
    const found = yield* driver.findCreated(account, record.id).pipe(Effect.result);
    if (found._tag === "Failure") {
      const detail = `The sandbox could not be confirmed after its create failed: ${found.failure.detail} Check the provider before retrying.`;
      yield* markFailed(record, detail);
      return yield* new SandboxError({ operation: "create-sandbox", detail });
    }
    if (found.success.length === 0) {
      yield* store.remove(record.id).pipe(Effect.ignore);
      return yield* createError;
    }
    const adopted = yield* adopt(record, found.success[0]!);
    yield* store.upsert(adopted).pipe(Effect.ignore);
    const outcomes = yield* deleteFound(driver, account, found.success);
    const failure = outcomes.find(Result.isFailure);
    if (failure === undefined) {
      yield* store.remove(record.id).pipe(Effect.ignore);
      return yield* createError;
    }
    const detail = `The sandbox was created but could not be cleaned up: ${failure.failure.detail} It is listed so it can be deleted.`;
    yield* store.upsert({ ...adopted, lastError: detail }).pipe(Effect.ignore);
    return yield* new SandboxError({ operation: "create-sandbox", detail });
  });

  const reconcile = Effect.fn("SandboxLifecycle.reconcile")(function* () {
    const records = (yield* store.list).filter((record) => record.providerSandboxId === null);
    for (const record of records) {
      const resolved = yield* accounts.resolve(record.accountId).pipe(Effect.result);
      if (resolved._tag === "Failure") {
        yield* markFailed(
          record,
          "The connected account is unavailable, so the provider could not be checked.",
        );
        continue;
      }
      const found = yield* resolved.success.driver
        .findCreated(resolved.success.account, record.id)
        .pipe(Effect.result);
      if (found._tag === "Failure") {
        yield* markFailed(record, `The provider could not be checked: ${found.failure.detail}`);
        continue;
      }
      const adoptable = found.success.filter(
        (sandbox) =>
          sandbox.status !== "failed" &&
          (sandbox.status !== "paused" || resolved.success.driver.capabilities.pauseResume),
      );
      // Prefer a running sandbox over a paused duplicate so a provider without pause
      // never adopts a sandbox it cannot wake.
      const first = adoptable.find((sandbox) => sandbox.status === "running") ?? adoptable[0];
      if (first === undefined) {
        // The provider only reports failed, deleted, or unusable paused sandboxes. Delete them
        // so they stop billing instead of adopting one as a running record.
        const outcomes = yield* deleteFound(
          resolved.success.driver,
          resolved.success.account,
          found.success,
        );
        const failure = outcomes.find(Result.isFailure);
        if (failure === undefined) {
          yield* store.remove(record.id).pipe(Effect.ignore);
        } else {
          yield* markFailed(
            record,
            `The failed sandbox could not be deleted: ${failure.failure.detail}`,
          );
        }
        continue;
      }
      const adopted = yield* adopt(record, first);
      const extras = found.success.filter(
        (sandbox) => sandbox.providerSandboxId !== first.providerSandboxId,
      );
      const outcomes = yield* deleteFound(
        resolved.success.driver,
        resolved.success.account,
        extras,
      );
      const failure = outcomes.find(Result.isFailure);
      yield* store
        .upsert(
          failure === undefined
            ? adopted
            : {
                ...adopted,
                lastError: `A stale sandbox could not be deleted: ${failure.failure.detail}`,
              },
        )
        .pipe(Effect.ignore);
    }
  });

  const listTestSandboxes = reconcile().pipe(
    Effect.andThen(store.list),
    Effect.map((sandboxes) => ({
      sandboxes: sandboxes
        .filter((sandbox) => sandbox.threadId === undefined)
        .sort((left, right) => right.createdAt.localeCompare(left.createdAt)),
    })),
  );

  const createSandbox = Effect.fn("SandboxLifecycle.createSandbox")(function* (
    input: SandboxCreateTestSandboxInput & {
      readonly threadId?: ThreadId;
      readonly initialStatus?: SandboxInstanceStatus;
    },
  ) {
    return yield* lifecycleLock.withPermits(1)(
      Effect.gen(function* () {
        const { account, driver, size, region } = yield* accounts.resolve(input.accountId);
        const createdAt = DateTime.formatIso(yield* DateTime.now);
        const id = SandboxInstanceId.make(yield* random.uuidV4());
        const record: SandboxTestSandbox = {
          id,
          accountId: input.accountId,
          providerKind: account.kind,
          providerSandboxId: null,
          ...(input.threadId === undefined ? {} : { threadId: input.threadId }),
          status: input.initialStatus ?? "provisioning",
          template: account.template,
          ...(size === null ? {} : { size }),
          ...(region === null ? {} : { region }),
          lastActivityAt: createdAt,
          createdAt,
          updatedAt: createdAt,
        };
        yield* save(record, "create-sandbox");

        const created = yield* driver
          .create({
            account,
            instanceId: id,
            template: account.template,
            timeoutMs: (input.timeoutMinutes ?? DEFAULT_TEST_TIMEOUT_MINUTES) * 60_000,
            ...(size === null ? {} : { size }),
            ...(region === null ? {} : { region }),
          })
          .pipe(Effect.result);
        if (created._tag === "Failure") {
          return yield* handleCreateFailure(
            record,
            account,
            driver,
            new SandboxError({ operation: "create-sandbox", detail: created.failure.detail }),
          );
        }

        const running: SandboxTestSandbox = {
          ...record,
          providerSandboxId: created.success.providerSandboxId,
          status: record.status === "provisioning" ? "running" : record.status,
          updatedAt: DateTime.formatIso(yield* DateTime.now),
        };
        const persisted = yield* store.upsert(running).pipe(Effect.result);
        if (persisted._tag === "Failure") {
          const deleted = yield* driver.delete(account, created.success.providerSandboxId).pipe(
            Effect.catchIf(isNotFound, () => Effect.void),
            Effect.result,
          );
          if (deleted._tag === "Success") {
            yield* store.remove(id).pipe(Effect.ignore);
            return yield* new SandboxError({
              operation: "create-sandbox",
              detail:
                "The sandbox was created but its state could not be saved, so it was deleted. Try again.",
            });
          }
          const detail = `The sandbox was created but its state could not be saved, and it could not be deleted: ${deleted.failure.detail} It is listed so it can be deleted.`;
          yield* store
            .upsert({
              ...running,
              lastError: detail,
              updatedAt: DateTime.formatIso(yield* DateTime.now),
            })
            .pipe(Effect.ignore);
          return yield* new SandboxError({ operation: "create-sandbox", detail });
        }
        return running;
      }),
    );
  });

  const createTestSandbox = Effect.fn("SandboxLifecycle.createTestSandbox")(function* (
    input: SandboxCreateTestSandboxInput,
  ) {
    return { sandbox: yield* createSandbox(input) };
  });

  const createThreadSandbox: SandboxLifecycle["Service"]["createThreadSandbox"] = Effect.fn(
    "SandboxLifecycle.createThreadSandbox",
  )(function* (input) {
    return yield* createSandbox({
      accountId: input.accountId,
      ...(input.timeoutMinutes === undefined ? {} : { timeoutMinutes: input.timeoutMinutes }),
      threadId: input.threadId,
      // The lifecycle owns the record from the first write; the thread's clone stage follows.
      initialStatus: "provisioning",
    });
  });

  const pauseTestSandbox = Effect.fn("SandboxLifecycle.pauseTestSandbox")(function* (
    input: SandboxTestSandboxInput,
  ) {
    return yield* lifecycleLock.withPermits(1)(
      Effect.gen(function* () {
        const record = yield* getRecord(input.sandboxId);
        if (record.status === "paused") return { sandbox: record };
        if (record.status === "provisioning" || record.providerSandboxId === null) {
          return yield* new SandboxError({
            operation: "pause-sandbox",
            detail: "Wait for the sandbox to finish provisioning before pausing it.",
          });
        }
        const { account, driver } = yield* accounts.resolve(record.accountId);
        yield* driver
          .pause(account, record.providerSandboxId)
          .pipe(Effect.mapError((error) => toSandboxError("pause-sandbox", error)));
        const paused: SandboxTestSandbox = {
          ...withoutLastError(record),
          status: "paused",
          updatedAt: DateTime.formatIso(yield* DateTime.now),
        };
        yield* save(paused, "pause-sandbox");
        return { sandbox: paused };
      }),
    );
  });

  const resumeTestSandbox = Effect.fn("SandboxLifecycle.resumeTestSandbox")(function* (
    input: SandboxTestSandboxInput,
  ) {
    return yield* lifecycleLock.withPermits(1)(
      Effect.gen(function* () {
        const record = yield* getRecord(input.sandboxId);
        if (record.status === "running") return { sandbox: record };
        if (record.status === "provisioning" || record.providerSandboxId === null) {
          return yield* new SandboxError({
            operation: "resume-sandbox",
            detail: "Wait for the sandbox to finish provisioning before resuming it.",
          });
        }
        const { account, driver } = yield* accounts.resolve(record.accountId);
        yield* driver
          .resume(account, record.providerSandboxId)
          .pipe(Effect.mapError((error) => toSandboxError("resume-sandbox", error)));
        const running: SandboxTestSandbox = {
          ...withoutLastError(record),
          status: "running",
          updatedAt: DateTime.formatIso(yield* DateTime.now),
        };
        yield* save(running, "resume-sandbox");
        return { sandbox: running };
      }),
    );
  });

  const deleteTestSandbox = Effect.fn("SandboxLifecycle.deleteTestSandbox")(function* (
    input: SandboxTestSandboxInput,
  ) {
    return yield* lifecycleLock.withPermits(1)(
      Effect.gen(function* () {
        const record = yield* getRecord(input.sandboxId);
        // A disabled account still has to be able to clean up the sandboxes it already made.
        const { account, driver } = yield* accounts.resolve(record.accountId, {
          allowDisabled: true,
        });
        const found =
          record.providerSandboxId === null
            ? yield* driver
                .findCreated(account, record.id)
                .pipe(Effect.mapError((error) => toSandboxError("delete-sandbox", error)))
            : [
                {
                  providerSandboxId: record.providerSandboxId,
                  status: runningStatus(record),
                },
              ];
        const deleting: SandboxTestSandbox = {
          ...record,
          providerSandboxId: found[0]?.providerSandboxId ?? record.providerSandboxId,
          status: "deleting",
          updatedAt: DateTime.formatIso(yield* DateTime.now),
        };
        yield* store.upsert(deleting).pipe(Effect.ignore);
        yield* deleteAllOrFail("delete-sandbox", driver, account, found, deleting);
      }),
    );
  });

  const deleteThreadSandbox: SandboxLifecycle["Service"]["deleteThreadSandbox"] = Effect.fn(
    "SandboxLifecycle.deleteThreadSandbox",
  )(function* (input) {
    yield* deleteTestSandbox(input);
  });

  const getSandbox: SandboxLifecycle["Service"]["getSandbox"] = Effect.fn(
    "SandboxLifecycle.getSandbox",
  )(function* (sandboxId) {
    yield* reconcile();
    return yield* store.get(sandboxId);
  });

  const checkTestSandbox = Effect.fn("SandboxLifecycle.checkTestSandbox")(function* (
    input: SandboxTestSandboxInput,
  ) {
    const record = yield* getRecord(input.sandboxId);
    if (record.status !== "running" || record.providerSandboxId === null) {
      return yield* new SandboxError({
        operation: "check-sandbox",
        detail: "Resume the sandbox before checking the thread host.",
      });
    }
    const { account, driver } = yield* accounts.resolve(record.accountId);
    const execution = yield* driver
      .exec({
        account,
        providerSandboxId: record.providerSandboxId,
        command: THREAD_HOST_BOOTSTRAP_COMMAND,
        timeoutMs: 120_000,
      })
      .pipe(Effect.mapError((error) => toSandboxError("check-sandbox", error)));
    if (execution.exitCode !== 0) {
      const detail = `The thread host bootstrap exited with code ${execution.exitCode}.`;
      yield* markFailed(record, detail);
      return yield* new SandboxError({ operation: "check-sandbox", detail });
    }
    const handshake = yield* parseThreadHostHandshake(execution.stdout).pipe(
      Effect.mapError(threadHostError),
    );
    const health: SandboxHealth = {
      status: "ready",
      checkedAt: DateTime.formatIso(yield* DateTime.now),
      protocolVersion: handshake.protocolVersion,
      capabilities: handshake.capabilities,
    };
    const checked: SandboxTestSandbox = {
      ...withoutLastError(record),
      lastHealth: health,
      updatedAt: health.checkedAt,
    };
    const persisted = yield* store.upsert(checked).pipe(Effect.result);
    if (persisted._tag === "Failure") {
      return yield* new SandboxError({
        operation: "check-sandbox",
        detail: "The thread host is ready, but its health could not be saved.",
      });
    }
    return { sandbox: checked, health };
  });

  const disconnectAccount = Effect.fn("SandboxLifecycle.disconnectAccount")(function* (
    input: SandboxAccountInput,
  ) {
    return yield* lifecycleLock.withPermits(1)(
      Effect.gen(function* () {
        const records = (yield* store.list).filter(
          (record) => record.accountId === input.accountId,
        );
        for (const record of records) {
          const resolved = yield* accounts.resolve(record.accountId).pipe(
            Effect.catchTags({
              SandboxError: (error) =>
                new SandboxError({
                  operation: "disconnect-account",
                  detail: error.detail,
                }),
            }),
          );
          const found =
            record.providerSandboxId === null
              ? yield* resolved.driver.findCreated(resolved.account, record.id).pipe(
                  Effect.mapError(
                    (error) =>
                      new SandboxError({
                        operation: "disconnect-account",
                        detail: `Check the ${record.providerKind} test sandbox before disconnecting. ${error.detail}`,
                      }),
                  ),
                )
              : [
                  {
                    providerSandboxId: record.providerSandboxId,
                    status: runningStatus(record),
                  },
                ];
          const outcomes = yield* deleteFound(resolved.driver, resolved.account, found);
          const failure = outcomes.find(Result.isFailure);
          if (failure !== undefined) {
            return yield* new SandboxError({
              operation: "disconnect-account",
              detail: `Delete the ${record.providerKind} test sandbox before disconnecting. ${failure.failure.detail}`,
            });
          }
          yield* store.remove(record.id).pipe(Effect.ignore);
        }
        yield* accounts.disconnect(input.accountId);
      }),
    );
  });

  return SandboxLifecycle.of({
    listTestSandboxes,
    createTestSandbox,
    createThreadSandbox,
    deleteThreadSandbox,
    getSandbox,
    pauseTestSandbox,
    resumeTestSandbox,
    deleteTestSandbox,
    checkTestSandbox,
    disconnectAccount,
    reconcile: reconcile(),
  });
});

export const layer = Layer.effect(SandboxLifecycle, make);
