import {
  SandboxError,
  SandboxInstanceId,
  type SandboxAccountInput,
  type SandboxCreateTestSandboxInput,
  type SandboxCreateTestSandboxResult,
  type SandboxHealth,
  type SandboxHealthCheckResult,
  type SandboxOperation,
  type SandboxTestSandbox,
  type SandboxTestSandboxInput,
  type SandboxTestSandboxListResult,
  type SandboxTestSandboxResult,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Semaphore from "effect/Semaphore";

import * as SandboxAccounts from "./SandboxAccounts.ts";
import type { SandboxProviderError } from "./SandboxProvider.ts";
import * as SandboxStore from "./SandboxStore.ts";
import {
  parseThreadHostHandshake,
  THREAD_HOST_BOOTSTRAP_COMMAND,
  type ThreadHostHandshakeError,
} from "./ThreadHost.ts";

const DEFAULT_TEST_TIMEOUT_MINUTES = 30;

const toSandboxError = (operation: SandboxOperation, error: SandboxProviderError) =>
  new SandboxError({ operation, detail: error.detail });

const threadHostError = (error: ThreadHostHandshakeError) =>
  new SandboxError({ operation: "check-sandbox", detail: error.message });

const isNotFound = (error: SandboxProviderError) => error.statusCode === 404;

const withoutLastError = (record: SandboxTestSandbox) => {
  const { lastError: _lastError, ...rest } = record;
  return rest;
};

export class SandboxLifecycle extends Context.Service<
  SandboxLifecycle,
  {
    readonly listTestSandboxes: Effect.Effect<SandboxTestSandboxListResult>;
    readonly createTestSandbox: (
      input: SandboxCreateTestSandboxInput,
    ) => Effect.Effect<SandboxCreateTestSandboxResult, SandboxError>;
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

  const listTestSandboxes = store.list.pipe(
    Effect.map((sandboxes) => ({
      sandboxes: [...sandboxes].sort((left, right) =>
        right.createdAt.localeCompare(left.createdAt),
      ),
    })),
  );

  const createTestSandbox = Effect.fn("SandboxLifecycle.createTestSandbox")(function* (
    input: SandboxCreateTestSandboxInput,
  ) {
    return yield* lifecycleLock.withPermits(1)(
      Effect.gen(function* () {
        const { account, driver } = yield* accounts.resolve(input.accountId);
        const createdAt = DateTime.formatIso(yield* DateTime.now);
        const id = SandboxInstanceId.make(yield* random.uuidV4());
        const record: SandboxTestSandbox = {
          id,
          accountId: input.accountId,
          providerKind: account.kind,
          providerSandboxId: null,
          status: "provisioning",
          template: account.template,
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
          })
          .pipe(
            Effect.mapError((error) => toSandboxError("create-sandbox", error)),
            Effect.tapError(() => store.remove(id).pipe(Effect.ignore)),
          );

        const running: SandboxTestSandbox = {
          ...record,
          providerSandboxId: created.providerSandboxId,
          status: "running",
          updatedAt: DateTime.formatIso(yield* DateTime.now),
        };
        const persisted = yield* store.upsert(running).pipe(Effect.result);
        if (persisted._tag === "Failure") {
          yield* driver.delete(account, created.providerSandboxId).pipe(Effect.ignore);
          yield* store.remove(id).pipe(Effect.ignore);
          return yield* new SandboxError({
            operation: "create-sandbox",
            detail:
              "The sandbox was created but its state could not be saved, so it was deleted. Try again.",
          });
        }
        return { sandbox: running };
      }),
    );
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
        if (record.providerSandboxId === null) {
          yield* store.remove(record.id).pipe(Effect.ignore);
          return;
        }
        const { account, driver } = yield* accounts.resolve(record.accountId);
        const deleting: SandboxTestSandbox = {
          ...record,
          status: "deleting",
          updatedAt: DateTime.formatIso(yield* DateTime.now),
        };
        yield* store.upsert(deleting).pipe(Effect.ignore);
        yield* driver.delete(account, record.providerSandboxId).pipe(
          Effect.catchIf(isNotFound, () => Effect.void),
          Effect.mapError((error) => toSandboxError("delete-sandbox", error)),
          Effect.tapError((error) => markFailed(deleting, error.detail)),
        );
        yield* store.remove(record.id).pipe(Effect.ignore);
      }),
    );
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
          if (record.providerSandboxId === null) {
            yield* store.remove(record.id).pipe(Effect.ignore);
            continue;
          }
          const resolved = yield* accounts.resolve(record.accountId).pipe(
            Effect.catchTags({
              SandboxError: (error) =>
                new SandboxError({
                  operation: "disconnect-account",
                  detail: error.detail,
                }),
            }),
          );
          yield* resolved.driver.delete(resolved.account, record.providerSandboxId).pipe(
            Effect.catchIf(isNotFound, () => Effect.void),
            Effect.mapError(
              (error) =>
                new SandboxError({
                  operation: "disconnect-account",
                  detail: `Delete the ${record.providerKind} test sandbox before disconnecting. ${error.detail}`,
                }),
            ),
          );
          yield* store.remove(record.id).pipe(Effect.ignore);
        }
        yield* accounts.disconnect(input.accountId);
      }),
    );
  });

  return SandboxLifecycle.of({
    listTestSandboxes,
    createTestSandbox,
    pauseTestSandbox,
    resumeTestSandbox,
    deleteTestSandbox,
    checkTestSandbox,
    disconnectAccount,
  });
});

export const layer = Layer.effect(SandboxLifecycle, make);
