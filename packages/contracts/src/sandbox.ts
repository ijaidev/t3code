import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  IsoDateTime,
  NonNegativeInt,
  PositiveInt,
  ProjectId,
  ThreadId,
  TrimmedNonEmptyString,
} from "./baseSchemas.ts";
import { SettingsSecretValue } from "./settingsSecretValue.ts";

export const SandboxProviderKind = TrimmedNonEmptyString.check(
  Schema.isMaxLength(64),
  Schema.isPattern(/^[a-z][a-z0-9-]*$/),
).pipe(Schema.brand("SandboxProviderKind"));
export type SandboxProviderKind = typeof SandboxProviderKind.Type;

const ConfiguredSandboxProviderAccountId = TrimmedNonEmptyString.check(
  Schema.isMaxLength(128),
  Schema.isPattern(/^[a-zA-Z0-9][a-zA-Z0-9_-]*$/),
);

export const SandboxProviderAccountId = ConfiguredSandboxProviderAccountId.pipe(
  Schema.brand("SandboxProviderAccountId"),
);
export type SandboxProviderAccountId = typeof SandboxProviderAccountId.Type;

export const SandboxInstanceId = TrimmedNonEmptyString.pipe(Schema.brand("SandboxInstanceId"));
export type SandboxInstanceId = typeof SandboxInstanceId.Type;

export const SandboxProviderLabel = TrimmedNonEmptyString.check(Schema.isMaxLength(64));

export const SandboxProviderSize = Schema.Struct({
  id: TrimmedNonEmptyString,
  label: TrimmedNonEmptyString,
  cpuCount: PositiveInt,
  memoryMB: PositiveInt,
});
export type SandboxProviderSize = typeof SandboxProviderSize.Type;

export const SandboxProviderRegion = Schema.Struct({
  id: TrimmedNonEmptyString,
  label: TrimmedNonEmptyString,
});
export type SandboxProviderRegion = typeof SandboxProviderRegion.Type;

export const SandboxProviderCapabilities = Schema.Struct({
  create: Schema.Boolean,
  exec: Schema.Boolean,
  pauseResume: Schema.Boolean,
  delete: Schema.Boolean,
  snapshots: Schema.Boolean,
  privatePorts: Schema.Boolean,
  sizes: Schema.Array(SandboxProviderSize),
  regions: Schema.Array(SandboxProviderRegion),
});
export type SandboxProviderCapabilities = typeof SandboxProviderCapabilities.Type;

export const SandboxProviderSummary = Schema.Struct({
  kind: SandboxProviderKind,
  label: TrimmedNonEmptyString,
  capabilities: SandboxProviderCapabilities,
});
export type SandboxProviderSummary = typeof SandboxProviderSummary.Type;

/** Stored under `sandboxProviders`; the settings record key is the account id. */
export const SandboxProviderAccountConfig = Schema.Struct({
  kind: SandboxProviderKind,
  label: SandboxProviderLabel,
  /** Kept in the secret store on the controlling environment, never in a sandbox. */
  apiKey: SettingsSecretValue.pipe(
    Schema.withDecodingDefault(Effect.succeed("")),
    Schema.annotateKey({ settingsSecret: true }),
  ),
  template: TrimmedNonEmptyString.check(Schema.isMaxLength(128)).pipe(
    Schema.withDecodingDefault(Effect.succeed("base")),
  ),
  /** A provider-declared size id; absent uses the provider's first declared size. */
  sizeId: Schema.optionalKey(TrimmedNonEmptyString.check(Schema.isMaxLength(64))),
  /** A provider-declared region id; absent uses the provider's first declared region. */
  regionId: Schema.optionalKey(TrimmedNonEmptyString.check(Schema.isMaxLength(64))),
  enabled: Schema.Boolean.pipe(Schema.withDecodingDefault(Effect.succeed(true))),
});
export type SandboxProviderAccountConfig = typeof SandboxProviderAccountConfig.Type;

export const SandboxProviderAccountSummary = Schema.Struct({
  id: SandboxProviderAccountId,
  kind: SandboxProviderKind,
  label: SandboxProviderLabel,
  template: TrimmedNonEmptyString,
  enabled: Schema.Boolean,
});
export type SandboxProviderAccountSummary = typeof SandboxProviderAccountSummary.Type;

export const SandboxProviderListResult = Schema.Struct({
  providers: Schema.Array(SandboxProviderSummary),
  accounts: Schema.Array(SandboxProviderAccountSummary),
});
export type SandboxProviderListResult = typeof SandboxProviderListResult.Type;

export const SandboxAccountConnectInput = Schema.Struct({
  /** Omitted for a new account; the server chooses an id. */
  id: Schema.optionalKey(SandboxProviderAccountId),
  config: SandboxProviderAccountConfig,
});
export type SandboxAccountConnectInput = typeof SandboxAccountConnectInput.Type;

export const SandboxAccountInput = Schema.Struct({
  accountId: SandboxProviderAccountId,
});
export type SandboxAccountInput = typeof SandboxAccountInput.Type;

export const SandboxAccountConnectResult = Schema.Struct({
  account: SandboxProviderAccountSummary,
  capabilities: SandboxProviderCapabilities,
});
export type SandboxAccountConnectResult = typeof SandboxAccountConnectResult.Type;

export const SandboxAccountTestResult = Schema.Struct({
  account: SandboxProviderAccountSummary,
  capabilities: SandboxProviderCapabilities,
  checkedAt: IsoDateTime,
});
export type SandboxAccountTestResult = typeof SandboxAccountTestResult.Type;

/** One cloud target the target picker can offer for a project. */
export const SandboxCloudTarget = Schema.Struct({
  accountId: SandboxProviderAccountId,
  providerKind: SandboxProviderKind,
  label: TrimmedNonEmptyString,
  enabled: Schema.Boolean,
  /** Why an unavailable target is disabled; clients translate this code. */
  disabledReason: Schema.optionalKey(
    Schema.Literals([
      "no-remote",
      "unsafe-remote",
      "account-disabled",
      "provider-unavailable",
      "no-create",
      "no-exec",
    ]),
  ),
});
export type SandboxCloudTarget = typeof SandboxCloudTarget.Type;

export const SandboxListCloudTargetsInput = Schema.Struct({
  projectId: ProjectId,
});
export type SandboxListCloudTargetsInput = typeof SandboxListCloudTargetsInput.Type;

export const SandboxListCloudTargetsResult = Schema.Struct({
  targets: Schema.Array(SandboxCloudTarget),
});
export type SandboxListCloudTargetsResult = typeof SandboxListCloudTargetsResult.Type;

export const SandboxInstanceStatus = Schema.Literals([
  "provisioning",
  "running",
  "paused",
  "deleting",
]);
export type SandboxInstanceStatus = typeof SandboxInstanceStatus.Type;

export const SandboxHealthStatus = Schema.Literals(["ready", "failed"]);
export type SandboxHealthStatus = typeof SandboxHealthStatus.Type;

export const SandboxHealth = Schema.Struct({
  status: SandboxHealthStatus,
  checkedAt: IsoDateTime,
  protocolVersion: Schema.optionalKey(TrimmedNonEmptyString),
  capabilities: Schema.Array(TrimmedNonEmptyString),
  detail: Schema.optionalKey(Schema.String),
});
export type SandboxHealth = typeof SandboxHealth.Type;

export const SandboxTestSandbox = Schema.Struct({
  id: SandboxInstanceId,
  accountId: SandboxProviderAccountId,
  providerKind: SandboxProviderKind,
  providerSandboxId: Schema.NullOr(TrimmedNonEmptyString),
  /** Set on a cloud thread's sandbox; absent on sandboxes created for the settings test. */
  threadId: Schema.optionalKey(ThreadId),
  status: SandboxInstanceStatus,
  template: TrimmedNonEmptyString,
  /** Provider-declared size and region snapshots, so a lost sandbox can still be described. */
  size: Schema.optionalKey(SandboxProviderSize),
  region: Schema.optionalKey(SandboxProviderRegion),
  /** Last turn, preview, or lifecycle action; absent on sandboxes created before this field. */
  lastActivityAt: Schema.optionalKey(IsoDateTime),
  /** When an idle sweep paused the sandbox; absent while running or provisioning. */
  sleepingSince: Schema.optionalKey(IsoDateTime),
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
  lastHealth: Schema.optionalKey(SandboxHealth),
  lastError: Schema.optionalKey(Schema.String),
});
export type SandboxTestSandbox = typeof SandboxTestSandbox.Type;

export const SandboxTestSandboxListResult = Schema.Struct({
  sandboxes: Schema.Array(SandboxTestSandbox),
});
export type SandboxTestSandboxListResult = typeof SandboxTestSandboxListResult.Type;

export const SandboxCreateTestSandboxInput = Schema.Struct({
  accountId: SandboxProviderAccountId,
  /** E2B keeps a running sandbox for this long before it expires. */
  timeoutMinutes: Schema.optionalKey(
    Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 60 })),
  ),
});
export type SandboxCreateTestSandboxInput = typeof SandboxCreateTestSandboxInput.Type;

export const CloudSandboxStatus = Schema.Literals([
  "provisioning",
  "waking",
  "running",
  "sleeping",
  "deleting",
  "lost",
  "failed",
]);
export type CloudSandboxStatus = typeof CloudSandboxStatus.Type;

export const CloudSandboxSleepDisabledReason = Schema.Literals([
  "provider-no-sleep",
  "provider-unavailable",
  "active-turn",
  "open-preview",
  "not-running",
]);
export type CloudSandboxSleepDisabledReason = typeof CloudSandboxSleepDisabledReason.Type;

/** What every client shows for one cloud thread's sandbox. */
export const CloudSandboxState = Schema.Struct({
  threadId: ThreadId,
  accountId: SandboxProviderAccountId,
  providerKind: SandboxProviderKind,
  providerLabel: TrimmedNonEmptyString,
  status: CloudSandboxStatus,
  /** Null once the sandbox is deleted or lost; the thread keeps its record. */
  sandboxId: Schema.NullOr(SandboxInstanceId),
  template: TrimmedNonEmptyString,
  size: Schema.NullOr(SandboxProviderSize),
  region: Schema.NullOr(SandboxProviderRegion),
  createdAt: IsoDateTime,
  lastActiveAt: IsoDateTime,
  sleepingSince: Schema.NullOr(IsoDateTime),
  canSleep: Schema.Boolean,
  canWake: Schema.Boolean,
  canDelete: Schema.Boolean,
  /** Why sleep is unavailable even though the sandbox exists. */
  sleepDisabledReason: Schema.optionalKey(CloudSandboxSleepDisabledReason),
  lastError: Schema.optionalKey(Schema.String),
});
export type CloudSandboxState = typeof CloudSandboxState.Type;

export const CloudSandboxThreadInput = Schema.Struct({
  threadId: ThreadId,
});
export type CloudSandboxThreadInput = typeof CloudSandboxThreadInput.Type;

export const CloudSandboxDeleteRisk = Schema.Struct({
  hasUnpushedChanges: Schema.Boolean,
  changedFiles: NonNegativeInt,
  unpushedCommits: NonNegativeInt,
});
export type CloudSandboxDeleteRisk = typeof CloudSandboxDeleteRisk.Type;

export const CloudSandboxDeletePreview = Schema.Struct({
  state: CloudSandboxState,
  risk: CloudSandboxDeleteRisk,
});
export type CloudSandboxDeletePreview = typeof CloudSandboxDeletePreview.Type;

export const CloudSandboxDeleteInput = Schema.Struct({
  threadId: ThreadId,
  /** Required when the delete preview reports unpushed changes. */
  acknowledgedUnpushedChanges: Schema.Boolean.pipe(
    Schema.withDecodingDefault(Effect.succeed(false)),
  ),
});
export type CloudSandboxDeleteInput = typeof CloudSandboxDeleteInput.Type;

export const CloudSandboxStateResult = Schema.Struct({
  state: CloudSandboxState,
});
export type CloudSandboxStateResult = typeof CloudSandboxStateResult.Type;

export const SandboxTestSandboxInput = Schema.Struct({
  sandboxId: SandboxInstanceId,
});
export type SandboxTestSandboxInput = typeof SandboxTestSandboxInput.Type;

export const SandboxCreateTestSandboxResult = Schema.Struct({
  sandbox: SandboxTestSandbox,
});
export type SandboxCreateTestSandboxResult = typeof SandboxCreateTestSandboxResult.Type;

export const SandboxTestSandboxResult = Schema.Struct({
  sandbox: SandboxTestSandbox,
});
export type SandboxTestSandboxResult = typeof SandboxTestSandboxResult.Type;

export const SandboxHealthCheckResult = Schema.Struct({
  sandbox: SandboxTestSandbox,
  health: SandboxHealth,
});
export type SandboxHealthCheckResult = typeof SandboxHealthCheckResult.Type;

export const SandboxOperation = Schema.Literals([
  "connect-account",
  "test-account",
  "disconnect-account",
  "create-sandbox",
  "pause-sandbox",
  "resume-sandbox",
  "delete-sandbox",
  "check-sandbox",
]);
export type SandboxOperation = typeof SandboxOperation.Type;

export class SandboxError extends Schema.TaggedError<
  SandboxError,
  { readonly brand: unique symbol }
>()("SandboxError", {
  operation: SandboxOperation,
  detail: Schema.String,
}) {
  override get message(): string {
    return this.detail;
  }
}
