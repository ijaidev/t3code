import {
  CodexSettings,
  ProviderDriverKind,
  type SandboxCloudTarget,
  type SandboxProviderAccountSummary,
  type SandboxProviderSummary,
} from "@t3tools/contracts";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import { cloudProviderHostSupportsDriver } from "./CloudProviderHost.ts";

/** The public remote shapes the project resolver distinguishes. */
export type CloudTargetRemote = { readonly type: "url" } | { readonly type: "credentials" } | null;

type CloudTargetDisabledReason = NonNullable<SandboxCloudTarget["disabledReason"]>;

/** What the selected provider instance can do inside a sandbox. */
export interface CloudHarnessCapability {
  /** The sandbox host has an adapter and runtime for this provider. */
  readonly supported: boolean;
  /** The provider's configured sign-in mode survives into a sandbox. */
  readonly credentialSupported: boolean;
}

type CloudTargetAvailability =
  | { readonly enabled: true }
  | { readonly enabled: false; readonly reason: CloudTargetDisabledReason };

const CODEX_DRIVER = ProviderDriverKind.make("codex");
const CLAUDE_DRIVER = ProviderDriverKind.make("claudeAgent");
const OPENCODE_DRIVER = ProviderDriverKind.make("opencode");
const decodeCodexSettings = Schema.decodeUnknownOption(CodexSettings);

const CLAUDE_ANTHROPIC_CREDENTIALS = [
  "ANTHROPIC_API_KEY",
  "ANTHROPIC_AUTH_TOKEN",
  "CLAUDE_CODE_OAUTH_TOKEN",
] as const;
const CLAUDE_BEDROCK_CREDENTIALS = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"] as const;
const CODEX_SANDBOX_CREDENTIALS = ["OPENAI_API_KEY"] as const;

const hasEnvironmentValue = (
  environment: ReadonlyArray<{ readonly name: string; readonly value: string }>,
  name: string,
): boolean =>
  environment.some((variable) => variable.name === name && variable.value.trim() !== "");

const hasEveryEnvironmentValue = (
  environment: ReadonlyArray<{ readonly name: string; readonly value: string }>,
  names: ReadonlyArray<string>,
): boolean => names.every((name) => hasEnvironmentValue(environment, name));

/**
 * The sandbox host clears provider home directories and receives only the resolved
 * environment, so a controller-local OAuth/keychain/auth.json sign-in cannot reach it.
 */
const sandboxCredentialSupported = (
  driver: ProviderDriverKind,
  environment: ReadonlyArray<{ readonly name: string; readonly value: string }>,
): boolean => {
  if (driver === CLAUDE_DRIVER) {
    return (
      CLAUDE_ANTHROPIC_CREDENTIALS.some((name) => hasEnvironmentValue(environment, name)) ||
      (hasEnvironmentValue(environment, "CLAUDE_CODE_USE_BEDROCK") &&
        hasEveryEnvironmentValue(environment, CLAUDE_BEDROCK_CREDENTIALS))
    );
  }
  if (driver === CODEX_DRIVER) {
    return hasEveryEnvironmentValue(environment, CODEX_SANDBOX_CREDENTIALS);
  }
  if (driver === OPENCODE_DRIVER) {
    // OpenCode's provider set is open, so any configured variable may be its credential.
    return environment.some((variable) => variable.value.trim() !== "");
  }
  return false;
};

export const cloudHarnessCapability = (input: {
  readonly driver: ProviderDriverKind;
  readonly config: unknown;
  readonly environment?: ReadonlyArray<{ readonly name: string; readonly value: string }>;
}): CloudHarnessCapability => {
  const codex = input.driver === CODEX_DRIVER ? decodeCodexSettings(input.config) : Option.none();
  return {
    supported: cloudProviderHostSupportsDriver(input.driver),
    credentialSupported:
      // Managed ChatGPT sign-in keeps its token on the controller; the sandbox cannot reuse it.
      !(Option.isSome(codex) && codex.value.setupMode === "managed") &&
      sandboxCredentialSupported(input.driver, input.environment ?? []),
  };
};

/**
 * A target is enabled only when this project can clone, the account can create and
 * run a sandbox, the provider can host a harness, and the selected harness can run
 * there. Every other case keeps the target visible with a reason.
 */
const cloudTargetAvailability = (input: {
  readonly remote: CloudTargetRemote;
  readonly account: Pick<SandboxProviderAccountSummary, "enabled" | "hasCredential">;
  readonly provider: SandboxProviderSummary | undefined;
  readonly harness?: CloudHarnessCapability;
}): CloudTargetAvailability => {
  if (input.remote === null) return { enabled: false, reason: "no-remote" };
  if (input.remote.type === "credentials") return { enabled: false, reason: "unsafe-remote" };
  if (!input.account.enabled) return { enabled: false, reason: "account-disabled" };
  if (input.account.hasCredential === false) return { enabled: false, reason: "no-credential" };
  const provider = input.provider;
  if (provider === undefined) return { enabled: false, reason: "provider-unavailable" };
  if (provider.capabilities.create !== true) return { enabled: false, reason: "no-create" };
  if (provider.capabilities.exec !== true) return { enabled: false, reason: "no-exec" };
  if (provider.capabilities.harness !== true) return { enabled: false, reason: "no-harness" };
  if (input.harness !== undefined && !input.harness.supported) {
    return { enabled: false, reason: "harness-unavailable" };
  }
  if (input.harness !== undefined && !input.harness.credentialSupported) {
    return { enabled: false, reason: "sign-in-unavailable" };
  }
  return { enabled: true };
};

/** Reasons a client built before the harness/credential reasons can still decode. */
const LEGACY_DISABLED_REASONS = new Set([
  "no-remote",
  "unsafe-remote",
  "account-disabled",
  "provider-unavailable",
  "no-create",
  "no-exec",
]);

const LEGACY_FALLBACK_REASON = "provider-unavailable" as const;

/**
 * Older clients decode `disabledReason` as the six-value union they shipped. Keep their
 * response decodable instead of failing the whole target list on a newer reason.
 */
export const legacySandboxCloudTargets = (
  targets: ReadonlyArray<SandboxCloudTarget>,
): ReadonlyArray<SandboxCloudTarget> =>
  targets.map((target) =>
    target.disabledReason === undefined || LEGACY_DISABLED_REASONS.has(target.disabledReason)
      ? target
      : { ...target, disabledReason: LEGACY_FALLBACK_REASON },
  );

export const sandboxCloudTargets = (input: {
  readonly remote: CloudTargetRemote;
  readonly accounts: ReadonlyArray<SandboxProviderAccountSummary>;
  readonly providers: ReadonlyArray<SandboxProviderSummary>;
  readonly harness?: CloudHarnessCapability;
}): ReadonlyArray<SandboxCloudTarget> =>
  input.accounts.map((account) => {
    const provider = input.providers.find((candidate) => candidate.kind === account.kind);
    const availability = cloudTargetAvailability({
      remote: input.remote,
      account,
      provider,
      ...(input.harness === undefined ? {} : { harness: input.harness }),
    });
    return {
      accountId: account.id,
      providerKind: account.kind,
      label: account.label,
      enabled: availability.enabled,
      ...(availability.enabled ? {} : { disabledReason: availability.reason }),
    };
  });
