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
const decodeCodexSettings = Schema.decodeUnknownOption(CodexSettings);

export const cloudHarnessCapability = (input: {
  readonly driver: ProviderDriverKind;
  readonly config: unknown;
}): CloudHarnessCapability => {
  const codex = input.driver === CODEX_DRIVER ? decodeCodexSettings(input.config) : Option.none();
  return {
    supported: cloudProviderHostSupportsDriver(input.driver),
    // Managed ChatGPT sign-in keeps its token on the controller; the sandbox cannot reuse it.
    credentialSupported: !(Option.isSome(codex) && codex.value.setupMode === "managed"),
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
