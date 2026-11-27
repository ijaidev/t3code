import type {
  SandboxCloudTarget,
  SandboxProviderAccountSummary,
  SandboxProviderSummary,
} from "@t3tools/contracts";

/** The public remote shapes the project resolver distinguishes. */
export type CloudTargetRemote = { readonly type: "url" } | { readonly type: "credentials" } | null;

/**
 * A target is enabled only when this project can clone, and the account can create
 * and run a sandbox. Every other case keeps the target visible with a reason.
 */
export const sandboxCloudTargets = (input: {
  readonly remote: CloudTargetRemote;
  readonly accounts: ReadonlyArray<SandboxProviderAccountSummary>;
  readonly providers: ReadonlyArray<SandboxProviderSummary>;
}): ReadonlyArray<SandboxCloudTarget> =>
  input.accounts.map((account) => {
    const provider = input.providers.find((candidate) => candidate.kind === account.kind);
    const enabled =
      input.remote?.type === "url" &&
      account.enabled &&
      provider?.capabilities.create === true &&
      provider.capabilities.exec === true;
    const disabledReason =
      input.remote === null
        ? ("no-remote" as const)
        : input.remote.type === "credentials"
          ? ("unsafe-remote" as const)
          : !account.enabled
            ? ("account-disabled" as const)
            : provider === undefined
              ? ("provider-unavailable" as const)
              : provider.capabilities.create !== true
                ? ("no-create" as const)
                : provider.capabilities.exec !== true
                  ? ("no-exec" as const)
                  : undefined;
    return {
      accountId: account.id,
      providerKind: account.kind,
      label: account.label,
      enabled,
      ...(disabledReason === undefined ? {} : { disabledReason }),
    };
  });
