import {
  type ProviderDriverKind,
  type ServerProvider,
  type ServerProviderCompatibilityAdvisory,
} from "@t3tools/contracts";
import {
  ProviderCompatibilityPolicy,
  resolveProviderCompatibility as resolveCompatibility,
} from "@t3tools/provider-core/server/providerCompatibility";
import packageJson from "../../package.json" with { type: "json" };

export { ProviderCompatibilityPolicy };
export { brokenRuntimeError } from "@t3tools/provider-core/server/compatibility";

export const resolveProviderCompatibility = (
  policies: Parameters<typeof resolveCompatibility>[0],
  driver: ProviderDriverKind,
  version: string | null,
  t3CodeVersion = packageJson.version,
): ServerProviderCompatibilityAdvisory | undefined =>
  resolveCompatibility(policies, driver, version, t3CodeVersion);

export function applyProviderCompatibility(
  snapshot: ServerProvider,
  policies: ReadonlyArray<ProviderCompatibilityPolicy> | undefined,
  fallback: ReadonlyArray<ProviderCompatibilityPolicy> | undefined,
): ServerProvider {
  const { compatibilityAdvisory: _previous, ...base } = snapshot;
  if (!snapshot.enabled || !snapshot.installed) return base;
  const resolve = (version: string | null) =>
    resolveProviderCompatibility(policies, snapshot.driver, version) ??
    resolveProviderCompatibility(fallback, snapshot.driver, version);
  const advisory = resolve(snapshot.version);
  const latestVersion = snapshot.versionAdvisory?.latestVersion;
  const latestAdvisory = latestVersion ? resolve(latestVersion) : undefined;
  const updateCommand = snapshot.versionAdvisory?.updateCommand;
  // The maintenance resolver emits this command only for confirmed Homebrew installs.
  const needsHomebrewRefresh =
    updateCommand?.startsWith("brew upgrade ") &&
    (advisory?.status === "unsupported" || advisory?.status === "broken") &&
    (latestAdvisory?.status === "unsupported" || latestAdvisory?.status === "broken");
  return advisory
    ? {
        ...base,
        compatibilityAdvisory: {
          ...advisory,
          message: needsHomebrewRefresh
            ? `${advisory.message} Homebrew's local package metadata may be stale. Run brew update && ${updateCommand} on this environment's host, then refresh provider status.`
            : advisory.message,
          ...(latestAdvisory ? { latestVersionStatus: latestAdvisory.status } : {}),
        },
      }
    : base;
}
