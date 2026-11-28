import {
  ServerProviderCompatibilityStatus,
  TrimmedNonEmptyString,
  type ProviderDriverKind,
  type ServerProviderCompatibilityAdvisory,
} from "@t3tools/contracts";
import { satisfiesSemverRange } from "@t3tools/shared/semver";
import * as Schema from "effect/Schema";

const StableVersion = TrimmedNonEmptyString.pipe(
  Schema.check(Schema.makeFilter((value) => /^\d+\.\d+\.\d+$/.test(value))),
);
const VersionRange = TrimmedNonEmptyString.pipe(
  Schema.check(
    Schema.makeFilter((value) =>
      value.split("||").every((group) => {
        const tokens = group.trim().split(/\s+/);
        return tokens.every((token) => /^(?:\^|>=|>|<=|<|=)?v?\d+(?:\.\d+){0,2}$/.test(token));
      }),
    ),
  ),
);
const compilePattern = (pattern: string) => {
  try {
    return new RegExp(pattern, "u");
  } catch {
    return undefined;
  }
};
const Policy = Schema.Struct({
  driver: TrimmedNonEmptyString,
  t3CodeRange: VersionRange,
  /** Matches a reported version that is not plain `X.Y.Z`; its first group is the version to rate. */
  versionPattern: Schema.optionalKey(
    TrimmedNonEmptyString.pipe(
      Schema.check(Schema.makeFilter((value) => compilePattern(value) !== undefined)),
    ),
  ),
  recommendedRange: Schema.optionalKey(VersionRange),
  recommendedVersion: Schema.optionalKey(StableVersion),
  ranges: Schema.Array(
    Schema.Struct({
      range: VersionRange,
      status: ServerProviderCompatibilityStatus,
    }),
  ),
});

export const ProviderCompatibilityPolicy = Policy.pipe(
  Schema.check(
    Schema.makeFilter(
      (policy) => {
        const version = policy.recommendedVersion;
        if (version === undefined) return true;
        return (
          (policy.recommendedRange === undefined ||
            satisfiesSemverRange(version, policy.recommendedRange)) &&
          policy.ranges.find((entry) => satisfiesSemverRange(version, entry.range))?.status ===
            "supported"
        );
      },
      { expected: "a recommended version in a supported range" },
    ),
  ),
);
export type ProviderCompatibilityPolicy = typeof ProviderCompatibilityPolicy.Type;

export function resolveProviderCompatibility(
  policies: ReadonlyArray<ProviderCompatibilityPolicy> | undefined,
  driver: ProviderDriverKind,
  version: string | null,
  t3CodeVersion: string,
): ServerProviderCompatibilityAdvisory | undefined {
  const policy = policies?.find(
    (entry) => entry.driver === driver && satisfiesSemverRange(t3CodeVersion, entry.t3CodeRange),
  );
  if (!policy) return undefined;
  const unprefixed = version?.replace(/^v/, "");
  // Without a pattern, build hashes, revisions and prereleases stay unknown.
  const pattern = policy.versionPattern ? compilePattern(policy.versionPattern) : undefined;
  const stable = (pattern && unprefixed?.match(pattern)?.[1]) || unprefixed;
  const status =
    stable && /^\d+\.\d+\.\d+$/.test(stable)
      ? (policy.ranges.find((entry) => satisfiesSemverRange(stable, entry.range))?.status ??
        "unknown")
      : "unknown";
  const message =
    status === "broken"
      ? "This provider version is known to be incompatible with this T3 Code release."
      : status === "unsupported"
        ? "This provider version is outside the supported range for this T3 Code release."
        : status === "graceful"
          ? "This provider version has limited compatibility with this T3 Code release."
          : null;
  const recommendedVersion = policy.recommendedVersion ?? null;
  const recommendedRange = policy.recommendedRange ?? null;
  const recommendation = recommendedVersion ?? recommendedRange;
  return {
    status,
    message: message && recommendation ? `${message} Use ${recommendation}.` : message,
    recommendedVersion,
    recommendedRange,
  };
}
