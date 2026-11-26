import type { EnvironmentId } from "@t3tools/contracts";

export type ReviewGitSectionKind = "working-tree" | "branch-range";

export function reviewDiffPreviewRequest(
  environmentId: EnvironmentId,
  cwd: string,
  sourceKind: ReviewGitSectionKind,
) {
  return { environmentId, input: { cwd, sourceKind } };
}
