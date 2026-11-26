/** A cloud thread's checkout is its sandbox workspace, never the local project root. */
export function resolveSelectedThreadWorkspace(input: {
  readonly cloudWorkspacePath: string | null;
  readonly shellWorktreePath: string | null;
  readonly detailWorktreePath: string | null;
  readonly projectWorkspaceRoot: string | null;
}) {
  const worktreePath = input.detailWorktreePath ?? input.shellWorktreePath ?? null;
  return input.cloudWorkspacePath ?? worktreePath ?? input.projectWorkspaceRoot ?? null;
}
