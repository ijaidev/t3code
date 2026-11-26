import { useSelectedThreadWorktreePath } from "./use-thread-detail";
import { useThreadSelection } from "./use-thread-selection";
import { resolvePreferredThreadWorktreePath } from "../features/terminal/terminalLaunchContext";
import { resolveSelectedThreadWorkspace } from "./selected-thread-workspace";

export function useSelectedThreadWorktree() {
  const { selectedThread, selectedThreadProject } = useThreadSelection();
  const detailWorktreePath = useSelectedThreadWorktreePath();

  const cloudWorkspacePath = selectedThread?.cloudWorkspace?.workspacePath ?? null;
  const selectedThreadWorktreePath = resolvePreferredThreadWorktreePath({
    threadShellWorktreePath: selectedThread?.worktreePath ?? null,
    threadDetailWorktreePath: detailWorktreePath,
  });

  return {
    selectedThreadWorktreePath: cloudWorkspacePath ?? selectedThreadWorktreePath,
    selectedThreadCwd: resolveSelectedThreadWorkspace({
      cloudWorkspacePath,
      shellWorktreePath: selectedThread?.worktreePath ?? null,
      detailWorktreePath,
      projectWorkspaceRoot: selectedThreadProject?.workspaceRoot ?? null,
    }),
  };
}
