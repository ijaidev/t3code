import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import type { EnvironmentId, ScopedThreadRef } from "@t3tools/contracts";

import { useProjects } from "~/state/entities";

import { useHandleNewThread } from "./useHandleNewThread";

export interface ActiveProjectTarget {
  readonly environmentId: EnvironmentId;
  readonly cwd: string;
  readonly projectName: string;
  readonly threadRef: ScopedThreadRef;
}

export function resolveActiveProjectTargetCwd(input: {
  readonly cloudWorkspacePath: string | null;
  readonly worktreePath: string | null;
  readonly projectWorkspaceRoot: string | null;
}): string | null {
  return input.cloudWorkspacePath ?? input.worktreePath ?? input.projectWorkspaceRoot;
}

export function useActiveProjectTarget(): ActiveProjectTarget | null {
  const { activeDraftThread, activeThread } = useHandleNewThread();
  const projects = useProjects();
  const thread = activeThread ?? activeDraftThread;
  const threadId = activeThread?.id ?? activeDraftThread?.threadId;
  const project = thread
    ? projects.find(
        (candidate) =>
          candidate.environmentId === thread.environmentId && candidate.id === thread.projectId,
      )
    : null;
  const cwd = resolveActiveProjectTargetCwd({
    cloudWorkspacePath: activeThread?.cloudWorkspace?.workspacePath ?? null,
    worktreePath: thread?.worktreePath ?? null,
    projectWorkspaceRoot: project?.workspaceRoot ?? null,
  });

  if (!thread || !threadId || !project || !cwd) return null;

  return {
    environmentId: project.environmentId,
    cwd,
    projectName: project.title,
    threadRef: scopeThreadRef(thread.environmentId, threadId),
  };
}
