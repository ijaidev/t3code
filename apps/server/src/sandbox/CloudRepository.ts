import type { ProjectId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import type * as GitWorkflow from "../git/GitWorkflowService.ts";
import type * as ProjectService from "../project/ProjectService.ts";

/** A public clone URL for a remote; SSH remotes still clone over HTTPS without keys. */
function publicCloneUrl(remoteUrl: string): string {
  const trimmed = remoteUrl.trim();
  const scp = /^(?:[^@/]+@)?([^:/]+):(.+)$/.exec(trimmed);
  if (scp !== null && !trimmed.includes("://")) {
    return `https://${scp[1]}/${scp[2]}`;
  }
  const ssh = /^ssh:\/\/(?:[^@/]+@)?([^/]+)\/(.+)$/.exec(trimmed);
  if (ssh !== null) {
    return `https://${ssh[1]}/${ssh[2]}`;
  }
  return trimmed;
}

export const projectPublicRemote = Effect.fn("cloudThreads.projectPublicRemote")(function* (
  projects: ProjectService.ProjectService["Service"],
  git: GitWorkflow.GitWorkflowService["Service"],
  projectId: ProjectId,
) {
  const project = yield* projects
    .getById(projectId)
    .pipe(Effect.orElseSucceed(() => Option.none()));
  if (Option.isNone(project)) return null;
  const remotes = yield* git
    .listRemotes(project.value.workspaceRoot)
    .pipe(Effect.orElseSucceed(() => ({ remotes: [] as const })));
  const remote =
    remotes.remotes.find((candidate) => candidate.isPrimary) ??
    remotes.remotes.find((candidate) => candidate.name === "origin") ??
    remotes.remotes[0];
  if (remote === undefined) return null;
  return { remoteUrl: remote.url, cloneUrl: publicCloneUrl(remote.url) };
});
