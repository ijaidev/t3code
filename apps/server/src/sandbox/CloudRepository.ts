import type { ProjectId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import type * as GitWorkflow from "../git/GitWorkflowService.ts";
import type * as ProjectService from "../project/ProjectService.ts";

export type PublicCloneUrl =
  | { readonly type: "url"; readonly url: string }
  | { readonly type: "credentials" };

/**
 * A public clone URL for a remote. SSH remotes still clone over HTTPS without keys, and a
 * remote that embeds a credential is refused so the secret never reaches a client or sandbox.
 */
export function publicCloneUrl(remoteUrl: string): PublicCloneUrl {
  const trimmed = remoteUrl.trim();
  const parsed = URL.parse(trimmed);
  if (parsed !== null) {
    if (parsed.username !== "" || parsed.password !== "") return { type: "credentials" };
    if (parsed.protocol === "http:" || parsed.protocol === "https:") {
      parsed.username = "";
      parsed.password = "";
      parsed.search = "";
      parsed.hash = "";
      return { type: "url", url: parsed.toString() };
    }
    if (parsed.protocol === "ssh:" || parsed.protocol === "git+ssh:") {
      return { type: "url", url: `https://${parsed.host}${parsed.pathname}` };
    }
    return { type: "credentials" };
  }
  const scp = /^(?:(?<user>[^@/]+)@)?(?<host>[^:/]+):(?<path>.+)$/.exec(trimmed);
  if (scp?.groups === undefined) return { type: "credentials" };
  const user = scp.groups["user"];
  // `git@host:path` and a bare `host:path` are public clone forms; any other user is a credential.
  if (user !== undefined && user !== "git" && user !== "hg") return { type: "credentials" };
  return {
    type: "url",
    url: `https://${scp.groups["host"]}/${scp.groups["path"]}`,
  };
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
  return publicCloneUrl(remote.url);
});
