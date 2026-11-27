import { describe, expect, it } from "vite-plus/test";

import { resolveActiveProjectTargetCwd } from "./useActiveProjectTarget";

describe("resolveActiveProjectTargetCwd", () => {
  it("targets the sandbox checkout for a cloud thread", () => {
    expect(
      resolveActiveProjectTargetCwd({
        cloudWorkspacePath: "/home/t3/.t3/threads/thread-cloud",
        worktreePath: null,
        projectWorkspaceRoot: "/repo",
      }),
    ).toBe("/home/t3/.t3/threads/thread-cloud");
  });

  it("keeps worktree and project precedence for local threads", () => {
    expect(
      resolveActiveProjectTargetCwd({
        cloudWorkspacePath: null,
        worktreePath: "/repo-worktree",
        projectWorkspaceRoot: "/repo",
      }),
    ).toBe("/repo-worktree");
    expect(
      resolveActiveProjectTargetCwd({
        cloudWorkspacePath: null,
        worktreePath: null,
        projectWorkspaceRoot: "/repo",
      }),
    ).toBe("/repo");
  });
});
