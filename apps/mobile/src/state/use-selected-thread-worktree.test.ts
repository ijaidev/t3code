import { describe, expect, it } from "vite-plus/test";

import { resolveSelectedThreadWorkspace } from "./selected-thread-workspace";

describe("resolveSelectedThreadWorkspace", () => {
  it("uses the sandbox checkout for a cloud thread", () => {
    expect(
      resolveSelectedThreadWorkspace({
        cloudWorkspacePath: "/home/t3/.t3/threads/thread-1",
        shellWorktreePath: null,
        detailWorktreePath: null,
        projectWorkspaceRoot: "/repo",
      }),
    ).toBe("/home/t3/.t3/threads/thread-1");
  });

  it("keeps local worktrees and project fallbacks", () => {
    expect(
      resolveSelectedThreadWorkspace({
        cloudWorkspacePath: null,
        shellWorktreePath: "/repo-worktree",
        detailWorktreePath: null,
        projectWorkspaceRoot: "/repo",
      }),
    ).toBe("/repo-worktree");
    expect(
      resolveSelectedThreadWorkspace({
        cloudWorkspacePath: null,
        shellWorktreePath: null,
        detailWorktreePath: null,
        projectWorkspaceRoot: "/repo",
      }),
    ).toBe("/repo");
  });
});
