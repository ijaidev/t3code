// @effect-diagnostics-next-line nodeBuiltinImport:off -- the test drives real Git with a local bare repository.
import * as NodeChildProcess from "node:child_process";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- the test creates a temporary repository fixture.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- the test builds fixture paths.
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";

import { expect, it } from "@effect/vitest";

import { cloudCloneCommand } from "./CloudThreads.ts";

const exec = NodeUtil.promisify(NodeChildProcess.execFile);

const git = async (cwd: string, args: ReadonlyArray<string>) => {
  const result = await exec("git", [...args], { cwd });
  return result.stdout.trim();
};

const write = async (path: string, contents: string) => {
  await NodeFSP.mkdir(NodePath.dirname(path), { recursive: true });
  await NodeFSP.writeFile(path, contents, "utf8");
};

const seedRepository = async (root: string, bareName: string) => {
  const bare = NodePath.join(root, bareName);
  const seed = NodePath.join(root, `${bareName}-seed`);
  await NodeFSP.mkdir(bare, { recursive: true });
  await git(bare, ["init", "--bare"]);
  await NodeFSP.mkdir(seed, { recursive: true });
  await git(seed, ["init"]);
  await git(seed, ["config", "user.email", "test@example.com"]);
  await git(seed, ["config", "user.name", "Test"]);
  await git(seed, ["config", "commit.gpgsign", "false"]);
  await write(NodePath.join(seed, "README.md"), "base\n");
  await git(seed, ["add", "README.md"]);
  await git(seed, ["commit", "-m", "base"]);
  await git(seed, ["branch", "-M", "main"]);
  await git(seed, ["remote", "add", "origin", bare]);
  await git(seed, ["push", "origin", "main"]);
  const mainSha = await git(seed, ["rev-parse", "HEAD"]);
  return { bare, seed, mainSha };
};

it("clones the pushed thread branch when recreating a sandbox", async () => {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-cloud-clone-"));
  try {
    const { bare, seed, mainSha } = await seedRepository(root, "remote.git");
    await git(seed, ["checkout", "-b", "t3/feature"]);
    await write(NodePath.join(seed, "feature.txt"), "feature\n");
    await git(seed, ["add", "feature.txt"]);
    await git(seed, ["commit", "-m", "feature"]);
    await git(seed, ["push", "origin", "t3/feature"]);
    const featureSha = await git(seed, ["rev-parse", "HEAD"]);

    const workspace = NodePath.join(root, "workspace");
    await exec(
      "sh",
      [
        "-c",
        cloudCloneCommand({
          cloneUrl: bare,
          baseRef: "refs/remotes/origin/main",
          branch: "t3/feature",
          workspacePath: workspace,
          startFrom: "branch",
        }),
      ],
      { cwd: root },
    );
    expect(await git(workspace, ["rev-parse", "--abbrev-ref", "HEAD"])).toBe("t3/feature");
    expect(await git(workspace, ["rev-parse", "HEAD"])).toBe(featureSha);
    expect(await git(workspace, ["rev-parse", "HEAD"])).not.toBe(mainSha);
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true });
  }
});

it("falls back to the base ref on the thread branch when it was never pushed", async () => {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-cloud-clone-"));
  try {
    const { bare, mainSha } = await seedRepository(root, "remote.git");
    const workspace = NodePath.join(root, "workspace");
    await exec(
      "sh",
      [
        "-c",
        cloudCloneCommand({
          cloneUrl: bare,
          baseRef: "refs/remotes/origin/main",
          branch: "t3/feature",
          workspacePath: workspace,
          startFrom: "branch",
        }),
      ],
      { cwd: root },
    );
    expect(await git(workspace, ["rev-parse", "--abbrev-ref", "HEAD"])).toBe("t3/feature");
    expect(await git(workspace, ["rev-parse", "HEAD"])).toBe(mainSha);
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true });
  }
});
