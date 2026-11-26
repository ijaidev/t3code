// @effect-diagnostics nodeBuiltinImport:off -- this integration test runs local Git against a temporary checkout.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import {
  SandboxInstanceId,
  SandboxProviderAccountId,
  SandboxProviderKind,
  ThreadId,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";

import * as CloudReview from "./CloudReview.ts";
import type * as CloudThreadStore from "./CloudThreadStore.ts";
import * as CloudThreads from "./CloudThreads.ts";

const record: CloudThreadStore.CloudThreadRecord = {
  threadId: ThreadId.make("thread-cloud-review"),
  sandboxId: SandboxInstanceId.make("sandbox-review"),
  accountId: SandboxProviderAccountId.make("e2b-work"),
  providerKind: SandboxProviderKind.make("e2b"),
  repositoryUrl: "https://github.com/acme/private.git",
  branch: "feature",
  baseRef: "main",
  workspacePath: "/home/t3/.t3/threads/thread-cloud-review",
  homePath: "/home/t3",
  status: "ready",
  createdAt: "2026-11-24T00:00:00Z",
  updatedAt: "2026-11-24T00:00:00Z",
};
const sha = "a".repeat(40);

const service = (
  calls: Ref.Ref<ReadonlyArray<string>>,
  activeRecord: CloudThreadStore.CloudThreadRecord = record,
) =>
  CloudThreads.CloudThreads.of({
    prepare: () => Effect.die("unused"),
    getSandbox: () => Effect.succeedNone,
    getRecord: () => Effect.succeedSome(activeRecord),
    forWorkspace: (cwd) =>
      Effect.succeed(
        cwd === activeRecord.workspacePath ? Option.some(activeRecord) : Option.none(),
      ),
    exec: (input) =>
      Effect.gen(function* () {
        yield* Ref.update(calls, (current) => [...current, input.command]);
        if (input.command.includes("git merge-base")) {
          return { exitCode: 0, stdout: `${sha}\n`, stderr: "" };
        }
        if (input.command.includes("--name-status")) {
          return { exitCode: 0, stdout: "M\0src/a.ts\0", stderr: "" };
        }
        if (input.command.includes("--numstat")) {
          return { exitCode: 0, stdout: "2\t1\tsrc/a.ts\0", stderr: "" };
        }
        if (input.command.includes("--patch")) {
          return {
            exitCode: 0,
            stdout: "diff --git a/src/a.ts b/src/a.ts\n",
            stderr: "",
          };
        }
        if (input.command.includes("--format=%H")) {
          return {
            exitCode: 0,
            stdout: `${sha}\0Cloud commit\0${"2026-11-24T00:00:00Z"}\0`,
            stderr: "",
          };
        }
        if (input.command.includes("git rev-parse --verify --quiet HEAD")) {
          return { exitCode: 0, stdout: `${sha}\n`, stderr: "" };
        }
        if (input.command.includes("git show -s")) {
          return { exitCode: 0, stdout: "Cloud commit\n", stderr: "" };
        }
        if (input.command.includes("git show ")) {
          return { exitCode: 0, stdout: "old contents\n", stderr: "" };
        }
        if (input.command.includes("cat ")) {
          return { exitCode: 0, stdout: "new contents\n", stderr: "" };
        }
        return { exitCode: 0, stdout: `${sha}\n`, stderr: "" };
      }),
    writeAttachment: () => Effect.succeed(false),
    openHost: () => Effect.die("unused"),
    deleteForThread: () => Effect.void,
  });

const layer = (
  calls: Ref.Ref<ReadonlyArray<string>>,
  activeRecord: CloudThreadStore.CloudThreadRecord = record,
) =>
  CloudReview.layer.pipe(
    Layer.provideMerge(Layer.succeed(CloudThreads.CloudThreads, service(calls, activeRecord))),
  );

describe("cloud review", () => {
  it.effect("reads branch commits, diffs, and file contents through the sandbox", () => {
    const scenario = Effect.gen(function* () {
      const calls = yield* Ref.make<ReadonlyArray<string>>([]);
      return yield* Effect.gen(function* () {
        const review = yield* CloudReview.CloudReview;
        const commits = yield* review.getBranchCommits({
          cwd: record.workspacePath,
          baseRef: "main",
        });
        expect(commits.commits[0]?.subject).toBe("Cloud commit");

        const preview = yield* review.getDiffPreview({
          cwd: record.workspacePath,
          baseRef: "main",
          sourceKind: "branch-range",
        });
        expect(preview.sources[0]?.kind).toBe("branch-range");
        expect(preview.sources[0]?.files?.[0]).toMatchObject({
          path: "src/a.ts",
          additions: 2,
          deletions: 1,
        });
        expect(preview.sources[0]?.diff).toContain("diff --git");

        const contents = yield* review.getDiffFileContents({
          cwd: record.workspacePath,
          sourceKind: "working-tree",
          changeType: "change",
          baseRef: "HEAD",
          headRef: null,
          oldPath: "src/a.ts",
          newPath: "src/a.ts",
        });
        expect(contents).toEqual({ oldContents: "old contents\n", newContents: "new contents\n" });

        const commands = yield* Ref.get(calls);
        expect(commands.length).toBeGreaterThan(0);
      }).pipe(Effect.provide(layer(calls)));
    });
    return scenario;
  });

  it.effect("runs its Git commands in a real sandbox checkout", () => {
    const scenario = Effect.gen(function* () {
      const repo = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-cloud-review-"));
      const git = (...args: string[]) =>
        NodeChildProcess.execFileSync("git", ["-c", "commit.gpgsign=false", ...args], {
          cwd: repo,
          encoding: "utf8",
          stdio: "pipe",
        });
      try {
        git("init", "--initial-branch=main");
        git(
          "-c",
          "user.name=Test",
          "-c",
          "user.email=test@example.com",
          "commit",
          "--allow-empty",
          "-m",
          "initial",
        );
        NodeFS.writeFileSync(NodePath.join(repo, "file.txt"), "old\n");
        git("add", "file.txt");
        git(
          "-c",
          "user.name=Test",
          "-c",
          "user.email=test@example.com",
          "commit",
          "-m",
          "add file",
        );
        NodeFS.writeFileSync(NodePath.join(repo, "file.txt"), "new\n");
        const calls = yield* Ref.make<ReadonlyArray<string>>([]);
        const realRecord = { ...record, workspacePath: repo };
        const review = yield* CloudReview.CloudReview.pipe(
          Effect.provide(
            CloudReview.layer.pipe(
              Layer.provideMerge(
                Layer.succeed(CloudThreads.CloudThreads, {
                  ...service(calls, realRecord),
                  exec: (input) =>
                    Effect.sync(() => {
                      try {
                        const stdout = NodeChildProcess.execFileSync(
                          "/bin/sh",
                          ["-lc", input.command],
                          {
                            cwd: input.cwd ?? repo,
                            encoding: "utf8",
                            stdio: "pipe",
                          },
                        );
                        return { exitCode: 0, stdout, stderr: "" };
                      } catch (cause) {
                        const error = cause as {
                          readonly status?: number;
                          readonly stdout?: string;
                          readonly stderr?: string;
                        };
                        return {
                          exitCode: error.status ?? 1,
                          stdout: String(error.stdout ?? ""),
                          stderr: String(error.stderr ?? ""),
                        };
                      }
                    }),
                }),
              ),
            ),
          ),
        );
        const preview = yield* review.getDiffPreview({
          cwd: repo,
          sourceKind: "working-tree",
        });
        expect(preview.sources[0]?.files?.some((file) => file.path === "file.txt")).toBe(true);
        expect(preview.sources[0]?.diff).toContain("-old");
        expect(preview.sources[0]?.diff).toContain("+new");
        const contents = yield* review.getDiffFileContents({
          cwd: repo,
          sourceKind: "working-tree",
          changeType: "change",
          baseRef: "HEAD",
          headRef: null,
          oldPath: "file.txt",
          newPath: "file.txt",
        });
        expect(contents).toEqual({ oldContents: "old\n", newContents: "new\n" });
      } finally {
        NodeFS.rmSync(repo, { recursive: true, force: true });
      }
    });
    return scenario;
  });

  it.effect("rejects a workspace that is not a recorded cloud checkout", () => {
    const scenario = Effect.gen(function* () {
      const calls = yield* Ref.make<ReadonlyArray<string>>([]);
      return yield* Effect.gen(function* () {
        const review = yield* CloudReview.CloudReview;
        const error = yield* review
          .getDiffPreview({ cwd: "/repo", sourceKind: "working-tree" })
          .pipe(Effect.flip);
        expect(error._tag).toBe("GitCommandError");
        if (error._tag === "GitCommandError") {
          expect(error.detail).toContain("not a recorded cloud thread checkout");
        }
      }).pipe(Effect.provide(layer(calls)));
    });
    return scenario;
  });
});
