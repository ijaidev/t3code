import { VcsProcessExitError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import type * as VcsDriver from "../vcs/VcsDriver.ts";
import * as CloudThreads from "./CloudThreads.ts";

const CHECKPOINT_IDENTITY = {
  GIT_AUTHOR_NAME: "T3 Code",
  GIT_AUTHOR_EMAIL: "t3code@users.noreply.github.com",
  GIT_COMMITTER_NAME: "T3 Code",
  GIT_COMMITTER_EMAIL: "t3code@users.noreply.github.com",
} as const;

const shellQuote = (value: string) => `'${value.replaceAll("'", `'\\''`)}'`;

const gitFailure = (input: {
  readonly operation: string;
  readonly cwd: string;
  readonly command: string;
  readonly exitCode: number;
  readonly detail: string;
}) =>
  new VcsProcessExitError({
    operation: input.operation,
    cwd: input.cwd,
    command: input.command,
    exitCode: input.exitCode,
    detail: input.detail,
  });

/**
 * The subset of the local checkpoint driver that cloud workspaces need, run through the
 * sandbox's own Git. Every ref and object stays inside the sandbox checkout.
 */
export const makeCloudCheckpointOps = (
  cloudThreads: CloudThreads.CloudThreads["Service"],
): VcsDriver.VcsCheckpointOps => {
  const exec = Effect.fn("CloudCheckpointOps.exec")(function* (
    operation: string,
    cwd: string,
    command: string,
    env: Readonly<Record<string, string>> | undefined,
    timeoutMs: number | undefined,
  ) {
    const record = yield* cloudThreads.forWorkspace(cwd);
    if (Option.isNone(record)) {
      return yield* gitFailure({
        operation,
        cwd,
        command: "git",
        exitCode: 1,
        detail: "This cloud workspace is not recorded.",
      });
    }
    const result = yield* cloudThreads
      .exec({
        threadId: record.value.threadId,
        command,
        cwd,
        ...(env === undefined ? {} : { envs: env }),
        ...(timeoutMs === undefined ? {} : { timeoutMs }),
      })
      .pipe(
        Effect.mapError((error) =>
          gitFailure({
            operation,
            cwd,
            command: "git",
            exitCode: 1,
            detail: error.detail,
          }),
        ),
      );
    return result;
  });

  const captureCheckpoint: VcsDriver.VcsCheckpointOps["captureCheckpoint"] = Effect.fn(
    "CloudCheckpointOps.captureCheckpoint",
  )(function* (input) {
    const exclusions = (input.excludePaths ?? []).map(
      (path) => `'${`:(exclude,literal)${path}`.replaceAll("'", `'\\''`)}'`,
    );
    const script = [
      "set -eu",
      'index=$(mktemp "${TMPDIR:-/tmp}/t3-checkpoint-index.XXXXXX")',
      "trap 'rm -f \"$index\"' EXIT",
      'export GIT_INDEX_FILE="$index"',
      "if git rev-parse --verify -q HEAD >/dev/null; then git read-tree HEAD; fi",
      `git add -A -- . ${exclusions.join(" ")}`,
      "tree=$(git write-tree)",
      "if git rev-parse --verify -q HEAD >/dev/null; then",
      `  commit=$(git commit-tree "$tree" -p HEAD -m ${shellQuote(`t3 checkpoint ref=${input.checkpointRef}`)})`,
      "else",
      `  commit=$(git commit-tree "$tree" -m ${shellQuote(`t3 checkpoint ref=${input.checkpointRef}`)})`,
      "fi",
      `git update-ref ${shellQuote(input.checkpointRef)} "$commit"`,
    ].join("\n");
    const result = yield* exec(
      "CloudCheckpointOps.captureCheckpoint",
      input.cwd,
      script,
      CHECKPOINT_IDENTITY,
      120_000,
    );
    if (result.exitCode !== 0) {
      return yield* gitFailure({
        operation: "CloudCheckpointOps.captureCheckpoint",
        cwd: input.cwd,
        command: "git add",
        exitCode: result.exitCode,
        detail: result.stderr.trim() || result.stdout.trim() || "The sandbox could not capture.",
      });
    }
  });

  const resolveCleanHead: VcsDriver.VcsCheckpointOps["resolveCleanHead"] = Effect.fn(
    "CloudCheckpointOps.resolveCleanHead",
  )(function* (cwd) {
    const result = yield* exec(
      "CloudCheckpointOps.resolveCleanHead",
      cwd,
      "git status --porcelain=v2 --branch --untracked-files=normal --ignore-submodules=none",
      { GIT_OPTIONAL_LOCKS: "0" },
      30_000,
    );
    if (result.exitCode !== 0) return null;
    const lines = result.stdout.split("\n").filter((line) => line.length > 0);
    if (lines.some((line) => !line.startsWith("# "))) return null;
    const head = lines.find((line) => line.startsWith("# branch.oid "))?.slice(13);
    if (head === undefined || !/^[0-9a-f]{40,64}$/.test(head)) return null;
    return head;
  });

  const captureHeadCheckpoint: VcsDriver.VcsCheckpointOps["captureHeadCheckpoint"] = Effect.fn(
    "CloudCheckpointOps.captureHeadCheckpoint",
  )(function* (input) {
    const script = [
      "set -eu",
      `commit=$(git commit-tree ${shellQuote(`${input.head}^{tree}`)} -p ${shellQuote(input.head)} -m ${shellQuote(`t3 checkpoint ref=${input.checkpointRef}`)})`,
      `git update-ref ${shellQuote(input.checkpointRef)} "$commit"`,
    ].join("\n");
    const result = yield* exec(
      "CloudCheckpointOps.captureHeadCheckpoint",
      input.cwd,
      script,
      CHECKPOINT_IDENTITY,
      60_000,
    );
    if (result.exitCode !== 0) {
      return yield* gitFailure({
        operation: "CloudCheckpointOps.captureHeadCheckpoint",
        cwd: input.cwd,
        command: "git commit-tree",
        exitCode: result.exitCode,
        detail: result.stderr.trim() || "The sandbox could not capture HEAD.",
      });
    }
  });

  const hasCheckpointRef: VcsDriver.VcsCheckpointOps["hasCheckpointRef"] = (input) =>
    exec(
      "CloudCheckpointOps.hasCheckpointRef",
      input.cwd,
      `git rev-parse --verify --quiet ${shellQuote(`${input.checkpointRef}^{commit}`)}`,
      undefined,
      30_000,
    ).pipe(Effect.map((result) => result.exitCode === 0));

  const restoreCheckpoint: VcsDriver.VcsCheckpointOps["restoreCheckpoint"] = Effect.fn(
    "CloudCheckpointOps.restoreCheckpoint",
  )(function* (input) {
    const exclusions = (input.excludePaths ?? []).map((path) => `:(exclude,literal)${path}`);
    const pathspec = ["--", ".", ...exclusions].map(shellQuote).join(" ");
    const script = [
      "set -eu",
      `if ! git rev-parse --verify -q ${shellQuote(`${input.checkpointRef}^{commit}`)} >/dev/null; then`,
      ...(input.fallbackToHead === true
        ? ["  git rev-parse --verify -q HEAD >/dev/null"]
        : ["  exit 3"]),
      "fi",
      `source=${shellQuote(`${input.checkpointRef}^{commit}`)}`,
      `if ! git rev-parse --verify -q "$source" >/dev/null; then source=HEAD; fi`,
      `git reset --quiet ${pathspec}`,
      `git restore --source "$source" --worktree --staged ${pathspec}`,
      `git clean -fd -- . ${exclusions.map(shellQuote).join(" ")}`,
    ].join("\n");
    const result = yield* exec(
      "CloudCheckpointOps.restoreCheckpoint",
      input.cwd,
      script,
      undefined,
      120_000,
    );
    if (result.exitCode === 3) return false;
    if (result.exitCode !== 0) {
      return yield* gitFailure({
        operation: "CloudCheckpointOps.restoreCheckpoint",
        cwd: input.cwd,
        command: "git restore",
        exitCode: result.exitCode,
        detail: result.stderr.trim() || "The sandbox could not restore the checkpoint.",
      });
    }
    return true;
  });

  const diffCheckpoints: VcsDriver.VcsCheckpointOps["diffCheckpoints"] = Effect.fn(
    "CloudCheckpointOps.diffCheckpoints",
  )(function* (input) {
    if (input.filePaths?.length === 0) return "";
    const exclusions = (input.excludePaths ?? []).map((path) => `:(exclude,literal)${path}`);
    const paths =
      input.filePaths === undefined
        ? exclusions.length === 0
          ? []
          : ["--", ":(top)", ...exclusions]
        : ["--", ...input.filePaths.map((file) => `:(top,literal)${file}`), ...exclusions];
    const args = [
      "git",
      "-c",
      "diff.relative=false",
      "-c",
      "diff.suppressBlankEmpty=false",
      "diff",
      ...(input.format === "numstat" ? ["--numstat", "-z"] : ["--patch"]),
      "--no-color",
      "--no-ext-diff",
      "--no-textconv",
      "--submodule=short",
      ...(input.ignoreWhitespace ? ["--ignore-all-space"] : []),
      `${input.fromCheckpointRef}^{commit}`,
      `${input.toCheckpointRef}^{commit}`,
      ...paths,
    ]
      .map(shellQuote)
      .join(" ");
    const maxBytes = input.maxOutputBytes;
    const command =
      maxBytes === undefined
        ? args
        : [
            "set -eu",
            'out=$(mktemp "${TMPDIR:-/tmp}/t3-diff.XXXXXX")',
            "trap 'rm -f \"$out\"' EXIT",
            `${args} > "$out"`,
            `head -c ${Math.max(1, maxBytes)} "$out"`,
          ].join("\n");
    const result = yield* exec(
      "CloudCheckpointOps.diffCheckpoints",
      input.cwd,
      command,
      undefined,
      120_000,
    );
    if (result.exitCode !== 0) {
      return yield* gitFailure({
        operation: "CloudCheckpointOps.diffCheckpoints",
        cwd: input.cwd,
        command: "git diff",
        exitCode: result.exitCode,
        detail: result.stderr.trim() || "The sandbox could not diff the checkpoints.",
      });
    }
    return result.stdout;
  });

  const listAuthoredPaths: VcsDriver.VcsCheckpointOps["listAuthoredPaths"] = () =>
    Effect.succeed(null);

  const deleteCheckpointRefs: VcsDriver.VcsCheckpointOps["deleteCheckpointRefs"] = (input) => {
    if (input.checkpointRefs.length === 0) return Effect.void;
    const command = input.checkpointRefs
      .map((ref) => `git update-ref -d ${shellQuote(ref)}`)
      .join("\n");
    return exec(
      "CloudCheckpointOps.deleteCheckpointRefs",
      input.cwd,
      command,
      undefined,
      60_000,
    ).pipe(
      Effect.flatMap((result) =>
        result.exitCode === 0
          ? Effect.void
          : gitFailure({
              operation: "CloudCheckpointOps.deleteCheckpointRefs",
              cwd: input.cwd,
              command: "git update-ref -d",
              exitCode: result.exitCode,
              detail:
                result.stderr.trim() ||
                result.stdout.trim() ||
                "The sandbox could not delete checkpoint refs.",
            }),
      ),
    );
  };

  return {
    captureCheckpoint,
    resolveCleanHead,
    captureHeadCheckpoint,
    hasCheckpointRef,
    restoreCheckpoint,
    diffCheckpoints,
    listAuthoredPaths,
    deleteCheckpointRefs,
  } satisfies VcsDriver.VcsCheckpointOps;
};
