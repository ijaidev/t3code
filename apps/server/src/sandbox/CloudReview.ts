// @effect-diagnostics-next-line nodeBuiltinImport:off -- review hashes are server-local.
import * as NodeCrypto from "node:crypto";

import {
  GitCommandError,
  type ReviewBranchCommitsInput,
  type ReviewBranchCommitsResult,
  type ReviewDiffChangeType,
  type ReviewDiffFileContentsInput,
  type ReviewDiffFileContentsResult,
  type ReviewDiffFileStat,
  type ReviewDiffPreviewError,
  type ReviewDiffPreviewInput,
  type ReviewDiffPreviewResult,
  type ReviewDiffPreviewSource,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import * as CloudThreads from "./CloudThreads.ts";

const BRANCH_COMMIT_LIMIT = 50;
const PATCH_MAX_BYTES = 5_000_000;
const METADATA_MAX_BYTES = 2_000_000;

const shellQuote = (value: string) => `'${value.replaceAll("'", `'\\''`)}'`;

const firstLine = (value: string) => value.trim().split("\n")[0] ?? value.trim();

const section = (cwd: string, operation: string, detail: string, cause?: unknown) =>
  new GitCommandError({
    operation,
    command: "git",
    cwd,
    detail,
    ...(cause === undefined ? {} : { cause }),
  });

const filePaths = (input: ReviewDiffPreviewInput) =>
  input.file === undefined
    ? []
    : (input.file.previousPath === null
        ? [`:(top,literal)${input.file.path}`]
        : [`:(top,literal)${input.file.previousPath}`, `:(top,literal)${input.file.path}`]
      ).map(shellQuote);

const changeTypeFor = (status: string): ReviewDiffChangeType => {
  switch (status[0]) {
    case "A":
      return "new";
    case "D":
      return "deleted";
    case "R":
      return status[1] === "1" ? "rename-changed" : "rename-pure";
    default:
      return "change";
  }
};

const parseNameStatus = (stdout: string) => {
  const fields = stdout.split("\0");
  const files = new Map<
    string,
    { previousPath: string | null; changeType: ReviewDiffChangeType }
  >();
  for (let index = 0; index < fields.length;) {
    const status = fields[index++];
    if (status === undefined || status === "") continue;
    if (status.startsWith("R") || status.startsWith("C")) {
      const previousPath = fields[index++] ?? "";
      const path = fields[index++] ?? "";
      if (path !== "") files.set(path, { previousPath, changeType: changeTypeFor(status) });
      continue;
    }
    const path = fields[index++] ?? "";
    if (path !== "") files.set(path, { previousPath: null, changeType: changeTypeFor(status) });
  }
  return files;
};

const parseNumstat = (stdout: string) => {
  const counts = new Map<string, { additions: number; deletions: number }>();
  const fields = stdout.split("\0");
  for (let index = 0; index < fields.length; index++) {
    const field = fields[index];
    if (field === undefined || field === "") continue;
    const [additions = "0", deletions = "0", path = ""] = field.split("\t");
    if (path === "") continue;
    counts.set(path, {
      additions: Number(additions) || 0,
      deletions: Number(deletions) || 0,
    });
  }
  return counts;
};

const mergeFiles = (
  statuses: ReadonlyMap<string, { previousPath: string | null; changeType: ReviewDiffChangeType }>,
  counts: ReadonlyMap<string, { additions: number; deletions: number }>,
  untracked: ReadonlyArray<string> = [],
): ReviewDiffFileStat[] => {
  const paths = new Set([...statuses.keys(), ...counts.keys(), ...untracked]);
  return [...paths]
    .map((path) => ({
      path,
      previousPath: statuses.get(path)?.previousPath ?? null,
      ...(statuses.get(path) === undefined ? {} : { changeType: statuses.get(path)!.changeType }),
      additions: counts.get(path)?.additions ?? 0,
      deletions: counts.get(path)?.deletions ?? 0,
    }))
    .toSorted((left, right) => left.path.localeCompare(right.path));
};

export class CloudReview extends Context.Service<
  CloudReview,
  {
    readonly forWorkspace: (
      cwd: string,
    ) => Effect.Effect<Option.Option<{ readonly threadId: string }>>;
    readonly getBranchCommits: (
      input: ReviewBranchCommitsInput,
    ) => Effect.Effect<ReviewBranchCommitsResult, ReviewDiffPreviewError>;
    readonly getDiffPreview: (
      input: ReviewDiffPreviewInput,
    ) => Effect.Effect<ReviewDiffPreviewResult, ReviewDiffPreviewError>;
    readonly getDiffFileContents: (
      input: ReviewDiffFileContentsInput,
    ) => Effect.Effect<ReviewDiffFileContentsResult, ReviewDiffPreviewError>;
  }
>()("t3/sandbox/CloudReview") {}

const make = Effect.gen(function* () {
  const cloudThreads = yield* CloudThreads.CloudThreads;

  const recordFor = Effect.fn("CloudReview.recordFor")(function* (cwd: string, operation: string) {
    const record = yield* cloudThreads
      .forWorkspace(cwd)
      .pipe(Effect.orElseSucceed(() => Option.none()));
    if (Option.isNone(record)) {
      return yield* section(
        cwd,
        operation,
        "This workspace is not a recorded cloud thread checkout.",
      );
    }
    return record.value;
  });

  const exec = Effect.fn("CloudReview.exec")(function* (input: {
    readonly cwd: string;
    readonly operation: string;
    readonly command: string;
    readonly maxBytes?: number;
    readonly allowNonZero?: boolean;
  }) {
    const record = yield* recordFor(input.cwd, input.operation);
    const command =
      input.maxBytes === undefined
        ? input.command
        : [
            "set -u",
            'out=$(mktemp "${TMPDIR:-/tmp}/t3-review.XXXXXX")',
            "trap 'rm -f \"$out\"' EXIT",
            `(${input.command}) > "$out"; status=$?`,
            `head -c ${Math.max(1, input.maxBytes)} "$out"`,
            "exit $status",
          ].join("\n");
    const result = yield* cloudThreads
      .exec({
        threadId: record.threadId,
        command,
        cwd: input.cwd,
        timeoutMs: 120_000,
      })
      .pipe(Effect.mapError((error) => section(input.cwd, input.operation, error.detail, error)));
    if (result.exitCode !== 0 && input.allowNonZero !== true) {
      return yield* section(
        input.cwd,
        input.operation,
        firstLine(result.stderr || result.stdout) || "The sandbox Git command failed.",
      );
    }
    return {
      stdout: result.stdout,
      exitCode: result.exitCode,
      stderr: result.stderr,
      truncated: input.maxBytes !== undefined && Buffer.byteLength(result.stdout) >= input.maxBytes,
    };
  });

  const resolveMergeBase = Effect.fn("CloudReview.resolveMergeBase")(function* (
    cwd: string,
    baseRef: string | undefined,
  ) {
    const base = baseRef?.trim();
    const candidates =
      base === undefined || base === ""
        ? ["HEAD"]
        : base.startsWith("refs/")
          ? [base]
          : [base.startsWith("origin/") ? base : `origin/${base}`, base, "HEAD"];
    for (const candidate of candidates) {
      const result = yield* exec({
        cwd,
        operation: "CloudReview.resolveMergeBase",
        command: `git merge-base HEAD ${shellQuote(candidate)}`,
        allowNonZero: true,
      });
      const resolved = result.stdout.trim();
      if (result.exitCode === 0 && /^[0-9a-f]{40,64}$/.test(resolved)) return resolved;
    }
    return "HEAD";
  });

  const previewFiles = Effect.fn("CloudReview.previewFiles")(function* (input: {
    readonly cwd: string;
    readonly operation: string;
    readonly revisionArgs: ReadonlyArray<string>;
    readonly pathArgs: ReadonlyArray<string>;
    readonly untracked: ReadonlyArray<string>;
  }) {
    const revisions = input.revisionArgs.map(shellQuote).join(" ");
    const status = yield* exec({
      cwd: input.cwd,
      operation: `${input.operation}.status`,
      command: `git diff ${revisions} --name-status -z --no-renames -- ${input.pathArgs.join(" ")}`,
      maxBytes: METADATA_MAX_BYTES,
    });
    const numstat = yield* exec({
      cwd: input.cwd,
      operation: `${input.operation}.numstat`,
      command: `git diff ${revisions} --numstat -z --no-renames -- ${input.pathArgs.join(" ")}`,
      maxBytes: METADATA_MAX_BYTES,
    });
    return mergeFiles(
      parseNameStatus(status.stdout),
      parseNumstat(numstat.stdout),
      input.untracked,
    );
  });

  const patch = Effect.fn("CloudReview.patch")(function* (input: {
    readonly cwd: string;
    readonly operation: string;
    readonly revisionArgs: ReadonlyArray<string>;
    readonly pathArgs: ReadonlyArray<string>;
    readonly ignoreWhitespace: boolean;
  }) {
    const result = yield* exec({
      cwd: input.cwd,
      operation: input.operation,
      command: [
        "git",
        "--no-pager",
        "-c",
        "diff.relative=false",
        "-c",
        "diff.suppressBlankEmpty=false",
        "diff",
        "--patch",
        "--no-color",
        "--no-ext-diff",
        "--no-textconv",
        "--submodule=short",
        ...(input.ignoreWhitespace ? ["--ignore-all-space"] : []),
        ...input.revisionArgs,
        ...(input.pathArgs.length === 0 ? [] : ["--", ...input.pathArgs]),
      ]
        .map(shellQuote)
        .join(" "),
      maxBytes: PATCH_MAX_BYTES,
    });
    return result.stdout;
  });

  const diffHash = (diff: string, files: ReadonlyArray<ReviewDiffFileStat>) =>
    NodeCrypto.createHash("sha256")
      .update(JSON.stringify([diff, files]))
      .digest("hex");

  const getBranchCommits: CloudReview["Service"]["getBranchCommits"] = Effect.fn(
    "CloudReview.getBranchCommits",
  )(function* (input) {
    yield* recordFor(input.cwd, "CloudReview.getBranchCommits");
    const head = yield* exec({
      cwd: input.cwd,
      operation: "CloudReview.getBranchCommits.head",
      command: "git rev-parse --verify --quiet HEAD",
      allowNonZero: true,
    });
    if (head.exitCode !== 0) return { commits: [], truncated: false };
    const mergeBase = yield* resolveMergeBase(input.cwd, input.baseRef);
    const result = yield* exec({
      cwd: input.cwd,
      operation: "CloudReview.getBranchCommits",
      command: [
        "git",
        "-c",
        "log.showSignature=false",
        "log",
        "-z",
        "--no-notes",
        "--format=%H%x00%s%x00%cI",
        `--max-count=${BRANCH_COMMIT_LIMIT + 1}`,
        `${mergeBase}..HEAD`,
      ]
        .map(shellQuote)
        .join(" "),
      maxBytes: METADATA_MAX_BYTES,
    });
    const fields = result.stdout.split("\0");
    const commits: Array<{ sha: string; subject: string; committedAt: string }> = [];
    for (let index = 0; index + 2 < fields.length; index += 3) {
      const sha = fields[index] ?? "";
      const subject = fields[index + 1] ?? "";
      const committedAt = fields[index + 2] ?? "";
      if (!/^[0-9a-f]{40,64}$/i.test(sha) || committedAt === "") continue;
      commits.push({ sha, subject: subject.slice(0, 300), committedAt });
    }
    return {
      commits: commits.slice(0, BRANCH_COMMIT_LIMIT),
      truncated: commits.length > BRANCH_COMMIT_LIMIT,
    };
  });

  const getDiffPreview: CloudReview["Service"]["getDiffPreview"] = Effect.fn(
    "CloudReview.getDiffPreview",
  )(function* (input) {
    yield* recordFor(input.cwd, "CloudReview.getDiffPreview");
    const sourceKind = input.file?.sourceKind ?? input.sourceKind;
    if (
      input.sourceKind !== undefined &&
      input.file &&
      input.file.sourceKind !== input.sourceKind
    ) {
      return yield* section(
        input.cwd,
        "CloudReview.getDiffPreview",
        "Review source selectors must agree.",
      );
    }
    const pathArgs = filePaths(input);
    const generatedAt = yield* DateTime.now;
    const allUntracked =
      sourceKind === "working-tree" && input.trackedOnly !== true
        ? yield* exec({
            cwd: input.cwd,
            operation: "CloudReview.getDiffPreview.untracked",
            command: "git ls-files --others --exclude-standard -z -- .",
            maxBytes: METADATA_MAX_BYTES,
          }).pipe(Effect.map((result) => result.stdout.split("\0").filter(Boolean)))
        : [];
    const untracked =
      input.file === undefined
        ? allUntracked
        : allUntracked.filter((path) => path === input.file!.path);
    const sources: ReviewDiffPreviewSource[] = [];

    if (input.commitSha !== undefined || sourceKind === "commit") {
      const commitSha = input.commitSha;
      if (commitSha === undefined) {
        return yield* section(
          input.cwd,
          "CloudReview.getDiffPreview",
          "Selected commit request is invalid.",
        );
      }
      const commit = yield* exec({
        cwd: input.cwd,
        operation: "CloudReview.getDiffPreview.commit",
        command: `git rev-parse ${shellQuote(commitSha)}^{commit}`,
      });
      const sha = commit.stdout.trim();
      const parent = yield* exec({
        cwd: input.cwd,
        operation: "CloudReview.getDiffPreview.commitParent",
        command: `git rev-parse ${shellQuote(sha)}^`,
        allowNonZero: true,
      });
      const base = parent.exitCode === 0 ? parent.stdout.trim() : sha;
      const subject = yield* exec({
        cwd: input.cwd,
        operation: "CloudReview.getDiffPreview.commitSubject",
        command: `git show -s --format=%s ${shellQuote(sha)}`,
      });
      const files = yield* previewFiles({
        cwd: input.cwd,
        operation: "CloudReview.getDiffPreview.commitFiles",
        revisionArgs: [base, sha],
        pathArgs,
        untracked: [],
      });
      const diff =
        input.metadataOnly === true && input.file === undefined
          ? ""
          : yield* patch({
              cwd: input.cwd,
              operation: "CloudReview.getDiffPreview.commitPatch",
              revisionArgs: [base, sha],
              pathArgs,
              ignoreWhitespace: input.ignoreWhitespace === true,
            });
      sources.push({
        id: sha,
        kind: "commit",
        title: `Commit ${sha.slice(0, 7)}`,
        ...(subject.stdout.trim() ? { commitSubject: subject.stdout.trim() } : {}),
        baseRef: base,
        headRef: sha,
        diff,
        files,
        diffHash: diffHash(diff, files),
        truncated: diff.length >= PATCH_MAX_BYTES,
        ...(input.metadataOnly === true && input.file === undefined
          ? { patchesDeferred: true }
          : {}),
      });
    } else {
      const requestedMergeBase =
        input.mergeBase !== undefined && /^[0-9a-f]{40,64}$/.test(input.mergeBase)
          ? input.mergeBase
          : null;
      const mergeBase =
        sourceKind === "working-tree"
          ? null
          : (requestedMergeBase ?? (yield* resolveMergeBase(input.cwd, input.baseRef)));
      const sourcesToRead: ReadonlyArray<"working-tree" | "branch-range"> =
        sourceKind === "working-tree"
          ? ["working-tree"]
          : sourceKind === "branch-range"
            ? ["branch-range"]
            : ["working-tree", "branch-range"];
      for (const kind of sourcesToRead) {
        const revisionArgs = kind === "working-tree" ? "HEAD" : (mergeBase ?? "HEAD");
        const files = yield* previewFiles({
          cwd: input.cwd,
          operation: `CloudReview.getDiffPreview.${kind}Files`,
          revisionArgs: [revisionArgs],
          pathArgs,
          untracked: kind === "working-tree" ? untracked : [],
        });
        const trackedDiff =
          input.metadataOnly === true && input.file === undefined
            ? ""
            : yield* patch({
                cwd: input.cwd,
                operation: `CloudReview.getDiffPreview.${kind}Patch`,
                revisionArgs: [revisionArgs],
                pathArgs,
                ignoreWhitespace: input.ignoreWhitespace === true,
              });
        const untrackedDiff =
          kind === "working-tree" && input.metadataOnly !== true
            ? (yield* Effect.forEach(
                untracked,
                (path) =>
                  exec({
                    cwd: input.cwd,
                    operation: "CloudReview.getDiffPreview.untrackedPatch",
                    command: `git diff --no-index -- /dev/null ${shellQuote(path)}`,
                    allowNonZero: true,
                    maxBytes: PATCH_MAX_BYTES,
                  }).pipe(Effect.map((result) => result.stdout)),
                { concurrency: 4 },
              )).join("")
            : "";
        const diff = trackedDiff + untrackedDiff;
        sources.push({
          id: kind,
          kind,
          title:
            kind === "working-tree"
              ? "Uncommitted"
              : input.baseRef
                ? `Changes vs ${input.baseRef}`
                : "Changes",
          baseRef: kind === "working-tree" ? "HEAD" : (input.baseRef ?? null),
          headRef: kind === "working-tree" ? null : "HEAD",
          ...(kind === "branch-range" && mergeBase !== null ? { mergeBase } : {}),
          diff,
          files,
          diffHash: diffHash(diff, files),
          truncated: diff.length >= PATCH_MAX_BYTES,
          ...(input.metadataOnly === true && input.file === undefined
            ? { patchesDeferred: true }
            : {}),
        });
      }
    }

    return {
      cwd: input.cwd,
      generatedAt,
      sources,
      ...(input.trackedOnly === true ? { incomplete: true } : {}),
    };
  });

  const isValidMergeBase = (value: string | undefined): value is string =>
    value !== undefined && /^[0-9a-f]{40,64}$/.test(value);

  const assertRelativePath = Effect.fn("CloudReview.assertRelativePath")(function* (
    cwd: string,
    operation: string,
    rawPath: string,
  ) {
    if (
      rawPath === "" ||
      rawPath.startsWith("/") ||
      rawPath.includes("\\") ||
      rawPath.includes("\0") ||
      rawPath.split("/").includes("..")
    ) {
      return yield* section(
        cwd,
        operation,
        "Review file paths must stay inside the sandbox checkout.",
      );
    }
  });

  const resolveWorktreePath = Effect.fn("CloudReview.resolveWorktreePath")(function* (
    cwd: string,
    operation: string,
    rawPath: string,
  ) {
    const root = yield* exec({
      cwd,
      operation: `${operation}.root`,
      command: `realpath -- ${shellQuote(cwd)}`,
      allowNonZero: true,
    });
    const target = yield* exec({
      cwd,
      operation: `${operation}.target`,
      command: `realpath -- ${shellQuote(`${cwd}/${rawPath}`)}`,
      allowNonZero: true,
    });
    const rootPath = root.exitCode === 0 ? root.stdout.trim() : cwd;
    const targetPath = target.exitCode === 0 ? target.stdout.trim() : "";
    if (targetPath === "" || !(targetPath === rootPath || targetPath.startsWith(`${rootPath}/`))) {
      return yield* section(
        cwd,
        operation,
        "Review file paths must stay inside the sandbox checkout.",
      );
    }
    return targetPath;
  });

  const assertPartOfDiff = Effect.fn("CloudReview.assertPartOfDiff")(function* (
    cwd: string,
    operation: string,
    input: ReviewDiffFileContentsInput,
  ) {
    const revisions =
      input.sourceKind === "commit"
        ? [input.baseRef ?? "HEAD", input.headRef ?? "HEAD"]
        : input.sourceKind === "branch-range"
          ? [
              isValidMergeBase(input.mergeBase)
                ? input.mergeBase
                : yield* resolveMergeBase(cwd, input.baseRef ?? undefined),
            ]
          : ["HEAD"];
    const changed = yield* exec({
      cwd,
      operation: `${operation}.paths`,
      command: `git diff --name-only -z ${revisions.map(shellQuote).join(" ")} -- ${shellQuote(input.oldPath)} ${shellQuote(input.newPath)}`,
      allowNonZero: true,
      maxBytes: METADATA_MAX_BYTES,
    });
    const paths = new Set(changed.stdout.split("\0").filter(Boolean));
    if (paths.has(input.oldPath) || paths.has(input.newPath)) return;
    if (input.sourceKind === "working-tree") {
      const untracked = yield* exec({
        cwd,
        operation: `${operation}.untracked`,
        command: `git ls-files --others --exclude-standard -z -- ${shellQuote(input.newPath)}`,
        allowNonZero: true,
        maxBytes: METADATA_MAX_BYTES,
      });
      if (untracked.stdout.split("\0").includes(input.newPath)) return;
    }
    return yield* section(cwd, operation, "This file is not part of the requested diff.");
  });

  const getDiffFileContents: CloudReview["Service"]["getDiffFileContents"] = Effect.fn(
    "CloudReview.getDiffFileContents",
  )(function* (input) {
    yield* recordFor(input.cwd, "CloudReview.getDiffFileContents");
    yield* assertRelativePath(input.cwd, "CloudReview.getDiffFileContents.old", input.oldPath);
    yield* assertRelativePath(input.cwd, "CloudReview.getDiffFileContents.new", input.newPath);
    yield* assertPartOfDiff(input.cwd, "CloudReview.getDiffFileContents", input);
    const worktreeTarget =
      input.sourceKind !== "working-tree" || input.changeType === "deleted"
        ? null
        : yield* resolveWorktreePath(input.cwd, "CloudReview.getDiffFileContents", input.newPath);
    const readRevision = (revision: string | null, filePath: string) =>
      revision === null
        ? Effect.succeed("")
        : exec({
            cwd: input.cwd,
            operation: "CloudReview.getDiffFileContents.revision",
            command: `git show ${shellQuote(`${revision}:${filePath}`)}`,
            allowNonZero: true,
          }).pipe(Effect.map((result) => (result.exitCode === 0 ? result.stdout : "")));
    const oldRevision =
      input.sourceKind === "branch-range"
        ? isValidMergeBase(input.mergeBase)
          ? input.mergeBase
          : yield* resolveMergeBase(input.cwd, input.baseRef ?? undefined)
        : (input.baseRef ?? "HEAD");
    const oldContents =
      input.changeType === "new" ? "" : yield* readRevision(oldRevision, input.oldPath);
    const newContents =
      input.changeType === "deleted"
        ? ""
        : input.sourceKind === "working-tree"
          ? yield* exec({
              cwd: input.cwd,
              operation: "CloudReview.getDiffFileContents.worktree",
              command: `cat ${shellQuote(worktreeTarget ?? input.newPath)}`,
              allowNonZero: true,
            }).pipe(Effect.map((result) => (result.exitCode === 0 ? result.stdout : "")))
          : yield* readRevision(input.headRef ?? "HEAD", input.newPath);
    return { oldContents, newContents };
  });

  const forWorkspace: CloudReview["Service"]["forWorkspace"] = (cwd) =>
    cloudThreads.forWorkspace(cwd).pipe(
      Effect.map((record) => Option.map(record, (value) => ({ threadId: String(value.threadId) }))),
      Effect.orElseSucceed(() => Option.none()),
    );

  return CloudReview.of({
    forWorkspace,
    getBranchCommits,
    getDiffPreview,
    getDiffFileContents,
  });
});

export const layer = Layer.effect(CloudReview, make);
