// @effect-diagnostics-next-line nodeBuiltinImport:off -- temporary PR body paths and branch ids are server-local.
import * as NodeCrypto from "node:crypto";

import {
  type ChangeRequest,
  type GitActionProgressEvent,
  type GitActionProgressPhase,
  type GitManagerServiceError,
  GitManagerError,
  type GitRunStackedActionInput,
  type GitRunStackedActionResult,
  type SourceControlProviderInfo,
  type VcsCreateRefInput,
  type VcsCreateRefResult,
  type VcsListRefsInput,
  type VcsListRefsResult,
  type VcsPullResult,
  type VcsStatusLocalResult,
  type VcsSwitchRefInput,
  type VcsSwitchRefResult,
  type VcsStatusRemoteResult,
  type VcsStatusResult,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";

import type * as SourceControlProvider from "@t3tools/source-control-core/server/SourceControlProvider";

import * as CloudRepositoryCredential from "./CloudRepositoryCredential.ts";
import * as CloudThreadStore from "./CloudThreadStore.ts";
import * as CloudThreads from "./CloudThreads.ts";
import { gitCredentialEnvironment } from "./CloudThreads.ts";
import { sourceControlDefinitions } from "../sourceControl/sourceControlDefinitions.ts";
import * as SourceControlProviderRegistry from "../sourceControl/SourceControlProviderRegistry.ts";

const STATUS_SCRIPT = [
  "set +e",
  "printf '%s\\n' '@@STATUS@@'",
  "git status --porcelain=v2 --branch --untracked-files=normal --ignore-submodules=none",
  "printf '%s\\n' '@@NUMSTAT@@'",
  "git diff --numstat --no-renames HEAD -- .",
  "printf '%s\\n' '@@UNTRACKED@@'",
  "git ls-files --others --exclude-standard -- .",
  "printf '%s\\n' '@@REMOTES@@'",
  "git remote -v",
  "printf '%s\\n' '@@DEFAULT@@'",
  "git symbolic-ref -q refs/remotes/origin/HEAD",
].join("\n");

const shellQuote = (value: string) => `'${value.replaceAll("'", `'\\''`)}'`;

const section = (stdout: string, name: string) => {
  const marker = `@@${name}@@`;
  const start = stdout.indexOf(marker);
  if (start === -1) return "";
  const bodyStart = stdout.indexOf("\n", start) + 1;
  const next = stdout.indexOf("\n@@", bodyStart);
  return stdout.slice(bodyStart, next === -1 ? undefined : next).trimEnd();
};

const firstLine = (value: string) => value.trim().split("\n")[0] ?? value.trim();

const failure = (operation: string, cwd: string, detail: string, cause?: unknown) =>
  new GitManagerError({
    operation,
    cwd,
    detail,
    ...(cause === undefined ? {} : { cause }),
  });

const branchName = (baseRef: string) => {
  const withoutRefPrefix = baseRef
    .replace(/^refs\/remotes\/[^/]+\//, "")
    .replace(/^refs\/heads\//, "");
  return withoutRefPrefix.startsWith("origin/")
    ? withoutRefPrefix.slice("origin/".length)
    : withoutRefPrefix;
};

const toStatusPr = (changeRequest: ChangeRequest) => ({
  number: changeRequest.number,
  title: changeRequest.title,
  url: changeRequest.url,
  baseRef: changeRequest.baseRefName,
  headRef: changeRequest.headRefName,
  state: changeRequest.state,
  ...(changeRequest.isDraft === undefined ? {} : { isDraft: changeRequest.isDraft }),
});

interface ParsedStatus {
  readonly isRepo: boolean;
  readonly refName: string | null;
  readonly upstreamRef: string | null;
  readonly aheadCount: number;
  readonly behindCount: number;
  readonly hasWorkingTreeChanges: boolean;
  readonly workingTree: VcsStatusLocalResult["workingTree"];
  readonly hasPrimaryRemote: boolean;
  readonly defaultBranch: string | null;
  readonly remoteUrls: ReadonlyArray<string>;
}

const parseStatus = (record: CloudThreadStore.CloudThreadRecord, stdout: string): ParsedStatus => {
  const statusLines = section(stdout, "STATUS")
    .split("\n")
    .filter((line) => line.length > 0);
  const isRepo = statusLines.some((line) => line.startsWith("# branch.oid "));
  const refLine = statusLines.find((line) => line.startsWith("# branch.head "));
  const rawBranch = refLine?.slice("# branch.head ".length).trim() ?? "";
  const refName = rawBranch === "" || rawBranch === "(detached)" ? null : rawBranch;
  const upstream = statusLines
    .find((line) => line.startsWith("# branch.upstream "))
    ?.slice("# branch.upstream ".length)
    .trim();
  const [aheadCount = 0, behindCount = 0] =
    statusLines
      .find((line) => line.startsWith("# branch.ab "))
      ?.slice("# branch.ab ".length)
      .trim()
      .split(" ")
      .map((part) => Math.max(0, Number(part.replace(/^[+-]/, "")) || 0)) ?? [];

  const files = new Map<string, { insertions: number; deletions: number }>();
  for (const line of section(stdout, "NUMSTAT").split("\n")) {
    if (line === "") continue;
    const [insertions = "0", deletions = "0", ...pathParts] = line.split("\t");
    const path = pathParts.join("\t");
    if (path === "") continue;
    files.set(path, {
      insertions: Number(insertions) || 0,
      deletions: Number(deletions) || 0,
    });
  }
  for (const path of section(stdout, "UNTRACKED").split("\n")) {
    if (path === "" || files.has(path)) continue;
    files.set(path, { insertions: 0, deletions: 0 });
  }
  const workingTreeFiles = [...files]
    .map(([path, counts]) => ({ path, ...counts }))
    .toSorted((left, right) => left.path.localeCompare(right.path));
  const insertions = workingTreeFiles.reduce((total, file) => total + file.insertions, 0);
  const deletions = workingTreeFiles.reduce((total, file) => total + file.deletions, 0);
  const remotes = section(stdout, "REMOTES")
    .split("\n")
    .filter((line) => line.length > 0);
  const defaultRef = section(stdout, "DEFAULT").split("\n")[0] ?? "";
  const defaultBranch =
    defaultRef === "" ? null : (defaultRef.split("/").filter(Boolean).at(-1) ?? null);

  return {
    isRepo,
    refName,
    upstreamRef: upstream === undefined || upstream === "" ? null : upstream,
    aheadCount,
    behindCount,
    hasWorkingTreeChanges: workingTreeFiles.length > 0,
    workingTree: {
      rootPath: record.workspacePath,
      files: workingTreeFiles,
      insertions,
      deletions,
    },
    hasPrimaryRemote: remotes.length > 0,
    defaultBranch,
    remoteUrls: remotes.map((line) => line.split("\t")[1] ?? line),
  };
};

interface ResolvedProvider {
  readonly provider: SourceControlProvider.SourceControlProvider["Service"];
  readonly info: SourceControlProviderInfo;
}

const providerFor = Effect.fn("CloudThreadGit.providerFor")(function* (
  registry: SourceControlProviderRegistry.SourceControlProviderRegistry["Service"],
  record: CloudThreadStore.CloudThreadRecord,
): Effect.fn.Return<
  Option.Option<ResolvedProvider>,
  import("@t3tools/contracts").SourceControlProviderError
> {
  const provider = sourceControlDefinitions.recognizeRemote(record.repositoryUrl);
  if (provider === null) return Option.none<ResolvedProvider>();
  const handle = yield* registry.resolveHandle({
    cwd: record.workspacePath,
    context: { provider, remoteName: "origin", remoteUrl: record.repositoryUrl },
  });
  if (handle.context === null || handle.context.provider.kind === "unknown") {
    return Option.none<ResolvedProvider>();
  }
  return Option.some({ provider: handle.provider, info: handle.context.provider });
});

const readStatus = Effect.fn("CloudThreadGit.readStatus")(function* (
  cloudThreads: CloudThreads.CloudThreads["Service"],
  record: CloudThreadStore.CloudThreadRecord,
) {
  const result = yield* cloudThreads
    .exec({
      threadId: record.threadId,
      command: STATUS_SCRIPT,
      cwd: record.workspacePath,
      timeoutMs: 60_000,
    })
    .pipe(
      Effect.mapError((error) =>
        failure("CloudThreadGit.status", record.workspacePath, error.detail, error),
      ),
    );
  if (result.exitCode !== 0 && !result.stdout.includes("@@STATUS@@")) {
    return yield* failure(
      "CloudThreadGit.status",
      record.workspacePath,
      firstLine(result.stderr || result.stdout) || "The sandbox could not read Git status.",
    );
  }
  return parseStatus(record, result.stdout);
});

const statusParts = Effect.fn("CloudThreadGit.statusParts")(function* (
  registry: SourceControlProviderRegistry.SourceControlProviderRegistry["Service"],
  cloudThreads: CloudThreads.CloudThreads["Service"],
  record: CloudThreadStore.CloudThreadRecord,
) {
  const parsed = yield* readStatus(cloudThreads, record);
  const provider = yield* providerFor(registry, record);
  let defaultBranch = parsed.defaultBranch;
  if (defaultBranch === null && Option.isSome(provider)) {
    defaultBranch = yield* provider.value.provider
      .getDefaultBranch({ cwd: record.workspacePath })
      .pipe(Effect.orElseSucceed(() => null));
  }
  const local: VcsStatusLocalResult = {
    isRepo: parsed.isRepo,
    ...(Option.isSome(provider) ? { sourceControlProvider: provider.value.info } : {}),
    hasPrimaryRemote: parsed.hasPrimaryRemote,
    isDefaultRef:
      defaultBranch !== null && parsed.refName !== null && parsed.refName === defaultBranch,
    refName: parsed.refName,
    hasWorkingTreeChanges: parsed.hasWorkingTreeChanges,
    workingTree: parsed.workingTree,
  };
  return { parsed, local, provider, defaultBranch };
});

const readPullRequest = (
  record: CloudThreadStore.CloudThreadRecord,
  branch: string,
  provider: Option.Option<ResolvedProvider>,
) => {
  if (Option.isNone(provider)) return Effect.succeed(null);
  return provider.value.provider
    .listChangeRequests({
      cwd: record.workspacePath,
      headSelector: branch,
      state: "open",
      limit: 1,
    })
    .pipe(
      Effect.map((changes) => changes[0] ?? null),
      Effect.catch((cause) =>
        Effect.logWarning("Cloud thread pull request lookup failed", {
          threadId: record.threadId,
          cause: String(cause),
        }).pipe(Effect.as(null)),
      ),
    );
};

export class CloudThreadGit extends Context.Service<
  CloudThreadGit,
  {
    readonly forCwd: (
      cwd: string,
    ) => Effect.Effect<Option.Option<CloudThreadStore.CloudThreadRecord>>;
    readonly currentBranch: (cwd: string) => Effect.Effect<string | null, GitManagerServiceError>;
    readonly localStatus: (
      cwd: string,
    ) => Effect.Effect<VcsStatusLocalResult, GitManagerServiceError>;
    readonly remoteStatus: (
      cwd: string,
    ) => Effect.Effect<VcsStatusRemoteResult, GitManagerServiceError>;
    readonly status: (cwd: string) => Effect.Effect<VcsStatusResult, GitManagerServiceError>;
    readonly runStackedAction: (
      input: GitRunStackedActionInput,
      options?: {
        readonly actionId?: string;
        readonly progressReporter?: {
          readonly publish: (event: GitActionProgressEvent) => Effect.Effect<void>;
        };
      },
    ) => Effect.Effect<GitRunStackedActionResult, GitManagerServiceError>;
    readonly listRefs: (
      input: VcsListRefsInput,
    ) => Effect.Effect<VcsListRefsResult, GitManagerServiceError>;
    readonly pullCurrentBranch: (
      cwd: string,
    ) => Effect.Effect<VcsPullResult, GitManagerServiceError>;
    readonly createRef: (
      input: VcsCreateRefInput,
    ) => Effect.Effect<VcsCreateRefResult, GitManagerServiceError>;
    readonly switchRef: (
      input: VcsSwitchRefInput,
    ) => Effect.Effect<VcsSwitchRefResult, GitManagerServiceError>;
  }
>()("t3/sandbox/CloudThreadGit") {}

const make = Effect.gen(function* () {
  const cloudThreads = yield* CloudThreads.CloudThreads;
  const registry = yield* SourceControlProviderRegistry.SourceControlProviderRegistry;
  const credentialResolver = yield* CloudRepositoryCredential.CloudRepositoryCredentialResolver;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;

  const forCwd: CloudThreadGit["Service"]["forCwd"] = (cwd) =>
    cloudThreads.forWorkspace(cwd).pipe(Effect.orElseSucceed(() => Option.none()));

  const requireRecord = Effect.fn("CloudThreadGit.requireRecord")(function* (cwd: string) {
    const record = yield* forCwd(cwd);
    if (Option.isNone(record)) {
      return yield* failure(
        "CloudThreadGit",
        cwd,
        "This workspace is not a cloud thread checkout.",
      );
    }
    return record.value;
  });

  const run = Effect.fn("CloudThreadGit.run")(function* (input: {
    readonly operation: string;
    readonly record: CloudThreadStore.CloudThreadRecord;
    readonly command: string;
    readonly envs?: Readonly<Record<string, string>>;
    readonly timeoutMs?: number;
  }) {
    const result = yield* cloudThreads
      .exec({
        threadId: input.record.threadId,
        command: input.command,
        cwd: input.record.workspacePath,
        ...(input.envs === undefined ? {} : { envs: input.envs }),
        ...(input.timeoutMs === undefined ? {} : { timeoutMs: input.timeoutMs }),
      })
      .pipe(
        Effect.mapError((error) =>
          failure(input.operation, input.record.workspacePath, error.detail, error),
        ),
      );
    if (result.exitCode !== 0) {
      return yield* failure(
        input.operation,
        input.record.workspacePath,
        firstLine(result.stderr || result.stdout) || "The sandbox Git command failed.",
      );
    }
    return result;
  });

  const currentBranch: CloudThreadGit["Service"]["currentBranch"] = Effect.fn(
    "CloudThreadGit.currentBranch",
  )(function* (cwd) {
    const record = yield* requireRecord(cwd);
    const result = yield* run({
      operation: "CloudThreadGit.currentBranch",
      record,
      command: "git branch --show-current",
    });
    const branch = result.stdout.trim();
    return branch === "" ? null : branch;
  });

  const statusFor = Effect.fn("CloudThreadGit.statusFor")(function* (cwd: string) {
    const record = yield* requireRecord(cwd);
    return yield* statusParts(registry, cloudThreads, record);
  });

  const localStatus: CloudThreadGit["Service"]["localStatus"] = Effect.fn(
    "CloudThreadGit.localStatus",
  )(function* (cwd) {
    const parts = yield* statusFor(cwd);
    return parts.local;
  });

  const remoteStatus: CloudThreadGit["Service"]["remoteStatus"] = Effect.fn(
    "CloudThreadGit.remoteStatus",
  )(function* (cwd) {
    const { parsed, local, provider, defaultBranch } = yield* statusFor(cwd);
    const record = yield* requireRecord(cwd);
    const pr =
      parsed.refName === null ? null : yield* readPullRequest(record, parsed.refName, provider);
    return {
      hasUpstream: parsed.upstreamRef !== null,
      aheadCount: parsed.aheadCount,
      behindCount: parsed.behindCount,
      pr: pr === null ? null : toStatusPr(pr),
      ...(defaultBranch !== null && local.refName === defaultBranch
        ? { aheadOfDefaultCount: 0 }
        : {}),
    };
  });

  const status: CloudThreadGit["Service"]["status"] = Effect.fn("CloudThreadGit.status")(
    function* (cwd) {
      const local = yield* localStatus(cwd);
      const remote = yield* remoteStatus(cwd);
      return { ...local, ...remote };
    },
  );

  const providerCredential = (record: CloudThreadStore.CloudThreadRecord) =>
    credentialResolver.resolve(record.repositoryUrl);

  type ProgressEvent =
    | { readonly kind: "action_started"; readonly phases: ReadonlyArray<GitActionProgressPhase> }
    | {
        readonly kind: "phase_started";
        readonly phase: GitActionProgressPhase;
        readonly label: string;
      }
    | { readonly kind: "action_finished"; readonly result: GitRunStackedActionResult }
    | {
        readonly kind: "action_failed";
        readonly phase: GitActionProgressPhase | null;
        readonly message: string;
      };

  const emit = (
    options: Parameters<CloudThreadGit["Service"]["runStackedAction"]>[1],
    input: GitRunStackedActionInput,
    event: ProgressEvent,
  ) =>
    options?.progressReporter === undefined || options.actionId === undefined
      ? Effect.void
      : options.progressReporter.publish({
          ...event,
          actionId: options.actionId,
          cwd: input.cwd,
          action: input.action,
        } as GitActionProgressEvent);

  const defaultBranchFor = Effect.fn("CloudThreadGit.defaultBranchFor")(function* (
    record: CloudThreadStore.CloudThreadRecord,
  ): Effect.fn.Return<string, import("@t3tools/contracts").SourceControlProviderError> {
    const provider = yield* providerFor(registry, record);
    if (Option.isSome(provider)) {
      const branch = yield* provider.value.provider
        .getDefaultBranch({ cwd: record.workspacePath })
        .pipe(Effect.orElseSucceed(() => null));
      if (branch !== null) return branch;
    }
    return branchName(record.baseRef);
  });

  const createPullRequest = Effect.fn("CloudThreadGit.createPullRequest")(function* (input: {
    readonly record: CloudThreadStore.CloudThreadRecord;
    readonly branch: string;
    readonly title: string;
    readonly body: string;
  }) {
    const provider = yield* providerFor(registry, input.record);
    if (Option.isNone(provider)) {
      return yield* failure(
        "CloudThreadGit.createPullRequest",
        input.record.workspacePath,
        "No source-control account is connected for this repository. Connect it in Settings → Source control & issues, then retry.",
      );
    }
    const existing = yield* provider.value.provider.listChangeRequests({
      cwd: input.record.workspacePath,
      headSelector: input.branch,
      state: "open",
      limit: 1,
    });
    if (existing[0] !== undefined) return { status: "opened_existing" as const, pr: existing[0] };
    const baseBranch = yield* defaultBranchFor(input.record);
    if (input.branch === baseBranch) {
      return yield* failure(
        "CloudThreadGit.createPullRequest",
        input.record.workspacePath,
        `${baseBranch} is the base branch. Use a feature branch before opening a pull request.`,
      );
    }
    const bodyPath = path.join(
      yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-cloud-pr-" }),
      "body.md",
    );
    yield* fileSystem.writeFileString(bodyPath, input.body);
    yield* provider.value.provider.createChangeRequest({
      cwd: input.record.workspacePath,
      baseRefName: baseBranch,
      headSelector: input.branch,
      title: input.title,
      bodyFile: bodyPath,
    });
    const created = yield* provider.value.provider.listChangeRequests({
      cwd: input.record.workspacePath,
      headSelector: input.branch,
      state: "open",
      limit: 1,
    });
    return { status: "created" as const, ...(created[0] === undefined ? {} : { pr: created[0] }) };
  }, Effect.scoped);

  const runStackedAction: CloudThreadGit["Service"]["runStackedAction"] = Effect.fn(
    "CloudThreadGit.runStackedAction",
  )(function* (
    input: GitRunStackedActionInput,
    options?: Parameters<CloudThreadGit["Service"]["runStackedAction"]>[1],
  ) {
    return yield* Effect.gen(function* () {
      const record = yield* requireRecord(input.cwd);
      const before = yield* statusParts(registry, cloudThreads, record);
      const action = input.action;
      const wantsCommit =
        action === "commit" || action === "commit_push" || action === "commit_push_pr";
      const wantsPush =
        action === "push" ||
        action === "commit_push" ||
        action === "commit_push_pr" ||
        action === "create_pr";
      const wantsPr = action === "create_pr" || action === "commit_push_pr";
      if (wantsPr && input.featureBranch !== true && before.parsed.refName !== null) {
        const baseBranch = yield* defaultBranchFor(record);
        if (before.parsed.refName === baseBranch) {
          const provider = yield* providerFor(registry, record);
          const existing = yield* readPullRequest(record, before.parsed.refName, provider);
          if (existing === null) {
            return yield* failure(
              "CloudThreadGit.runStackedAction",
              input.cwd,
              `${baseBranch} is the base branch. Use a feature branch before opening a pull request.`,
            );
          }
        }
      }
      const phases = [
        ...(input.featureBranch === true ? (["branch"] as const) : []),
        ...(wantsCommit ? (["commit"] as const) : []),
        ...(wantsPush ? (["push"] as const) : []),
        ...(wantsPr ? (["pr"] as const) : []),
      ];
      yield* emit(options, input, { kind: "action_started", phases });
      if (input.featureBranch === true && wantsCommit && input.commitMessage === undefined) {
        // Feature branch creation without a generated name is the common quick action.
      }
      if (!input.featureBranch && (wantsPush || wantsPr) && before.parsed.refName === null) {
        return yield* failure(
          "CloudThreadGit.runStackedAction",
          input.cwd,
          "Cannot push or create a pull request from detached HEAD.",
        );
      }

      let branch = before.parsed.refName;
      const branchStep: GitRunStackedActionResult["branch"] =
        input.featureBranch === true
          ? yield* Effect.gen(function* () {
              yield* emit(options, input, {
                kind: "phase_started",
                phase: "branch",
                label: "Preparing feature branch...",
              });
              const seed = `t3/cloud-${NodeCrypto.randomUUID().slice(0, 8)}`;
              yield* run({
                operation: "CloudThreadGit.featureBranch",
                record,
                command: `git switch -c ${shellQuote(seed)}`,
              });
              branch = seed;
              return { status: "created" as const, name: seed };
            })
          : { status: "skipped_not_requested" as const };

      let commit: GitRunStackedActionResult["commit"] = { status: "skipped_not_requested" };
      if (wantsCommit) {
        yield* emit(options, input, {
          kind: "phase_started",
          phase: "commit",
          label: "Committing...",
        });
        const stage =
          input.filePaths === undefined
            ? "git add -A -- ."
            : `git add -- ${input.filePaths.map(shellQuote).join(" ")}`;
        yield* run({ operation: "CloudThreadGit.stage", record, command: stage });
        const staged = yield* cloudThreads
          .exec({
            threadId: record.threadId,
            command: "git diff --cached --quiet",
            cwd: record.workspacePath,
          })
          .pipe(
            Effect.mapError((error) =>
              failure("CloudThreadGit.stage", input.cwd, error.detail, error),
            ),
          );
        if (staged.exitCode === 0) {
          commit = { status: "skipped_no_changes" };
        } else {
          const message =
            input.commitMessage?.trim() ||
            `Update from T3 Code cloud thread ${record.threadId.slice(0, 8)}`;
          const subject = firstLine(message);
          yield* run({
            operation: "CloudThreadGit.commit",
            record,
            command: [
              "git",
              "-c",
              "user.name=T3 Code",
              "-c",
              "user.email=t3code@users.noreply.github.com",
              "commit",
              "-m",
              shellQuote(message),
            ].join(" "),
          });
          const sha = yield* run({
            operation: "CloudThreadGit.commitSha",
            record,
            command: "git rev-parse HEAD",
          });
          commit = { status: "created", commitSha: sha.stdout.trim(), subject };
        }
      }

      let push: GitRunStackedActionResult["push"] = { status: "skipped_not_requested" };
      if (wantsPush) {
        yield* emit(options, input, { kind: "phase_started", phase: "push", label: "Pushing..." });
        if (branch === null) {
          return yield* failure(
            "CloudThreadGit.runStackedAction",
            input.cwd,
            "Cannot push from detached HEAD.",
          );
        }
        const current = yield* statusParts(registry, cloudThreads, record);
        if (
          current.parsed.upstreamRef !== null &&
          current.parsed.aheadCount === 0 &&
          current.parsed.behindCount === 0
        ) {
          push = {
            status: "skipped_up_to_date",
            branch,
            upstreamBranch: current.parsed.upstreamRef,
          };
        } else {
          const credential = yield* providerCredential(record);
          const result = yield* cloudThreads
            .exec({
              threadId: record.threadId,
              command: `git push -u origin ${shellQuote(branch)}`,
              cwd: record.workspacePath,
              ...(Option.isSome(credential)
                ? { envs: gitCredentialEnvironment(credential.value) }
                : {}),
              timeoutMs: 5 * 60_000,
            })
            .pipe(
              Effect.mapError((error) =>
                failure("CloudThreadGit.push", input.cwd, error.detail, error),
              ),
            );
          if (result.exitCode !== 0) {
            return yield* failure(
              "CloudThreadGit.push",
              input.cwd,
              `The sandbox could not push ${branch}. ${firstLine(result.stderr || result.stdout)}`,
            );
          }
          push = {
            status: "pushed",
            branch,
            upstreamBranch: `origin/${branch}`,
            setUpstream: true,
          };
        }
      }

      let pr: GitRunStackedActionResult["pr"] = { status: "skipped_not_requested" };
      if (wantsPr) {
        yield* emit(options, input, {
          kind: "phase_started",
          phase: "pr",
          label: "Preparing PR...",
        });
        if (branch === null) {
          return yield* failure(
            "CloudThreadGit.runStackedAction",
            input.cwd,
            "Cannot create a pull request from detached HEAD.",
          );
        }
        const baseBranch = yield* defaultBranchFor(record);
        const title =
          commit.status === "created" && commit.subject !== undefined
            ? commit.subject
            : `Cloud thread ${branch}`;
        const created = yield* createPullRequest({
          record,
          branch,
          title,
          body: `Changes made by a T3 Code cloud thread.\n\nBranch: \`${branch}\``,
        }).pipe(
          Effect.mapError((error) =>
            error._tag === "GitManagerError"
              ? error
              : failure("CloudThreadGit.createPullRequest", input.cwd, String(error)),
          ),
        );
        pr =
          created.pr === undefined
            ? { status: created.status, baseBranch, headBranch: branch, title }
            : {
                status: created.status,
                url: created.pr.url,
                number: created.pr.number,
                baseBranch: created.pr.baseRefName,
                headBranch: created.pr.headRefName,
                title: created.pr.title,
              };
      }

      const toastTitle =
        pr.status === "created" || pr.status === "opened_existing"
          ? "Pull request ready"
          : push.status === "pushed"
            ? "Branch pushed"
            : commit.status === "created"
              ? "Changes committed"
              : "Nothing to commit";
      const result: GitRunStackedActionResult = {
        action,
        branch: branchStep,
        commit,
        push,
        pr,
        toast: {
          title: toastTitle,
          ...(pr.url === undefined ? {} : { description: pr.url }),
          cta:
            pr.url === undefined
              ? { kind: "none" }
              : { kind: "open_pr", label: "View PR", url: pr.url },
        },
      };
      yield* emit(options, input, { kind: "action_finished", result });
      return result;
    }).pipe(
      Effect.tapError((error) =>
        emit(options, input, {
          kind: "action_failed",
          phase: null,
          message: error.message,
        }),
      ),
    );
  });

  const listRefs: CloudThreadGit["Service"]["listRefs"] = Effect.fn("CloudThreadGit.listRefs")(
    function* (input) {
      const record = yield* requireRecord(input.cwd);
      const result = yield* run({
        operation: "CloudThreadGit.listRefs",
        record,
        command:
          "git for-each-ref --format='%(refname:short)%09%(upstream:short)' refs/heads refs/remotes",
      });
      const refs = result.stdout
        .split("\n")
        .filter((line) => line.length > 0)
        .map((line) => {
          const [name = "", upstream = ""] = line.split("\t");
          const isRemote = name.startsWith("origin/");
          return {
            name,
            current: name === record.branch,
            isDefault: false,
            worktreePath: null,
            ...(isRemote ? { isRemote: true, remoteName: "origin" } : {}),
            ...(upstream === "" ? {} : { upstreamRef: upstream }),
          };
        });
      return {
        refs,
        isRepo: true,
        hasPrimaryRemote: true,
        nextCursor: null,
        totalCount: refs.length,
      };
    },
  );

  const pullCurrentBranch: CloudThreadGit["Service"]["pullCurrentBranch"] = Effect.fn(
    "CloudThreadGit.pullCurrentBranch",
  )(function* (cwd) {
    const record = yield* requireRecord(cwd);
    const before = yield* statusParts(registry, cloudThreads, record);
    if (before.parsed.refName === null) {
      return yield* failure("CloudThreadGit.pull", cwd, "Cannot pull on detached HEAD.");
    }
    if (before.parsed.behindCount === 0) {
      return {
        status: "skipped_up_to_date" as const,
        refName: before.parsed.refName,
        upstreamRef: before.parsed.upstreamRef,
      };
    }
    const credential = yield* providerCredential(record);
    const result = yield* cloudThreads
      .exec({
        threadId: record.threadId,
        command: "git pull --ff-only",
        cwd: record.workspacePath,
        ...(Option.isSome(credential) ? { envs: gitCredentialEnvironment(credential.value) } : {}),
        timeoutMs: 5 * 60_000,
      })
      .pipe(Effect.mapError((error) => failure("CloudThreadGit.pull", cwd, error.detail, error)));
    if (result.exitCode !== 0) {
      return yield* failure(
        "CloudThreadGit.pull",
        cwd,
        firstLine(result.stderr || result.stdout) || "The sandbox could not pull.",
      );
    }
    return {
      status: "pulled" as const,
      refName: before.parsed.refName,
      upstreamRef: before.parsed.upstreamRef,
    };
  });

  const createRef: CloudThreadGit["Service"]["createRef"] = Effect.fn("CloudThreadGit.createRef")(
    function* (input) {
      const record = yield* requireRecord(input.cwd);
      yield* run({
        operation: "CloudThreadGit.createRef",
        record,
        command:
          input.switchRef === true
            ? `git switch -c ${shellQuote(input.refName)}`
            : `git branch ${shellQuote(input.refName)}`,
      });
      return { refName: input.refName };
    },
  );

  const switchRef: CloudThreadGit["Service"]["switchRef"] = Effect.fn("CloudThreadGit.switchRef")(
    function* (input) {
      const record = yield* requireRecord(input.cwd);
      yield* run({
        operation: "CloudThreadGit.switchRef",
        record,
        command: `git switch ${shellQuote(input.refName)}`,
      });
      const current = yield* run({
        operation: "CloudThreadGit.currentBranch",
        record,
        command: "git branch --show-current",
      });
      return { refName: current.stdout.trim() || input.refName };
    },
  );

  return CloudThreadGit.of({
    forCwd,
    currentBranch,
    localStatus,
    remoteStatus,
    status,
    runStackedAction,
    listRefs,
    pullCurrentBranch,
    createRef,
    switchRef,
  });
});

export const layer = Layer.effect(CloudThreadGit, make);
