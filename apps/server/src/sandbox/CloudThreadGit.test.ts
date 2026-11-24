import {
  SandboxInstanceId,
  SandboxProviderAccountId,
  SandboxProviderKind,
  SourceControlProviderKind,
  ThreadId,
} from "@t3tools/contracts";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";

import type * as SourceControlProvider from "@t3tools/source-control-core/server/SourceControlProvider";

import * as SourceControlProviderRegistry from "../sourceControl/SourceControlProviderRegistry.ts";
import * as CloudRepositoryCredential from "./CloudRepositoryCredential.ts";
import * as CloudThreadGit from "./CloudThreadGit.ts";
import * as CloudThreads from "./CloudThreads.ts";

const threadId = ThreadId.make("thread-cloud-git");
const E2B = SandboxProviderKind.make("e2b");
const accountId = SandboxProviderAccountId.make("e2b-work");

const record = {
  threadId,
  sandboxId: SandboxInstanceId.make("sandbox-1"),
  accountId,
  providerKind: E2B,
  repositoryUrl: "https://github.com/acme/private.git",
  branch: "feature",
  baseRef: "main",
  workspacePath: "/home/t3/.t3/threads/thread-cloud-git",
  homePath: "/home/t3",
  status: "ready" as const,
  createdAt: "2026-11-24T00:00:00Z",
  updatedAt: "2026-11-24T00:00:00Z",
};

const statusOutput = [
  "@@STATUS@@",
  "# branch.oid 0123456789012345678901234567890123456789",
  "# branch.head feature",
  "# branch.upstream origin/feature",
  "# branch.ab +1 -0",
  "@@NUMSTAT@@",
  "@@UNTRACKED@@",
  "@@REMOTES@@",
  "origin\thttps://github.com/acme/private.git (fetch)",
  "origin\thttps://github.com/acme/private.git (push)",
  "@@DEFAULT@@",
  "refs/remotes/origin/main",
].join("\n");

const pullRequest = {
  provider: "github",
  number: 7,
  title: "Cloud change",
  url: "https://github.com/acme/private/pull/7",
  baseRefName: "main",
  headRefName: "feature",
  state: "open" as const,
  updatedAt: Option.none(),
};

interface FakeState {
  readonly execs: Ref.Ref<
    ReadonlyArray<{ command: string; envs?: Readonly<Record<string, string>> }>
  >;
  openPullRequests: number;
}

const fakeProvider = (state: FakeState): SourceControlProvider.SourceControlProvider["Service"] =>
  ({
    kind: SourceControlProviderKind.make("github"),
    listChangeRequests: () => Effect.sync(() => (state.openPullRequests > 0 ? [pullRequest] : [])),
    createChangeRequest: (input: unknown) =>
      Effect.sync(() => {
        state.openPullRequests += 1;
        return input;
      }),
    getDefaultBranch: () => Effect.succeed("main"),
  }) as unknown as SourceControlProvider.SourceControlProvider["Service"];

const testLayer = (state: FakeState) => {
  const cloud = CloudThreads.CloudThreads.of({
    prepare: () => Effect.die("unused"),
    getSandbox: () => Effect.succeedNone,
    getRecord: () => Effect.succeedSome(record),
    forWorkspace: () => Effect.succeedSome(record),
    exec: (input) =>
      Effect.gen(function* () {
        yield* Ref.update(state.execs, (execs) => [
          ...execs,
          { command: input.command, ...(input.envs === undefined ? {} : { envs: input.envs }) },
        ]);
        if (input.command.includes("@@STATUS@@")) {
          return { exitCode: 0, stdout: statusOutput, stderr: "" };
        }
        if (input.command.includes("git diff --cached --quiet")) {
          return { exitCode: 1, stdout: "", stderr: "" };
        }
        if (input.command.includes("git rev-parse HEAD")) {
          return {
            exitCode: 0,
            stdout: "fedcba9876543210fedcba9876543210fedcba98\n",
            stderr: "",
          };
        }
        return { exitCode: 0, stdout: "", stderr: "" };
      }),
    writeAttachment: () => Effect.succeed(false),
    openHost: () => Effect.die("unused"),
    deleteForThread: () => Effect.void,
  });
  const registry = Layer.succeed(SourceControlProviderRegistry.SourceControlProviderRegistry, {
    resolveHandle: () =>
      Effect.succeed({
        provider: fakeProvider(state),
        context: {
          provider: {
            kind: SourceControlProviderKind.make("github"),
            name: "GitHub",
            baseUrl: "https://github.com",
          },
          remoteName: "origin",
          remoteUrl: record.repositoryUrl,
        },
      }),
  } as unknown as SourceControlProviderRegistry.SourceControlProviderRegistry["Service"]);
  const credentials = Layer.succeed(CloudRepositoryCredential.CloudRepositoryCredentialResolver, {
    resolve: () =>
      Effect.succeedSome({
        username: "x-access-token",
        token: "repo-secret",
        url: record.repositoryUrl,
      }),
  });
  return CloudThreadGit.layer.pipe(
    Layer.provideMerge(Layer.succeed(CloudThreads.CloudThreads, cloud)),
    Layer.provideMerge(registry),
    Layer.provideMerge(credentials),
    Layer.provideMerge(Layer.merge(NodeServices.layer, NodeCrypto.layer)),
  );
};

const makeState = Effect.gen(function* () {
  return {
    execs: yield* Ref.make<
      ReadonlyArray<{ command: string; envs?: Readonly<Record<string, string>> }>
    >([]),
    openPullRequests: 0,
  } satisfies FakeState;
});

describe("cloud thread git", () => {
  it.effect("pushes and opens a pull request without putting the credential in the command", () => {
    const run = Effect.gen(function* () {
      const state = yield* makeState;
      return yield* Effect.gen(function* () {
        const git = yield* CloudThreadGit.CloudThreadGit;
        const result = yield* git.runStackedAction(
          {
            actionId: "action-1",
            cwd: record.workspacePath,
            action: "commit_push_pr",
            commitMessage: "Update cloud thread",
          },
          {},
        );
        expect(result.commit.status).toBe("created");
        expect(result.push.status).toBe("pushed");
        expect(result.pr.status).toBe("created");
        const execs = yield* Ref.get(state.execs);
        const commands = execs.map((entry) => entry.command);
        expect(commands.every((command) => !command.includes("repo-secret"))).toBe(true);
        const push = execs.find((entry) => entry.command.includes("git push"));
        expect(push?.envs?.["GIT_CONFIG_VALUE_0"]).toBe(
          `AUTHORIZATION: basic ${Buffer.from("x-access-token:repo-secret").toString("base64")}`,
        );
        expect(push?.envs?.["GIT_CONFIG_KEY_0"]).toBe(`http.${record.repositoryUrl}.extraheader`);
        expect(push?.command).toContain(record.repositoryUrl === "" ? "" : "git push -u origin");
      }).pipe(Effect.provide(testLayer(state)));
    });
    return run;
  });
});
