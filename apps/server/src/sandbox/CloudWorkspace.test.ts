// @effect-diagnostics nodeBuiltinImport:off -- the test executes the sandbox workspace scripts against a local temporary checkout.
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
import * as Stream from "effect/Stream";

import * as CloudWorkspace from "./CloudWorkspace.ts";
import type * as CloudThreadStore from "./CloudThreadStore.ts";
import * as CloudThreads from "./CloudThreads.ts";

interface TestWorkspace {
  readonly root: string;
  readonly local: string;
  readonly record: CloudThreadStore.CloudThreadRecord;
}

const makeWorkspace = (): TestWorkspace => {
  const base = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-cloud-workspace-"));
  const root = NodePath.join(base, "sandbox");
  const local = NodePath.join(base, "local");
  NodeFS.mkdirSync(NodePath.join(root, "src"), { recursive: true });
  NodeFS.mkdirSync(NodePath.join(root, "node_modules"), { recursive: true });
  NodeFS.mkdirSync(local, { recursive: true });
  NodeFS.writeFileSync(NodePath.join(root, ".gitignore"), "node_modules/\n*.log\n");
  NodeFS.writeFileSync(NodePath.join(root, "README.md"), "# Sandbox project\n");
  NodeFS.writeFileSync(NodePath.join(root, "src", "app.ts"), "export const value = 'sandbox';\n");
  NodeFS.writeFileSync(NodePath.join(root, "node_modules", "ignored.txt"), "ignored\n");
  NodeFS.writeFileSync(NodePath.join(root, "debug.log"), "ignored\n");
  NodeFS.writeFileSync(NodePath.join(local, "app.ts"), "local checkout\n");
  NodeChildProcess.execFileSync("git", ["init", "--initial-branch=main"], { cwd: root });
  NodeChildProcess.execFileSync("git", ["config", "user.email", "test@example.com"], { cwd: root });
  NodeChildProcess.execFileSync("git", ["config", "user.name", "Test"], { cwd: root });
  NodeChildProcess.execFileSync("git", ["add", "."], { cwd: root });
  NodeChildProcess.execFileSync("git", ["commit", "-m", "initial"], { cwd: root });
  const threadId = ThreadId.make("thread-cloud-workspace");
  return {
    root,
    local,
    record: {
      threadId,
      sandboxId: SandboxInstanceId.make("sandbox-workspace"),
      accountId: SandboxProviderAccountId.make("e2b-work"),
      providerKind: SandboxProviderKind.make("e2b"),
      repositoryUrl: "https://github.com/acme/private.git",
      branch: "feature",
      baseRef: "main",
      workspacePath: root,
      homePath: base,
      status: "ready",
      createdAt: "2026-11-24T00:00:00Z",
      updatedAt: "2026-11-24T00:00:00Z",
    },
  };
};

const machine = (input: TestWorkspace) =>
  CloudThreads.CloudThreads.of({
    prepare: () => Effect.die("unused"),
    getSandbox: () => Effect.succeedNone,
    getRecord: (threadId) =>
      Effect.succeed(
        threadId === input.record.threadId ? Option.some(input.record) : Option.none(),
      ),
    forWorkspace: (cwd) =>
      Effect.succeed(
        cwd === input.record.workspacePath ? Option.some(input.record) : Option.none(),
      ),
    exec: (command) =>
      Effect.promise(
        () =>
          new Promise<{ exitCode: number; stdout: string; stderr: string }>((resolve) => {
            const child = NodeChildProcess.spawn("/bin/sh", ["-lc", command.command], {
              cwd: command.cwd ?? input.record.workspacePath,
            });
            let stdout = "";
            let stderr = "";
            child.stdout.on("data", (data) => {
              stdout += String(data);
            });
            child.stderr.on("data", (data) => {
              stderr += String(data);
            });
            child.on("close", (code) => {
              resolve({ exitCode: code ?? 1, stdout, stderr });
            });
          }),
      ),
    writeAttachment: (_threadId, path, data) =>
      Effect.tryPromise({
        try: () => NodeFS.promises.writeFile(path, data),
        catch: () => undefined,
      }).pipe(
        Effect.as(true),
        Effect.orElseSucceed(() => false),
      ),
    beginRecovery: () => Effect.void,
    endRecovery: () => Effect.void,
    openHost: () => Effect.die("unused"),
    openProviderHost: () => Effect.die("unused"),
    deleteForThread: () => Effect.void,
    getCloudSandboxState: () => Effect.succeedNone,
    verifyCloudSandbox: () => Effect.succeedNone,
    markCloudSandboxLost: () => Effect.void,
    recreateCloudSandbox: () => Effect.die("unused"),
    subscribeCloudSandbox: () => Stream.empty,
    sleepCloudSandbox: () => Effect.die("unused"),
    wakeCloudSandbox: () => Effect.die("unused"),
    previewCloudSandboxDelete: () => Effect.die("unused"),
    deleteCloudSandbox: () => Effect.die("unused"),
    beginTurn: () => Effect.void,
    endTurn: () => Effect.void,
    holdPreview: () => Effect.void,
    releasePreview: () => Effect.void,
    sweepIdle: Effect.succeed(0),
  });

const layer = (input: TestWorkspace) =>
  CloudWorkspace.layer.pipe(
    Layer.provideMerge(Layer.succeed(CloudThreads.CloudThreads, machine(input))),
  );

describe("cloud workspace", () => {
  it.effect("reads, writes, lists, and searches only the sandbox checkout", () => {
    const scenario = Effect.gen(function* () {
      const input = makeWorkspace();
      return yield* Effect.gen(function* () {
        const workspace = yield* CloudWorkspace.CloudWorkspace;

        const read = yield* workspace.readFile({
          cwd: input.record.workspacePath,
          relativePath: "src/app.ts",
        });
        expect(read.contents).toContain("sandbox");

        yield* workspace.writeFile({
          cwd: input.record.workspacePath,
          relativePath: "notes/new.txt",
          contents: "written in the sandbox\n",
        });
        expect(NodeFS.readFileSync(NodePath.join(input.root, "notes", "new.txt"), "utf8")).toBe(
          "written in the sandbox\n",
        );
        expect(NodeFS.existsSync(NodePath.join(input.local, "notes", "new.txt"))).toBe(false);
        expect(NodeFS.readFileSync(NodePath.join(input.local, "app.ts"), "utf8")).toBe(
          "local checkout\n",
        );

        const metadata = yield* workspace.getMetadata({
          paths: [
            NodePath.join(input.root, "src", "app.ts"),
            NodePath.join(input.root, "missing.txt"),
          ],
        });
        expect(metadata.entries[0]).toMatchObject({ kind: "file" });
        expect(metadata.entries[1]).toBeNull();

        const listing = yield* workspace.list({
          cwd: input.record.workspacePath,
        });
        expect(listing.entries).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ path: "README.md", kind: "file" }),
            expect.objectContaining({ path: "src", kind: "directory" }),
          ]),
        );

        const ignored = yield* workspace.listIgnoredFiles({
          cwd: input.record.workspacePath,
        });
        expect(ignored.files).toEqual([{ path: "debug.log", byteLength: 8 }]);

        const paths = yield* workspace.search({
          cwd: input.record.workspacePath,
          query: "app",
          limit: 20,
        });
        expect(paths.entries).toEqual(
          expect.arrayContaining([expect.objectContaining({ path: "src/app.ts", kind: "file" })]),
        );

        const contents = yield* workspace.searchContents({
          cwd: input.record.workspacePath,
          query: "sandbox",
          limit: 20,
          caseSensitive: false,
          wholeWord: false,
          useRegex: false,
        });
        expect(contents.matches).toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              path: "src/app.ts",
              lineNumber: 1,
            }),
          ]),
        );
        const appMatch = contents.matches.find((match) => match.path === "src/app.ts");
        expect(appMatch?.matchRanges[0]).toMatchObject({ start: 22, end: 29 });
      }).pipe(Effect.provide(layer(input)));
    });
    return scenario;
  });

  it.effect("rejects paths outside the sandbox checkout", () => {
    const scenario = Effect.gen(function* () {
      const input = makeWorkspace();
      return yield* Effect.gen(function* () {
        const workspace = yield* CloudWorkspace.CloudWorkspace;
        const traversal = yield* workspace
          .readFile({
            cwd: input.record.workspacePath,
            relativePath: "../local/app.ts",
          })
          .pipe(Effect.flip);
        expect(traversal._tag).toBe("WorkspacePathOutsideRootError");

        const absolute = yield* workspace
          .readFile({
            cwd: input.record.workspacePath,
            relativePath: NodePath.join(input.local, "app.ts"),
          })
          .pipe(Effect.flip);
        expect(absolute._tag).toBe("WorkspacePathOutsideRootError");

        const unknown = yield* workspace
          .readFile({
            cwd: input.local,
            relativePath: "app.ts",
          })
          .pipe(Effect.flip);
        expect(unknown._tag).toBe("WorkspacePathOutsideRootError");
      }).pipe(Effect.provide(layer(input)));
    });
    return scenario;
  });
});
