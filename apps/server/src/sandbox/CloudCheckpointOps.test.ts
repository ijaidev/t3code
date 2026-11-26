import {
  CheckpointRef,
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
import * as Ref from "effect/Ref";

import * as CheckpointStore from "../checkpointing/CheckpointStore.ts";
import * as VcsDriverRegistry from "../vcs/VcsDriverRegistry.ts";
import * as CloudCheckpointOps from "./CloudCheckpointOps.ts";
import type * as CloudThreadStore from "./CloudThreadStore.ts";
import * as CloudThreads from "./CloudThreads.ts";

const record: CloudThreadStore.CloudThreadRecord = {
  threadId: ThreadId.make("thread-cloud-checkpoints"),
  sandboxId: SandboxInstanceId.make("sandbox-checkpoints"),
  accountId: SandboxProviderAccountId.make("e2b-work"),
  providerKind: SandboxProviderKind.make("e2b"),
  repositoryUrl: "https://github.com/acme/private.git",
  branch: "feature",
  baseRef: "main",
  workspacePath: "/home/t3/.t3/threads/thread-cloud-checkpoints",
  homePath: "/home/t3",
  status: "ready",
  createdAt: "2026-11-24T00:00:00Z",
  updatedAt: "2026-11-24T00:00:00Z",
};

const checkpointRef = CheckpointRef.make("refs/t3/test/ordinal/1");

interface ExecCall {
  readonly command: string;
  readonly cwd: string;
}

const service = (
  calls: Ref.Ref<ReadonlyArray<ExecCall>>,
  outputs: Readonly<Record<string, { exitCode: number; stdout?: string; stderr?: string }>>,
) =>
  CloudThreads.CloudThreads.of({
    prepare: () => Effect.die("unused"),
    getSandbox: () => Effect.succeedNone,
    getRecord: () => Effect.succeedSome(record),
    forWorkspace: (cwd) =>
      Effect.succeed(cwd === record.workspacePath ? Option.some(record) : Option.none()),
    exec: (input) =>
      Effect.gen(function* () {
        yield* Ref.update(calls, (current) => [
          ...current,
          { command: input.command, cwd: input.cwd ?? record.workspacePath },
        ]);
        const matching = Object.entries(outputs).find(([needle]) => input.command.includes(needle));
        return {
          exitCode: matching?.[1].exitCode ?? 0,
          stdout: matching?.[1].stdout ?? "",
          stderr: matching?.[1].stderr ?? "",
        };
      }),
    writeAttachment: () => Effect.succeed(false),
    openHost: () => Effect.die("unused"),
    deleteForThread: () => Effect.void,
    getCloudSandboxState: () => Effect.succeedNone,
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

describe("cloud checkpoint ops", () => {
  it.effect("captures, diffs, and restores entirely through the sandbox workspace", () => {
    const scenario = Effect.gen(function* () {
      const calls = yield* Ref.make<ReadonlyArray<ExecCall>>([]);
      const ops = CloudCheckpointOps.makeCloudCheckpointOps(
        service(calls, {
          diff: { exitCode: 0, stdout: "diff --git a/file b/file\n" },
          "git status": {
            exitCode: 0,
            stdout: "# branch.oid 0123456789012345678901234567890123456789\n",
          },
          "git rev-parse --verify": { exitCode: 0, stdout: "abc\n" },
        }),
      );

      yield* ops.captureCheckpoint({ cwd: record.workspacePath, checkpointRef });
      const diff = yield* ops.diffCheckpoints({
        cwd: record.workspacePath,
        fromCheckpointRef: CheckpointRef.make("refs/t3/test/ordinal/0"),
        toCheckpointRef: checkpointRef,
        ignoreWhitespace: true,
        maxOutputBytes: 1024,
      });
      const restored = yield* ops.restoreCheckpoint({
        cwd: record.workspacePath,
        checkpointRef,
      });
      const head = yield* ops.resolveCleanHead(record.workspacePath);

      expect(diff).toContain("diff --git");
      expect(restored).toBe(true);
      expect(head).toBe("0123456789012345678901234567890123456789");

      const seen = yield* Ref.get(calls);
      const all = seen.map((call) => call.command).join("\n");
      expect(seen.every((call) => call.cwd === record.workspacePath)).toBe(true);
      expect(all).toContain("git add -A -- .");
      expect(all).toContain(`git update-ref '${checkpointRef}'`);
      expect(all).toContain("'diff'");
      expect(all).toContain("git restore --source");
      expect(all).toContain("head -c 1024");
    });
    return scenario;
  });

  it.effect("routes a cloud workspace's checkpoint calls to the sandbox", () => {
    const scenario = Effect.gen(function* () {
      const calls = yield* Ref.make<ReadonlyArray<ExecCall>>([]);
      const layer = CheckpointStore.layer.pipe(
        Layer.provide(Layer.succeed(VcsDriverRegistry.VcsDriverRegistry, {} as never)),
        Layer.provide(Layer.succeed(CloudThreads.CloudThreads, service(calls, {}))),
      );
      return yield* Effect.gen(function* () {
        const store = yield* CheckpointStore.CheckpointStore;
        expect(yield* store.isGitRepository(record.workspacePath)).toBe(true);
        yield* store.captureCheckpoint({ cwd: record.workspacePath, checkpointRef });
        const seen = yield* Ref.get(calls);
        expect(seen.length).toBe(1);
        expect(seen[0]?.cwd).toBe(record.workspacePath);
        expect(seen[0]?.command).toContain("git add -A -- .");
      }).pipe(Effect.provide(layer));
    });
    return scenario;
  });
});
