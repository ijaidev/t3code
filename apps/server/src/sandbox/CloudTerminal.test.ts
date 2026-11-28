// @effect-diagnostics nodeBuiltinImport:off -- the test checks sandbox paths without a provider.
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
import * as Stream from "effect/Stream";

import * as CloudTerminal from "./CloudTerminal.ts";
import type * as CloudThreadStore from "./CloudThreadStore.ts";
import * as CloudThreads from "./CloudThreads.ts";

const threadId = ThreadId.make("thread-cloud-terminal");
const workspacePath = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-cloud-terminal-"));

const record: CloudThreadStore.CloudThreadRecord = {
  threadId,
  sandboxId: SandboxInstanceId.make("sandbox-terminal"),
  accountId: SandboxProviderAccountId.make("e2b-work"),
  providerKind: SandboxProviderKind.make("e2b"),
  repositoryUrl: "https://github.com/acme/private.git",
  branch: "feature",
  baseRef: "main",
  workspacePath,
  homePath: NodePath.dirname(workspacePath),
  status: "ready",
  createdAt: "2026-11-24T00:00:00Z",
  updatedAt: "2026-11-24T00:00:00Z",
};

const machine = (state: {
  readonly starts: Ref.Ref<
    ReadonlyArray<{
      cwd: string;
      command: string;
      envs: Readonly<Record<string, string>> | undefined;
    }>
  >;
  readonly writes: Ref.Ref<ReadonlyArray<string>>;
  readonly kills: Ref.Ref<number>;
}) =>
  CloudThreads.CloudThreads.of({
    prepare: () => Effect.die("unused"),
    getSandbox: () => Effect.succeedNone,
    getRecord: (id) => Effect.succeed(id === threadId ? Option.some(record) : Option.none()),
    forWorkspace: (cwd) =>
      Effect.succeed(cwd === workspacePath ? Option.some(record) : Option.none()),
    exec: () => Effect.succeed({ exitCode: 0, stdout: "", stderr: "" }),
    startProcess: (input) =>
      Effect.gen(function* () {
        yield* Ref.update(state.starts, (current) => [
          ...current,
          { cwd: input.cwd ?? "", command: input.command, envs: input.envs },
        ]);
        input.onStdout(`${input.cwd ?? ""}\n`);
        return {
          pid: 4242,
          sendStdin: (data: string) =>
            Ref.update(state.writes, (current) => [...current, data]).pipe(Effect.asVoid),
          kill: Ref.update(state.kills, (current) => current + 1).pipe(Effect.asVoid),
          wait: Effect.never,
        };
      }),
    writeAttachment: () => Effect.succeed(false),
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

describe("cloud terminal", () => {
  it.effect("opens, streams, writes, resizes, clears, and closes in the sandbox", () => {
    const scenario = Effect.gen(function* () {
      const starts = yield* Ref.make<
        ReadonlyArray<{
          cwd: string;
          command: string;
          envs: Readonly<Record<string, string>> | undefined;
        }>
      >([]);
      const writes = yield* Ref.make<ReadonlyArray<string>>([]);
      const kills = yield* Ref.make(0);
      const cloud = machine({ starts, writes, kills });
      return yield* Effect.gen(function* () {
        const terminal = yield* CloudTerminal.CloudTerminal;
        expect(yield* terminal.hasThread(threadId)).toBe(true);

        const open = yield* terminal.open({
          threadId,
          terminalId: "terminal-1",
          cwd: workspacePath,
          env: { T3CODE_PROJECT_ROOT: "/controller/repo", FOO: "bar" },
        });
        expect(open).toMatchObject({
          threadId,
          terminalId: "terminal-1",
          cwd: workspacePath,
          status: "running",
          pid: 4242,
        });
        expect(open.history).toContain(workspacePath);
        expect(yield* terminal.hasLiveSession({ threadId, terminalId: "terminal-1" })).toBe(true);

        const events = yield* terminal
          .attachOutputStream({ threadId, terminalId: "terminal-1" })
          .pipe(Stream.take(1), Stream.runCollect);
        const snapshot = events[0];
        expect(snapshot?.type).toBe("snapshot");
        expect(snapshot?.type === "snapshot" ? snapshot.snapshot.history : "").toContain(
          workspacePath,
        );

        yield* terminal.write({ threadId, terminalId: "terminal-1", data: "pwd\n" });
        expect(yield* Ref.get(writes)).toEqual(["pwd\n"]);

        yield* terminal.resize({
          threadId,
          terminalId: "terminal-1",
          cols: 100,
          rows: 40,
        });
        yield* terminal.clear({ threadId, terminalId: "terminal-1" });
        expect(yield* terminal.listMetadata).toEqual([
          expect.objectContaining({ terminalId: "terminal-1", cwd: workspacePath }),
        ]);

        const calls = yield* Ref.get(starts);
        expect(calls).toEqual([expect.objectContaining({ cwd: workspacePath })]);
        expect(calls[0]?.command).toContain("script");
        expect(calls[0]?.envs).toMatchObject({
          T3CODE_PROJECT_ROOT: workspacePath,
          FOO: "bar",
          TERM: "xterm-256color",
        });

        yield* terminal.close({ threadId, terminalId: "terminal-1" });
        expect(yield* terminal.listMetadata).toEqual([]);
        expect(yield* terminal.hasLiveSession({ threadId, terminalId: "terminal-1" })).toBe(false);
        expect(yield* Ref.get(kills)).toBe(1);
      }).pipe(
        Effect.provide(
          CloudTerminal.layer.pipe(
            Layer.provideMerge(Layer.succeed(CloudThreads.CloudThreads, cloud)),
          ),
        ),
      );
    });
    return scenario;
  });

  it("allows terminal-only attach to a live session and requires orchestration to start one", () => {
    expect(
      CloudTerminal.cloudTerminalAccess({
        hasLiveSession: true,
        hasOrchestrationScope: false,
      }),
    ).toBe("attach");
    expect(
      CloudTerminal.cloudTerminalAccess({
        hasLiveSession: false,
        hasOrchestrationScope: false,
      }),
    ).toBe("denied");
    expect(
      CloudTerminal.cloudTerminalAccess({
        hasLiveSession: false,
        hasOrchestrationScope: true,
      }),
    ).toBe("start");
  });

  it.effect("rejects a cwd outside the thread's sandbox", () => {
    const scenario = Effect.gen(function* () {
      const starts = yield* Ref.make<
        ReadonlyArray<{
          cwd: string;
          command: string;
          envs: Readonly<Record<string, string>> | undefined;
        }>
      >([]);
      const writes = yield* Ref.make<ReadonlyArray<string>>([]);
      const kills = yield* Ref.make(0);
      const cloud = machine({ starts, writes, kills });
      return yield* Effect.gen(function* () {
        const terminal = yield* CloudTerminal.CloudTerminal;
        const error = yield* terminal
          .open({
            threadId,
            terminalId: "terminal-2",
            cwd: "/tmp/not-the-sandbox",
          })
          .pipe(Effect.flip);
        expect(error._tag).toBe("TerminalCwdNotFoundError");

        const deniedStart = yield* terminal
          .open(
            {
              threadId,
              terminalId: "terminal-3",
              cwd: workspacePath,
            },
            { allowStart: false },
          )
          .pipe(Effect.flip);
        expect(deniedStart._tag).toBe("TerminalSessionLookupError");
        expect(yield* Ref.get(starts)).toEqual([]);
      }).pipe(
        Effect.provide(
          CloudTerminal.layer.pipe(
            Layer.provideMerge(Layer.succeed(CloudThreads.CloudThreads, cloud)),
          ),
        ),
      );
    });
    return scenario;
  });
});
