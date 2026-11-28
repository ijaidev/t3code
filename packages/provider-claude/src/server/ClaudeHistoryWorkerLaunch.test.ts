import { describe, expect, it } from "@effect/vitest";

import { claudeHistoryWorkerArguments } from "./ClaudeHistoryWorkerLaunch.ts";

describe("Claude history worker launch", () => {
  it("runs the worker inside a single executable", () => {
    expect(
      claudeHistoryWorkerArguments({
        fromSource: false,
        executable: true,
        modulePath: "/app/t3",
        sourceEntryPath: "/app/claude-history-worker.ts",
        controllerEntryPath: "/app/bin.mjs",
        controllerEntryExists: true,
      }),
    ).toEqual(["__claude-history"]);
  });

  it("runs the sibling TypeScript entry from source", () => {
    expect(
      claudeHistoryWorkerArguments({
        fromSource: true,
        executable: false,
        modulePath: "/repo/packages/provider-claude/src/server/adapter.ts",
        sourceEntryPath: "/repo/packages/provider-claude/src/claude-history-worker.ts",
        controllerEntryPath: "/repo/packages/provider-claude/src/server/bin.mjs",
        controllerEntryExists: false,
      }),
    ).toEqual([
      "--experimental-strip-types",
      "/repo/packages/provider-claude/src/claude-history-worker.ts",
    ]);
  });

  it("uses the CLI entry beside a split controller bundle", () => {
    expect(
      claudeHistoryWorkerArguments({
        fromSource: false,
        executable: false,
        modulePath: "/app/dist/adapter-abc.mjs",
        sourceEntryPath: "/app/dist/claude-history-worker.ts",
        controllerEntryPath: "/app/dist/bin.mjs",
        controllerEntryExists: true,
      }),
    ).toEqual(["/app/dist/bin.mjs", "__claude-history"]);
  });

  it("dispatches through the provider host bundle when no CLI entry sits beside it", () => {
    expect(
      claudeHistoryWorkerArguments({
        fromSource: false,
        executable: false,
        modulePath: "/home/t3/.t3/provider-host/provider-adapter-host.mjs",
        sourceEntryPath: "/home/t3/.t3/provider-host/claude-history-worker.ts",
        controllerEntryPath: "/home/t3/.t3/provider-host/bin.mjs",
        controllerEntryExists: false,
      }),
    ).toEqual(["/home/t3/.t3/provider-host/provider-adapter-host.mjs", "__claude-history"]);
  });
});
