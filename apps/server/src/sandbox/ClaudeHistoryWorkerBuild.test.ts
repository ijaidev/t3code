// @effect-diagnostics nodeBuiltinImport:off -- this test runs the built CLI artifact.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

import { expect, it } from "vite-plus/test";

const distBin = NodePath.resolve(import.meta.dirname, "../../dist/bin.mjs");
const verifyBuiltArtifacts = process.env["T3CODE_VERIFY_BUILT_ARTIFACTS"] === "1";
const builtArtifactAvailable = NodeFS.existsSync(distBin);

// The controller bundle splits adapters into chunks; the launch path must still reach the CLI.
it.skipIf(!verifyBuiltArtifacts && !builtArtifactAvailable)(
  "runs the built server's __claude-history subcommand",
  () => {
    expect(builtArtifactAvailable).toBe(true);
    const result = NodeChildProcess.spawnSync(
      process.execPath,
      [distBin, "__claude-history", "getSessionMessages", "no-such-session"],
      { encoding: "utf8" },
    );
    expect(result.status).toBe(0);
    expect(result.stdout.trim()).toBe("[]");
  },
);
