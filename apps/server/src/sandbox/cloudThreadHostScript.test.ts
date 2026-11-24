// @effect-diagnostics-next-line nodeBuiltinImport:off -- the test drives a real host process over stdio.
import * as NodeChildProcess from "node:child_process";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- the test creates a temporary workspace fixture.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- the test builds fixture paths.
import * as NodePath from "node:path";
import * as NodeReadline from "node:readline";

import { describe, expect, it } from "@effect/vitest";

import { CLOUD_THREAD_HOST_SCRIPT } from "./cloudThreadHostScript.ts";

interface HostProcess {
  readonly child: NodeChildProcess.ChildProcessWithoutNullStreams;
  readonly iterator: AsyncIterator<string>;
}

const nextLine = async (iterator: AsyncIterator<string>): Promise<string> => {
  const result = await iterator.next();
  if (result.done) throw new Error("The thread host stopped unexpectedly.");
  return result.value;
};

const startHost = async (root: string): Promise<HostProcess> => {
  const script = NodePath.join(root, "thread-host.py");
  const workspace = NodePath.join(root, "workspace");
  await NodeFSP.mkdir(workspace, { recursive: true });
  await NodeFSP.writeFile(script, CLOUD_THREAD_HOST_SCRIPT, "utf8");
  const child = NodeChildProcess.spawn("python3", ["-u", script], {
    cwd: workspace,
    env: { ...process.env, HOME: root },
  });
  const lines = NodeReadline.createInterface({ input: child.stdout });
  const iterator = lines[Symbol.asyncIterator]();
  const ready = JSON.parse(await nextLine(iterator)) as { readonly type: string };
  expect(ready.type).toBe("ready");
  return { child, iterator };
};

const runTurn = async (host: HostProcess, text: string) => {
  const seenRequests: Array<unknown> = [];
  host.child.stdin.write(
    `${JSON.stringify({
      type: "turn",
      runId: "run-1",
      model: "claude-sonnet-4-5",
      maxTokens: 8192,
      system: "test",
      text,
      attachments: [],
      history: [],
      approvalMode: "none",
    })}\n`,
  );
  while (true) {
    const message = JSON.parse(await nextLine(host.iterator)) as Record<string, unknown>;
    if (message["type"] === "model_request") {
      seenRequests.push(message["request"]);
      host.child.stdin.write(
        `${JSON.stringify({
          type: "model_response",
          id: message["id"],
          ok: true,
          content: [{ type: "text", text: `answer to ${text}` }],
        })}\n`,
      );
      continue;
    }
    if (message["type"] === "turn_end") return seenRequests;
  }
};

const stopHost = async (host: HostProcess) => {
  host.child.kill("SIGKILL");
  await new Promise<void>((resolve) => host.child.once("exit", () => resolve()));
};

describe("cloud thread host history", () => {
  it("reloads the conversation after the host process restarts", async () => {
    const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-cloud-host-"));
    try {
      const first = await startHost(root);
      await runTurn(first, "first prompt");
      await stopHost(first);

      const persisted = JSON.parse(
        await NodeFSP.readFile(NodePath.join(root, "history.json"), "utf8"),
      ) as unknown;
      expect(JSON.stringify(persisted)).toContain("first prompt");

      const second = await startHost(root);
      const requests = await runTurn(second, "second prompt");
      await stopHost(second);
      const serialized = JSON.stringify(requests);
      expect(serialized).toContain("first prompt");
      expect(serialized).toContain("answer to first prompt");
      expect(serialized).toContain("second prompt");
    } finally {
      await NodeFSP.rm(root, { recursive: true, force: true });
    }
  });
});
