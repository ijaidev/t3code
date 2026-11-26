// @effect-diagnostics-next-line nodeBuiltinImport:off -- the test drives a real host process over stdio.
import * as NodeChildProcess from "node:child_process";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- the test creates a temporary workspace fixture.
import * as NodeFSP from "node:fs/promises";
import * as NodeNet from "node:net";
import * as NodeOS from "node:os";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- the test builds fixture paths.
import * as NodePath from "node:path";
import * as NodeReadline from "node:readline";

import * as HostProcess from "@t3tools/shared/HostProcess";
import { describe, expect, it } from "@effect/vitest";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";

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
  const child = NodeChildProcess.spawn("python3", ["-u", script, "--stdio"], {
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

interface HostBridge {
  readonly child: NodeChildProcess.ChildProcessWithoutNullStreams;
  readonly ready: Record<string, unknown>;
  readonly next: () => Promise<Record<string, unknown>>;
  readonly send: (message: unknown) => void;
}

const writeHostScript = async (root: string) => {
  const script = NodePath.join(root, "thread-host.py");
  await NodeFSP.mkdir(NodePath.join(root, "workspace"), { recursive: true });
  await NodeFSP.writeFile(script, CLOUD_THREAD_HOST_SCRIPT, "utf8");
  return script;
};

const startBridge = async (root: string, since: number): Promise<HostBridge> => {
  const script = NodePath.join(root, "thread-host.py");
  const workspace = NodePath.join(root, "workspace");
  let child!: NodeChildProcess.ChildProcessWithoutNullStreams;
  let iterator!: AsyncIterator<string>;
  const next = async () => {
    const timedOut = new Promise<never>((_, reject) => {
      AbortSignal.timeout(20_000).addEventListener(
        "abort",
        () => reject(new Error("The bridge stayed silent.")),
        { once: true },
      );
    });
    const result = await Promise.race([iterator.next(), timedOut]);
    if (result.done) throw new Error("The thread host bridge stopped unexpectedly.");
    return JSON.parse(result.value) as Record<string, unknown>;
  };
  for (let attempt = 0; attempt < 50; attempt += 1) {
    child = NodeChildProcess.spawn("python3", ["-u", script, "--connect", String(since)], {
      cwd: workspace,
      env: { ...process.env, HOME: root },
    });
    const lines = NodeReadline.createInterface({ input: child.stdout });
    iterator = lines[Symbol.asyncIterator]();
    try {
      const message = await next();
      if (message["type"] === "ready") {
        return {
          child,
          ready: message,
          next,
          send: (outbound) => child.stdin.write(`${JSON.stringify(outbound)}\n`),
        };
      }
    } catch {
      // The daemon may still be starting; retry with a fresh bridge.
    }
    child.kill("SIGKILL");
    await new Promise<void>((resolve) => child.once("exit", () => resolve()));
  }
  throw new Error("The sandbox thread host did not accept a bridge connection.");
};

const stopBridge = async (bridge: HostBridge) => {
  bridge.child.kill("SIGKILL");
  await new Promise<void>((resolve) => bridge.child.once("exit", () => resolve()));
};

const stopDaemon = async (root: string) => {
  const stopper = NodeChildProcess.spawn(
    "python3",
    ["-u", NodePath.join(root, "thread-host.py"), "--stop"],
    {
      cwd: NodePath.join(root, "workspace"),
      env: { ...process.env, HOME: root },
    },
  );
  await new Promise<void>((resolve) => stopper.once("exit", () => resolve()));
};

describe("cloud thread host ports", () => {
  it.effect("reports a listening port so the thread can preview it", () =>
    Effect.gen(function* () {
      if ((yield* HostProcess.Platform) !== "linux") return;
      const root = yield* Effect.acquireRelease(
        Effect.promise(() => NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-cloud-ports-"))),
        (path) => Effect.promise(() => NodeFSP.rm(path, { recursive: true, force: true })),
      );
      const listener = NodeNet.createServer((socket) => socket.end());
      yield* Effect.acquireRelease(
        Effect.promise(
          () =>
            new Promise<NodeNet.Server>((resolve) => {
              listener.listen(0, "127.0.0.1", () => resolve(listener));
            }),
        ),
        (server) =>
          Effect.promise(() => new Promise<void>((resolve) => server.close(() => resolve()))),
      );
      const address = listener.address();
      if (address === null || typeof address === "string") throw new Error("no listener port");
      const host = yield* Effect.acquireRelease(
        Effect.promise(() => startHost(root)),
        (started) => Effect.promise(() => stopHost(started)),
      );
      const deadline = (yield* Clock.currentTimeMillis) + 8_000;
      while ((yield* Clock.currentTimeMillis) < deadline) {
        const message = JSON.parse(yield* Effect.promise(() => nextLine(host.iterator))) as {
          readonly type?: string;
          readonly ports?: ReadonlyArray<{ readonly port?: number }>;
        };
        if (message.type === "ports" && message.ports?.some((port) => port.port === address.port)) {
          return;
        }
      }
      throw new Error("The host did not report the listening port.");
    }),
  );
});

describe("cloud thread host replay", () => {
  it("keeps a turn alive across a controller drop and replays it once", async () => {
    const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-cloud-replay-"));
    try {
      await writeHostScript(root);
      const first = await startBridge(root, 0);
      first.send({
        type: "turn",
        runId: "run-1",
        model: "claude-sonnet-4-5",
        maxTokens: 8192,
        system: "test",
        text: "first prompt",
        attachments: [],
        history: [],
        approvalMode: "none",
      });
      let request: Record<string, unknown> = {};
      while (request["type"] !== "model_request") request = await first.next();
      first.send({
        type: "model_response",
        id: request["id"],
        ok: true,
        content: [
          { type: "text", text: "Running the command." },
          { type: "tool_use", id: "tool-1", name: "bash", input: { command: "echo replayed" } },
        ],
      });
      while (request["type"] !== "tool_end") request = await first.next();
      expect(request["output"]).toContain("replayed");
      while (request["type"] !== "model_request") request = await first.next();

      // The controller dies while the sandbox waits for the next model response.
      await stopBridge(first);

      const second = await startBridge(root, 0);
      expect(second.ready["activeRunId"]).toBe("run-1");
      const replayed: Array<string> = [];
      let pending: Record<string, unknown> = {};
      while (pending["type"] !== "model_request") {
        pending = await second.next();
        if (pending["type"] !== "ports") {
          replayed.push(String(pending["type"]));
        }
      }
      expect(replayed.filter((type) => type === "turn_started")).toHaveLength(1);
      expect(replayed.filter((type) => type === "text")).toHaveLength(1);
      expect(replayed.filter((type) => type === "tool_start")).toHaveLength(1);
      expect(replayed.filter((type) => type === "tool_end")).toHaveLength(1);
      expect(replayed).not.toContain("turn_end");

      second.send({
        type: "model_response",
        id: pending["id"],
        ok: true,
        content: [{ type: "text", text: "Finished after reconnect." }],
      });
      let ended: Record<string, unknown> = {};
      while (ended["type"] !== "turn_end") ended = await second.next();
      expect(ended["status"]).toBe("completed");
      await stopBridge(second);
    } finally {
      await stopDaemon(root);
      await NodeFSP.rm(root, { recursive: true, force: true });
    }
  });
});

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

describe("cloud thread host history persistence", () => {
  it.effect("does not acknowledge a reset it could not persist", () =>
    Effect.gen(function* () {
      const root = yield* Effect.acquireRelease(
        Effect.promise(() => NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-cloud-reset-"))),
        (path) => Effect.promise(() => NodeFSP.rm(path, { recursive: true, force: true })),
      );
      // A directory where the history file belongs makes os.replace fail even for root.
      yield* Effect.promise(() => NodeFSP.mkdir(NodePath.join(root, "history.json")));
      const host = yield* Effect.acquireRelease(
        Effect.promise(() => startHost(root)),
        (started) => Effect.promise(() => stopHost(started)),
      );
      host.child.stdin.write(`${JSON.stringify({ type: "reset_history" })}\n`);
      let message: { readonly type?: string; readonly ok?: boolean } = {};
      for (let attempt = 0; attempt < 10 && message.type !== "history_reset"; attempt++) {
        message = JSON.parse(yield* Effect.promise(() => nextLine(host.iterator))) as {
          readonly type?: string;
          readonly ok?: boolean;
        };
      }
      expect(message.type).toBe("history_reset");
      expect(message.ok).toBe(false);
    }),
  );
});
