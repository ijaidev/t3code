import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSocketServer from "@effect/platform-node/NodeSocketServer";
import { serveProviderAdapterHost } from "@t3tools/provider-core/server/ProviderAdapterHost";
import { runClaudeHistoryWorker } from "@t3tools/provider-claude/claudeHistoryWorker";
import * as Effect from "effect/Effect";

import {
  clearProviderHostControlEnvironment,
  PROVIDER_ADAPTER_HOST_BUILD,
} from "./ProviderAdapterHostEnvironment.ts";
import { createProviderAdapterHostAdapter } from "./ProviderAdapterHostAdapters.ts";

const DEFAULT_PORT = 8790;

const listeningLine = (port: number) =>
  `${JSON.stringify({
    type: "t3-provider-adapter-host",
    state: "listening",
    port,
  })}\n`;

export const runClaudeHistoryWorkerIfRequested = async (): Promise<boolean> => {
  const [subcommand, method, sessionId, options] = process.argv.slice(2);
  if (subcommand !== "__claude-history") return false;
  await runClaudeHistoryWorker(method ?? "", sessionId ?? "", options);
  return true;
};

export const runProviderAdapterHostMain = async (): Promise<void> => {
  const token = process.env["T3_PROVIDER_HOST_TOKEN"];
  if (token === undefined || token === "") {
    process.stderr.write("T3_PROVIDER_HOST_TOKEN is required.\n");
    process.exitCode = 1;
    return;
  }
  const port = Number(process.env["T3_PROVIDER_HOST_PORT"] ?? DEFAULT_PORT);
  const host = process.env["T3_PROVIDER_HOST_BIND"] ?? "0.0.0.0";
  const build = process.env["T3_PROVIDER_HOST_BUILD"] ?? PROVIDER_ADAPTER_HOST_BUILD;
  // The harness and every tool it spawns must not inherit the controller channel.
  clearProviderHostControlEnvironment(process.env);

  await Effect.runPromise(
    Effect.gen(function* () {
      const server = yield* NodeSocketServer.makeWebSocket({ host, port }).pipe(Effect.orDie);
      const address = server.address;
      if (address._tag !== "InetAddressV4") {
        return yield* Effect.die("Provider adapter host bound to an unexpected address.");
      }
      process.stdout.write(listeningLine(address.port));
      yield* server
        .run((socket) =>
          serveProviderAdapterHost(socket, {
            token,
            hostVersion: build,
            createAdapter: createProviderAdapterHostAdapter,
          }).pipe(Effect.scoped),
        )
        .pipe(Effect.forkScoped);
      return yield* Effect.never;
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
};
