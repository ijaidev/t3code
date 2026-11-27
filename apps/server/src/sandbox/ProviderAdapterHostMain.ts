import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSocketServer from "@effect/platform-node/NodeSocketServer";
import { ProviderDriverKind, settingsSecretLiteral } from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as ProviderEventLoggers from "@t3tools/provider-core/server/ProviderEventLoggers";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import {
  ProviderAdapterHostRefusal,
  serveProviderAdapterHost,
} from "@t3tools/provider-core/server/ProviderAdapterHost";
import type { ProviderAdapterHostAdapterCreateInput } from "@t3tools/provider-core/server/ProviderAdapterHostProtocol";
import * as SandboxProviderHost from "@t3tools/provider-core/server/SandboxProviderHost";
import * as OpenCodeRuntime from "@t3tools/provider-opencode/server/OpenCodeRuntime";
import * as OpenCodeServerLedger from "@t3tools/provider-opencode/server/OpenCodeServerLedger";
import * as OpenCode2AdapterV2 from "@t3tools/provider-opencode/server/v2/adapter";
import * as OpenCode2Client from "@t3tools/provider-opencode/server/v2/OpenCode2Client";
import * as OpenCode2Server from "@t3tools/provider-opencode/server/v2/OpenCode2Server";
import { OpenCodeSettings } from "@t3tools/provider-opencode/settings";
import * as Digest from "effect/crypto/Digest";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Effect from "effect/Effect";
import { FetchHttpClient } from "effect/http";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

const DEFAULT_PORT = 8790;
const HOST_VERSION = "t3-provider-adapter-host-v1";
const OPENCODE_DRIVER = ProviderDriverKind.make("opencode");

const decodeOpenCodeSettings = Schema.decodeUnknownEffect(OpenCodeSettings);

const listeningLine = (port: number) =>
  `${JSON.stringify({
    type: "t3-provider-adapter-host",
    state: "listening",
    port,
  })}\n`;

const createAdapter = Effect.fn("ProviderAdapterHostMain.createAdapter")(function* (
  input: ProviderAdapterHostAdapterCreateInput,
) {
  if (input.driver !== OPENCODE_DRIVER) {
    return yield* new ProviderAdapterHostRefusal({
      code: "unsupported_driver",
      reason: `This sandbox build cannot run the ${input.driver} provider.`,
    });
  }
  const config = yield* decodeOpenCodeSettings(input.config).pipe(
    Effect.mapError(
      () =>
        new ProviderAdapterHostRefusal({
          code: "invalid_config",
          reason: "The sandbox host rejected the OpenCode provider settings.",
        }),
    ),
  );
  const host = yield* SandboxProviderHost.make({
    paths: input.paths,
    settings: input.settings,
  });
  const openCodeRuntimeLayer = OpenCodeRuntime.layer.pipe(
    Layer.provide(OpenCodeServerLedger.layer({ stateDir: input.paths.stateDir })),
  );
  const adapter = yield* Effect.gen(function* () {
    const client = yield* OpenCode2Client.make;
    const runtime = yield* OpenCodeRuntime.OpenCodeRuntime;
    const server = yield* OpenCode2Server.make({
      binaryPath: config.binaryPath,
      serverUrl: config.serverUrl,
      serverPassword: settingsSecretLiteral(config.serverPassword),
      directory: input.paths.cwd,
      environment: process.env,
    }).pipe(
      Effect.provideService(OpenCode2Client.OpenCode2Client, client),
      Effect.provideService(OpenCodeRuntime.OpenCodeRuntime, runtime),
    );
    return yield* OpenCode2AdapterV2.make(input.instanceId).pipe(
      Effect.provideService(OpenCode2Server.OpenCode2Server, server),
    );
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        Layer.succeed(ProviderHost.ProviderHost, host.host),
        Layer.succeed(
          ProviderEventLoggers.ProviderEventLoggers,
          ProviderEventLoggers.NoOpProviderEventLoggers,
        ),
        IdAllocator.layer,
        McpProviderSessions.layer,
        ProviderContinuationRequests.layer,
        Digest.layerWebCrypto,
        SecureRandom.layerWebCrypto,
      ),
    ),
    Effect.provide(openCodeRuntimeLayer),
    Effect.provide(FetchHttpClient.layer),
    Effect.mapError(
      () =>
        new ProviderAdapterHostRefusal({
          code: "adapter_unavailable",
          reason: "The sandbox could not start the OpenCode provider adapter.",
        }),
    ),
  );
  return { adapter, onStop: host.clearCredentials };
});

export const runProviderAdapterHostMain = async (): Promise<void> => {
  const token = process.env["T3_PROVIDER_HOST_TOKEN"];
  if (token === undefined || token === "") {
    process.stderr.write("T3_PROVIDER_HOST_TOKEN is required.\n");
    process.exitCode = 1;
    return;
  }
  const port = Number(process.env["T3_PROVIDER_HOST_PORT"] ?? DEFAULT_PORT);
  const host = process.env["T3_PROVIDER_HOST_BIND"] ?? "0.0.0.0";

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
            hostVersion: process.env["T3_PROVIDER_HOST_BUILD"] ?? HOST_VERSION,
            createAdapter,
          }).pipe(Effect.scoped),
        )
        .pipe(Effect.forkScoped);
      return yield* Effect.never;
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
};
