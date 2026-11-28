import * as NodeServices from "@effect/platform-node/NodeServices";
import { ClaudeSettings, ProviderDriverKind, settingsSecretLiteral } from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as ProviderEventLoggers from "@t3tools/provider-core/server/ProviderEventLoggers";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import { ProviderAdapterHostRefusal } from "@t3tools/provider-core/server/ProviderAdapterHost";
import type { ProviderAdapterHostAdapterCreateInput } from "@t3tools/provider-core/server/ProviderAdapterHostProtocol";
import * as ProviderInstallationGate from "@t3tools/provider-core/server/ProviderInstallationGate";
import * as SandboxProviderHost from "@t3tools/provider-core/server/SandboxProviderHost";
import {
  createClaudeAdapterV2,
  makeLayerQueryRunner as makeClaudeQueryRunnerLayer,
} from "@t3tools/provider-claude/server/adapter";
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

import { sandboxProviderEnvironment } from "./ProviderAdapterHostEnvironment.ts";

const OPENCODE_DRIVER = ProviderDriverKind.make("opencode");
const CLAUDE_DRIVER = ProviderDriverKind.make("claudeAgent");

const decodeClaudeSettings = Schema.decodeUnknownEffect(ClaudeSettings);

const decodeOpenCodeSettings = Schema.decodeUnknownEffect(OpenCodeSettings);

const createClaudeAdapter = Effect.fn("ProviderAdapterHostMain.createClaudeAdapter")(function* (
  input: ProviderAdapterHostAdapterCreateInput,
) {
  const config = yield* decodeClaudeSettings(input.config).pipe(
    Effect.mapError(
      () =>
        new ProviderAdapterHostRefusal({
          code: "invalid_config",
          reason: "The sandbox host rejected the Claude provider settings.",
        }),
    ),
  );
  const host = yield* SandboxProviderHost.make({
    paths: input.paths,
    settings: input.settings,
  });
  const adapter = yield* createClaudeAdapterV2(
    {
      instanceId: input.instanceId,
      displayName: input.displayName,
      accentColor: input.accentColor,
      environment: undefined,
      enabled: true,
      config,
    },
    { attachmentsDir: input.paths.attachmentsDir },
  ).pipe(
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
        ProviderInstallationGate.layer,
        Digest.layerWebCrypto,
        SecureRandom.layerWebCrypto,
        makeClaudeQueryRunnerLayer().pipe(
          Layer.provide(
            Layer.mergeAll(
              Layer.succeed(
                ProviderEventLoggers.ProviderEventLoggers,
                ProviderEventLoggers.NoOpProviderEventLoggers,
              ),
              SecureRandom.layerWebCrypto,
            ),
          ),
          Layer.provide(NodeServices.layer),
        ),
      ),
    ),
    Effect.provide(NodeServices.layer),
    Effect.mapError(
      () =>
        new ProviderAdapterHostRefusal({
          code: "adapter_unavailable",
          reason: "The sandbox could not start the Claude provider adapter.",
        }),
    ),
  );
  return { adapter, onStop: host.clearCredentials };
});

export const createProviderAdapterHostAdapter = Effect.fn("ProviderAdapterHost.createAdapter")(
  function* (input: ProviderAdapterHostAdapterCreateInput) {
    if (input.driver === CLAUDE_DRIVER) {
      return yield* createClaudeAdapter(input);
    }
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
        environment: sandboxProviderEnvironment(process.env),
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
  },
);
