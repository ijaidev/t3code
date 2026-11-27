// @effect-diagnostics nodeBuiltinImport:off -- a temp file keeps the test's bundle lookup synchronous.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import {
  DEFAULT_SERVER_SETTINGS,
  ProviderDriverKind,
  ProviderInstanceId,
  SandboxProviderAccountId,
  SandboxProviderKind,
  type SandboxProviderCapabilities,
  ThreadId,
} from "@t3tools/contracts";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSocketServer from "@effect/platform-node/NodeSocketServer";
import * as SecretResolver from "@t3tools/secret-provider-core/server/SecretResolver";
import { describe, expect, it } from "@effect/vitest";
import { onTestFinished } from "vite-plus/test";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";

import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { serveProviderAdapterHost } from "@t3tools/provider-core/server/ProviderAdapterHost";

import * as ServerConfig from "../config.ts";
import * as ServerSettingsModule from "../serverSettings.ts";
import * as CloudRepositoryCredential from "./CloudRepositoryCredential.ts";
import * as CloudProviderHost from "./CloudProviderHost.ts";
import * as CloudThreadStore from "./CloudThreadStore.ts";
import * as CloudThreads from "./CloudThreads.ts";
import { PROVIDER_ADAPTER_HOST_BUILD } from "./ProviderAdapterHostEnvironment.ts";
import * as SandboxAccounts from "./SandboxAccounts.ts";
import { type SandboxProviderDriver } from "./SandboxProvider.ts";
import * as SandboxLifecycle from "./SandboxLifecycle.ts";
import * as SandboxStore from "./SandboxStore.ts";

const e2b = SandboxProviderKind.make("e2b");
const accountId = SandboxProviderAccountId.make("e2b-work");
const threadId = ThreadId.make("thread-provider-host");
const instanceId = ProviderInstanceId.make("opencode");
const driverKind = ProviderDriverKind.make("opencode");

const capabilities: SandboxProviderCapabilities = {
  create: true,
  exec: true,
  pauseResume: true,
  delete: true,
  snapshots: true,
  privatePorts: true,
  sizes: [{ id: "small", label: "Small", cpuCount: 2, memoryMB: 1024 }],
  regions: [{ id: "us", label: "United States" }],
};

const providerAdapter = {
  instanceId,
  driver: driverKind,
  getCapabilities: () => Effect.succeed({}),
  planSelectionTransition: () => Effect.die("unused planSelectionTransition"),
  openSession: () => Effect.die("unused openSession"),
} as unknown as ProviderAdapter.ProviderAdapterV2["Service"];

const makeDriver = (
  state: {
    readonly execs: Ref.Ref<ReadonlyArray<string>>;
    readonly starts: Ref.Ref<number>;
  },
  hostScope: Scope.Scope,
  options: { readonly failBeforeStart?: string } = {},
): SandboxProviderDriver => {
  let port = 0;
  return {
    kind: e2b,
    label: "E2B",
    capabilities,
    validateCredential: () => Effect.void,
    create: () => Effect.succeed({ providerSandboxId: "e2b-1" }),
    findCreated: () => Effect.succeed([]),
    get: () => Effect.succeed({ status: "running" as const }),
    pause: () => Effect.void,
    resume: () => Effect.void,
    delete: () => Effect.void,
    writeFile: (input) =>
      Ref.update(state.execs, (commands) => [...commands, `write:${input.path}`]).pipe(
        Effect.asVoid,
      ),
    exec: (input) =>
      Ref.update(state.execs, (commands) => [...commands, input.command]).pipe(
        Effect.as(
          input.command.includes("$HOME")
            ? { exitCode: 0, stdout: "/home/t3\n", stderr: "" }
            : { exitCode: 0, stdout: "", stderr: "" },
        ),
      ),
    startProcess: (input) =>
      Effect.gen(function* () {
        yield* Ref.update(state.starts, (count) => count + 1);
        if (options.failBeforeStart !== undefined) {
          input.onStderr(`${options.failBeforeStart}\n`);
          return {
            pid: 1,
            sendStdin: () => Effect.void,
            kill: Effect.void,
            wait: Effect.succeed({ exitCode: 1 }),
          };
        }
        const token = input.envs?.["T3_PROVIDER_HOST_TOKEN"];
        if (token === undefined) return yield* Effect.die("missing host token");
        const server = yield* NodeSocketServer.makeWebSocket({
          host: "127.0.0.1",
          port: 0,
        }).pipe(Effect.orDie);
        const address = server.address;
        if (address._tag !== "InetAddressV4") return yield* Effect.die("unexpected address");
        port = address.port;
        yield* server
          .run((socket) =>
            serveProviderAdapterHost(socket, {
              token,
              hostVersion: PROVIDER_ADAPTER_HOST_BUILD,
              createAdapter: () =>
                Effect.succeed({ adapter: providerAdapter, onStop: Effect.void }),
            }).pipe(Effect.scoped),
          )
          .pipe(Effect.forkIn(hostScope));
        input.onStdout(
          `${JSON.stringify({ type: "t3-provider-adapter-host", state: "listening", port })}\n`,
        );
        const exited = yield* Deferred.make<void>();
        return {
          pid: 1,
          sendStdin: () => Effect.void,
          kill: Deferred.succeed(exited, undefined).pipe(Effect.asVoid),
          wait: Deferred.await(exited).pipe(Effect.as({ exitCode: 0 })),
        };
      }).pipe(Effect.provideService(Scope.Scope, hostScope)),
    portAccess: () => Effect.succeed({ url: `http://127.0.0.1:${port}`, headers: {} }),
  };
};

const testLayer = (driver: SandboxProviderDriver) => {
  const accounts = SandboxAccounts.layerFromDrivers([driver]);
  const store = SandboxStore.layer;
  const cloudStore = CloudThreadStore.layer;
  const lifecycle = SandboxLifecycle.layer.pipe(Layer.provide(accounts), Layer.provide(store));
  const cloud = CloudThreads.layer.pipe(
    Layer.provide(accounts),
    Layer.provide(store),
    Layer.provide(cloudStore),
    Layer.provide(lifecycle),
    Layer.provide(
      Layer.succeed(CloudRepositoryCredential.CloudRepositoryCredentialResolver, {
        resolve: () => Effect.succeedNone,
      }),
    ),
  );
  return Layer.mergeAll(accounts, store, cloudStore, lifecycle, cloud).pipe(
    Layer.provideMerge(ServerSettingsModule.layerTest({})),
    Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "t3-provider-host-" })),
    Layer.provide(NodeServices.layer),
    Layer.provide(NodeCrypto.layer),
    Layer.provide(Layer.mock(SecretResolver.SecretResolver)({})),
  );
};

const mockProviderHostBundle = () => {
  const bundleDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-provider-host-bundle-"));
  const bundlePath = NodePath.join(bundleDir, "provider-adapter-host.mjs");
  NodeFS.writeFileSync(bundlePath, "process.exit(0);\n");
  const previousBundle = process.env["T3CODE_PROVIDER_HOST_BUNDLE"];
  process.env["T3CODE_PROVIDER_HOST_BUNDLE"] = bundlePath;
  onTestFinished(() => {
    if (previousBundle === undefined) delete process.env["T3CODE_PROVIDER_HOST_BUNDLE"];
    else process.env["T3CODE_PROVIDER_HOST_BUNDLE"] = previousBundle;
    NodeFS.rmSync(bundleDir, { recursive: true, force: true });
  });
};

describe("cloud provider host wiring", () => {
  it.effect("installs the pinned runtime once and reuses the host process", () => {
    const execs = Ref.makeUnsafe<ReadonlyArray<string>>([]);
    const starts = Ref.makeUnsafe(0);
    mockProviderHostBundle();
    return Effect.scoped(
      Effect.gen(function* () {
        const hostScope = yield* Effect.scope;
        const driver = makeDriver({ execs, starts }, hostScope);
        return yield* Effect.gen(function* () {
          const accounts = yield* SandboxAccounts.SandboxAccounts;
          yield* accounts.connect({
            id: accountId,
            config: {
              kind: e2b,
              label: "E2B",
              apiKey: "valid-key",
              template: "base",
              enabled: true,
            },
          });
          const cloud = yield* CloudThreads.CloudThreads;
          const sandbox = yield* (yield* SandboxLifecycle.SandboxLifecycle).createThreadSandbox({
            threadId,
            accountId,
          });
          yield* (yield* CloudThreadStore.CloudThreadStore).upsert({
            threadId,
            sandboxId: sandbox.id,
            accountId,
            providerKind: e2b,
            repositoryUrl: "https://github.com/example/public.git",
            branch: "main",
            baseRef: "main",
            workspacePath: "/home/t3/.t3/threads/thread-provider-host",
            homePath: "/home/t3",
            status: "ready",
            createdAt: "2026-11-27T00:00:00.000Z",
            updatedAt: "2026-11-27T00:00:00.000Z",
          });

          return yield* Effect.scoped(
            Effect.gen(function* () {
              const input = {
                driver: driverKind,
                instanceId,
                config: { binaryPath: "/home/t3/.t3/provider-host/runtime/bin/opencode" },
                settings: DEFAULT_SERVER_SETTINGS,
                environment: [{ name: "OPENAI_API_KEY", value: "sk-test" }],
              };
              const first = yield* cloud.openProviderHost(threadId, input);
              const second = yield* cloud.openProviderHost(threadId, input);

              expect(yield* Ref.get(starts)).toBe(1);
              expect(
                (yield* Ref.get(execs)).some((command) =>
                  command.includes(
                    `'@opencode/cli@${CloudProviderHost.CLOUD_PROVIDER_OPENCODE_VERSION}'`,
                  ),
                ),
              ).toBe(true);
              expect(first.protocolVersion).toBe("1");
              expect(first.hostVersion).toBe(PROVIDER_ADAPTER_HOST_BUILD);
              expect(second.paths.attachmentsDir).toBe("/home/t3/.t3/attachments");

              const rotated = yield* cloud.openProviderHost(threadId, {
                ...input,
                environment: [{ name: "OPENAI_API_KEY", value: "sk-rotated" }],
              });
              expect(yield* Ref.get(starts)).toBe(2);
              yield* Effect.all([first.close, second.close, rotated.close]);
            }),
          );
        }).pipe(Effect.provide(testLayer(driver)));
      }),
    );
  });

  it.effect("caps and redacts the host failure surfaced by openProviderHost", () => {
    const execs = Ref.makeUnsafe<ReadonlyArray<string>>([]);
    const starts = Ref.makeUnsafe(0);
    mockProviderHostBundle();
    const failure = `provider failed sk-live-abc e2b_secretxyz ${"x".repeat(600)}`;
    return Effect.scoped(
      Effect.gen(function* () {
        const hostScope = yield* Effect.scope;
        const driver = makeDriver({ execs, starts }, hostScope, { failBeforeStart: failure });
        return yield* Effect.gen(function* () {
          const accounts = yield* SandboxAccounts.SandboxAccounts;
          yield* accounts.connect({
            id: accountId,
            config: {
              kind: e2b,
              label: "E2B",
              apiKey: "valid-key",
              template: "base",
              enabled: true,
            },
          });
          const cloud = yield* CloudThreads.CloudThreads;
          const sandbox = yield* (yield* SandboxLifecycle.SandboxLifecycle).createThreadSandbox({
            threadId,
            accountId,
          });
          yield* (yield* CloudThreadStore.CloudThreadStore).upsert({
            threadId,
            sandboxId: sandbox.id,
            accountId,
            providerKind: e2b,
            repositoryUrl: "https://github.com/example/public.git",
            branch: "main",
            baseRef: "main",
            workspacePath: "/home/t3/.t3/threads/thread-provider-host",
            homePath: "/home/t3",
            status: "ready",
            createdAt: "2026-11-27T00:00:00.000Z",
            updatedAt: "2026-11-27T00:00:00.000Z",
          });

          const error = yield* cloud
            .openProviderHost(threadId, {
              driver: driverKind,
              instanceId,
              config: { binaryPath: "/home/t3/.t3/provider-host/runtime/bin/opencode" },
              settings: DEFAULT_SERVER_SETTINGS,
              environment: [],
            })
            .pipe(Effect.flip);
          expect(error.detail.length).toBeLessThanOrEqual(400);
          expect(error.detail).not.toContain("sk-live-abc");
          expect(error.detail).not.toContain("e2b_secretxyz");
          expect(error.detail).toContain("[redacted]");
        }).pipe(Effect.provide(testLayer(driver)));
      }),
    );
  });
});
