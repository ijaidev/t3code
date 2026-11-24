import {
  SandboxProviderAccountId,
  SandboxProviderKind,
  type SandboxProviderCapabilities,
} from "@t3tools/contracts";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as SecretResolver from "@t3tools/secret-provider-core/server/SecretResolver";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";

import * as ServerConfig from "../config.ts";
import * as ServerSettingsModule from "../serverSettings.ts";
import * as SandboxAccounts from "./SandboxAccounts.ts";
import { SandboxProviderError, type SandboxProviderDriver } from "./SandboxProvider.ts";
import * as SandboxLifecycle from "./SandboxLifecycle.ts";
import * as SandboxStore from "./SandboxStore.ts";

const E2B = SandboxProviderKind.make("e2b");

const capabilities = (
  overrides: Partial<SandboxProviderCapabilities> = {},
): SandboxProviderCapabilities => ({
  create: true,
  exec: true,
  pauseResume: true,
  delete: true,
  snapshots: true,
  privatePorts: true,
  sizes: [{ id: "small", label: "Small", cpuCount: 2, memoryMB: 1024 }],
  regions: [{ id: "us", label: "United States" }],
  ...overrides,
});

interface FakeDriverState {
  readonly calls: Ref.Ref<ReadonlyArray<string>>;
  readonly validKey: string;
  readonly failCreate: boolean;
  readonly handshake: string;
}

const fakeDriver = (
  kind: SandboxProviderKind,
  state: FakeDriverState,
  driverCapabilities = capabilities(),
): SandboxProviderDriver => ({
  kind,
  label: kind === E2B ? "E2B" : "Acme cloud",
  capabilities: driverCapabilities,
  validateCredential: (account) =>
    account.apiKey === state.validKey
      ? Effect.void
      : Effect.fail(
          new SandboxProviderError({
            providerKind: kind,
            operation: "validate-credential",
            detail: "The provider rejected the API key.",
            statusCode: 401,
          }),
        ),
  create: (_input) =>
    Effect.gen(function* () {
      yield* Ref.update(state.calls, (calls) => [...calls, "create"]);
      if (state.failCreate) {
        return yield* new SandboxProviderError({
          providerKind: kind,
          operation: "create",
          detail: "The provider has no capacity right now.",
        });
      }
      return { providerSandboxId: `${kind}-sandbox-1` };
    }),
  get: () => Effect.succeed({ status: "running" as const }),
  pause: () => Ref.update(state.calls, (calls) => [...calls, "pause"]).pipe(Effect.asVoid),
  resume: () => Ref.update(state.calls, (calls) => [...calls, "resume"]).pipe(Effect.asVoid),
  delete: () => Ref.update(state.calls, (calls) => [...calls, "delete"]).pipe(Effect.asVoid),
  exec: () =>
    Ref.update(state.calls, (calls) => [...calls, "exec"]).pipe(
      Effect.as({
        exitCode: 0,
        stdout: state.handshake,
        stderr: "",
      }),
    ),
});

const testLayer = (
  drivers: ReadonlyArray<SandboxProviderDriver>,
  settings: Parameters<typeof ServerSettingsModule.layerTest>[0] = {},
) => {
  const accountsLayer = SandboxAccounts.layerFromDrivers(drivers);
  const storeLayer = SandboxStore.layer;
  return Layer.merge(
    Layer.mergeAll(accountsLayer, storeLayer),
    SandboxLifecycle.layer.pipe(Layer.provide(accountsLayer), Layer.provide(storeLayer)),
  ).pipe(
    Layer.provideMerge(ServerSettingsModule.layerTest(settings)),
    Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "t3-sandbox-test-" })),
    Layer.provide(NodeServices.layer),
    Layer.provide(NodeCrypto.layer),
    Layer.provide(Layer.mock(SecretResolver.SecretResolver)({})),
  );
};

const accountId = SandboxProviderAccountId.make("e2b-work");

const connect = (kind = E2B) =>
  SandboxAccounts.SandboxAccounts.pipe(
    Effect.flatMap((accounts) =>
      accounts.connect({
        id: accountId,
        config: {
          kind,
          label: kind === E2B ? "E2B" : "Acme",
          apiKey: "valid-key",
          template: "base",
          enabled: true,
        },
      }),
    ),
  );

const state = (overrides: Partial<FakeDriverState> = {}) =>
  Effect.gen(function* () {
    return {
      calls: yield* Ref.make<ReadonlyArray<string>>([]),
      validKey: "valid-key",
      failCreate: false,
      handshake: JSON.stringify({
        kind: "t3-thread-host",
        protocolVersion: "1",
        capabilities: ["exec", "filesystem", "git", "ports"],
      }),
      ...overrides,
    };
  });

describe("sandbox accounts", () => {
  it.effect("rejects an invalid key without saving the account", () =>
    Effect.gen(function* () {
      const fake = yield* state();
      yield* Effect.gen(function* () {
        const accounts = yield* SandboxAccounts.SandboxAccounts;
        const settings = yield* ServerSettingsModule.ServerSettingsService;
        const failure = yield* Effect.flip(
          accounts.connect({
            id: accountId,
            config: {
              kind: E2B,
              label: "E2B",
              apiKey: "wrong-key",
              template: "base",
              enabled: true,
            },
          }),
        );
        expect(failure.detail).toBe("The provider rejected the API key.");
        expect((yield* settings.getSettings).sandboxProviders).toEqual({});
      }).pipe(Effect.provide(testLayer([fakeDriver(E2B, fake)])));
    }),
  );

  it.effect("validates before saving and redacts the key from clients", () =>
    Effect.gen(function* () {
      const fake = yield* state();
      yield* Effect.gen(function* () {
        const accounts = yield* SandboxAccounts.SandboxAccounts;
        const settings = yield* ServerSettingsModule.ServerSettingsService;
        const connected = yield* connect();
        expect(connected.account.label).toBe("E2B");
        expect(connected.capabilities.pauseResume).toBe(true);
        const saved = yield* settings.getSettings;
        expect(saved.sandboxProviders[accountId]?.apiKey).toBe("valid-key");
        const redacted = ServerSettingsModule.redactServerSettingsForClient(saved, {
          secretReferences: false,
        });
        expect(redacted.sandboxProviders[accountId]?.apiKey).not.toBe("valid-key");
        const listed = yield* accounts.list;
        expect(listed.providers.map((provider) => provider.kind)).toEqual([E2B]);
        expect(listed.accounts[0]?.id).toBe(accountId);
      }).pipe(Effect.provide(testLayer([fakeDriver(E2B, fake)])));
    }),
  );
});

describe("sandbox lifecycle", () => {
  it.effect("creates, pauses, resumes, checks, and deletes one test sandbox", () =>
    Effect.gen(function* () {
      const fake = yield* state();
      yield* Effect.gen(function* () {
        yield* connect();
        const lifecycle = yield* SandboxLifecycle.SandboxLifecycle;
        const store = yield* SandboxStore.SandboxStore;
        const created = yield* lifecycle.createTestSandbox({ accountId });
        expect(created.sandbox.status).toBe("running");
        expect(created.sandbox.providerSandboxId).toBe("e2b-sandbox-1");

        const paused = yield* lifecycle.pauseTestSandbox({ sandboxId: created.sandbox.id });
        expect(paused.sandbox.status).toBe("paused");
        const resumed = yield* lifecycle.resumeTestSandbox({ sandboxId: created.sandbox.id });
        expect(resumed.sandbox.status).toBe("running");
        const checked = yield* lifecycle.checkTestSandbox({ sandboxId: created.sandbox.id });
        expect(checked.health.status).toBe("ready");
        expect(checked.sandbox.lastHealth?.capabilities).toEqual([
          "exec",
          "filesystem",
          "git",
          "ports",
        ]);
        expect((yield* store.list).length).toBe(1);

        yield* lifecycle.deleteTestSandbox({ sandboxId: created.sandbox.id });
        expect(yield* store.list).toEqual([]);
        expect(yield* Ref.get(fake.calls)).toEqual(["create", "pause", "resume", "exec", "delete"]);
      }).pipe(Effect.provide(testLayer([fakeDriver(E2B, fake)])));
    }),
  );

  it.effect("leaves no record when a create fails", () =>
    Effect.gen(function* () {
      const fake = yield* state({ failCreate: true });
      yield* Effect.gen(function* () {
        yield* connect();
        const lifecycle = yield* SandboxLifecycle.SandboxLifecycle;
        const store = yield* SandboxStore.SandboxStore;
        const failure = yield* Effect.flip(lifecycle.createTestSandbox({ accountId }));
        expect(failure.detail).toBe("The provider has no capacity right now.");
        expect(yield* store.list).toEqual([]);
        expect(yield* Ref.get(fake.calls)).toEqual(["create"]);
      }).pipe(Effect.provide(testLayer([fakeDriver(E2B, fake)])));
    }),
  );

  it.effect("runs the same lifecycle through a second provider's declared capabilities", () =>
    Effect.gen(function* () {
      const second = SandboxProviderKind.make("acme");
      const fake = yield* state();
      yield* Effect.gen(function* () {
        const accounts = yield* SandboxAccounts.SandboxAccounts;
        yield* connect(second);
        const listed = yield* accounts.list;
        expect(listed.providers[0]?.capabilities.pauseResume).toBe(false);
        const lifecycle = yield* SandboxLifecycle.SandboxLifecycle;
        const store = yield* SandboxStore.SandboxStore;
        const created = yield* lifecycle.createTestSandbox({
          accountId,
          timeoutMinutes: 5,
        });
        expect(created.sandbox.providerKind).toBe(second);
        yield* lifecycle.deleteTestSandbox({ sandboxId: created.sandbox.id });
        expect(yield* store.list).toEqual([]);
      }).pipe(
        Effect.provide(
          testLayer([
            fakeDriver(second, fake, capabilities({ pauseResume: false, privatePorts: false })),
          ]),
        ),
      );
    }),
  );
});

it.effect("disconnecting deletes the account's test sandboxes before removing its key", () =>
  Effect.gen(function* () {
    const fake = yield* state();
    yield* Effect.gen(function* () {
      yield* connect();
      const lifecycle = yield* SandboxLifecycle.SandboxLifecycle;
      const store = yield* SandboxStore.SandboxStore;
      const settings = yield* ServerSettingsModule.ServerSettingsService;
      const created = yield* lifecycle.createTestSandbox({ accountId });
      yield* lifecycle.disconnectAccount({ accountId });
      expect(yield* store.list).toEqual([]);
      expect((yield* settings.getSettings).sandboxProviders).toEqual({});
      expect(yield* Ref.get(fake.calls)).toEqual(["create", "delete"]);
      expect(created.sandbox.accountId).toBe(accountId);
    }).pipe(Effect.provide(testLayer([fakeDriver(E2B, fake)])));
  }),
);
