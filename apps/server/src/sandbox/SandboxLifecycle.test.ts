import {
  SandboxInstanceId,
  SandboxProviderAccountId,
  SandboxProviderKind,
  type SandboxProviderCapabilities,
  type SandboxTestSandbox,
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

interface FakeSandbox {
  readonly instanceId: string;
  readonly providerSandboxId: string;
  readonly status: "running" | "paused";
}

interface FakeDriverState {
  readonly calls: Ref.Ref<ReadonlyArray<string>>;
  readonly created: Ref.Ref<ReadonlyArray<FakeSandbox>>;
  readonly validKey: string;
  failCreate: boolean;
  failCreateAfterAccept: boolean;
  failFindCreated: boolean;
  failDelete: boolean;
  readonly handshake: string;
}

const failure = (kind: SandboxProviderKind, operation: "create" | "find-created" | "delete") =>
  new SandboxProviderError({
    providerKind: kind,
    operation,
    detail:
      operation === "create"
        ? "The provider has no capacity right now."
        : `The provider failed to ${operation}.`,
  });

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
  create: (input) =>
    Effect.gen(function* () {
      yield* Ref.update(state.calls, (calls) => [...calls, "create"]);
      if (state.failCreate) return yield* failure(kind, "create");
      const sandbox: FakeSandbox = {
        instanceId: input.instanceId,
        providerSandboxId: `${kind}-${input.instanceId}`,
        status: "running",
      };
      yield* Ref.update(state.created, (created) => [...created, sandbox]);
      if (state.failCreateAfterAccept) return yield* failure(kind, "create");
      return { providerSandboxId: sandbox.providerSandboxId };
    }),
  findCreated: (_account, instanceId) =>
    Effect.gen(function* () {
      yield* Ref.update(state.calls, (calls) => [...calls, "find-created"]);
      if (state.failFindCreated) return yield* failure(kind, "find-created");
      const created = yield* Ref.get(state.created);
      return created
        .filter((sandbox) => sandbox.instanceId === instanceId)
        .map(({ instanceId: _instanceId, ...sandbox }) => sandbox);
    }),
  get: () => Effect.succeed({ status: "running" as const }),
  pause: () => Ref.update(state.calls, (calls) => [...calls, "pause"]).pipe(Effect.asVoid),
  resume: () => Ref.update(state.calls, (calls) => [...calls, "resume"]).pipe(Effect.asVoid),
  delete: (_account, providerSandboxId) =>
    Effect.gen(function* () {
      yield* Ref.update(state.calls, (calls) => [...calls, "delete"]);
      if (state.failDelete) return yield* failure(kind, "delete");
      yield* Ref.update(state.created, (created) =>
        created.filter((sandbox) => sandbox.providerSandboxId !== providerSandboxId),
      );
    }),
  exec: () =>
    Ref.update(state.calls, (calls) => [...calls, "exec"]).pipe(
      Effect.as({
        exitCode: 0,
        stdout: state.handshake,
        stderr: "",
      }),
    ),
});

const failingStoreLayer = (path: string) =>
  Layer.effect(
    SandboxStore.SandboxStore,
    Effect.gen(function* () {
      const real = yield* SandboxStore.SandboxStore;
      const upserts = yield* Ref.make(0);
      return SandboxStore.SandboxStore.of({
        ...real,
        upsert: (record) =>
          Ref.updateAndGet(upserts, (count) => count + 1).pipe(
            Effect.flatMap((count) =>
              count === 2
                ? Effect.fail(
                    new SandboxStore.SandboxStoreError({
                      operation: "write",
                      path,
                      cause: new Error("forced settings write failure"),
                    }),
                  )
                : real.upsert(record),
            ),
          ),
      });
    }),
  ).pipe(Layer.provide(SandboxStore.layer));

const testLayer = (
  drivers: ReadonlyArray<SandboxProviderDriver>,
  settings: Parameters<typeof ServerSettingsModule.layerTest>[0] = {},
  storeLayer: typeof SandboxStore.layer | ReturnType<typeof failingStoreLayer> = SandboxStore.layer,
) => {
  const accountsLayer = SandboxAccounts.layerFromDrivers(drivers);
  return Layer.merge(
    Layer.mergeAll(accountsLayer, SandboxStore.layer),
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
      created: yield* Ref.make<ReadonlyArray<FakeSandbox>>([]),
      validKey: "valid-key",
      failCreate: false,
      failCreateAfterAccept: false,
      failFindCreated: false,
      failDelete: false,
      handshake: JSON.stringify({
        kind: "t3-thread-host",
        protocolVersion: "1",
        capabilities: ["exec", "filesystem", "git", "ports"],
      }),
      ...overrides,
    };
  });

const provisioningRecord = (id: string, account = accountId): SandboxTestSandbox => ({
  id: SandboxInstanceId.make(id),
  accountId: account,
  providerKind: E2B,
  providerSandboxId: null,
  status: "provisioning",
  template: "base",
  createdAt: "2026-11-24T00:00:00.000Z",
  updatedAt: "2026-11-24T00:00:00.000Z",
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
        expect(created.sandbox.providerSandboxId).toBe(`e2b-${created.sandbox.id}`);

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

  it.effect("leaves no record when a create fails before the provider accepts it", () =>
    Effect.gen(function* () {
      const fake = yield* state({ failCreate: true });
      yield* Effect.gen(function* () {
        yield* connect();
        const lifecycle = yield* SandboxLifecycle.SandboxLifecycle;
        const store = yield* SandboxStore.SandboxStore;
        const failure = yield* Effect.flip(lifecycle.createTestSandbox({ accountId }));
        expect(failure.detail).toBe("The provider has no capacity right now.");
        expect(yield* store.list).toEqual([]);
        expect(yield* Ref.get(fake.calls)).toEqual(["create", "find-created"]);
      }).pipe(Effect.provide(testLayer([fakeDriver(E2B, fake)])));
    }),
  );

  it.effect("deletes a sandbox the provider accepted before the create failed", () =>
    Effect.gen(function* () {
      const fake = yield* state({ failCreateAfterAccept: true });
      yield* Effect.gen(function* () {
        yield* connect();
        const lifecycle = yield* SandboxLifecycle.SandboxLifecycle;
        const store = yield* SandboxStore.SandboxStore;
        const failure = yield* Effect.flip(lifecycle.createTestSandbox({ accountId }));
        expect(failure.detail).toBe("The provider has no capacity right now.");
        expect(yield* store.list).toEqual([]);
        expect(yield* Ref.get(fake.created)).toEqual([]);
        expect(yield* Ref.get(fake.calls)).toEqual(["create", "find-created", "delete"]);
      }).pipe(Effect.provide(testLayer([fakeDriver(E2B, fake)])));
    }),
  );

  it.effect("keeps a recoverable record when cleanup after a failed create cannot finish", () =>
    Effect.gen(function* () {
      const fake = yield* state({ failCreateAfterAccept: true, failDelete: true });
      yield* Effect.gen(function* () {
        yield* connect();
        const lifecycle = yield* SandboxLifecycle.SandboxLifecycle;
        const store = yield* SandboxStore.SandboxStore;
        const failure = yield* Effect.flip(lifecycle.createTestSandbox({ accountId }));
        expect(failure.detail).toContain("could not be cleaned up");
        const kept = yield* store.list;
        expect(kept.length).toBe(1);
        expect(kept[0]?.providerSandboxId).not.toBeNull();
        expect(kept[0]?.lastError).toContain("could not be cleaned up");

        fake.failDelete = false;
        yield* lifecycle.deleteTestSandbox({ sandboxId: kept[0]!.id });
        expect(yield* store.list).toEqual([]);
        expect(yield* Ref.get(fake.created)).toEqual([]);
      }).pipe(Effect.provide(testLayer([fakeDriver(E2B, fake)])));
    }),
  );

  it.effect("reconciles provisioning records on restart, adopting or dropping provider state", () =>
    Effect.gen(function* () {
      const fake = yield* state();
      yield* Effect.gen(function* () {
        yield* connect();
        const lifecycle = yield* SandboxLifecycle.SandboxLifecycle;
        const store = yield* SandboxStore.SandboxStore;
        const adopted = provisioningRecord("adopted");
        const missing = provisioningRecord("missing");
        yield* store.upsert(adopted);
        yield* store.upsert(missing);
        yield* Ref.update(fake.created, (created) => [
          ...created,
          {
            instanceId: adopted.id,
            providerSandboxId: "e2b-adopted",
            status: "paused" as const,
          },
        ]);

        yield* lifecycle.reconcile;

        const records = yield* store.list;
        expect(records.length).toBe(1);
        expect(records[0]?.id).toBe(adopted.id);
        expect(records[0]?.providerSandboxId).toBe("e2b-adopted");
        expect(records[0]?.status).toBe("paused");
        expect(yield* Ref.get(fake.calls)).toEqual(["find-created", "find-created"]);
      }).pipe(Effect.provide(testLayer([fakeDriver(E2B, fake)])));
    }),
  );

  it.effect("asks the provider before deleting a record with no saved sandbox id", () =>
    Effect.gen(function* () {
      const fake = yield* state();
      yield* Effect.gen(function* () {
        yield* connect();
        const lifecycle = yield* SandboxLifecycle.SandboxLifecycle;
        const store = yield* SandboxStore.SandboxStore;
        const record = provisioningRecord("unconfirmed");
        yield* store.upsert(record);
        yield* Ref.update(fake.created, (created) => [
          ...created,
          {
            instanceId: record.id,
            providerSandboxId: "e2b-unconfirmed",
            status: "running" as const,
          },
        ]);

        yield* lifecycle.deleteTestSandbox({ sandboxId: record.id });

        expect(yield* store.list).toEqual([]);
        expect(yield* Ref.get(fake.calls)).toEqual(["find-created", "delete"]);
        expect(yield* Ref.get(fake.created)).toEqual([]);
      }).pipe(Effect.provide(testLayer([fakeDriver(E2B, fake)])));
    }),
  );

  it.effect(
    "reports a cleanup failure when a created sandbox cannot be deleted after save failure",
    () =>
      Effect.gen(function* () {
        const fake = yield* state({ failDelete: true });
        yield* Effect.gen(function* () {
          yield* connect();
          const lifecycle = yield* SandboxLifecycle.SandboxLifecycle;
          const store = yield* SandboxStore.SandboxStore;
          const failure = yield* Effect.flip(lifecycle.createTestSandbox({ accountId }));
          expect(failure.detail).toContain("could not be deleted");
          const kept = yield* store.list;
          expect(kept.length).toBe(1);
          expect(kept[0]?.providerSandboxId).not.toBeNull();
        }).pipe(
          Effect.provide(testLayer([fakeDriver(E2B, fake)], {}, failingStoreLayer(process.cwd()))),
        );
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
