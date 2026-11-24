import {
  SandboxError,
  type SandboxAccountConnectInput,
  type SandboxAccountConnectResult,
  type SandboxAccountTestResult,
  SandboxProviderAccountId,
  type SandboxProviderAccountConfig,
  type SandboxProviderAccountSummary,
  type SandboxProviderListResult,
  type ServerSettingsError,
} from "@t3tools/contracts";
import * as SecretResolver from "@t3tools/secret-provider-core/server/SecretResolver";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SecureRandom from "effect/crypto/SecureRandom";

import * as ServerSettingsService from "../serverSettings.ts";
import { requireSettingsSecret } from "../secrets/settingsSecretValue.ts";
import type { SandboxProviderAccount, SandboxProviderDriver } from "./SandboxProvider.ts";
import * as SandboxProviderRegistry from "./SandboxProviderRegistry.ts";

type ResolvedSandboxAccount = {
  readonly account: SandboxProviderAccount;
  readonly driver: SandboxProviderDriver;
};

const accountSummary = (
  id: SandboxProviderAccountId,
  account: SandboxProviderAccountConfig,
): SandboxProviderAccountSummary => ({
  id,
  kind: account.kind,
  label: account.label,
  template: account.template,
  enabled: account.enabled,
});

const settingsError = (operation: "connect-account" | "disconnect-account", cause: unknown) =>
  new SandboxError({
    operation,
    detail: cause instanceof Error ? cause.message : "Could not save the sandbox provider account.",
  });

const settingsReadError = (error: ServerSettingsError) =>
  new SandboxError({ operation: "connect-account", detail: error.message });

export class SandboxAccounts extends Context.Service<
  SandboxAccounts,
  {
    readonly list: Effect.Effect<SandboxProviderListResult, SandboxError>;
    readonly connect: (
      input: SandboxAccountConnectInput,
    ) => Effect.Effect<SandboxAccountConnectResult, SandboxError>;
    readonly test: (
      accountId: SandboxProviderAccountId,
    ) => Effect.Effect<SandboxAccountTestResult, SandboxError>;
    readonly disconnect: (accountId: SandboxProviderAccountId) => Effect.Effect<void, SandboxError>;
    readonly resolve: (
      accountId: SandboxProviderAccountId,
    ) => Effect.Effect<ResolvedSandboxAccount, SandboxError>;
  }
>()("t3/sandbox/SandboxAccounts") {}

const make = Effect.gen(function* () {
  const registry = yield* SandboxProviderRegistry.SandboxProviderRegistry;
  const settingsService = yield* ServerSettingsService.ServerSettingsService;
  const secretResolver = yield* SecretResolver.SecretResolver;
  const random = yield* SecureRandom.SecureRandom;

  const resolveStored = Effect.fn("SandboxAccounts.resolveStored")(function* (
    accountId: SandboxProviderAccountId,
  ) {
    const settings = yield* settingsService.getSettings.pipe(Effect.mapError(settingsReadError));
    const account = settings.sandboxProviders[accountId];
    if (account === undefined) {
      return yield* new SandboxError({
        operation: "connect-account",
        detail: "That sandbox provider account is no longer connected.",
      });
    }
    if (!account.enabled) {
      return yield* new SandboxError({
        operation: "connect-account",
        detail: `${account.label} is turned off. Turn it on in Settings → Sandbox providers.`,
      });
    }
    const driver = registry.find(account.kind);
    if (driver === undefined) {
      return yield* new SandboxError({
        operation: "connect-account",
        detail: `${account.kind} is not supported by this server.`,
      });
    }
    const apiKey = yield* requireSettingsSecret(secretResolver, account.apiKey).pipe(
      Effect.catchTags({
        SettingsSecretUnresolvedError: (error) =>
          new SandboxError({ operation: "connect-account", detail: error.message }),
      }),
    );
    if (apiKey.trim() === "") {
      return yield* new SandboxError({
        operation: "connect-account",
        detail: `${account.label} has no API key. Reconnect the account with a key.`,
      });
    }
    return {
      account: {
        id: accountId,
        kind: account.kind,
        label: account.label,
        apiKey,
        template: account.template,
      },
      driver,
    } satisfies ResolvedSandboxAccount;
  });

  const list = settingsService.getSettings.pipe(
    Effect.mapError(settingsReadError),
    Effect.map((settings) => ({
      providers: registry.list,
      accounts: Object.entries(settings.sandboxProviders).map(([id, account]) =>
        accountSummary(id as SandboxProviderAccountId, account),
      ),
    })),
  );

  const connect = Effect.fn("SandboxAccounts.connect")(function* (
    input: SandboxAccountConnectInput,
  ) {
    const id = input.id ?? SandboxProviderAccountId.make(yield* random.uuidV4());
    const driver = registry.find(input.config.kind);
    if (driver === undefined) {
      return yield* new SandboxError({
        operation: "connect-account",
        detail: `${input.config.kind} is not a supported sandbox provider.`,
      });
    }
    const apiKey = yield* requireSettingsSecret(secretResolver, input.config.apiKey).pipe(
      Effect.catchTags({
        SettingsSecretUnresolvedError: (error) =>
          new SandboxError({ operation: "connect-account", detail: error.message }),
      }),
    );
    if (apiKey.trim() === "") {
      return yield* new SandboxError({
        operation: "connect-account",
        detail: "Enter the provider API key to connect this account.",
      });
    }
    const candidate: SandboxProviderAccount = {
      id,
      kind: input.config.kind,
      label: input.config.label,
      apiKey,
      template: input.config.template,
    };
    yield* driver.validateCredential(candidate).pipe(
      Effect.mapError(
        (error) =>
          new SandboxError({
            operation: "connect-account",
            detail: error.detail,
          }),
      ),
    );
    yield* settingsService.updateSettings({ sandboxProviders: { [id]: input.config } }).pipe(
      Effect.mapError((error) => settingsError("connect-account", error)),
      Effect.asVoid,
    );
    return {
      account: accountSummary(id, input.config),
      capabilities: driver.capabilities,
    };
  });

  const test = Effect.fn("SandboxAccounts.test")(function* (accountId: SandboxProviderAccountId) {
    const { account, driver } = yield* resolveStored(accountId);
    yield* driver.validateCredential(account).pipe(
      Effect.mapError(
        (error) =>
          new SandboxError({
            operation: "test-account",
            detail: error.detail,
          }),
      ),
    );
    return {
      account: accountSummary(accountId, {
        kind: account.kind,
        label: account.label,
        apiKey: "",
        template: account.template,
        enabled: true,
      }),
      capabilities: driver.capabilities,
      checkedAt: DateTime.formatIso(yield* DateTime.now),
    };
  });

  const disconnect = Effect.fn("SandboxAccounts.disconnect")(function* (
    accountId: SandboxProviderAccountId,
  ) {
    const settings = yield* settingsService.getSettings.pipe(Effect.mapError(settingsReadError));
    if (!Object.hasOwn(settings.sandboxProviders, accountId)) return;
    yield* settingsService.updateSettings({ sandboxProviders: { [accountId]: null } }).pipe(
      Effect.mapError((error) => settingsError("disconnect-account", error)),
      Effect.asVoid,
    );
  });

  return SandboxAccounts.of({ list, connect, test, disconnect, resolve: resolveStored });
});

const layerWithRegistry = (
  registryLayer: Layer.Layer<SandboxProviderRegistry.SandboxProviderRegistry>,
) => Layer.effect(SandboxAccounts, make).pipe(Layer.provide(registryLayer));

export const layerFromDrivers = (drivers: ReadonlyArray<SandboxProviderDriver>) =>
  layerWithRegistry(SandboxProviderRegistry.layerFromDrivers(drivers));

export const layer = layerWithRegistry(SandboxProviderRegistry.layer);
