import {
  ProviderDriverKind,
  ProviderInstanceId,
  SandboxProviderAccountId,
  SandboxProviderKind,
  type SandboxProviderCapabilities,
  type SandboxProviderSummary,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as ProviderInstanceRegistry from "../provider/ProviderInstanceRegistry.ts";
import * as CloudTargetList from "./CloudTargetList.ts";
import * as SandboxAccounts from "./SandboxAccounts.ts";

const e2b = SandboxProviderKind.make("e2b");
const accountId = SandboxProviderAccountId.make("e2b-work");
const instanceId = ProviderInstanceId.make("claude-work");

const capabilities: SandboxProviderCapabilities = {
  create: true,
  exec: true,
  pauseResume: false,
  delete: true,
  snapshots: false,
  privatePorts: true,
  harness: true,
  sizes: [{ id: "small", label: "Small", cpuCount: 2, memoryMB: 1024 }],
  regions: [{ id: "us", label: "United States" }],
};

const provider: SandboxProviderSummary = { kind: e2b, label: "E2B", capabilities };
const account = {
  id: accountId,
  kind: e2b,
  label: "E2B",
  template: "base",
  enabled: true,
  hasCredential: true,
};

const accountsLayer = Layer.mock(SandboxAccounts.SandboxAccounts)({
  list: Effect.succeed({ providers: [provider], accounts: [account] }),
});

const instanceRegistryLayer = (input: {
  readonly driver: ProviderDriverKind;
  readonly environment: ReadonlyArray<{ readonly name: string; readonly value: string }>;
}) =>
  Layer.succeed(
    ProviderInstanceRegistry.ProviderInstanceRegistry,
    ProviderInstanceRegistry.ProviderInstanceRegistry.of({
      getInstance: (id: ProviderInstanceId) =>
        Effect.succeed(
          id === instanceId
            ? ({ instanceId, driverKind: input.driver, enabled: true } as never)
            : undefined,
        ),
      getInstanceConfig: () => Effect.succeedSome({}),
      getResolvedEnvironment: () => Effect.succeed(input.environment),
    } as unknown as ProviderInstanceRegistry.ProviderInstanceRegistry["Service"]),
  );

const testLayer = (
  registry = instanceRegistryLayer({
    driver: ProviderDriverKind.make("claudeAgent"),
    environment: [],
  }),
) => CloudTargetList.layer.pipe(Layer.provide(accountsLayer), Layer.provide(registry));

const list = (input: {
  readonly extendedReasons?: boolean;
  readonly instanceId?: ProviderInstanceId;
}) =>
  Effect.gen(function* () {
    const service = yield* CloudTargetList.CloudTargetList;
    return yield* service.list({
      remote: { type: "url" },
      ...(input.instanceId === undefined ? {} : { instanceId: input.instanceId }),
      ...(input.extendedReasons === undefined ? {} : { extendedReasons: input.extendedReasons }),
    });
  });

describe("cloud target list", () => {
  it.effect("offers the account without a selected provider", () =>
    Effect.gen(function* () {
      const result = yield* list({});
      expect(result.targets).toEqual([
        { accountId, providerKind: e2b, label: "E2B", enabled: true },
      ]);
    }).pipe(Effect.provide(testLayer())),
  );

  it.effect("disables a harness with no sandbox credential", () =>
    Effect.gen(function* () {
      const result = yield* list({ instanceId, extendedReasons: true });
      expect(result.targets[0]?.disabledReason).toBe("sign-in-unavailable");
    }).pipe(Effect.provide(testLayer())),
  );

  it.effect("enables a harness with an injected credential environment", () =>
    Effect.gen(function* () {
      const result = yield* list({ instanceId, extendedReasons: true });
      expect(result.targets[0]?.enabled).toBe(true);
    }).pipe(
      Effect.provide(
        testLayer(
          instanceRegistryLayer({
            driver: ProviderDriverKind.make("claudeAgent"),
            environment: [{ name: "ANTHROPIC_API_KEY", value: "sk-test" }],
          }),
        ),
      ),
    ),
  );

  it.effect("falls back to an older client's reason vocabulary", () =>
    Effect.gen(function* () {
      const result = yield* list({ instanceId });
      expect(result.targets[0]?.disabledReason).toBe("provider-unavailable");
    }).pipe(Effect.provide(testLayer())),
  );

  it.effect("treats a missing live instance as unsupported", () =>
    Effect.gen(function* () {
      const result = yield* list({
        instanceId: ProviderInstanceId.make("missing"),
        extendedReasons: true,
      });
      expect(result.targets[0]?.disabledReason).toBe("harness-unavailable");
    }).pipe(Effect.provide(testLayer())),
  );
});
