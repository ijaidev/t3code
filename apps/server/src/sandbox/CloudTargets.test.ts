import {
  ProviderDriverKind,
  SandboxProviderAccountId,
  SandboxProviderKind,
  type SandboxCloudTarget,
  type SandboxProviderCapabilities,
  type SandboxProviderSummary,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import {
  cloudHarnessCapability,
  type CloudHarnessCapability,
  type CloudTargetRemote,
  legacySandboxCloudTargets,
  sandboxCloudTargets,
} from "./CloudTargets.ts";

const e2b = SandboxProviderKind.make("e2b");
const accountId = SandboxProviderAccountId.make("e2b-work");

const capabilities: SandboxProviderCapabilities = {
  create: true,
  exec: true,
  pauseResume: true,
  delete: true,
  snapshots: true,
  privatePorts: true,
  harness: true,
  sizes: [{ id: "small", label: "Small", cpuCount: 2, memoryMB: 1024 }],
  regions: [{ id: "us", label: "United States" }],
};

const account = {
  id: accountId,
  kind: e2b,
  label: "E2B",
  template: "base",
  enabled: true,
  hasCredential: true,
};
const provider = { kind: e2b, label: "E2B", capabilities };
const supportedHarness: CloudHarnessCapability = {
  supported: true,
  credentialSupported: true,
};

const target = (
  input: Partial<{
    readonly remote: CloudTargetRemote;
    readonly accounts: ReadonlyArray<typeof account>;
    readonly providers: ReadonlyArray<SandboxProviderSummary>;
    readonly harness: CloudHarnessCapability;
  }> = {},
) =>
  sandboxCloudTargets({
    remote: { type: "url" },
    accounts: [account],
    providers: [provider],
    harness: supportedHarness,
    ...input,
  })[0];

describe("sandbox cloud targets", () => {
  it("offers an enabled provider for a project with a Git remote", () => {
    expect(target()).toEqual({ accountId, providerKind: e2b, label: "E2B", enabled: true });
  });

  it("keeps the target visible with the reason it cannot start", () => {
    expect(target({ remote: null })?.disabledReason).toBe("no-remote");
    expect(target({ remote: { type: "credentials" } })?.disabledReason).toBe("unsafe-remote");
    expect(target({ accounts: [{ ...account, enabled: false }] })?.disabledReason).toBe(
      "account-disabled",
    );
    expect(target({ accounts: [{ ...account, hasCredential: false }] })?.disabledReason).toBe(
      "no-credential",
    );
    expect(target({ providers: [] })?.disabledReason).toBe("provider-unavailable");
    expect(
      target({
        providers: [{ ...provider, capabilities: { ...capabilities, create: false } }],
      })?.disabledReason,
    ).toBe("no-create");
    expect(
      target({
        providers: [{ ...provider, capabilities: { ...capabilities, exec: false } }],
      })?.disabledReason,
    ).toBe("no-exec");
    expect(
      target({
        providers: [{ ...provider, capabilities: { ...capabilities, harness: false } }],
      })?.disabledReason,
    ).toBe("no-harness");
  });

  it("checks the selected harness and its sign-in", () => {
    expect(
      target({ harness: { supported: false, credentialSupported: true } })?.disabledReason,
    ).toBe("harness-unavailable");
    expect(
      target({ harness: { supported: true, credentialSupported: false } })?.disabledReason,
    ).toBe("sign-in-unavailable");
  });
});

describe("cloud harness capability", () => {
  it.each([
    ["opencode", "OPENROUTER_API_KEY"],
    ["claudeAgent", "ANTHROPIC_API_KEY"],
    ["codex", "OPENAI_API_KEY"],
  ])("supports %s with a sandbox credential", (driver, variable) => {
    expect(
      cloudHarnessCapability({
        driver: ProviderDriverKind.make(driver),
        config: {},
        environment: [{ name: variable, value: "secret" }],
      }),
    ).toEqual({ supported: true, credentialSupported: true });
  });

  it.each(["opencode", "claudeAgent", "codex"])(
    "refuses %s without a sandbox credential",
    (driver) => {
      expect(
        cloudHarnessCapability({
          driver: ProviderDriverKind.make(driver),
          config: {},
          environment: [],
        }),
      ).toEqual({ supported: true, credentialSupported: false });
    },
  );

  it("accepts an existing Codex sign-in only through an injected key", () => {
    expect(
      cloudHarnessCapability({
        driver: ProviderDriverKind.make("codex"),
        config: { setupMode: "existing" },
        environment: [],
      }).credentialSupported,
    ).toBe(false);
    expect(
      cloudHarnessCapability({
        driver: ProviderDriverKind.make("codex"),
        config: { setupMode: "existing" },
        environment: [{ name: "OPENAI_API_KEY", value: "sk-test" }],
      }).credentialSupported,
    ).toBe(true);
  });

  it("refuses providers without a sandbox adapter", () => {
    expect(
      cloudHarnessCapability({
        driver: ProviderDriverKind.make("cursor"),
        config: {},
        environment: [{ name: "CURSOR_API_KEY", value: "secret" }],
      }),
    ).toEqual({ supported: false, credentialSupported: false });
  });

  it("refuses managed Codex sign-in, which cannot enter the sandbox", () => {
    expect(
      cloudHarnessCapability({
        driver: ProviderDriverKind.make("codex"),
        config: { setupMode: "managed" },
        environment: [{ name: "OPENAI_API_KEY", value: "sk-test" }],
      }),
    ).toEqual({ supported: true, credentialSupported: false });
  });
});

describe("legacy cloud target reasons", () => {
  const LegacySandboxCloudTarget = Schema.Struct({
    accountId: SandboxProviderAccountId,
    providerKind: SandboxProviderKind,
    label: Schema.String,
    enabled: Schema.Boolean,
    disabledReason: Schema.optionalKey(
      Schema.Literals([
        "no-remote",
        "unsafe-remote",
        "account-disabled",
        "provider-unavailable",
        "no-create",
        "no-exec",
      ]),
    ),
  });
  const decodeLegacy = Schema.decodeUnknownOption(LegacySandboxCloudTarget);
  const newReasons = [
    "no-credential",
    "no-harness",
    "harness-unavailable",
    "sign-in-unavailable",
  ] as const;
  const targetFor = (reason: SandboxCloudTarget["disabledReason"]): SandboxCloudTarget => ({
    accountId,
    providerKind: e2b,
    label: "E2B",
    enabled: false,
    ...(reason === undefined ? {} : { disabledReason: reason }),
  });

  it.each(newReasons)("falls back for an older client: %s", (reason) => {
    expect(Option.isNone(decodeLegacy(targetFor(reason)))).toBe(true);
    const [legacy] = legacySandboxCloudTargets([targetFor(reason)]);
    expect(legacy?.disabledReason).toBe("provider-unavailable");
    expect(Option.isSome(decodeLegacy(legacy))).toBe(true);
  });

  it("keeps reasons the older union already knows", () => {
    expect(legacySandboxCloudTargets([targetFor("account-disabled")])[0]?.disabledReason).toBe(
      "account-disabled",
    );
  });
});
