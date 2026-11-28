import {
  ProviderDriverKind,
  SandboxProviderAccountId,
  SandboxProviderKind,
  type SandboxProviderCapabilities,
  type SandboxProviderSummary,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";

import {
  cloudHarnessCapability,
  type CloudHarnessCapability,
  type CloudTargetRemote,
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
  it.each(["opencode", "claudeAgent", "codex"])("supports %s", (driver) => {
    expect(
      cloudHarnessCapability({
        driver: ProviderDriverKind.make(driver),
        config: {},
      }),
    ).toEqual({ supported: true, credentialSupported: true });
  });

  it("refuses providers without a sandbox adapter", () => {
    expect(
      cloudHarnessCapability({
        driver: ProviderDriverKind.make("cursor"),
        config: {},
      }),
    ).toEqual({ supported: false, credentialSupported: true });
  });

  it("refuses managed Codex sign-in, which cannot enter the sandbox", () => {
    expect(
      cloudHarnessCapability({
        driver: ProviderDriverKind.make("codex"),
        config: { setupMode: "managed" },
      }),
    ).toEqual({ supported: true, credentialSupported: false });
  });
});
