import {
  SandboxProviderAccountId,
  SandboxProviderKind,
  type SandboxProviderCapabilities,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";

import { sandboxCloudTargets } from "./CloudTargets.ts";

const e2b = SandboxProviderKind.make("e2b");
const accountId = SandboxProviderAccountId.make("e2b-work");

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

const account = { id: accountId, kind: e2b, label: "E2B", template: "base", enabled: true };
const provider = { kind: e2b, label: "E2B", capabilities };

describe("sandbox cloud targets", () => {
  it("offers an enabled provider for a project with a Git remote", () => {
    expect(
      sandboxCloudTargets({ remote: { type: "url" }, accounts: [account], providers: [provider] }),
    ).toEqual([{ accountId, providerKind: e2b, label: "E2B", enabled: true }]);
  });

  it("keeps the target visible with the reason it cannot start", () => {
    expect(
      sandboxCloudTargets({
        remote: null,
        accounts: [account],
        providers: [provider],
      })[0]?.disabledReason,
    ).toBe("no-remote");
    expect(
      sandboxCloudTargets({
        remote: { type: "credentials" },
        accounts: [account],
        providers: [provider],
      })[0]?.disabledReason,
    ).toBe("unsafe-remote");
    expect(
      sandboxCloudTargets({
        remote: { type: "url" },
        accounts: [{ ...account, enabled: false }],
        providers: [provider],
      })[0]?.disabledReason,
    ).toBe("account-disabled");
    expect(
      sandboxCloudTargets({ remote: { type: "url" }, accounts: [account], providers: [] })[0]
        ?.disabledReason,
    ).toBe("provider-unavailable");
    expect(
      sandboxCloudTargets({
        remote: { type: "url" },
        accounts: [account],
        providers: [{ ...provider, capabilities: { ...capabilities, create: false } }],
      })[0]?.disabledReason,
    ).toBe("no-create");
    expect(
      sandboxCloudTargets({
        remote: { type: "url" },
        accounts: [account],
        providers: [{ ...provider, capabilities: { ...capabilities, exec: false } }],
      })[0]?.disabledReason,
    ).toBe("no-exec");
  });
});
