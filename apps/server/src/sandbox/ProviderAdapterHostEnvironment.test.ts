import { describe, expect, it } from "@effect/vitest";

import {
  clearProviderHostControlEnvironment,
  sandboxProviderEnvironment,
} from "./ProviderAdapterHostEnvironment.ts";

describe("sandbox provider environment", () => {
  it("keeps provider credentials and drops host control variables", () => {
    const environment = {
      PATH: "/usr/bin",
      OPENCODE_PASSWORD: "provider-password",
      T3_PROVIDER_HOST_TOKEN: "controller-token",
      T3_PROVIDER_HOST_PORT: "8790",
      T3_PROVIDER_HOST_BIND: "0.0.0.0",
      T3_PROVIDER_HOST_BUILD: "build-hash",
    };

    expect(sandboxProviderEnvironment(environment)).toEqual({
      PATH: "/usr/bin",
      OPENCODE_PASSWORD: "provider-password",
    });
    expect(environment.T3_PROVIDER_HOST_TOKEN).toBe("controller-token");
  });

  it("clears host control variables before the adapter spawns a harness", () => {
    const environment: NodeJS.ProcessEnv = {
      PATH: "/usr/bin",
      ANTHROPIC_API_KEY: "provider-key",
      T3_PROVIDER_HOST_TOKEN: "controller-token",
      T3_PROVIDER_HOST_PORT: "8790",
    };

    clearProviderHostControlEnvironment(environment);

    expect(environment).toEqual({ PATH: "/usr/bin", ANTHROPIC_API_KEY: "provider-key" });
  });
});
