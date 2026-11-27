import { describe, expect, it } from "@effect/vitest";

import { sandboxProviderEnvironment } from "./ProviderAdapterHostEnvironment.ts";

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
});
