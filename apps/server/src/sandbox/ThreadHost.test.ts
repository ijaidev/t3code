import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import {
  parseThreadHostHandshake,
  THREAD_HOST_BOOTSTRAP_COMMAND,
  THREAD_HOST_PROTOCOL_VERSION,
} from "./ThreadHost.ts";

const handshake = (overrides: Record<string, unknown> = {}) =>
  JSON.stringify({
    kind: "t3-thread-host",
    protocolVersion: THREAD_HOST_PROTOCOL_VERSION,
    capabilities: ["exec", "filesystem", "git", "ports"],
    ...overrides,
  });

describe("thread host handshake", () => {
  it.effect("accepts the current protocol with every required capability", () =>
    Effect.gen(function* () {
      const parsed = yield* parseThreadHostHandshake(handshake());
      expect(parsed.protocolVersion).toBe(THREAD_HOST_PROTOCOL_VERSION);
      expect(parsed.capabilities).toContain("ports");
    }),
  );

  it.effect("fails closed on an unreadable handshake", () =>
    Effect.gen(function* () {
      const failure = yield* Effect.flip(parseThreadHostHandshake("not json"));
      expect(failure.stage).toBe("output");
    }),
  );

  it.effect("fails closed on a different protocol version", () =>
    Effect.gen(function* () {
      const failure = yield* Effect.flip(
        parseThreadHostHandshake(handshake({ protocolVersion: "999" })),
      );
      expect(failure.stage).toBe("version");
      expect(failure.detail).toContain("999");
    }),
  );

  it.effect("fails closed when a required capability is missing", () =>
    Effect.gen(function* () {
      const failure = yield* Effect.flip(
        parseThreadHostHandshake(handshake({ capabilities: ["exec", "filesystem"] })),
      );
      expect(failure.stage).toBe("capabilities");
      expect(failure.detail).toContain("git");
      expect(failure.detail).toContain("ports");
    }),
  );

  it("bootstraps only the host script, never a provider credential", () => {
    expect(THREAD_HOST_BOOTSTRAP_COMMAND).toContain("thread-host");
    expect(THREAD_HOST_BOOTSTRAP_COMMAND).not.toContain("apiKey");
    expect(THREAD_HOST_BOOTSTRAP_COMMAND).not.toContain("api_key");
  });
});
