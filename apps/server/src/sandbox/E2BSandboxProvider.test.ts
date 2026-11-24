import { SandboxProviderKind } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import {
  makeE2BSandboxProvider,
  type E2BClient,
  type E2BSandboxHandle,
} from "./E2BSandboxProvider.ts";
import type { SandboxProviderAccount } from "./SandboxProvider.ts";

const account: SandboxProviderAccount = {
  id: "e2b-work",
  kind: SandboxProviderKind.make("e2b"),
  label: "E2B",
  apiKey: "e2b_secret_key",
  template: "base",
};

const client = (overrides: Partial<E2BClient> = {}): E2BClient => ({
  create: async () => ({ sandboxId: "e2b-sandbox-1" }),
  connect: async () =>
    ({
      getHost: (port: number) => `3000-${port}.e2b.app`,
      commands: {
        run: async () => ({ exitCode: 0, stdout: "ok", stderr: "" }),
      },
      files: { write: async () => ({}) },
    }) as unknown as E2BSandboxHandle,
  getInfo: async () => ({ state: "running" }),
  pause: async () => true,
  delete: async () => true,
  list: async () => [],
  ...overrides,
});

describe("E2B sandbox provider", () => {
  it("declares the capabilities this ticket requires", () => {
    const driver = makeE2BSandboxProvider(client());
    expect(driver.kind).toBe("e2b");
    expect(driver.capabilities).toMatchObject({
      create: true,
      exec: true,
      pauseResume: true,
      delete: true,
      snapshots: true,
      privatePorts: true,
    });
    expect(driver.capabilities.sizes.length).toBeGreaterThan(0);
    expect(driver.capabilities.regions.length).toBeGreaterThan(0);
  });

  it.effect(
    "validates with a list, creates with the account key and template, and maps state",
    () =>
      Effect.gen(function* () {
        const seen: Array<Record<string, unknown>> = [];
        const driver = makeE2BSandboxProvider(
          client({
            list: async (input) => {
              seen.push({ list: input.apiKey });
              return [];
            },
            create: async (input) => {
              seen.push({ create: input });
              return { sandboxId: "e2b-sandbox-1" };
            },
            getInfo: async (input) => {
              seen.push({ getInfo: input });
              return { state: "paused" };
            },
          }),
        );
        yield* driver.validateCredential(account);
        const created = yield* driver.create({
          account,
          instanceId: "instance-1",
          template: "base",
          timeoutMs: 30_000,
        });
        expect(created.providerSandboxId).toBe("e2b-sandbox-1");
        const state = yield* driver.get(account, "e2b-sandbox-1");
        expect(state.status).toBe("paused");
        expect(seen).toEqual([
          { list: "e2b_secret_key" },
          {
            create: {
              template: "base",
              apiKey: "e2b_secret_key",
              timeoutMs: 30_000,
              idempotencyKey: "instance-1",
              metadata: { "t3.instance": "instance-1" },
              network: { allowPublicTraffic: false, maskRequestHost: "localhost" },
            },
          },
          { getInfo: { sandboxId: "e2b-sandbox-1", apiKey: "e2b_secret_key" } },
        ]);
      }),
  );

  it.effect("finds a create the provider accepted using its instance metadata", () =>
    Effect.gen(function* () {
      const seen: Array<Record<string, unknown>> = [];
      const driver = makeE2BSandboxProvider(
        client({
          list: async (input) => {
            seen.push(input);
            return [{ sandboxId: "e2b-sandbox-1", state: "paused" }];
          },
        }),
      );
      const found = yield* driver.findCreated(account, "instance-1");
      expect(seen).toEqual([
        { apiKey: "e2b_secret_key", metadata: { "t3.instance": "instance-1" } },
      ]);
      expect(found).toEqual([{ providerSandboxId: "e2b-sandbox-1", status: "paused" }]);
    }),
  );

  it.effect("turns a rejected key into an actionable error without echoing the key", () =>
    Effect.gen(function* () {
      const driver = makeE2BSandboxProvider(
        client({
          list: async () => {
            throw Object.assign(new Error(`bad key ${account.apiKey}`), { statusCode: 401 });
          },
        }),
      );
      const failure = yield* Effect.flip(driver.validateCredential(account));
      expect(failure.statusCode).toBe(401);
      expect(failure.detail).toContain("rejected the API key");
      expect(failure.detail).not.toContain(account.apiKey);
    }),
  );

  it.effect("returns a private port endpoint with the token kept server-side", () =>
    Effect.gen(function* () {
      const driver = makeE2BSandboxProvider(
        client({
          connect: async (input) => {
            expect(input).toEqual({ sandboxId: "e2b-sandbox-1", apiKey: "e2b_secret_key" });
            return {
              getHost: (port: number) => `3000-${port}.e2b.app`,
              trafficAccessToken: "traffic-token",
              commands: { run: async () => ({ exitCode: 0, stdout: "", stderr: "" }) },
              files: { write: async () => ({}) },
            } as unknown as E2BSandboxHandle;
          },
        }),
      );
      const access = yield* driver.portAccess!({
        account,
        providerSandboxId: "e2b-sandbox-1",
        port: 5173,
      });
      expect(access).toEqual({
        url: "https://3000-5173.e2b.app",
        headers: { "e2b-traffic-access-token": "traffic-token" },
      });
      expect(JSON.stringify(access)).not.toContain(account.apiKey);
    }),
  );

  it.effect("runs commands in the sandbox with the provider-neutral exec result", () =>
    Effect.gen(function* () {
      const driver = makeE2BSandboxProvider(
        client({
          connect: async () =>
            ({
              commands: {
                run: async (
                  command: string,
                  options: {
                    readonly shell?: ReadonlyArray<string>;
                    readonly cwd?: string;
                    readonly timeoutMs?: number;
                  },
                ) => {
                  expect(command).toBe("echo ready");
                  expect(options.shell).toEqual(["/bin/sh", "-lc"]);
                  return { exitCode: 0, stdout: "ready", stderr: "" };
                },
              },
              files: { write: async () => ({}) },
            }) as unknown as E2BSandboxHandle,
        }),
      );
      const result = yield* driver.exec({
        account,
        providerSandboxId: "e2b-sandbox-1",
        command: "echo ready",
      });
      expect(result).toEqual({ exitCode: 0, stdout: "ready", stderr: "" });
    }),
  );
});
