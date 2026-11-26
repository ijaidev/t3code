import {
  SandboxInstanceId,
  SandboxProviderAccountId,
  SandboxProviderKind,
  type SandboxProviderCapabilities,
  type SandboxTestSandbox,
  ThreadId,
} from "@t3tools/contracts";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as SecretResolver from "@t3tools/secret-provider-core/server/SecretResolver";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import * as ServerConfig from "../config.ts";
import * as ServerSettingsModule from "../serverSettings.ts";
import * as CloudRepositoryCredential from "./CloudRepositoryCredential.ts";
import * as CloudThreadStore from "./CloudThreadStore.ts";
import * as CloudThreads from "./CloudThreads.ts";
import {
  makeDaytonaSandboxProvider,
  type DaytonaClient,
  type DaytonaRequest,
  type DaytonaResponse,
} from "./DaytonaSandboxProvider.ts";
import {
  makeE2BSandboxProvider,
  type E2BClient,
  type E2BSandboxHandle,
} from "./E2BSandboxProvider.ts";
import * as SandboxAccounts from "./SandboxAccounts.ts";
import * as SandboxLifecycle from "./SandboxLifecycle.ts";
import {
  SandboxProviderError,
  type SandboxProviderAccount,
  type SandboxProviderDriver,
} from "./SandboxProvider.ts";
import * as SandboxStore from "./SandboxStore.ts";

const E2B_KIND = SandboxProviderKind.make("e2b");
const DAYTONA_KIND = SandboxProviderKind.make("daytona");
const FAKE_KIND = SandboxProviderKind.make("fake-cloud");

interface HarnessSandbox {
  readonly instanceId: string;
  readonly providerSandboxId: string;
  status: "running" | "paused" | "failed";
}

interface HarnessState {
  rejectKey: boolean;
  forbidden: boolean;
  failCreateAfterAccept: boolean;
  plainTextLogs: boolean;
  disableFollowLogs: boolean;
  calls: Array<string>;
  sandboxes: Array<HarnessSandbox>;
  writes: Array<{ readonly path: string; readonly data: string }>;
  lastCreateBody: Readonly<Record<string, unknown>> | null;
  lastExec: {
    readonly command: string;
    readonly cwd?: string;
    readonly envs?: Readonly<Record<string, string>>;
  } | null;
  lastProcessCommand: string | null;
  listQueries: Array<string>;
  logsRequests: number;
  lastLogsHeaders: Readonly<Record<string, string>> | null;
  followCalls: number;
  followChunks: Array<string>;
  followScripts: Array<Array<string>>;
  followClosed: boolean;
  exitAfterFollowCalls: number;
  growingLogs: boolean;
  exitAfterLogsRequests: number;
  session: {
    stdout: string;
    stderr: string;
    exitCode: number;
    input: Array<string>;
    deleted: boolean;
  } | null;
}

interface ProviderHarness {
  readonly driver: SandboxProviderDriver;
  readonly account: SandboxProviderAccount;
  readonly state: HarnessState;
}

const makeState = (): HarnessState => ({
  rejectKey: false,
  forbidden: false,
  failCreateAfterAccept: false,
  plainTextLogs: false,
  disableFollowLogs: false,
  calls: [],
  sandboxes: [],
  writes: [],
  lastCreateBody: null,
  lastExec: null,
  lastProcessCommand: null,
  listQueries: [],
  logsRequests: 0,
  lastLogsHeaders: null,
  followCalls: 0,
  followChunks: [],
  followScripts: [],
  followClosed: false,
  exitAfterFollowCalls: 0,
  growingLogs: false,
  exitAfterLogsRequests: 0,
  session: null,
});

const account = (kind: SandboxProviderKind, label: string, apiKey: string) => ({
  id: `${kind}-work`,
  kind,
  label,
  apiKey,
  template: "base",
});

const rejection = (kind: SandboxProviderKind, label: string) =>
  Object.assign(new Error(`${label} rejected the API key.`), { statusCode: 401 });

const fakeCapabilities: SandboxProviderCapabilities = {
  create: true,
  exec: true,
  pauseResume: false,
  delete: true,
  snapshots: false,
  privatePorts: false,
  sizes: [{ id: "small", label: "Small", cpuCount: 1, memoryMB: 1024 }],
  regions: [{ id: "us", label: "United States" }],
};

const asRecord = (value: unknown): Readonly<Record<string, unknown>> =>
  typeof value === "object" && value !== null ? (value as Readonly<Record<string, unknown>>) : {};

const labelInstance = (body: unknown): string | undefined => {
  const instance = asRecord(asRecord(body)["labels"])["t3.instance"];
  return typeof instance === "string" ? instance : undefined;
};

const makeE2BHarness = (): ProviderHarness => {
  const state = makeState();
  const provider = account(E2B_KIND, "E2B", "e2b_secret_key");
  const find = (providerSandboxId: string) =>
    state.sandboxes.find((sandbox) => sandbox.providerSandboxId === providerSandboxId);

  const handle = (sandbox: HarnessSandbox): E2BSandboxHandle =>
    ({
      trafficAccessToken: "e2b-preview-token",
      getHost: (port: number) => `${port}-${sandbox.providerSandboxId}.e2b.app`,
      commands: {
        run: async (
          command: string,
          options: {
            readonly cwd?: string;
            readonly envs?: Readonly<Record<string, string>>;
          },
        ) => {
          state.lastExec = {
            command,
            ...(options.cwd === undefined ? {} : { cwd: options.cwd }),
            ...(options.envs === undefined ? {} : { envs: options.envs }),
          };
          return { exitCode: 0, stdout: "ok", stderr: "" };
        },
      },
      files: {
        write: async (path: string, data: string | Uint8Array) => {
          state.writes.push({
            path,
            data: typeof data === "string" ? data : new TextDecoder().decode(data),
          });
        },
      },
    }) as unknown as E2BSandboxHandle;

  const client: E2BClient = {
    create: async (input) => {
      state.calls.push("create");
      const sandbox: HarnessSandbox = {
        instanceId: input.idempotencyKey,
        providerSandboxId: `e2b-${state.sandboxes.length + 1}`,
        status: "running",
      };
      state.sandboxes.push(sandbox);
      if (state.failCreateAfterAccept) throw new Error("E2B create failed after accepting.");
      return { sandboxId: sandbox.providerSandboxId };
    },
    connect: async (input) => {
      state.calls.push("connect");
      if (state.rejectKey) throw rejection(E2B_KIND, "E2B");
      const sandbox = find(input.sandboxId);
      if (sandbox === undefined) {
        throw Object.assign(new Error("missing"), { statusCode: 404 });
      }
      sandbox.status = "running";
      return handle(sandbox);
    },
    getInfo: async (input) => {
      const sandbox = find(input.sandboxId);
      if (sandbox === undefined) {
        throw Object.assign(new Error("missing"), { statusCode: 404 });
      }
      return { state: sandbox.status === "paused" ? "paused" : "running" };
    },
    pause: async (input) => {
      const sandbox = find(input.sandboxId);
      if (sandbox === undefined) {
        throw Object.assign(new Error("missing"), { statusCode: 404 });
      }
      sandbox.status = "paused";
      return true;
    },
    delete: async (input) => {
      state.calls.push("delete");
      const index = state.sandboxes.findIndex(
        (sandbox) => sandbox.providerSandboxId === input.sandboxId,
      );
      if (index === -1) throw Object.assign(new Error("missing"), { statusCode: 404 });
      state.sandboxes.splice(index, 1);
      return true;
    },
    list: async (input) => {
      state.calls.push("list");
      if (state.rejectKey) throw rejection(E2B_KIND, "E2B");
      const instanceId = input.metadata?.["t3.instance"];
      return state.sandboxes
        .filter((sandbox) => instanceId === undefined || sandbox.instanceId === instanceId)
        .map((sandbox) => ({
          sandboxId: sandbox.providerSandboxId,
          state: sandbox.status === "paused" ? ("paused" as const) : ("running" as const),
        }));
    },
  };

  return { driver: makeE2BSandboxProvider(client), account: provider, state };
};

const makeDaytonaHarness = (): ProviderHarness => {
  const state = makeState();
  const provider = account(DAYTONA_KIND, "Daytona", "daytona_secret_key");
  const find = (providerSandboxId: string) =>
    state.sandboxes.find((sandbox) => sandbox.providerSandboxId === providerSandboxId);
  const requireSandbox = (providerSandboxId: string) => {
    const sandbox = find(providerSandboxId);
    if (sandbox === undefined) throw Object.assign(new Error("missing"), { statusCode: 404 });
    return sandbox;
  };

  const respond = (status: number, body: unknown): DaytonaResponse => ({ status, body });

  const client: DaytonaClient = {
    request: async (input: DaytonaRequest) => {
      const url = new URL(input.url);
      const path = url.pathname;
      state.lastCreateBody =
        input.method === "POST" && path === "/api/sandbox"
          ? (input.body as Readonly<Record<string, unknown>>)
          : state.lastCreateBody;

      if (path === "/api/sandbox" && input.method === "GET") {
        state.calls.push("list");
        if (state.rejectKey) throw rejection(DAYTONA_KIND, "Daytona");
        if (state.forbidden) {
          throw Object.assign(
            new Error(
              "The API key is missing the sandboxes:write permission for this organization.",
            ),
            { statusCode: 403 },
          );
        }
        state.listQueries.push(url.search);
        const labels = url.searchParams.get("labels");
        const instanceId =
          labels === null
            ? undefined
            : (JSON.parse(labels) as Readonly<Record<string, string>>)["t3.instance"];
        const includeErroredDeleted = url.searchParams.get("includeErroredDeleted") === "true";
        return respond(200, {
          items: state.sandboxes
            .filter((sandbox) => instanceId === undefined || sandbox.instanceId === instanceId)
            .filter((sandbox) => includeErroredDeleted || sandbox.status !== "failed")
            .map((sandbox) => ({
              id: sandbox.providerSandboxId,
              state:
                sandbox.status === "running"
                  ? "started"
                  : sandbox.status === "paused"
                    ? "stopped"
                    : "error",
              labels: { "t3.instance": sandbox.instanceId },
            })),
        });
      }

      if (path === "/api/sandbox" && input.method === "POST") {
        state.calls.push("create");
        const body = asRecord(input.body);
        if (
          body["cpu"] !== undefined ||
          body["memory"] !== undefined ||
          body["disk"] !== undefined
        ) {
          throw Object.assign(new Error("Cannot specify Sandbox resources when using a snapshot"), {
            statusCode: 400,
          });
        }
        const sandbox: HarnessSandbox = {
          instanceId: labelInstance(input.body) ?? String(state.sandboxes.length + 1),
          providerSandboxId: `daytona-${state.sandboxes.length + 1}`,
          status: state.failCreateAfterAccept ? "failed" : "running",
        };
        state.sandboxes.push(sandbox);
        if (state.failCreateAfterAccept) throw new Error("Daytona create failed after accepting.");
        return respond(200, { id: sandbox.providerSandboxId, state: "creating" });
      }

      if (path.startsWith("/api/sandbox/") && path.endsWith("/toolbox-proxy-url")) {
        return respond(200, { url: "https://toolbox.daytona.test/toolbox" });
      }

      if (path.startsWith("/api/sandbox/") && path.endsWith("/preview-url")) {
        const parts = path.split("/");
        const providerSandboxId = parts[3] ?? "";
        const port = parts[5] ?? "";
        requireSandbox(providerSandboxId);
        return respond(200, {
          url: `https://${port}-${providerSandboxId}.preview.daytona.test`,
          token: "daytona-preview-token",
        });
      }

      if (path.startsWith("/api/sandbox/") && input.method === "GET") {
        const providerSandboxId = path.split("/")[3] ?? "";
        const sandbox = requireSandbox(providerSandboxId);
        return respond(200, {
          id: sandbox.providerSandboxId,
          state:
            sandbox.status === "running"
              ? "started"
              : sandbox.status === "paused"
                ? "stopped"
                : "error",
        });
      }

      if (path.startsWith("/api/sandbox/") && input.method === "DELETE") {
        state.calls.push("delete");
        const providerSandboxId = path.split("/")[3] ?? "";
        const index = state.sandboxes.findIndex(
          (sandbox) => sandbox.providerSandboxId === providerSandboxId,
        );
        if (index === -1) throw Object.assign(new Error("missing"), { statusCode: 404 });
        state.sandboxes.splice(index, 1);
        return respond(200, {});
      }

      if (path.endsWith("/process/execute")) {
        const body = input.body as {
          readonly command: string;
          readonly cwd?: string;
          readonly envs?: Readonly<Record<string, string>>;
        };
        state.lastExec = {
          command: body.command,
          ...(body.cwd === undefined ? {} : { cwd: body.cwd }),
          ...(body.envs === undefined ? {} : { envs: body.envs }),
        };
        return respond(200, { exitCode: 0, result: "ok" });
      }

      if (path.endsWith("/files/upload-v2")) {
        state.writes.push({
          path: url.searchParams.get("path") ?? "",
          data:
            typeof input.data === "string"
              ? input.data
              : new TextDecoder().decode(input.data ?? new Uint8Array()),
        });
        return respond(200, {});
      }

      if (path.endsWith("/process/session") && input.method === "POST") {
        state.session = state.session ?? {
          stdout: `${JSON.stringify({ type: "ready", protocolVersion: "1", pid: 1 })}\n`,
          stderr: "warn\n",
          exitCode: 0,
          input: [],
          deleted: false,
        };
        return respond(201, {});
      }

      if (
        path.includes("/process/session/") &&
        input.method === "DELETE" &&
        state.session !== null
      ) {
        state.session.deleted = true;
        return respond(204, {});
      }

      if (path.endsWith("/exec") && path.includes("/process/session/")) {
        state.lastProcessCommand =
          (input.body as { readonly command?: string } | undefined)?.command ?? null;
        return respond(200, { cmdId: "cmd-1" });
      }

      if (path.endsWith("/logs")) {
        state.logsRequests += 1;
        state.lastLogsHeaders = input.headers ?? null;
        if (state.growingLogs) {
          const count =
            state.exitAfterLogsRequests > 0
              ? Math.min(state.logsRequests, state.exitAfterLogsRequests)
              : state.logsRequests;
          return respond(
            200,
            Array.from({ length: count }, (_, index) => `line-${index + 1}\n`).join(""),
          );
        }
        const session = state.session;
        if (state.plainTextLogs) {
          return respond(200, `${session?.stdout ?? ""}${session?.stderr ?? ""}`);
        }
        return respond(200, {
          output: `${session?.stdout ?? ""}${session?.stderr ?? ""}`,
          stdout: session?.stdout ?? "",
          stderr: session?.stderr ?? "",
        });
      }

      if (path.endsWith("/input")) {
        const body = input.body as { readonly data: string };
        state.session?.input.push(body.data);
        return respond(204, {});
      }

      if (path.includes("/process/session/") && path.includes("/command/")) {
        const waitForFollow = state.followCalls < state.exitAfterFollowCalls;
        const waitForLogs = state.logsRequests < state.exitAfterLogsRequests;
        return respond(200, {
          id: "cmd-1",
          command: "python3 -u host.py",
          ...(waitForFollow || waitForLogs ? {} : { exitCode: state.session?.exitCode ?? 0 }),
        });
      }

      throw new Error(`Unexpected Daytona request: ${input.method} ${input.url}`);
    },
    followLogs: async (input) => {
      state.followCalls += 1;
      if (state.disableFollowLogs) throw new Error("The follow stream is unavailable.");
      const chunks = state.followScripts[state.followCalls - 1] ?? state.followChunks;
      for (const chunk of chunks) input.onData(chunk);
      input.onClose();
      return {
        close: () => {
          state.followClosed = true;
        },
      };
    },
  };

  return { driver: makeDaytonaSandboxProvider(client), account: provider, state };
};

const makeFakeHarness = (): ProviderHarness => {
  const state = makeState();
  const provider = account(FAKE_KIND, "Fake cloud", "fake_secret_key");

  const driver: SandboxProviderDriver = {
    kind: FAKE_KIND,
    label: "Fake cloud",
    capabilities: fakeCapabilities,
    validateCredential: (_candidate) =>
      state.rejectKey
        ? Effect.fail(
            new SandboxProviderError({
              providerKind: FAKE_KIND,
              operation: "validate-credential",
              detail: "Fake cloud rejected the API key.",
              statusCode: 401,
            }),
          )
        : Effect.void,
    create: (input) =>
      Effect.gen(function* () {
        state.calls.push("create");
        const sandbox: HarnessSandbox = {
          instanceId: input.instanceId,
          providerSandboxId: `fake-${state.sandboxes.length + 1}`,
          status: "running",
        };
        state.sandboxes.push(sandbox);
        if (state.failCreateAfterAccept) {
          return yield* new SandboxProviderError({
            providerKind: FAKE_KIND,
            operation: "create",
            detail: "Fake cloud create failed after accepting.",
          });
        }
        return { providerSandboxId: sandbox.providerSandboxId };
      }),
    findCreated: (_candidate, instanceId) =>
      Effect.succeed(
        state.sandboxes
          .filter((sandbox) => sandbox.instanceId === instanceId)
          .map(({ providerSandboxId, status }) => ({ providerSandboxId, status })),
      ),
    get: (_candidate, providerSandboxId) =>
      Effect.suspend(() => {
        const sandbox = state.sandboxes.find(
          (candidate) => candidate.providerSandboxId === providerSandboxId,
        );
        return sandbox === undefined || sandbox.status === "failed"
          ? Effect.fail(
              new SandboxProviderError({
                providerKind: FAKE_KIND,
                operation: "get",
                detail: "Fake cloud could not find that sandbox.",
                statusCode: 404,
              }),
            )
          : Effect.succeed({ status: sandbox.status });
      }),
    pause: () =>
      Effect.fail(
        new SandboxProviderError({
          providerKind: FAKE_KIND,
          operation: "pause",
          detail: "Fake cloud sandboxes cannot be paused by T3 Code.",
        }),
      ),
    resume: () =>
      Effect.fail(
        new SandboxProviderError({
          providerKind: FAKE_KIND,
          operation: "resume",
          detail: "Fake cloud sandboxes cannot be woken after sleeping in T3 Code.",
        }),
      ),
    delete: (_candidate, providerSandboxId) =>
      Effect.gen(function* () {
        state.calls.push("delete");
        const index = state.sandboxes.findIndex(
          (sandbox) => sandbox.providerSandboxId === providerSandboxId,
        );
        if (index === -1) {
          return yield* new SandboxProviderError({
            providerKind: FAKE_KIND,
            operation: "delete",
            detail: "Fake cloud could not find that sandbox.",
            statusCode: 404,
          });
        }
        state.sandboxes.splice(index, 1);
      }),
    exec: (input) =>
      Effect.sync(() => {
        state.lastExec = {
          command: input.command,
          ...(input.cwd === undefined ? {} : { cwd: input.cwd }),
          ...(input.envs === undefined ? {} : { envs: input.envs }),
        };
        return { exitCode: 0, stdout: "ok", stderr: "" };
      }),
    writeFile: (input) =>
      Effect.sync(() => {
        state.writes.push({
          path: input.path,
          data: typeof input.data === "string" ? input.data : new TextDecoder().decode(input.data),
        });
      }),
    startProcess: (input) =>
      Effect.sync(() => {
        input.onStdout(`${JSON.stringify({ type: "ready", protocolVersion: "1", pid: 1 })}\n`);
        return {
          pid: 1,
          sendStdin: () => Effect.void,
          kill: Effect.void,
          wait: Effect.succeed({ exitCode: 0 }),
        };
      }),
  };

  return { driver, account: provider, state };
};

const harnesses = [
  { name: "E2B", make: makeE2BHarness },
  { name: "Daytona", make: makeDaytonaHarness },
  { name: "a fake provider", make: makeFakeHarness },
] as const;

describe.each(harnesses)("$name sandbox provider", ({ make }) => {
  it("declares a capability set that matches its surface", () => {
    const { driver } = make();
    expect(driver.kind).not.toBe("");
    expect(driver.label).not.toBe("");
    expect(driver.capabilities.create).toBe(true);
    expect(driver.capabilities.exec).toBe(true);
    expect(typeof driver.writeFile).toBe("function");
    expect(typeof driver.startProcess).toBe("function");
    expect(driver.capabilities.privatePorts).toBe(driver.portAccess !== undefined);
    expect(driver.capabilities.sizes.length).toBeGreaterThan(0);
    expect(driver.capabilities.regions.length).toBeGreaterThan(0);
  });

  it.effect("validates a key and redacts a provider rejection", () =>
    Effect.gen(function* () {
      const { driver, account: credentials, state } = make();
      yield* driver.validateCredential(credentials);
      state.rejectKey = true;
      const failure = yield* Effect.flip(driver.validateCredential(credentials));
      expect(failure.statusCode).toBe(401);
      expect(failure.detail).toContain("rejected the API key");
      expect(failure.detail).not.toContain(credentials.apiKey);
    }),
  );

  it.effect("creates one sandbox, finds it by instance, and reports its state", () =>
    Effect.gen(function* () {
      const { driver, account: credentials, state } = make();
      const created = yield* driver.create({
        account: credentials,
        instanceId: "instance-1",
        template: "base",
        timeoutMs: 60_000,
        size: driver.capabilities.sizes[0]!,
        region: driver.capabilities.regions[0]!,
      });
      expect(created.providerSandboxId).not.toBe("");
      expect(state.sandboxes).toHaveLength(1);
      const found = yield* driver.findCreated(credentials, "instance-1");
      expect(found.map((sandbox) => sandbox.providerSandboxId)).toEqual([
        created.providerSandboxId,
      ]);
      const status = yield* driver.get(credentials, created.providerSandboxId);
      expect(status.status).toBe("running");
    }),
  );

  it.effect("runs commands and uploads files through the provider", () =>
    Effect.gen(function* () {
      const { driver, account: credentials, state } = make();
      const created = yield* driver.create({
        account: credentials,
        instanceId: "instance-1",
        template: "base",
        timeoutMs: 60_000,
      });
      const result = yield* driver.exec({
        account: credentials,
        providerSandboxId: created.providerSandboxId,
        command: "echo ok",
        cwd: "/workspace",
        envs: { T3_TEST: "1" },
      });
      expect(result).toEqual({ exitCode: 0, stdout: "ok", stderr: "" });
      expect(state.lastExec?.command).toBe("echo ok");
      expect(state.lastExec?.cwd).toBe("/workspace");
      expect(state.lastExec?.envs).toEqual({ T3_TEST: "1" });

      yield* driver.writeFile!({
        account: credentials,
        providerSandboxId: created.providerSandboxId,
        path: "/home/t3/.t3/host.py",
        data: "print('ready')\n",
      });
      expect(state.writes).toContainEqual({
        path: "/home/t3/.t3/host.py",
        data: "print('ready')\n",
      });
    }),
  );

  it.effect("keeps a port preview private or declares it unavailable", () =>
    Effect.gen(function* () {
      const { driver, account: credentials } = make();
      if (!driver.capabilities.privatePorts) {
        expect(driver.portAccess).toBeUndefined();
        return;
      }
      const created = yield* driver.create({
        account: credentials,
        instanceId: "instance-1",
        template: "base",
        timeoutMs: 60_000,
      });
      const access = yield* driver.portAccess!({
        account: credentials,
        providerSandboxId: created.providerSandboxId,
        port: 5173,
      });
      expect(access.url).toContain("5173");
      expect(Object.keys(access.headers).length).toBeGreaterThan(0);
      expect(JSON.stringify(access)).not.toContain(credentials.apiKey);
    }),
  );

  it.effect("uses pause only when declared and otherwise leaves it unavailable", () =>
    Effect.gen(function* () {
      const { driver, account: credentials } = make();
      const created = yield* driver.create({
        account: credentials,
        instanceId: "instance-1",
        template: "base",
        timeoutMs: 60_000,
      });
      if (driver.capabilities.pauseResume) {
        yield* driver.pause(credentials, created.providerSandboxId);
        expect((yield* driver.get(credentials, created.providerSandboxId)).status).toBe("paused");
        yield* driver.resume(credentials, created.providerSandboxId);
        expect((yield* driver.get(credentials, created.providerSandboxId)).status).toBe("running");
        return;
      }
      const pause = yield* Effect.flip(driver.pause(credentials, created.providerSandboxId));
      expect(pause.detail).toContain("cannot be paused");
      const resume = yield* Effect.flip(driver.resume(credentials, created.providerSandboxId));
      expect(resume.detail).toContain("cannot be woken");
    }),
  );

  it.effect("deletes a sandbox and then reports it missing", () =>
    Effect.gen(function* () {
      const { driver, account: credentials } = make();
      const created = yield* driver.create({
        account: credentials,
        instanceId: "instance-1",
        template: "base",
        timeoutMs: 60_000,
      });
      yield* driver.delete(credentials, created.providerSandboxId);
      expect(yield* driver.findCreated(credentials, "instance-1")).toEqual([]);
      const failure = yield* Effect.flip(driver.get(credentials, created.providerSandboxId));
      expect(failure.statusCode).toBe(404);
    }),
  );

  it.effect("keeps a sandbox the provider accepted before create failed discoverable", () =>
    Effect.gen(function* () {
      const { driver, account: credentials, state } = make();
      state.failCreateAfterAccept = true;
      const failure = yield* Effect.flip(
        driver.create({
          account: credentials,
          instanceId: "instance-1",
          template: "base",
          timeoutMs: 60_000,
        }),
      );
      expect(failure.detail.length).toBeGreaterThan(0);
      const found = yield* driver.findCreated(credentials, "instance-1");
      expect(found).toHaveLength(1);
      yield* driver.delete(credentials, found[0]!.providerSandboxId);
      expect(yield* driver.findCreated(credentials, "instance-1")).toEqual([]);
    }),
  );
});

const provisioningRecord = (id: string): SandboxTestSandbox => ({
  id: SandboxInstanceId.make(id),
  accountId: SandboxProviderAccountId.make("daytona-work"),
  providerKind: DAYTONA_KIND,
  providerSandboxId: null,
  status: "provisioning",
  template: "base",
  createdAt: "2026-11-26T00:00:00.000Z",
  updatedAt: "2026-11-26T00:00:00.000Z",
});

const daytonaCloudLayer = (driver: SandboxProviderDriver) => {
  const accounts = SandboxAccounts.layerFromDrivers([driver]);
  const store = SandboxStore.layer;
  const cloudStore = CloudThreadStore.layer;
  const lifecycle = SandboxLifecycle.layer.pipe(Layer.provide(accounts), Layer.provide(store));
  const cloud = CloudThreads.layer.pipe(
    Layer.provide(accounts),
    Layer.provide(store),
    Layer.provide(cloudStore),
    Layer.provide(lifecycle),
    Layer.provide(
      Layer.succeed(CloudRepositoryCredential.CloudRepositoryCredentialResolver, {
        resolve: () => Effect.succeedNone,
      }),
    ),
  );
  return Layer.mergeAll(accounts, store, cloudStore, lifecycle, cloud).pipe(
    Layer.provideMerge(ServerSettingsModule.layerTest({})),
    Layer.provide(
      ServerConfig.layerTest(process.cwd(), { prefix: "t3-daytona-conformance-test-" }),
    ),
    Layer.provide(NodeServices.layer),
    Layer.provide(NodeCrypto.layer),
    Layer.provide(Layer.mock(SecretResolver.SecretResolver)({})),
  );
};

describe("Daytona cloud thread", () => {
  it.effect("prepares the same provider-neutral thread and hides sleep", () =>
    Effect.gen(function* () {
      const { driver, account: credentials } = makeDaytonaHarness();
      const accountId = SandboxProviderAccountId.make("daytona-work");
      const threadId = ThreadId.make("thread-daytona-conformance");
      yield* Effect.gen(function* () {
        const accounts = yield* SandboxAccounts.SandboxAccounts;
        yield* accounts.connect({
          id: accountId,
          config: {
            kind: DAYTONA_KIND,
            label: "Daytona",
            apiKey: credentials.apiKey,
            template: "base",
            enabled: true,
          },
        });
        const cloud = yield* CloudThreads.CloudThreads;
        const sandbox = yield* cloud.prepare({
          threadId,
          accountId,
          cloneUrl: "https://github.com/example/public.git",
          baseRef: "main",
          branch: "main",
        });
        expect(sandbox.providerKind).toBe(DAYTONA_KIND);
        expect(sandbox.status).toBe("running");
        const state = yield* cloud.getCloudSandboxState(threadId);
        expect(Option.isSome(state)).toBe(true);
        if (Option.isSome(state)) {
          expect(state.value.providerLabel).toBe("Daytona");
          expect(state.value.canSleep).toBe(false);
          expect(state.value.sleepDisabledReason).toBe("provider-no-sleep");
          expect(state.value.canDelete).toBe(true);
        }
      }).pipe(Effect.provide(daytonaCloudLayer(driver)));
    }),
  );
});

describe("Daytona failures", () => {
  it.effect("deletes a sandbox the provider accepted before create failed", () =>
    Effect.gen(function* () {
      const { driver, account: credentials, state } = makeDaytonaHarness();
      state.failCreateAfterAccept = true;
      const accountId = SandboxProviderAccountId.make("daytona-work");
      yield* Effect.gen(function* () {
        const accounts = yield* SandboxAccounts.SandboxAccounts;
        yield* accounts.connect({
          id: accountId,
          config: {
            kind: DAYTONA_KIND,
            label: "Daytona",
            apiKey: credentials.apiKey,
            template: "base",
            enabled: true,
          },
        });
        const lifecycle = yield* SandboxLifecycle.SandboxLifecycle;
        const store = yield* SandboxStore.SandboxStore;
        const failure = yield* Effect.flip(lifecycle.createTestSandbox({ accountId }));
        expect(failure.detail).toContain("create failed");
        expect(yield* store.list).toEqual([]);
        expect(state.sandboxes).toEqual([]);
        expect(state.calls).toContain("delete");
        expect(
          state.listQueries.some((query) => query.includes("includeErroredDeleted=true")),
        ).toBe(true);
      }).pipe(Effect.provide(daytonaCloudLayer(driver)));
    }),
  );

  it.effect("deletes a failed sandbox instead of adopting it during reconcile", () =>
    Effect.gen(function* () {
      const { driver, account: credentials, state } = makeDaytonaHarness();
      const accountId = SandboxProviderAccountId.make("daytona-work");
      yield* Effect.gen(function* () {
        const accounts = yield* SandboxAccounts.SandboxAccounts;
        yield* accounts.connect({
          id: accountId,
          config: {
            kind: DAYTONA_KIND,
            label: "Daytona",
            apiKey: credentials.apiKey,
            template: "base",
            enabled: true,
          },
        });
        const lifecycle = yield* SandboxLifecycle.SandboxLifecycle;
        const store = yield* SandboxStore.SandboxStore;
        const record = provisioningRecord("failed-record");
        yield* store.upsert(record);
        state.sandboxes.push({
          instanceId: record.id,
          providerSandboxId: "daytona-failed",
          status: "failed",
        });

        yield* lifecycle.reconcile;

        expect(yield* store.list).toEqual([]);
        expect(state.sandboxes).toEqual([]);
        expect(state.calls).toContain("delete");
        expect(
          state.listQueries.some((query) => query.includes("includeErroredDeleted=true")),
        ).toBe(true);
      }).pipe(Effect.provide(daytonaCloudLayer(driver)));
    }),
  );

  it.effect("prefers a running duplicate over a paused sandbox it cannot wake", () =>
    Effect.gen(function* () {
      const { driver, account: credentials, state } = makeDaytonaHarness();
      const accountId = SandboxProviderAccountId.make("daytona-work");
      yield* Effect.gen(function* () {
        const accounts = yield* SandboxAccounts.SandboxAccounts;
        yield* accounts.connect({
          id: accountId,
          config: {
            kind: DAYTONA_KIND,
            label: "Daytona",
            apiKey: credentials.apiKey,
            template: "base",
            enabled: true,
          },
        });
        const lifecycle = yield* SandboxLifecycle.SandboxLifecycle;
        const store = yield* SandboxStore.SandboxStore;
        const record = provisioningRecord("duplicate-record");
        yield* store.upsert(record);
        state.sandboxes.push(
          {
            instanceId: record.id,
            providerSandboxId: "daytona-paused",
            status: "paused",
          },
          {
            instanceId: record.id,
            providerSandboxId: "daytona-running",
            status: "running",
          },
        );

        yield* lifecycle.reconcile;

        const records = yield* store.list;
        expect(records).toHaveLength(1);
        expect(records[0]?.providerSandboxId).toBe("daytona-running");
        expect(records[0]?.status).toBe("running");
        expect(state.sandboxes.map((sandbox) => sandbox.providerSandboxId)).toEqual([
          "daytona-running",
        ]);
      }).pipe(Effect.provide(daytonaCloudLayer(driver)));
    }),
  );
});

describe("Daytona adapter", () => {
  it("reserves Daytona's daemon ports from previews", () => {
    const { driver } = makeDaytonaHarness();
    expect(driver.capabilities.reservedPorts).toEqual(
      expect.arrayContaining([2280, 2281, 22220, 22222, 33333]),
    );
  });

  it.effect("maps the base size to a snapshot instead of rejected resources", () =>
    Effect.gen(function* () {
      const { driver, account: credentials, state } = makeDaytonaHarness();
      const size = driver.capabilities.sizes[1]!;
      const region = driver.capabilities.regions[0]!;
      yield* driver.create({
        account: credentials,
        instanceId: "instance-1",
        template: "base",
        timeoutMs: 60_000,
        size,
        region,
      });
      expect(state.lastCreateBody).toEqual({
        snapshot: "daytona-medium",
        labels: { "t3.instance": "instance-1" },
        public: false,
        autoStopInterval: 0,
        autoDeleteInterval: -1,
        target: region.id,
      });
      expect(state.lastCreateBody).not.toHaveProperty("cpu");
      expect(state.lastCreateBody).not.toHaveProperty("memory");
    }),
  );

  it.effect("sends only a custom template's snapshot, ignoring its size", () =>
    Effect.gen(function* () {
      const { driver, account: credentials, state } = makeDaytonaHarness();
      yield* driver.create({
        account: credentials,
        instanceId: "instance-1",
        template: "acme/daytona-base:latest",
        timeoutMs: 60_000,
        size: driver.capabilities.sizes[2]!,
      });
      expect(state.lastCreateBody).toMatchObject({
        snapshot: "acme/daytona-base:latest",
      });
      expect(state.lastCreateBody).not.toHaveProperty("cpu");
      expect(state.lastCreateBody).not.toHaveProperty("memory");
    }),
  );

  it.effect("uses the small snapshot for the default base template", () =>
    Effect.gen(function* () {
      const { driver, account: credentials, state } = makeDaytonaHarness();
      yield* driver.create({
        account: credentials,
        instanceId: "instance-1",
        template: "base",
        timeoutMs: 60_000,
      });
      expect(state.lastCreateBody).toMatchObject({ snapshot: "daytona-small" });
    }),
  );

  it.effect("reports a forbidden key with permission detail, not a bad-key message", () =>
    Effect.gen(function* () {
      const { driver, account: credentials, state } = makeDaytonaHarness();
      state.forbidden = true;
      const failure = yield* Effect.flip(driver.validateCredential(credentials));
      expect(failure.statusCode).toBe(403);
      expect(failure.detail).toContain("permission");
      expect(failure.detail).not.toContain("rejected the API key");
      expect(failure.detail).not.toContain(credentials.apiKey);
    }),
  );

  it.effect("streams growing session logs incrementally and cleans the session up", () =>
    Effect.gen(function* () {
      const { driver, account: credentials, state } = makeDaytonaHarness();
      state.followChunks = [
        `${JSON.stringify({ type: "ready", protocolVersion: "1", pid: 1 })}\n`,
        "line-2\n",
        "line-3\n",
      ];
      state.session = {
        stdout: state.followChunks.join(""),
        stderr: "",
        exitCode: 0,
        input: [],
        deleted: false,
      };
      const created = yield* driver.create({
        account: credentials,
        instanceId: "instance-1",
        template: "base",
        timeoutMs: 60_000,
      });
      let stdout = "";
      let stderr = "";
      const process = yield* driver.startProcess!({
        account: credentials,
        providerSandboxId: created.providerSandboxId,
        command: "python3 -u host.py",
        cwd: "/home/daytona/workspace",
        envs: { T3_TEST: "1" },
        onStdout: (data) => {
          stdout += data;
        },
        onStderr: (data) => {
          stderr += data;
        },
      });
      yield* process.sendStdin("ping\n");
      const result = yield* process.wait;
      expect(result.exitCode).toBe(0);
      expect(stdout).toContain('"type":"ready"');
      expect(stdout).toContain("line-3");
      expect(stderr).toBe("");
      expect(state.lastProcessCommand).toBe(
        "cd '/home/daytona/workspace' && env T3_TEST='1' python3 -u host.py",
      );
      expect(state.session?.input).toEqual(["ping\n"]);
      expect(state.session?.deleted).toBe(true);
      expect(state.followCalls).toBe(1);
      // The follow stream is the log source; at most one final snapshot confirms the tail.
      expect(state.logsRequests).toBeLessThanOrEqual(1);
    }),
  );

  it.live("reconnects when follow closes mid-turn and skips the replayed prefix", () =>
    Effect.gen(function* () {
      const { driver, account: credentials, state } = makeDaytonaHarness();
      const ready = `${JSON.stringify({ type: "ready", protocolVersion: "1", pid: 1 })}\n`;
      const later = "late-output\n";
      state.followScripts = [[ready], [ready, later]];
      state.followChunks = [];
      state.exitAfterFollowCalls = 2;
      state.session = {
        stdout: `${ready}${later}`,
        stderr: "",
        exitCode: 0,
        input: [],
        deleted: false,
      };
      const created = yield* driver.create({
        account: credentials,
        instanceId: "instance-1",
        template: "base",
        timeoutMs: 60_000,
      });
      let stdout = "";
      const process = yield* driver.startProcess!({
        account: credentials,
        providerSandboxId: created.providerSandboxId,
        command: "python3 -u host.py",
        onStdout: (data) => {
          stdout += data;
        },
        onStderr: () => {},
      });
      const result = yield* process.wait;
      expect(result.exitCode).toBe(0);
      expect(state.followCalls).toBe(2);
      expect(stdout).toBe(`${ready}${later}`);
      expect(stdout.match(/"type":"ready"/g)).toHaveLength(1);
      expect(state.session?.deleted).toBe(true);
    }),
  );

  it.live("emits only new fallback log lines as the body grows", () =>
    Effect.gen(function* () {
      const { driver, account: credentials, state } = makeDaytonaHarness();
      state.disableFollowLogs = true;
      state.growingLogs = true;
      state.exitAfterLogsRequests = 3;
      const created = yield* driver.create({
        account: credentials,
        instanceId: "instance-1",
        template: "base",
        timeoutMs: 60_000,
      });
      let stdout = "";
      const process = yield* driver.startProcess!({
        account: credentials,
        providerSandboxId: created.providerSandboxId,
        command: "python3 -u host.py",
        onStdout: (data) => {
          stdout += data;
        },
        onStderr: () => {},
      });
      const result = yield* process.wait;
      expect(result.exitCode).toBe(0);
      expect(stdout).toBe("line-1\nline-2\nline-3\n");
      expect(state.logsRequests).toBe(4);
      expect(state.session?.deleted).toBe(true);
    }),
  );

  it.effect("falls back to plain-text logs with an SDK version and still delivers ready", () =>
    Effect.gen(function* () {
      const { driver, account: credentials, state } = makeDaytonaHarness();
      state.disableFollowLogs = true;
      state.plainTextLogs = true;
      state.session = {
        stdout: `${JSON.stringify({ type: "ready", protocolVersion: "1", pid: 1 })}\n`,
        stderr: "warn\n",
        exitCode: 0,
        input: [],
        deleted: false,
      };
      const created = yield* driver.create({
        account: credentials,
        instanceId: "instance-1",
        template: "base",
        timeoutMs: 60_000,
      });
      let stdout = "";
      const process = yield* driver.startProcess!({
        account: credentials,
        providerSandboxId: created.providerSandboxId,
        command: "python3 -u host.py",
        onStdout: (data) => {
          stdout += data;
        },
        onStderr: () => {},
      });
      const result = yield* process.wait;
      expect(result.exitCode).toBe(0);
      expect(stdout).toContain('"type":"ready"');
      expect(state.logsRequests).toBeGreaterThan(0);
      expect(state.lastLogsHeaders?.["x-daytona-sdk-version"]).toBe("0.167.0");
      expect(state.session?.deleted).toBe(true);
    }),
  );
});
