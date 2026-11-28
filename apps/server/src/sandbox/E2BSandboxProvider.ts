import { SandboxProviderKind, type SandboxProviderCapabilities } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import {
  SandboxProviderError,
  type SandboxProviderAccount,
  type SandboxProviderDriver,
  type SandboxProviderExecInput,
  type SandboxProviderOperation,
  type SandboxProviderPortAccessInput,
  type SandboxProviderProcess,
  type SandboxProviderStartProcessInput,
  type SandboxProviderWriteFileInput,
} from "./SandboxProvider.ts";

const E2B_KIND = SandboxProviderKind.make("e2b");
const E2B_DEFAULT_API_DOMAIN = "e2b.app";
const E2B_CREATE_TIMEOUT_MS = 120_000;
/** Hobby accounts cap sandbox lifetimes at one hour; Pro accounts accept up to 24 hours. */
const E2B_HOBBY_MAX_TIMEOUT_MS = 60 * 60_000;
const E2B_REQUEST_TIMEOUT_MS = 60_000;
const E2B_EXEC_TIMEOUT_MS = 120_000;
const MAX_ERROR_DETAIL_LENGTH = 300;

const capabilities: SandboxProviderCapabilities = {
  create: true,
  exec: true,
  pauseResume: true,
  delete: true,
  snapshots: true,
  privatePorts: true,
  harness: true,
  reservedPorts: [49983, 50005],
  sizes: [
    { id: "small", label: "2 vCPU · 1 GiB", cpuCount: 2, memoryMB: 1024 },
    { id: "medium", label: "4 vCPU · 2 GiB", cpuCount: 4, memoryMB: 2048 },
    { id: "large", label: "8 vCPU · 4 GiB", cpuCount: 8, memoryMB: 4096 },
  ],
  regions: [
    { id: "us", label: "United States" },
    { id: "eu", label: "Europe" },
  ],
};

interface E2BCommandResult {
  readonly exitCode: number;
  readonly stdout: string;
  readonly stderr: string;
}

interface E2BCommandHandle {
  readonly pid: number;
  readonly exitCode: number | undefined;
  readonly sendStdin: (data: string | Uint8Array) => Promise<void>;
  readonly kill: () => Promise<boolean>;
  readonly wait: () => Promise<E2BCommandResult>;
  readonly disconnect: () => Promise<void>;
}

export interface E2BSandboxHandle {
  /** The sandbox's traffic token, when its public URLs require authentication. */
  readonly trafficAccessToken?: string;
  readonly getHost: (port: number) => string;
  readonly commands: {
    readonly run: {
      (
        command: string,
        options: {
          readonly cwd?: string;
          readonly envs?: Readonly<Record<string, string>>;
          readonly timeoutMs?: number;
          readonly shell?: ReadonlyArray<string>;
        },
      ): Promise<E2BCommandResult>;
      (
        command: string,
        options: {
          readonly background: true;
          readonly stdin: true;
          readonly cwd?: string;
          readonly envs?: Readonly<Record<string, string>>;
          readonly timeoutMs?: number;
          readonly retainOutput?: false;
          readonly onStdout?: (data: string) => void | Promise<void>;
          readonly onStderr?: (data: string) => void | Promise<void>;
          readonly shell?: ReadonlyArray<string>;
        },
      ): Promise<E2BCommandHandle>;
    };
  };
  readonly files: {
    readonly write: (path: string, data: string | Uint8Array) => Promise<unknown>;
  };
}

/** The slice of `e2b` this adapter uses, so tests can run it without the SDK or a network. */
export interface E2BClient {
  readonly create: (input: {
    readonly template: string;
    readonly apiKey: string;
    readonly timeoutMs: number;
    readonly idempotencyKey: string;
    readonly metadata: Readonly<Record<string, string>>;
    readonly network: {
      readonly allowPublicTraffic: false;
      readonly maskRequestHost: string;
    };
  }) => Promise<{ readonly sandboxId: string }>;
  readonly connect: (input: {
    readonly sandboxId: string;
    readonly apiKey: string;
  }) => Promise<E2BSandboxHandle>;
  readonly getInfo: (input: {
    readonly sandboxId: string;
    readonly apiKey: string;
  }) => Promise<{ readonly state: "running" | "paused" }>;
  readonly pause: (input: {
    readonly sandboxId: string;
    readonly apiKey: string;
  }) => Promise<boolean>;
  readonly delete: (input: {
    readonly sandboxId: string;
    readonly apiKey: string;
  }) => Promise<boolean>;
  readonly list: (input: {
    readonly apiKey: string;
    readonly metadata?: Readonly<Record<string, string>>;
  }) => Promise<
    ReadonlyArray<{
      readonly sandboxId: string;
      readonly state: "running" | "paused";
    }>
  >;
}

const statusCodeOf = (cause: unknown): number | undefined => {
  if (cause === null || typeof cause !== "object" || !("statusCode" in cause)) return undefined;
  const statusCode = (cause as { readonly statusCode?: unknown }).statusCode;
  return typeof statusCode === "number" ? statusCode : undefined;
};

const safeDetail = (cause: unknown, apiKey: string): string => {
  const raw = cause instanceof Error ? cause.message : String(cause);
  const redacted = apiKey.length > 0 ? raw.replaceAll(apiKey, "[redacted]") : raw;
  return redacted.slice(0, MAX_ERROR_DETAIL_LENGTH);
};

const providerError = (
  account: SandboxProviderAccount,
  operation: SandboxProviderOperation,
  cause: unknown,
): SandboxProviderError => {
  const statusCode = statusCodeOf(cause);
  if (statusCode === 401 || statusCode === 403) {
    return new SandboxProviderError({
      providerKind: E2B_KIND,
      operation,
      detail: "E2B rejected the API key. Check the key and reconnect the account.",
      statusCode,
    });
  }
  if (statusCode === 404) {
    return new SandboxProviderError({
      providerKind: E2B_KIND,
      operation,
      detail: "E2B could not find that sandbox. It may have expired or been deleted.",
      statusCode,
    });
  }
  if (statusCode === 429) {
    return new SandboxProviderError({
      providerKind: E2B_KIND,
      operation,
      detail: "E2B is rate limiting requests. Try again in a moment.",
      statusCode,
    });
  }
  return new SandboxProviderError({
    providerKind: E2B_KIND,
    operation,
    detail: safeDetail(cause, account.apiKey) || "E2B could not complete the request. Try again.",
    ...(statusCode === undefined ? {} : { statusCode }),
  });
};

const attempt = <A>(
  account: SandboxProviderAccount,
  operation: SandboxProviderOperation,
  run: () => Promise<A>,
) =>
  Effect.tryPromise({
    try: run,
    catch: (cause) => providerError(account, operation, cause),
  });

const defaultClient: E2BClient = {
  create: async (input) => {
    const { Sandbox } = await import("e2b");
    return Sandbox.create(input.template, {
      apiKey: input.apiKey,
      timeoutMs: input.timeoutMs,
      idempotencyKey: input.idempotencyKey,
      metadata: input.metadata,
      requestTimeoutMs: E2B_CREATE_TIMEOUT_MS,
      network: input.network,
    });
  },
  connect: async (input) => {
    const { Sandbox } = await import("e2b");
    return Sandbox.connect(input.sandboxId, {
      apiKey: input.apiKey,
      requestTimeoutMs: E2B_REQUEST_TIMEOUT_MS,
    });
  },
  getInfo: async (input) => {
    const { Sandbox } = await import("e2b");
    return Sandbox.getInfo(input.sandboxId, {
      apiKey: input.apiKey,
      requestTimeoutMs: E2B_REQUEST_TIMEOUT_MS,
    });
  },
  pause: async (input) => {
    const { Sandbox } = await import("e2b");
    return Sandbox.pause(input.sandboxId, {
      apiKey: input.apiKey,
      requestTimeoutMs: E2B_REQUEST_TIMEOUT_MS,
    });
  },
  delete: async (input) => {
    const { Sandbox } = await import("e2b");
    return Sandbox.kill(input.sandboxId, {
      apiKey: input.apiKey,
      requestTimeoutMs: E2B_REQUEST_TIMEOUT_MS,
    });
  },
  list: async (input) => {
    const { Sandbox } = await import("e2b");
    return Sandbox.list({
      apiKey: input.apiKey,
      domain: E2B_DEFAULT_API_DOMAIN,
      ...(input.metadata === undefined ? {} : { query: { metadata: input.metadata } }),
    }).nextItems();
  },
};

export const makeE2BSandboxProvider = (
  client: E2BClient = defaultClient,
): SandboxProviderDriver => ({
  kind: E2B_KIND,
  label: "E2B",
  capabilities,
  validateCredential: (account) =>
    attempt(account, "validate-credential", () => client.list({ apiKey: account.apiKey })).pipe(
      Effect.asVoid,
    ),
  create: (input) => {
    const create = (timeoutMs: number) =>
      attempt(input.account, "create", () =>
        client.create({
          template: input.template,
          apiKey: input.account.apiKey,
          timeoutMs,
          idempotencyKey: input.instanceId,
          metadata: { "t3.instance": input.instanceId },
          // A preview is only ever reachable through the controlling server, never from this URL.
          network: {
            allowPublicTraffic: false,
            // The port makes the app-visible Host agree with the preview's rewritten Origin.
            maskRequestHost: "localhost:${PORT}",
          },
        }),
      );
    return create(input.timeoutMs).pipe(
      Effect.catchIf(
        (error) =>
          error.statusCode === 400 &&
          error.detail.toLowerCase().includes("timeout cannot be greater"),
        () => create(E2B_HOBBY_MAX_TIMEOUT_MS),
      ),
      Effect.map((created) => ({ providerSandboxId: created.sandboxId })),
    );
  },
  findCreated: (account, instanceId) =>
    attempt(account, "find-created", () =>
      client.list({
        apiKey: account.apiKey,
        metadata: { "t3.instance": instanceId },
      }),
    ).pipe(
      Effect.map((sandboxes) =>
        sandboxes.map((sandbox) => ({
          providerSandboxId: sandbox.sandboxId,
          status: sandbox.state,
        })),
      ),
    ),
  get: (account, providerSandboxId) =>
    attempt(account, "get", () =>
      client.getInfo({ sandboxId: providerSandboxId, apiKey: account.apiKey }),
    ).pipe(Effect.map((info) => ({ status: info.state }))),
  pause: (account, providerSandboxId) =>
    attempt(account, "pause", () =>
      client.pause({ sandboxId: providerSandboxId, apiKey: account.apiKey }),
    ).pipe(Effect.asVoid),
  resume: (account, providerSandboxId) =>
    attempt(account, "resume", () =>
      client.connect({ sandboxId: providerSandboxId, apiKey: account.apiKey }),
    ).pipe(Effect.asVoid),
  delete: (account, providerSandboxId) =>
    attempt(account, "delete", () =>
      client.delete({ sandboxId: providerSandboxId, apiKey: account.apiKey }),
    ).pipe(Effect.asVoid),
  exec: Effect.fn("E2BSandboxProvider.exec")(function* (input: SandboxProviderExecInput) {
    const sandbox = yield* attempt(input.account, "exec", () =>
      client.connect({ sandboxId: input.providerSandboxId, apiKey: input.account.apiKey }),
    );
    const result = yield* attempt(input.account, "exec", () =>
      sandbox.commands.run(input.command, {
        ...(input.cwd === undefined ? {} : { cwd: input.cwd }),
        ...(input.envs === undefined ? {} : { envs: input.envs }),
        timeoutMs: input.timeoutMs ?? E2B_EXEC_TIMEOUT_MS,
        shell: ["/bin/sh", "-lc"],
      }),
    );
    return {
      exitCode: result.exitCode,
      stdout: result.stdout,
      stderr: result.stderr,
    };
  }),
  writeFile: Effect.fn("E2BSandboxProvider.writeFile")(function* (
    input: SandboxProviderWriteFileInput,
  ) {
    const sandbox = yield* attempt(input.account, "exec", () =>
      client.connect({ sandboxId: input.providerSandboxId, apiKey: input.account.apiKey }),
    );
    yield* attempt(input.account, "exec", () => sandbox.files.write(input.path, input.data));
  }),
  portAccess: Effect.fn("E2BSandboxProvider.portAccess")(function* (
    input: SandboxProviderPortAccessInput,
  ) {
    // Connecting also resumes a paused sandbox, so opening a preview wakes it before the request.
    const sandbox = yield* attempt(input.account, "exec", () =>
      client.connect({ sandboxId: input.providerSandboxId, apiKey: input.account.apiKey }),
    );
    const token = sandbox.trafficAccessToken;
    if (token === undefined || token === "") {
      return yield* new SandboxProviderError({
        providerKind: E2B_KIND,
        operation: "exec",
        detail: "E2B did not return a private preview token for this sandbox.",
      });
    }
    return {
      url: `https://${sandbox.getHost(input.port)}`,
      headers: { "e2b-traffic-access-token": token },
    };
  }),
  startProcess: Effect.fn("E2BSandboxProvider.startProcess")(function* (
    input: SandboxProviderStartProcessInput,
  ) {
    const sandbox = yield* attempt(input.account, "exec", () =>
      client.connect({ sandboxId: input.providerSandboxId, apiKey: input.account.apiKey }),
    );
    const handle = yield* attempt(input.account, "exec", () =>
      sandbox.commands.run(input.command, {
        background: true,
        stdin: true,
        retainOutput: false,
        timeoutMs: 60 * 60_000,
        ...(input.cwd === undefined ? {} : { cwd: input.cwd }),
        ...(input.envs === undefined ? {} : { envs: input.envs }),
        onStdout: input.onStdout,
        onStderr: input.onStderr,
        shell: ["/bin/sh", "-lc"],
      }),
    );
    const process: SandboxProviderProcess = {
      pid: handle.pid,
      sendStdin: (data) =>
        attempt(input.account, "exec", () => handle.sendStdin(data)).pipe(Effect.asVoid),
      kill: attempt(input.account, "exec", () => handle.kill()).pipe(Effect.asVoid),
      wait: attempt(input.account, "exec", async () => {
        try {
          const result = await handle.wait();
          return { exitCode: result.exitCode };
        } catch {
          return { exitCode: handle.exitCode ?? 1 };
        }
      }),
    };
    return process;
  }),
});
