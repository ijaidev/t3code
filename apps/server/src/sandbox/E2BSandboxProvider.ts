import { SandboxProviderKind, type SandboxProviderCapabilities } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import {
  SandboxProviderError,
  type SandboxProviderAccount,
  type SandboxProviderDriver,
  type SandboxProviderExecInput,
  type SandboxProviderOperation,
} from "./SandboxProvider.ts";

const E2B_KIND = SandboxProviderKind.make("e2b");
const E2B_DEFAULT_API_DOMAIN = "e2b.app";
const E2B_CREATE_TIMEOUT_MS = 120_000;
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

interface E2BSandboxHandle {
  readonly commands: {
    readonly run: (
      command: string,
      options: {
        readonly cwd?: string;
        readonly timeoutMs?: number;
        readonly shell?: ReadonlyArray<string>;
      },
    ) => Promise<E2BCommandResult>;
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
  readonly list: (input: { readonly apiKey: string }) => Promise<unknown>;
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
  create: (input) =>
    attempt(input.account, "create", () =>
      client.create({
        template: input.template,
        apiKey: input.account.apiKey,
        timeoutMs: input.timeoutMs,
        idempotencyKey: input.instanceId,
        metadata: { "t3.instance": input.instanceId },
      }),
    ).pipe(Effect.map((created) => ({ providerSandboxId: created.sandboxId }))),
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
});
