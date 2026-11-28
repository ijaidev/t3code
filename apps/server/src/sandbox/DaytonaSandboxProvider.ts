// @effect-diagnostics nodeBuiltinImport:off globalFetch:off globalFetchInEffect:off - This adapter is the HTTP boundary for Daytona's REST and toolbox APIs; the API key must never leave the server.
import {
  SandboxProviderKind,
  type SandboxProviderCapabilities,
  type SandboxProviderRegion,
  type SandboxProviderSize,
} from "@t3tools/contracts";
import * as NodeCrypto from "node:crypto";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import { WebSocket } from "ws";

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

const DAYTONA_KIND = SandboxProviderKind.make("daytona");
const DEFAULT_API_URL = "https://app.daytona.io/api";
const API_REQUEST_TIMEOUT_MS = 60_000;
const CREATE_READY_TIMEOUT_MS = 120_000;
const CREATE_POLL_INTERVAL = "500 millis";
const EXEC_TIMEOUT_MS = 120_000;
const MAX_ERROR_DETAIL_LENGTH = 300;
const BASE_TEMPLATE = "base";
const DAYTONA_SDK_VERSION = "0.167.0";
const MAX_FOLLOW_RECONNECTS = 5;

const capabilities: SandboxProviderCapabilities = {
  create: true,
  exec: true,
  // Daytona's pause is experimental and provider/plan specific. Hiding sleep is safer than
  // emulating it, and the lifecycle treats pause as an optional capability.
  pauseResume: false,
  delete: true,
  snapshots: false,
  privatePorts: true,
  harness: true,
  // The toolbox, terminal, and recordings dashboards are never app previews.
  reservedPorts: [22222, 2280, 2281, 22220, 33333],
  sizes: [
    { id: "small", label: "1 vCPU · 1 GiB", cpuCount: 1, memoryMB: 1024 },
    { id: "medium", label: "2 vCPU · 4 GiB", cpuCount: 2, memoryMB: 4096 },
    { id: "large", label: "4 vCPU · 8 GiB", cpuCount: 4, memoryMB: 8192 },
  ],
  regions: [
    { id: "us", label: "United States" },
    { id: "eu", label: "Europe" },
  ],
};

export type DaytonaMethod = "GET" | "POST" | "DELETE";

export interface DaytonaRequest {
  readonly method: DaytonaMethod;
  /** An absolute API or toolbox URL. */
  readonly url: string;
  readonly apiKey: string;
  readonly body?: unknown;
  readonly data?: string | Uint8Array;
  readonly headers?: Readonly<Record<string, string>>;
  readonly timeoutMs?: number;
  /** Logs are text even when the body itself looks like JSON. */
  readonly parseJson?: boolean;
}

export interface DaytonaResponse {
  readonly status: number;
  readonly body: unknown;
}

export interface DaytonaFollowLogsInput {
  /** The HTTPS logs URL; the client upgrades it to a WebSocket. */
  readonly url: string;
  readonly apiKey: string;
  readonly onData: (data: string) => void;
  readonly onClose: () => void;
}

export interface DaytonaLogFollow {
  readonly close: () => void;
}

/** The slice of Daytona's HTTP API this adapter uses, so tests can run it without a network. */
export interface DaytonaClient {
  readonly request: (input: DaytonaRequest) => Promise<DaytonaResponse>;
  /** Follows combined log chunks over Daytona's WebSocket; no full log body is re-downloaded. */
  readonly followLogs?: (input: DaytonaFollowLogsInput) => Promise<DaytonaLogFollow>;
}

const asRecord = (value: unknown): Readonly<Record<string, unknown>> =>
  typeof value === "object" && value !== null ? (value as Readonly<Record<string, unknown>>) : {};

const readString = (value: unknown): string | undefined =>
  typeof value === "string" && value !== "" ? value : undefined;

const readNumber = (value: unknown): number | undefined =>
  typeof value === "number" && Number.isFinite(value) ? value : undefined;

const parseBody = (text: string): unknown => {
  if (text.trim() === "") return undefined;
  try {
    return JSON.parse(text) as unknown;
  } catch {
    return text;
  }
};

const errorDetail = (body: unknown, status: number): string => {
  const record = asRecord(body);
  const message =
    readString(record["message"]) ??
    readString(asRecord(record["error"])["message"]) ??
    readString(body);
  return message ?? `Daytona responded with HTTP ${status}.`;
};

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
  if (statusCode === 401) {
    return new SandboxProviderError({
      providerKind: DAYTONA_KIND,
      operation,
      detail: "Daytona rejected the API key. Check the key and reconnect the account.",
      statusCode,
    });
  }
  if (statusCode === 403) {
    const detail = safeDetail(cause, account.apiKey);
    return new SandboxProviderError({
      providerKind: DAYTONA_KIND,
      operation,
      detail: `${detail || "Daytona denied the request."} Check that this Daytona key has permission for this organization and plan.`,
      statusCode,
    });
  }
  if (statusCode === 404) {
    return new SandboxProviderError({
      providerKind: DAYTONA_KIND,
      operation,
      detail: "Daytona could not find that sandbox. It may have expired or been deleted.",
      statusCode,
    });
  }
  if (statusCode === 429) {
    return new SandboxProviderError({
      providerKind: DAYTONA_KIND,
      operation,
      detail: "Daytona is rate limiting requests. Try again in a moment.",
      statusCode,
    });
  }
  return new SandboxProviderError({
    providerKind: DAYTONA_KIND,
    operation,
    detail:
      safeDetail(cause, account.apiKey) || "Daytona could not complete the request. Try again.",
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

const defaultClient: DaytonaClient = {
  request: async (input) => {
    const headers: Record<string, string> = {
      authorization: `Bearer ${input.apiKey}`,
      ...input.headers,
    };
    if (input.body !== undefined) headers["content-type"] = "application/json";
    const response = await fetch(input.url, {
      method: input.method,
      headers,
      ...(input.data === undefined
        ? input.body === undefined
          ? {}
          : { body: JSON.stringify(input.body) }
        : { body: input.data }),
      signal: AbortSignal.timeout(input.timeoutMs ?? API_REQUEST_TIMEOUT_MS),
    });
    const text = await response.text();
    const body = input.parseJson === false ? text : parseBody(text);
    if (!response.ok) {
      throw Object.assign(new Error(errorDetail(body, response.status)), {
        statusCode: response.status,
      });
    }
    return { status: response.status, body };
  },
  followLogs: async (input) => {
    const url = new URL(input.url);
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    url.searchParams.set("follow", "true");
    // No X-Daytona-SDK-Version here: the daemon then streams combined text chunks instead of
    // JSON or split-output frames, which is what the host line parser expects.
    const socket = new WebSocket(url, {
      headers: { authorization: `Bearer ${input.apiKey}` },
    });
    socket.on("message", (data) => input.onData(data.toString()));
    socket.on("close", input.onClose);
    socket.on("error", () => {
      // The close event reports the failure to the caller.
    });
    await new Promise<void>((resolve, reject) => {
      socket.once("open", resolve);
      socket.once("error", reject);
      socket.once("close", () =>
        reject(new Error("Daytona closed the log stream before it opened.")),
      );
    });
    return { close: () => socket.close() };
  },
};

const sandboxState = (value: unknown): string => readString(asRecord(value)["state"]) ?? "unknown";

const PAUSED_STATES = new Set([
  "paused",
  "pausing",
  "stopped",
  "stopping",
  "archived",
  "archiving",
]);
const FAILED_STATES = new Set(["destroyed", "destroying", "error", "build_failed", "unknown"]);

const providerStatus = (state: string): "running" | "paused" | "failed" =>
  FAILED_STATES.has(state) ? "failed" : PAUSED_STATES.has(state) ? "paused" : "running";

const shellQuote = (value: string) => `'${value.replaceAll("'", `'\\''`)}'`;

const sessionCommand = (input: {
  readonly command: string;
  readonly cwd?: string;
  readonly envs?: Readonly<Record<string, string>>;
}): string => {
  const environment = Object.entries(input.envs ?? {})
    .map(([key, value]) => `${key}=${shellQuote(value)}`)
    .join(" ");
  const command = environment === "" ? input.command : `env ${environment} ${input.command}`;
  // Daytona sessions have no cwd request field, so enter it before the shell command runs.
  return input.cwd === undefined ? command : `cd ${shellQuote(input.cwd)} && ${command}`;
};

const logsObject = (text: string): Readonly<Record<string, unknown>> | null => {
  try {
    const record = asRecord(JSON.parse(text) as unknown);
    return "stdout" in record || "stderr" in record || "output" in record ? record : null;
  } catch {
    return null;
  }
};

const LOG_STDOUT_PREFIX = "\u0001\u0001\u0001";
const LOG_STDERR_PREFIX = "\u0002\u0002\u0002";

const combinedLogs = (text: string): string =>
  text.replaceAll(LOG_STDOUT_PREFIX, "").replaceAll(LOG_STDERR_PREFIX, "");

const logsFromBody = (
  body: unknown,
): { readonly combined?: string; readonly stdout?: string; readonly stderr?: string } => {
  if (typeof body === "string") {
    const parsed = logsObject(body);
    return parsed === null ? { combined: combinedLogs(body) } : logsFromBody(parsed);
  }
  const record = asRecord(body);
  const output = readString(record["output"]);
  if (output !== undefined) {
    return { combined: combinedLogs(output) };
  }
  const stdout = readString(record["stdout"]);
  const stderr = readString(record["stderr"]);
  return {
    ...(stdout === undefined ? {} : { stdout }),
    ...(stderr === undefined ? {} : { stderr }),
  };
};

const BASE_SNAPSHOTS: Readonly<Record<SandboxProviderSize["id"], string>> = {
  small: "daytona-small",
  medium: "daytona-medium",
  large: "daytona-large",
};

const BASE_SNAPSHOT = "daytona-small";

const baseSnapshotFor = (size: SandboxProviderSize | undefined): string =>
  size === undefined ? BASE_SNAPSHOT : (BASE_SNAPSHOTS[size.id] ?? BASE_SNAPSHOT);

const createBody = (input: {
  readonly template: string;
  readonly instanceId: string;
  readonly size?: SandboxProviderSize;
  readonly region?: SandboxProviderRegion;
}): Readonly<Record<string, unknown>> => ({
  // Daytona rejects cpu/memory/disk together with a snapshot. Base sizes are snapshots, and a
  // custom template already names an image with its own resources.
  snapshot: input.template === BASE_TEMPLATE ? baseSnapshotFor(input.size) : input.template,
  labels: { "t3.instance": input.instanceId },
  public: false,
  // T3 owns sleep and delete for cloud threads; provider lifetime timers must not race them.
  autoStopInterval: 0,
  autoDeleteInterval: -1,
  ...(input.region === undefined ? {} : { target: input.region.id }),
});

export const makeDaytonaSandboxProvider = (
  client: DaytonaClient = defaultClient,
  apiUrl = DEFAULT_API_URL,
): SandboxProviderDriver => {
  const toolboxUrls = new Map<string, string>();

  const request = (
    account: SandboxProviderAccount,
    operation: SandboxProviderOperation,
    method: DaytonaMethod,
    url: string,
    options: Omit<DaytonaRequest, "method" | "url" | "apiKey"> = {},
  ) =>
    attempt(account, operation, () =>
      client.request({ method, url, apiKey: account.apiKey, ...options }),
    );

  const apiUrlFor = (path: string) => `${apiUrl.replace(/\/$/, "")}${path}`;

  const sandboxFrom = (body: unknown): string | undefined => readString(asRecord(body)["id"]);

  const instanceLabels = (instanceId: string): string =>
    JSON.stringify({ "t3.instance": instanceId });

  const waitUntilRunning = Effect.fn("DaytonaSandboxProvider.waitUntilRunning")(function* (
    account: SandboxProviderAccount,
    providerSandboxId: string,
  ) {
    const deadline = (yield* Clock.currentTimeMillis) + CREATE_READY_TIMEOUT_MS;
    while (true) {
      const response = yield* request(
        account,
        "create",
        "GET",
        apiUrlFor(`/sandbox/${encodeURIComponent(providerSandboxId)}`),
      );
      const state = sandboxState(response.body);
      if (state === "started" || state === "running") return;
      if (FAILED_STATES.has(state)) {
        return yield* new SandboxProviderError({
          providerKind: DAYTONA_KIND,
          operation: "create",
          detail: `Daytona could not start the sandbox (${state}).`,
        });
      }
      if ((yield* Clock.currentTimeMillis) >= deadline) {
        return yield* new SandboxProviderError({
          providerKind: DAYTONA_KIND,
          operation: "create",
          detail: "Daytona did not finish starting the sandbox in time.",
        });
      }
      yield* Effect.sleep(CREATE_POLL_INTERVAL);
    }
  });

  const toolboxUrl = Effect.fn("DaytonaSandboxProvider.toolboxUrl")(function* (
    account: SandboxProviderAccount,
    providerSandboxId: string,
  ) {
    const cached = toolboxUrls.get(providerSandboxId);
    if (cached !== undefined) return cached;
    const response = yield* request(
      account,
      "exec",
      "GET",
      apiUrlFor(`/sandbox/${encodeURIComponent(providerSandboxId)}/toolbox-proxy-url`),
    );
    const url = readString(asRecord(response.body)["url"]);
    if (url === undefined) {
      return yield* new SandboxProviderError({
        providerKind: DAYTONA_KIND,
        operation: "exec",
        detail: "Daytona did not return a toolbox URL for this sandbox.",
      });
    }
    toolboxUrls.set(providerSandboxId, url);
    return url;
  });

  const toolboxPath = (toolbox: string, providerSandboxId: string, path: string) =>
    `${toolbox.replace(/\/$/, "")}/${encodeURIComponent(providerSandboxId)}${path}`;

  return {
    kind: DAYTONA_KIND,
    label: "Daytona",
    capabilities,
    validateCredential: (account) =>
      request(account, "validate-credential", "GET", apiUrlFor("/sandbox?limit=1")).pipe(
        Effect.asVoid,
      ),
    create: Effect.fn("DaytonaSandboxProvider.create")(function* (input) {
      const response = yield* request(input.account, "create", "POST", apiUrlFor("/sandbox"), {
        body: createBody(input),
        timeoutMs: API_REQUEST_TIMEOUT_MS,
      });
      const providerSandboxId = sandboxFrom(response.body);
      if (providerSandboxId === undefined) {
        return yield* new SandboxProviderError({
          providerKind: DAYTONA_KIND,
          operation: "create",
          detail: "Daytona did not return the new sandbox id.",
        });
      }
      yield* waitUntilRunning(input.account, providerSandboxId);
      return { providerSandboxId };
    }),
    findCreated: Effect.fn("DaytonaSandboxProvider.findCreated")(function* (
      account: SandboxProviderAccount,
      instanceId: string,
    ) {
      const found: Array<{
        providerSandboxId: string;
        status: "running" | "paused" | "failed";
      }> = [];
      let cursor: string | undefined;
      for (let page = 0; page < 10; page += 1) {
        const query = new URLSearchParams({
          labels: instanceLabels(instanceId),
          limit: "200",
          // Failed creates can leave a sandbox Daytona hides from ordinary lists.
          includeErroredDeleted: "true",
          ...(cursor === undefined ? {} : { cursor }),
        });
        const response = yield* request(
          account,
          "find-created",
          "GET",
          apiUrlFor(`/sandbox?${query.toString()}`),
        );
        const body = asRecord(response.body);
        const items = Array.isArray(body["items"]) ? body["items"] : [];
        for (const item of items) {
          const providerSandboxId = readString(asRecord(item)["id"]);
          if (providerSandboxId === undefined) continue;
          found.push({
            providerSandboxId,
            status: providerStatus(sandboxState(item)),
          });
        }
        const next = readString(body["nextCursor"]);
        if (next === undefined || next === cursor) break;
        cursor = next;
      }
      return found;
    }),
    get: Effect.fn("DaytonaSandboxProvider.get")(function* (
      account: SandboxProviderAccount,
      providerSandboxId: string,
    ) {
      const response = yield* request(
        account,
        "get",
        "GET",
        apiUrlFor(`/sandbox/${encodeURIComponent(providerSandboxId)}`),
      );
      const state = sandboxState(response.body);
      const status = providerStatus(state);
      if (status === "failed") {
        return yield* new SandboxProviderError({
          providerKind: DAYTONA_KIND,
          operation: "get",
          detail: "Daytona could not find that sandbox. It may have expired or been deleted.",
          statusCode: 404,
        });
      }
      return { status };
    }),
    pause: (_account) =>
      Effect.fail(
        new SandboxProviderError({
          providerKind: DAYTONA_KIND,
          operation: "pause",
          detail: "Daytona sandboxes cannot be paused by T3 Code.",
        }),
      ),
    resume: (_account) =>
      Effect.fail(
        new SandboxProviderError({
          providerKind: DAYTONA_KIND,
          operation: "resume",
          detail: "Daytona sandboxes cannot be woken after sleeping in T3 Code.",
        }),
      ),
    delete: (account, providerSandboxId) =>
      request(
        account,
        "delete",
        "DELETE",
        apiUrlFor(`/sandbox/${encodeURIComponent(providerSandboxId)}`),
      ).pipe(
        Effect.andThen(
          Effect.sync(() => {
            toolboxUrls.delete(providerSandboxId);
          }),
        ),
        Effect.asVoid,
      ),
    exec: Effect.fn("DaytonaSandboxProvider.exec")(function* (input: SandboxProviderExecInput) {
      const toolbox = yield* toolboxUrl(input.account, input.providerSandboxId);
      const response = yield* request(
        input.account,
        "exec",
        "POST",
        toolboxPath(toolbox, input.providerSandboxId, "/process/execute"),
        {
          body: {
            command: input.command,
            ...(input.cwd === undefined ? {} : { cwd: input.cwd }),
            ...(input.envs === undefined ? {} : { envs: input.envs }),
            timeout: Math.max(1, Math.ceil((input.timeoutMs ?? EXEC_TIMEOUT_MS) / 1000)),
          },
          timeoutMs: (input.timeoutMs ?? EXEC_TIMEOUT_MS) + 30_000,
        },
      );
      const body = asRecord(response.body);
      return {
        exitCode: readNumber(body["exitCode"]) ?? 0,
        // The toolbox combines stdout and stderr into `result`.
        stdout: readString(body["result"]) ?? "",
        stderr: "",
      };
    }),
    writeFile: Effect.fn("DaytonaSandboxProvider.writeFile")(function* (
      input: SandboxProviderWriteFileInput,
    ) {
      const toolbox = yield* toolboxUrl(input.account, input.providerSandboxId);
      const path = encodeURIComponent(input.path);
      yield* request(
        input.account,
        "exec",
        "POST",
        toolboxPath(toolbox, input.providerSandboxId, `/files/upload-v2?path=${path}`),
        {
          data: input.data,
          headers: { "content-type": "application/octet-stream" },
        },
      );
    }),
    portAccess: Effect.fn("DaytonaSandboxProvider.portAccess")(function* (
      input: SandboxProviderPortAccessInput,
    ) {
      const response = yield* request(
        input.account,
        "exec",
        "GET",
        apiUrlFor(
          `/sandbox/${encodeURIComponent(input.providerSandboxId)}/ports/${input.port}/preview-url`,
        ),
      );
      const body = asRecord(response.body);
      const url = readString(body["url"]);
      const token = readString(body["token"]);
      if (url === undefined || token === undefined) {
        return yield* new SandboxProviderError({
          providerKind: DAYTONA_KIND,
          operation: "exec",
          detail: "Daytona did not return a private preview token for this sandbox.",
        });
      }
      return {
        url,
        headers: { "x-daytona-preview-token": token },
      };
    }),
    startProcess: Effect.fn("DaytonaSandboxProvider.startProcess")(function* (
      input: SandboxProviderStartProcessInput,
    ) {
      const toolbox = yield* toolboxUrl(input.account, input.providerSandboxId);
      const sessionId = `t3-${NodeCrypto.randomUUID()}`;
      yield* request(
        input.account,
        "exec",
        "POST",
        toolboxPath(toolbox, input.providerSandboxId, "/process/session"),
        { body: { sessionId } },
      );
      const started = yield* request(
        input.account,
        "exec",
        "POST",
        toolboxPath(toolbox, input.providerSandboxId, `/process/session/${sessionId}/exec`),
        {
          body: {
            command: sessionCommand(input),
            runAsync: true,
            suppressInputEcho: true,
          },
        },
      );
      const commandId = readString(asRecord(started.body)["cmdId"]);
      if (commandId === undefined) {
        return yield* new SandboxProviderError({
          providerKind: DAYTONA_KIND,
          operation: "exec",
          detail: "Daytona did not return a command id for the sandbox process.",
        });
      }

      const commandUrl = toolboxPath(
        toolbox,
        input.providerSandboxId,
        `/process/session/${sessionId}/command/${commandId}`,
      );
      const logsUrl = `${commandUrl}/logs`;
      const inputUrl = `${commandUrl}/input`;
      const sessionUrl = toolboxPath(
        toolbox,
        input.providerSandboxId,
        `/process/session/${sessionId}`,
      );
      let stdoutOffset = 0;
      let stderrOffset = 0;
      let followOffset = 0;
      let followSeen = 0;
      let followClosed = false;

      const emit = (next: string, offset: number, write: (data: string) => void): number => {
        if (next.length > offset) {
          write(next.slice(offset));
          return next.length;
        }
        if (next.length < offset) {
          // The provider rotated its log page; replay it rather than silently dropping output.
          write(next);
          return next.length;
        }
        return offset;
      };

      const readLogs = Effect.gen(function* () {
        const response = yield* request(input.account, "exec", "GET", logsUrl, {
          headers: { "x-daytona-sdk-version": DAYTONA_SDK_VERSION },
          // Log text can itself look like JSON (the host's ready line), so keep it as text.
          parseJson: false,
        });
        const logs = logsFromBody(response.body);
        if (logs.combined !== undefined) {
          stdoutOffset = emit(logs.combined, stdoutOffset, input.onStdout);
        } else {
          if (logs.stdout !== undefined) {
            stdoutOffset = emit(logs.stdout, stdoutOffset, input.onStdout);
          }
          if (logs.stderr !== undefined) {
            stderrOffset = emit(logs.stderr, stderrOffset, input.onStderr);
          }
        }
      }).pipe(Effect.ignore);

      const deleteSession = request(input.account, "exec", "DELETE", sessionUrl).pipe(
        Effect.catchIf(
          (error) => error.statusCode === 404,
          () => Effect.void,
        ),
        Effect.asVoid,
      );

      const followLogs = client.followLogs;
      const startFollow = (): Effect.Effect<DaytonaLogFollow | null> => {
        if (followLogs === undefined) return Effect.succeed(null);
        followClosed = false;
        followSeen = 0;
        return Effect.tryPromise({
          try: () =>
            followLogs({
              url: logsUrl,
              apiKey: input.account.apiKey,
              onData: (data) => {
                // A reconnect replays the retained log from the start. Skip the prefix this
                // process already emitted and forward only the new suffix.
                const start = Math.max(0, followOffset - followSeen);
                followSeen += data.length;
                if (start >= data.length) return;
                const next = data.slice(start);
                followOffset += next.length;
                input.onStdout(next);
              },
              onClose: () => {
                followClosed = true;
              },
            }),
          catch: (cause) => providerError(input.account, "exec", cause),
        }).pipe(Effect.orElseSucceed(() => null));
      };
      let follow = yield* startFollow();

      const waitForFollowClose = Effect.gen(function* () {
        for (let attempt = 0; attempt < 40; attempt += 1) {
          if (followClosed) return;
          yield* Effect.sleep("50 millis");
        }
      });

      const process: SandboxProviderProcess = {
        pid: 0,
        sendStdin: (data) =>
          request(input.account, "exec", "POST", inputUrl, {
            body: { data },
          }).pipe(Effect.asVoid),
        kill: deleteSession,
        wait: Effect.gen(function* () {
          let delayMs = 250;
          let reconnectAttempts = 0;
          while (true) {
            const command = yield* request(input.account, "exec", "GET", commandUrl).pipe(
              Effect.catchIf(
                (error) => error.statusCode === 404,
                () => Effect.succeed({ status: 404, body: undefined }),
              ),
            );
            const exitCode = readNumber(asRecord(command.body)["exitCode"]);
            if (exitCode !== undefined) {
              if (follow !== null && followClosed) {
                // The stream dropped before the process finished; read the remaining tail.
                stdoutOffset = Math.max(stdoutOffset, followOffset);
                yield* readLogs;
                follow.close();
              } else if (follow !== null) {
                // The follow stream delivers the final frames before it closes.
                yield* waitForFollowClose;
                follow.close();
              } else {
                yield* readLogs;
              }
              yield* deleteSession;
              return { exitCode };
            }
            if (command.status === 404) {
              follow?.close();
              yield* deleteSession;
              return { exitCode: 1 };
            }
            if (follow !== null && followClosed) {
              follow.close();
              follow = null;
              // Keep the HTTP fallback from replaying output the follow stream already sent.
              stdoutOffset = Math.max(stdoutOffset, followOffset);
              reconnectAttempts += 1;
              if (reconnectAttempts <= MAX_FOLLOW_RECONNECTS) {
                yield* Effect.sleep(`${delayMs} millis`);
                follow = yield* startFollow();
                delayMs = follow === null ? Math.min(delayMs * 2, 2_000) : 250;
              }
            }
            if (follow === null) {
              yield* readLogs;
            }
            yield* Effect.sleep(`${delayMs} millis`);
            delayMs = Math.min(delayMs * 2, 2_000);
          }
        }),
      };
      return process;
    }),
  };
};
