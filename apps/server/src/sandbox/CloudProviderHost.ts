// @effect-diagnostics nodeBuiltinImport:off -- SEA exposes the embedded host bundle through node:sea.
import * as NodeSea from "node:sea";

import { DEFAULT_SERVER_SETTINGS, type ServerSettings } from "@t3tools/contracts";
import type { ProviderAdapterHostPaths } from "@t3tools/provider-core/server/ProviderAdapterHostProtocol";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";

/**
 * Pinned with the server build. The sandbox installs these exact versions, so a host
 * never runs an adapter or OpenCode release the controller was not built against.
 */
export const CLOUD_PROVIDER_OPENCODE_VERSION = "2.4.2";
// E2B's base image ships Node 20; the server CLI requires ^22.16.
export const CLOUD_PROVIDER_NODE_VERSION = "22.16.0";

const HOST_DIR = ".t3/provider-host";
const RUNTIME_DIR = "runtime";

const shellQuote = (value: string) => `'${value.replaceAll("'", `'\\''`)}'`;

export const cloudProviderHostDir = (homePath: string) => `${homePath}/${HOST_DIR}`;

export const cloudProviderHostRuntimeDir = (homePath: string) =>
  `${cloudProviderHostDir(homePath)}/${RUNTIME_DIR}`;

export const cloudProviderHostPaths = (
  homePath: string,
  workspacePath: string,
): ProviderAdapterHostPaths => ({
  cwd: workspacePath,
  baseDir: cloudProviderHostDir(homePath),
  stateDir: `${cloudProviderHostDir(homePath)}/state`,
  providerStatusCacheDir: `${cloudProviderHostDir(homePath)}/cache`,
  // Matches the local layout so resolveAttachmentPath finds staged files by id.
  attachmentsDir: `${homePath}/.t3/attachments`,
});

const versionMarker = (version: string) => version.replaceAll(/[^a-zA-Z0-9._-]/g, "_");

/**
 * Installs the pinned Node and OpenCode runtimes into a private prefix instead of
 * the system installation, so the sandbox cannot substitute another version.
 */
export const cloudProviderHostInstallCommand = (input: {
  readonly hostDir: string;
  readonly opencodeVersion: string;
  readonly nodeVersion: string;
}) => {
  const prefix = `${input.hostDir}/${RUNTIME_DIR}`;
  const marker = `${prefix}/.installed-${versionMarker(input.nodeVersion)}-${versionMarker(input.opencodeVersion)}`;
  const npmFlags = '--global --prefix "$prefix" --no-audit --no-fund --loglevel=error';
  return [
    "set -eu",
    `prefix=${shellQuote(prefix)}`,
    `marker=${shellQuote(marker)}`,
    `if [ ! -f "$marker" ]; then`,
    `  mkdir -p "$prefix"`,
    `  npm install ${npmFlags} ${shellQuote(`node@${input.nodeVersion}`)}`,
    `  npm install ${npmFlags} ${shellQuote(`@opencode/cli@${input.opencodeVersion}`)}`,
    `  : > "$marker"`,
    "fi",
    `test -x "$prefix/bin/node"`,
    `test -x "$prefix/bin/opencode"`,
  ].join("\n");
};

/**
 * A stale host from a previous controller process may still own the port. Stop only
 * the process named by our own pid file, then start one replacement.
 */
export const cloudProviderHostStartCommand = (input: {
  readonly hostDir: string;
  readonly bundlePath: string;
}) => {
  const prefix = `${input.hostDir}/${RUNTIME_DIR}`;
  const pidFile = `${input.hostDir}/host.pid`;
  return [
    "set -eu",
    `host_dir=${shellQuote(input.hostDir)}`,
    `mkdir -p "$host_dir"`,
    `pid_file=${shellQuote(pidFile)}`,
    `if [ -f "$pid_file" ]; then`,
    `  pid="$(cat "$pid_file" 2>/dev/null || true)"`,
    `  case "$pid" in`,
    `    ''|*[!0-9]*) ;;`,
    `    *) if kill -0 "$pid" 2>/dev/null && tr '\\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null | grep -q '__provider-adapter-host'; then`,
    `         kill "$pid" 2>/dev/null || true; sleep 1`,
    `       fi ;;`,
    `  esac`,
    `  rm -f "$pid_file"`,
    "fi",
    `echo $$ > "$pid_file"`,
    `exec ${shellQuote(`${prefix}/bin/node`)} ${shellQuote(input.bundlePath)} __provider-adapter-host`,
  ].join("\n");
};

export const cloudProviderHostEnvironment = (input: {
  readonly token: string;
  readonly port: number;
  readonly build: string;
  readonly runtimeDir: string;
  readonly environment: ReadonlyArray<{ readonly name: string; readonly value: string }>;
}): Readonly<Record<string, string>> => {
  const environment: Record<string, string> = {};
  for (const variable of input.environment) {
    // Provider environment must not be able to take over the host control channel.
    if (variable.name.startsWith("T3_PROVIDER_HOST_")) continue;
    environment[variable.name] = variable.value;
  }
  return {
    ...environment,
    PATH: `${input.runtimeDir}/bin:${environment["PATH"] ?? "/usr/local/bin:/usr/bin:/bin"}`,
    T3_PROVIDER_HOST_TOKEN: input.token,
    T3_PROVIDER_HOST_PORT: String(input.port),
    T3_PROVIDER_HOST_BIND: "0.0.0.0",
    T3_PROVIDER_HOST_BUILD: input.build,
  };
};

export interface CloudProviderHostListening {
  readonly port: number;
}

export const parseCloudProviderHostListening = (
  line: string,
): CloudProviderHostListening | null => {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    return null;
  }
  if (typeof parsed !== "object" || parsed === null) return null;
  const record = parsed as Record<string, unknown>;
  if (record["type"] !== "t3-provider-adapter-host" || record["state"] !== "listening") {
    return null;
  }
  const port = record["port"];
  return typeof port === "number" && Number.isInteger(port) && port > 0 ? { port } : null;
};

/** E2B hands back an HTTPS origin; the adapter host speaks WebSocket on the same route. */
export const providerHostWebSocketUrl = (accessUrl: string): string => {
  const url = new URL(accessUrl);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url.toString();
};

/**
 * The host owns the OpenCode process. A user-configured external server or local
 * binary path does not exist in the sandbox, so the controller binds both to the
 * installed runtime.
 */
export const cloudOpenCodeHostConfig = (config: unknown, runtimeDir: string): unknown => {
  const base =
    typeof config === "object" && config !== null && !Array.isArray(config)
      ? (config as Readonly<Record<string, unknown>>)
      : {};
  return {
    ...base,
    binaryPath: `${runtimeDir}/bin/opencode`,
    serverUrl: "",
    serverPassword: "",
  };
};

/**
 * The host only needs non-secret server policy. Provider config travels separately,
 * and the sandbox account, speech, and secret-provider stores stay on the controller.
 */
export const cloudProviderHostSettings = (settings: ServerSettings): ServerSettings => ({
  ...DEFAULT_SERVER_SETTINGS,
  enableProviderUpdateChecks: settings.enableProviderUpdateChecks,
  backgroundActivity: settings.backgroundActivity,
  backgroundActivityProfile: settings.backgroundActivityProfile,
  enableAgentBrowserAccess: settings.enableAgentBrowserAccess,
  enableAgentDeviceAccess: settings.enableAgentDeviceAccess,
});

/** Host stderr is untrusted and may carry a credential; keep the public detail short. */
export const providerHostFailureDetail = (line: string | undefined): string => {
  if (line === undefined || line.trim() === "") {
    return "The sandbox provider host stopped before it was ready.";
  }
  return line
    .trim()
    .slice(0, 400)
    .replaceAll(/\b(sk-[A-Za-z0-9_-]+|e2b_[A-Za-z0-9_-]+|Bearer\s+[A-Za-z0-9._-]+)/g, "[redacted]");
};

const PROVIDER_HOST_BUNDLE_NAME = "provider-adapter-host.mjs";

/**
 * The bundle is built beside the server and uploaded into the sandbox; it is not
 * the npm `t3` package, which can lag this build.
 */
export const readProviderAdapterHostBundle = Effect.fn("CloudProviderHost.readBundle")(function* (
  fileSystem: FileSystem.FileSystem,
  path: Path.Path,
) {
  if (NodeSea.isSea()) {
    return yield* Effect.try({
      try: () => new Uint8Array(NodeSea.getRawAsset(PROVIDER_HOST_BUNDLE_NAME)),
      catch: () => null,
    }).pipe(Effect.orElseSucceed(() => null));
  }
  const override = Option.getOrUndefined(
    yield* Config.option(Config.String("T3CODE_PROVIDER_HOST_BUNDLE")).pipe(
      Effect.orElseSucceed(() => Option.none()),
    ),
  )?.trim();
  const candidates = [
    ...(override === undefined || override === "" ? [] : [override]),
    path.join(import.meta.dirname, PROVIDER_HOST_BUNDLE_NAME),
    path.join(import.meta.dirname, "../", PROVIDER_HOST_BUNDLE_NAME),
    path.join(import.meta.dirname, "../../dist", PROVIDER_HOST_BUNDLE_NAME),
  ];
  for (const candidate of candidates) {
    const bytes = yield* fileSystem
      .readFile(candidate)
      .pipe(Effect.catchCause(() => Effect.succeed(null)));
    if (bytes !== null && bytes.byteLength > 0) return bytes;
  }
  return null;
});
