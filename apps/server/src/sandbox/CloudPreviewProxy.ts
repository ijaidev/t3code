// @effect-diagnostics nodeBuiltinImport:off - a byte-stream preview proxy needs Node's HTTP transports.
import type { DiscoveredLocalServer, SandboxTestSandbox, ThreadId } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as NodeHttp from "node:http";
import * as NodeHttps from "node:https";
import type * as NodeStream from "node:stream";
import { WebSocket, WebSocketServer } from "ws";

import * as SandboxAccounts from "./SandboxAccounts.ts";
import type { SandboxProviderDriver } from "./SandboxProvider.ts";
import * as SandboxStore from "./SandboxStore.ts";

const PORT_ACCESS_TTL_MS = 60_000;
const UPSTREAM_IDLE_TIMEOUT_MS = 60_000;
const HOP_BY_HOP_HEADERS = new Set([
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
]);
const STRIPPED_REQUEST_HEADERS = new Set([
  ...HOP_BY_HOP_HEADERS,
  "cookie",
  "authorization",
  "host",
  "proxy-connection",
]);

interface ResolvedSandbox {
  readonly sandbox: SandboxTestSandbox;
  readonly account: SandboxAccounts.ResolvedSandboxAccount["account"];
  readonly driver: SandboxProviderDriver;
}

interface PortAccess {
  readonly url: string;
  readonly headers: Readonly<Record<string, string>>;
  readonly expiresAt: number;
}

interface ProxyEntry {
  readonly key: string;
  readonly threadId: ThreadId;
  readonly port: number;
  readonly resolved: ResolvedSandbox;
  readonly server: NodeHttp.Server;
  readonly webSocketServer: WebSocketServer;
  url: string;
  access: PortAccess | null;
}

export class CloudPreviewProxyError extends Schema.TaggedError<
  CloudPreviewProxyError,
  { readonly brand: unique symbol }
>()("CloudPreviewProxyError", {
  detail: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message(): string {
    return this.detail;
  }
}

export class CloudPreviewProxy extends Context.Service<
  CloudPreviewProxy,
  {
    /** Starts or reuses a loopback proxy for one sandbox port. */
    readonly ensure: (input: {
      readonly threadId: ThreadId;
      readonly port: number;
    }) => Effect.Effect<Option.Option<DiscoveredLocalServer>>;
    readonly release: (input: {
      readonly threadId: ThreadId;
      readonly port: number;
    }) => Effect.Effect<void>;
    readonly releaseThread: (threadId: ThreadId) => Effect.Effect<void>;
  }
>()("t3/sandbox/CloudPreviewProxy") {}

const entryKey = (threadId: ThreadId, port: number) => `${threadId}\u0000${port}`;

const forwardedRequestHeaders = (
  request: NodeHttp.IncomingMessage,
  entry: { readonly port: number },
  access: PortAccess,
): NodeHttp.OutgoingHttpHeaders => {
  const headers: NodeHttp.OutgoingHttpHeaders = {};
  const localOrigin = `http://localhost:${request.socket.localPort ?? 0}`;
  const sandboxOrigin = `http://localhost:${entry.port}`;
  const rewriteOrigin = (value: string) =>
    value.startsWith(localOrigin) ? `${sandboxOrigin}${value.slice(localOrigin.length)}` : value;
  for (const [name, value] of Object.entries(request.headers)) {
    if (value === undefined || STRIPPED_REQUEST_HEADERS.has(name)) continue;
    if ((name === "origin" || name === "referer") && typeof value === "string") {
      headers[name] = rewriteOrigin(value);
      continue;
    }
    headers[name] = value;
  }
  headers["x-forwarded-proto"] = "http";
  headers["x-forwarded-host"] = request.headers.host ?? localOrigin;
  return { ...headers, ...access.headers };
};

const responseHeaders = (
  headers: NodeHttp.IncomingHttpHeaders,
  entry: { readonly port: number; readonly url: string },
  upstream: URL,
): NodeHttp.OutgoingHttpHeaders => {
  const out: NodeHttp.OutgoingHttpHeaders = {};
  const localOrigin = new URL(entry.url).origin;
  const upstreamOrigin = upstream.origin;
  const sandboxOrigin = `http://localhost:${entry.port}`;
  const rewrite = (value: string) => {
    try {
      const location = new URL(value, upstream);
      if (
        location.origin === upstreamOrigin ||
        location.origin === sandboxOrigin ||
        (location.hostname === "localhost" && Number(location.port) === entry.port)
      ) {
        return `${localOrigin}${location.pathname}${location.search}${location.hash}`;
      }
    } catch {}
    return value;
  };
  for (const [name, value] of Object.entries(headers)) {
    if (value === undefined || HOP_BY_HOP_HEADERS.has(name)) continue;
    out[name] = name === "location" && typeof value === "string" ? rewrite(value) : value;
  }
  return out;
};

const webSocketProtocols = (header: string | undefined): ReadonlyArray<string> =>
  header === undefined
    ? []
    : header
        .split(",")
        .map((value) => value.trim())
        .filter((value) => value.length > 0);

const writeError = (response: NodeHttp.ServerResponse, status: number, detail: string) => {
  if (response.headersSent) return;
  response.writeHead(status, { "content-type": "text/plain; charset=utf-8" });
  response.end(detail);
};

const proxyHttpRequest = (
  entry: ProxyEntry,
  access: PortAccess,
  request: NodeHttp.IncomingMessage,
  response: NodeHttp.ServerResponse,
) =>
  new Promise<void>((resolve) => {
    const upstream = new URL(access.url);
    const transport = upstream.protocol === "https:" ? NodeHttps : NodeHttp;
    const target = new URL(
      `${upstream.pathname.replace(/\/$/, "")}${request.url ?? "/"}`,
      upstream,
    );
    const outgoing = transport.request(
      {
        protocol: target.protocol,
        hostname: target.hostname,
        port: target.port === "" ? undefined : target.port,
        method: request.method,
        path: `${target.pathname}${target.search}`,
        headers: forwardedRequestHeaders(request, entry, access),
      },
      (incoming) => {
        response.writeHead(
          incoming.statusCode ?? 502,
          responseHeaders(incoming.headers, entry, upstream),
        );
        incoming.pipe(response);
        incoming.on("end", resolve);
        incoming.on("error", () => {
          response.destroy();
          resolve();
        });
      },
    );
    outgoing.setTimeout(UPSTREAM_IDLE_TIMEOUT_MS, () =>
      outgoing.destroy(new Error("The sandbox preview request timed out.")),
    );
    outgoing.on("error", (error) => {
      writeError(response, 502, `The sandbox preview could not be reached: ${error.message}`);
      resolve();
    });
    response.on("close", () => outgoing.destroy());
    request.pipe(outgoing);
  });

const proxyWebSocket = (
  entry: ProxyEntry,
  access: PortAccess,
  request: NodeHttp.IncomingMessage,
  socket: NodeStream.Duplex,
  head: Buffer,
) => {
  const upstream = new URL(access.url);
  const target = new URL(`${upstream.pathname.replace(/\/$/, "")}${request.url ?? "/"}`, upstream);
  target.protocol = upstream.protocol === "https:" ? "wss:" : "ws:";
  const protocols = webSocketProtocols(request.headers["sec-websocket-protocol"]);
  const headers = forwardedRequestHeaders(request, entry, access);
  delete headers["sec-websocket-key"];
  delete headers["sec-websocket-version"];
  delete headers["sec-websocket-extensions"];
  delete headers["sec-websocket-protocol"];
  const upstreamSocket = new WebSocket(
    target,
    protocols.length === 0 ? undefined : [...protocols],
    { headers },
  );
  upstreamSocket.once("error", () => socket.destroy());
  upstreamSocket.once("open", () => {
    entry.webSocketServer.handleUpgrade(request, socket, head, (client) => {
      client.on("message", (data, isBinary) => upstreamSocket.send(data, { binary: isBinary }));
      upstreamSocket.on("message", (data, isBinary) => client.send(data, { binary: isBinary }));
      client.on("close", () => upstreamSocket.close());
      upstreamSocket.on("close", () => client.close());
      client.on("error", () => upstreamSocket.terminate());
      upstreamSocket.on("error", () => client.terminate());
    });
  });
};

/** @public */
export const make = Effect.gen(function* CloudPreviewProxyMake() {
  const store = yield* SandboxStore.SandboxStore;
  const accounts = yield* SandboxAccounts.SandboxAccounts;
  const runPromise = Effect.runPromiseWith(yield* Effect.context<never>());
  const entries = yield* Ref.make<ReadonlyMap<string, ProxyEntry>>(new Map());
  const lock = yield* Semaphore.make(1);

  const closeEntry = (entry: ProxyEntry) =>
    Effect.sync(() => {
      entry.webSocketServer.close();
      entry.server.close();
      entry.server.closeAllConnections();
    });

  const resolveSandbox = Effect.fn("CloudPreviewProxy.resolveSandbox")(function* (
    threadId: ThreadId,
  ) {
    const sandboxes = yield* store.list.pipe(Effect.orElseSucceed(() => []));
    const sandbox = sandboxes.find((candidate) => candidate.threadId === threadId);
    if (
      sandbox === undefined ||
      sandbox.providerSandboxId === null ||
      sandbox.status === "deleting" ||
      sandbox.status === "provisioning"
    ) {
      return Option.none<ResolvedSandbox>();
    }
    const resolved = yield* accounts.resolve(sandbox.accountId).pipe(Effect.option);
    if (Option.isNone(resolved)) return Option.none<ResolvedSandbox>();
    if (
      !resolved.value.driver.capabilities.privatePorts ||
      resolved.value.driver.portAccess === undefined
    ) {
      return Option.none<ResolvedSandbox>();
    }
    return Option.some({
      sandbox,
      account: resolved.value.account,
      driver: resolved.value.driver,
    });
  });

  const readAccess = Effect.fn("CloudPreviewProxy.readAccess")(function* (
    entry: ProxyEntry,
    force: boolean,
  ) {
    const now = yield* Clock.currentTimeMillis;
    if (!force && entry.access !== null && entry.access.expiresAt > now) return entry.access;
    const portAccess = entry.resolved.driver.portAccess;
    if (portAccess === undefined) {
      return yield* new CloudPreviewProxyError({
        detail: "This sandbox provider cannot preview ports.",
      });
    }
    const access = yield* portAccess({
      account: entry.resolved.account,
      providerSandboxId: entry.resolved.sandbox.providerSandboxId!,
      port: entry.port,
    });
    const next: PortAccess = {
      url: access.url,
      headers: access.headers,
      expiresAt: (yield* Clock.currentTimeMillis) + PORT_ACCESS_TTL_MS,
    };
    entry.access = next;
    return next;
  });

  const handleRequest = (
    entry: ProxyEntry,
    request: NodeHttp.IncomingMessage,
    response: NodeHttp.ServerResponse,
  ) => {
    void runPromise(
      Effect.gen(function* () {
        const access = yield* readAccess(entry, false).pipe(
          Effect.catch(() => readAccess(entry, true)),
        );
        yield* Effect.promise(() => proxyHttpRequest(entry, access, request, response));
      }).pipe(
        Effect.catch((error) => {
          const message = error instanceof Error ? error.message : String(error);
          writeError(response, 502, message);
          return Effect.void;
        }),
      ),
    );
  };

  const handleUpgrade = (
    entry: ProxyEntry,
    request: NodeHttp.IncomingMessage,
    socket: NodeStream.Duplex,
    head: Buffer,
  ) => {
    void runPromise(
      Effect.gen(function* () {
        const access = yield* readAccess(entry, false).pipe(
          Effect.catch(() => readAccess(entry, true)),
        );
        yield* Effect.sync(() => proxyWebSocket(entry, access, request, socket, head));
      }).pipe(
        Effect.catch((error) => {
          const message = error instanceof Error ? error.message : String(error);
          socket.end(`HTTP/1.1 502 Bad Gateway\r\ncontent-type: text/plain\r\n\r\n${message}`);
          return Effect.void;
        }),
      ),
    );
  };

  const listen = (entryWithoutServer: Omit<ProxyEntry, "server" | "webSocketServer" | "url">) =>
    Effect.callback<ProxyEntry, CloudPreviewProxyError>((resume) => {
      const webSocketServer = new WebSocketServer({ noServer: true });
      const server = NodeHttp.createServer();
      const entry: ProxyEntry = { ...entryWithoutServer, server, webSocketServer, url: "" };
      server.on("request", (request, response) => handleRequest(entry, request, response));
      server.on("upgrade", (request, socket, head) => handleUpgrade(entry, request, socket, head));
      const failed = (cause: unknown) => {
        webSocketServer.close();
        server.close();
        resume(
          Effect.fail(
            new CloudPreviewProxyError({
              detail: "The sandbox preview listener failed.",
              cause,
            }),
          ),
        );
      };
      server.once("error", failed);
      server.listen(0, "127.0.0.1", () => {
        server.off("error", failed);
        const address = server.address();
        if (address === null || typeof address === "string") {
          failed("The sandbox preview listener has no port.");
          return;
        }
        entry.url = `http://localhost:${address.port}`;
        resume(Effect.succeed(entry));
      });
      return Effect.sync(() => {
        webSocketServer.close();
        server.close();
        server.closeAllConnections();
      });
    });

  const ensure: CloudPreviewProxy["Service"]["ensure"] = Effect.fn("CloudPreviewProxy.ensure")(
    function* (input) {
      return yield* lock.withPermits(1)(
        Effect.gen(function* () {
          const key = entryKey(input.threadId, input.port);
          const current = yield* Ref.get(entries);
          const existing = current.get(key);
          if (existing !== undefined) {
            return Option.some({
              host: "localhost",
              port: input.port,
              url: existing.url,
              processName: null,
              pid: null,
              terminal: null,
              agent: { threadId: input.threadId },
              sandboxPort: true,
            } satisfies DiscoveredLocalServer);
          }
          const resolved = yield* resolveSandbox(input.threadId);
          if (Option.isNone(resolved)) return Option.none();
          const created = yield* listen({
            key,
            threadId: input.threadId,
            port: input.port,
            resolved: resolved.value,
            access: null,
          }).pipe(
            Effect.catch((error) =>
              Effect.logWarning("Cloud preview listener failed", { cause: error }).pipe(
                Effect.as(null),
              ),
            ),
          );
          if (created === null) return Option.none();
          yield* Ref.update(entries, (all) => new Map(all).set(key, created));
          return Option.some({
            host: "localhost",
            port: input.port,
            url: created.url,
            processName: null,
            pid: null,
            terminal: null,
            agent: { threadId: input.threadId },
            sandboxPort: true,
          } satisfies DiscoveredLocalServer);
        }),
      );
    },
  );

  const release: CloudPreviewProxy["Service"]["release"] = (input) =>
    lock.withPermits(1)(
      Effect.gen(function* () {
        const key = entryKey(input.threadId, input.port);
        const current = yield* Ref.get(entries);
        const entry = current.get(key);
        if (entry === undefined) return;
        yield* Ref.update(entries, (all) => {
          const next = new Map(all);
          next.delete(key);
          return next;
        });
        yield* closeEntry(entry);
      }),
    );

  const releaseThread: CloudPreviewProxy["Service"]["releaseThread"] = (threadId) =>
    lock.withPermits(1)(
      Effect.gen(function* () {
        const current = yield* Ref.get(entries);
        const matching = [...current.values()].filter((entry) => entry.threadId === threadId);
        if (matching.length === 0) return;
        yield* Ref.update(entries, (all) => {
          const next = new Map(all);
          for (const entry of matching) next.delete(entry.key);
          return next;
        });
        yield* Effect.forEach(matching, closeEntry, { discard: true });
      }),
    );

  return CloudPreviewProxy.of({ ensure, release, releaseThread });
});

export const layer = Layer.effect(CloudPreviewProxy, make);
