// @effect-diagnostics globalFetch:off globalFetchInEffect:off - the test drives the loopback proxy exactly as a browser does.
// @effect-diagnostics nodeBuiltinImport:off - the test stands up a real upstream HTTP server.
import {
  SandboxInstanceId,
  SandboxProviderAccountId,
  SandboxProviderKind,
  type SandboxProviderCapabilities,
  type SandboxTestSandbox,
  ThreadId,
} from "@t3tools/contracts";
import * as NodeHttp from "node:http";
import * as NodeNet from "node:net";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SecureRandom from "effect/crypto/SecureRandom";
import { WebSocket, WebSocketServer } from "ws";

import * as SandboxAccounts from "./SandboxAccounts.ts";
import type { SandboxProviderDriver } from "./SandboxProvider.ts";
import * as SandboxStore from "./SandboxStore.ts";
import * as CloudPreviewProxy from "./CloudPreviewProxy.ts";

const E2B = SandboxProviderKind.make("e2b");
const accountId = SandboxProviderAccountId.make("e2b-work");
const threadId = ThreadId.make("thread-cloud-preview");
const sandboxId = SandboxInstanceId.make("sandbox-cloud-preview");

const capabilities: SandboxProviderCapabilities = {
  create: true,
  exec: true,
  pauseResume: true,
  delete: true,
  snapshots: true,
  privatePorts: true,
  sizes: [{ id: "small", label: "Small", cpuCount: 2, memoryMB: 1024 }],
  regions: [{ id: "us", label: "United States" }],
};

const sandbox: SandboxTestSandbox = {
  id: sandboxId,
  accountId,
  providerKind: E2B,
  providerSandboxId: "e2b-sandbox-1",
  threadId,
  status: "running",
  template: "base",
  createdAt: "2026-11-24T00:00:00.000Z",
  updatedAt: "2026-11-24T00:00:00.000Z",
};

const storeLayer = Layer.succeed(
  SandboxStore.SandboxStore,
  SandboxStore.SandboxStore.of({
    path: "/tmp/sandboxes.json",
    list: Effect.succeed([sandbox]),
    get: () => Effect.succeedSome(sandbox),
    upsert: () => Effect.void,
    remove: () => Effect.void,
  }),
);

const fakeDriver = (portAccess: SandboxProviderDriver["portAccess"]) => {
  const driver = {
    kind: E2B,
    label: "E2B",
    capabilities,
    validateCredential: () => Effect.void,
    create: () => Effect.die("unused"),
    findCreated: () => Effect.succeed([]),
    get: () => Effect.succeed({ status: "running" as const }),
    pause: () => Effect.void,
    resume: () => Effect.void,
    delete: () => Effect.void,
    exec: () => Effect.die("unused"),
  } satisfies SandboxProviderDriver;
  return portAccess === undefined ? driver : { ...driver, portAccess };
};

const accountsLayer = (driver: SandboxProviderDriver) =>
  Layer.succeed(
    SandboxAccounts.SandboxAccounts,
    SandboxAccounts.SandboxAccounts.of({
      list: Effect.die("unused"),
      connect: () => Effect.die("unused"),
      test: () => Effect.die("unused"),
      disconnect: () => Effect.die("unused"),
      resolve: () =>
        Effect.succeed({
          account: {
            id: accountId,
            kind: E2B,
            label: "E2B",
            apiKey: "e2b_secret_key",
            template: "base",
          },
          driver,
          size: null,
          region: null,
        }),
    }),
  );

const testLayer = (driver: SandboxProviderDriver) =>
  CloudPreviewProxy.layer.pipe(
    Layer.provide(storeLayer),
    Layer.provide(accountsLayer(driver)),
    Layer.provide(
      Layer.succeed(
        SecureRandom.SecureRandom,
        SecureRandom.make((size) => new Uint8Array(size).fill(7)),
      ),
    ),
  );

const startUpstream = async (
  handler: (request: NodeHttp.IncomingMessage, response: NodeHttp.ServerResponse) => void,
): Promise<{ readonly url: string; readonly close: () => Promise<void> }> => {
  const server = NodeHttp.createServer(handler);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("no upstream port");
  return {
    url: `http://127.0.0.1:${address.port}`,
    close: () =>
      new Promise<void>((resolve) => {
        server.closeAllConnections();
        server.close(() => resolve());
      }),
  };
};

const bootstrapPreview = async (
  serverUrl: string,
): Promise<{ readonly origin: string; readonly cookie: string; readonly response: Response }> => {
  const response = await fetch(serverUrl, { redirect: "manual" });
  const cookie = response.headers.getSetCookie()[0]?.split(";")[0] ?? "";
  const origin = new URL(serverUrl).origin;
  return { origin, cookie, response };
};

const rawRequest = (port: number, target: string, upgrade = false): Promise<string> =>
  new Promise((resolve) => {
    const socket = NodeNet.connect(port, "127.0.0.1");
    let data = "";
    let settled = false;
    const finish = () => {
      if (settled) return;
      settled = true;
      socket.destroy();
      resolve(data);
    };
    socket.setEncoding("utf8");
    socket.setTimeout(2_000, finish);
    socket.on("data", (chunk) => {
      data += chunk;
    });
    socket.on("close", finish);
    socket.on("error", finish);
    socket.write(
      `GET ${target} HTTP/1.1\r\nHost: localhost\r\nConnection: ${upgrade ? "Upgrade" : "close"}\r\n${
        upgrade
          ? "Upgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n"
          : ""
      }\r\n`,
    );
  });

const authorizedFetch = (
  origin: string,
  path: string,
  cookie: string,
  init: RequestInit = {},
): Promise<Response> => {
  const headers = new Headers(init.headers);
  headers.set("cookie", cookie);
  headers.set("origin", origin);
  return fetch(`${origin}${path}`, { ...init, headers });
};

describe("cloud preview proxy", () => {
  it.effect("requires the preview ticket and keeps provider details out of the client", () => {
    const seen: Array<{
      readonly url: string | undefined;
      readonly token: string | undefined;
      readonly cookie: string | undefined;
      readonly origin: string | undefined;
    }> = [];
    let upstreamUrl = "";
    const driver = fakeDriver(() =>
      Effect.succeed({
        url: upstreamUrl,
        headers: { "x-sandbox-token": "traffic-token" },
      }),
    );
    return Effect.gen(function* () {
      const remote = yield* Effect.acquireRelease(
        Effect.promise(() =>
          startUpstream((request, response) => {
            seen.push({
              url: request.url,
              token: request.headers["x-sandbox-token"] as string | undefined,
              cookie: request.headers.cookie,
              origin: request.headers.origin,
            });
            if (request.url === "/redirect") {
              response.writeHead(302, { location: "http://localhost/next" });
              response.end();
              return;
            }
            response.writeHead(200, { "content-type": "text/plain" });
            response.end("sandbox page");
          }),
        ),
        (server) => Effect.promise(() => server.close()),
      );
      upstreamUrl = remote.url;
      const proxy = yield* CloudPreviewProxy.CloudPreviewProxy;
      const ensured = yield* proxy.ensure({ threadId, port: 5173 });
      expect(ensured._tag).toBe("ready");
      if (ensured._tag !== "ready") throw new Error("preview proxy was not created");
      const entry = ensured.server;
      expect(entry.sandboxPort).toBe(true);
      expect(entry.agent?.threadId).toBe(threadId);
      expect(entry.url).not.toContain("127.0.0.1");
      expect(entry.url).not.toContain("e2b");
      expect(entry.url).toContain("localhost");
      expect(entry.url).toContain("t3-preview=");

      const localPort = Number(new URL(entry.url).port);
      const malformedTarget = yield* Effect.promise(() => rawRequest(localPort, "http://["));
      expect(malformedTarget === "" || malformedTarget.startsWith("HTTP/1.1 400")).toBe(true);
      const malformedPort = yield* Effect.promise(() =>
        rawRequest(localPort, "http://localhost:999999/"),
      );
      expect(malformedPort === "" || malformedPort.startsWith("HTTP/1.1 400")).toBe(true);
      const malformedUpgrade = yield* Effect.promise(() => rawRequest(localPort, "http://[", true));
      expect(malformedUpgrade === "" || malformedUpgrade.startsWith("HTTP/1.1 400")).toBe(true);

      const boot = yield* Effect.promise(() => bootstrapPreview(entry.url));
      expect(boot.response.status).toBe(302);
      expect(boot.cookie.length).toBeGreaterThan(0);
      expect(boot.response.headers.get("location")).toBe("/");

      const denied = yield* Effect.promise(() => fetch(`${boot.origin}/hello`));
      expect(denied.status).toBe(401);
      expect(seen).toEqual([]);

      const response = yield* Effect.promise(() =>
        authorizedFetch(boot.origin, "/hello?x=1", boot.cookie),
      );
      expect(response.status).toBe(200);
      expect(yield* Effect.promise(() => response.text())).toBe("sandbox page");
      expect(seen[0]?.url).toBe("/hello?x=1");
      expect(seen[0]?.token).toBe("traffic-token");
      expect(seen[0]?.cookie).toBeUndefined();
      expect(seen[0]?.origin).toBe("http://localhost:5173");

      const redirect = yield* Effect.promise(() =>
        authorizedFetch(boot.origin, "/redirect", boot.cookie, { redirect: "manual" }),
      );
      expect(redirect.status).toBe(302);
      expect(redirect.headers.get("location")).toBe(`${boot.origin}/next`);

      yield* proxy.release({ threadId, port: 5173 });
      const refused = yield* Effect.promise(() =>
        fetch(`${boot.origin}/hello`).then(
          () => false,
          () => true,
        ),
      );
      expect(refused).toBe(true);
    }).pipe(Effect.provide(testLayer(driver)));
  });

  it.effect("returns a generic 502 without the provider host", () => {
    const driver = fakeDriver(() => Effect.succeed({ url: "http://127.0.0.1:1", headers: {} }));
    return Effect.gen(function* () {
      const proxy = yield* CloudPreviewProxy.CloudPreviewProxy;
      const ensured = yield* proxy.ensure({ threadId, port: 5173 });
      if (ensured._tag !== "ready") throw new Error("preview proxy was not created");
      const boot = yield* Effect.promise(() => bootstrapPreview(ensured.server.url));
      const response = yield* Effect.promise(() => authorizedFetch(boot.origin, "/", boot.cookie));
      expect(response.status).toBe(502);
      const body = yield* Effect.promise(() => response.text());
      expect(body).toBe("The sandbox preview is unavailable.");
      expect(body).not.toContain("127.0.0.1");
    }).pipe(Effect.provide(testLayer(driver)));
  });

  it.effect("carries and closes preview WebSockets with the preview", () => {
    let upstreamUrl = "";
    let token: string | undefined;
    const driver = fakeDriver(() =>
      Effect.succeed({
        url: upstreamUrl,
        headers: { "x-sandbox-token": "traffic-token" },
      }),
    );
    return Effect.gen(function* () {
      const upstream = yield* Effect.acquireRelease(
        Effect.promise(
          () =>
            new Promise<WebSocketServer>((resolve) => {
              const server: WebSocketServer = new WebSocketServer({
                host: "127.0.0.1",
                port: 0,
              });
              server.on("listening", () => resolve(server));
              server.on("connection", (socket, request) => {
                token = request.headers["x-sandbox-token"] as string | undefined;
                socket.on("message", (data, isBinary) => socket.send(data, { binary: isBinary }));
              });
            }),
        ),
        (server) =>
          Effect.promise(() => new Promise<void>((resolve) => server.close(() => resolve()))),
      );
      const address = upstream.address();
      if (address === null || typeof address === "string") throw new Error("no upstream port");
      upstreamUrl = `http://127.0.0.1:${address.port}`;
      const proxy = yield* CloudPreviewProxy.CloudPreviewProxy;
      const ensured = yield* proxy.ensure({ threadId, port: 5173 });
      if (ensured._tag !== "ready") throw new Error("preview proxy was not created");
      const boot = yield* Effect.promise(() => bootstrapPreview(ensured.server.url));
      const local = new URL(boot.origin);
      local.hostname = "127.0.0.1";
      const socket = new WebSocket(local.href.replace("http:", "ws:") + "ws", {
        headers: { cookie: boot.cookie },
      });
      const echoed = yield* Effect.promise(
        () =>
          new Promise<string>((resolve, reject) => {
            socket.once("open", () => socket.send("hello"));
            socket.once("message", (data) => resolve(String(data)));
            socket.once("error", reject);
          }),
      );
      expect(echoed).toBe("hello");
      expect(token).toBe("traffic-token");
      const closed = new Promise<void>((resolve) => socket.once("close", () => resolve()));
      yield* proxy.release({ threadId, port: 5173 });
      yield* Effect.promise(() => closed);
      expect(socket.readyState).toBe(WebSocket.CLOSED);
    }).pipe(Effect.provide(testLayer(driver)));
  });

  it.effect("hides ports when the provider has no private port endpoint", () =>
    Effect.gen(function* () {
      const proxy = yield* CloudPreviewProxy.CloudPreviewProxy;
      const ensured = yield* proxy.ensure({ threadId, port: 5173 });
      expect(ensured).toEqual({ _tag: "unavailable", reason: "provider-no-private-ports" });
    }).pipe(Effect.provide(testLayer(fakeDriver(undefined)))),
  );
});
