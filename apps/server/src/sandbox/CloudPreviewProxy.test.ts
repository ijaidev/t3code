// @effect-diagnostics globalFetchInEffect:off - the test drives the loopback proxy exactly as a browser does.
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
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
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
        }),
    }),
  );

const testLayer = (driver: SandboxProviderDriver) =>
  CloudPreviewProxy.layer.pipe(Layer.provide(storeLayer), Layer.provide(accountsLayer(driver)));

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

describe("cloud preview proxy", () => {
  it.effect("proxies a sandbox port without exposing the provider URL or its token", () => {
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
              response.writeHead(302, { location: "http://localhost:5173/next" });
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
      expect(Option.isSome(ensured)).toBe(true);
      if (Option.isNone(ensured)) throw new Error("preview proxy was not created");
      const entry = ensured.value;
      expect(entry.sandboxPort).toBe(true);
      expect(entry.agent?.threadId).toBe(threadId);
      expect(entry.url).not.toContain("127.0.0.1");
      expect(entry.url).not.toContain("e2b");
      expect(entry.url).toContain("localhost");

      const local = new URL(entry.url);
      local.hostname = "127.0.0.1";
      const response = yield* Effect.promise(() =>
        fetch(`${local.origin}/hello?x=1`, {
          headers: { cookie: "t3_session=secret", origin: entry.url },
        }),
      );
      expect(response.status).toBe(200);
      expect(yield* Effect.promise(() => response.text())).toBe("sandbox page");
      expect(seen[0]?.url).toBe("/hello?x=1");
      expect(seen[0]?.token).toBe("traffic-token");
      expect(seen[0]?.cookie).toBeUndefined();
      expect(seen[0]?.origin).toBe("http://localhost:5173");

      const redirect = yield* Effect.promise(() =>
        fetch(`${local.origin}/redirect`, { redirect: "manual" }),
      );
      expect(redirect.status).toBe(302);
      expect(redirect.headers.get("location")).toBe(`${entry.url}/next`);

      yield* proxy.release({ threadId, port: 5173 });
      const refused = yield* Effect.promise(() =>
        fetch(`${local.origin}/hello`).then(
          () => false,
          () => true,
        ),
      );
      expect(refused).toBe(true);
    }).pipe(Effect.provide(testLayer(driver)));
  });

  it.effect("carries a preview WebSocket through the same private endpoint", () => {
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
      if (Option.isNone(ensured)) throw new Error("preview proxy was not created");
      const local = new URL(ensured.value.url);
      local.hostname = "127.0.0.1";
      const echo = yield* Effect.promise(
        () =>
          new Promise<string>((resolve, reject) => {
            const socket = new WebSocket(local);
            socket.once("open", () => socket.send("hello"));
            socket.once("message", (data) => {
              socket.close();
              resolve(String(data));
            });
            socket.once("error", reject);
          }),
      );
      expect(echo).toBe("hello");
      expect(token).toBe("traffic-token");
    }).pipe(Effect.provide(testLayer(driver)));
  });

  it.effect("hides ports when the provider has no private port endpoint", () =>
    Effect.gen(function* () {
      const proxy = yield* CloudPreviewProxy.CloudPreviewProxy;
      const ensured = yield* proxy.ensure({ threadId, port: 5173 });
      expect(Option.isNone(ensured)).toBe(true);
    }).pipe(Effect.provide(testLayer(fakeDriver(undefined)))),
  );
});
