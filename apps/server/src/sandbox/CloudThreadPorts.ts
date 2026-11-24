import type { DiscoveredLocalServer, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Semaphore from "effect/Semaphore";
import type * as Scope from "effect/Scope";

import * as CloudPreviewProxy from "./CloudPreviewProxy.ts";

const INTERNAL_PORTS = new Set([49983, 50005]);

export interface CloudThreadPort {
  readonly port: number;
  readonly processName: string | null;
}

interface State {
  readonly ports: ReadonlyMap<string, ReadonlyArray<CloudThreadPort>>;
  readonly servers: ReadonlyMap<string, DiscoveredLocalServer>;
}

const emptyState: State = { ports: new Map(), servers: new Map() };

const threadKey = (threadId: ThreadId) => `${threadId}`;

const decodePorts = (input: ReadonlyArray<unknown>): ReadonlyArray<CloudThreadPort> => {
  const seen = new Set<number>();
  const ports: CloudThreadPort[] = [];
  for (const value of input) {
    if (typeof value !== "object" || value === null) continue;
    const record = value as Readonly<Record<string, unknown>>;
    const port = Number(record["port"]);
    if (!Number.isInteger(port) || port <= 0 || port >= 65_536) continue;
    if (INTERNAL_PORTS.has(port) || seen.has(port)) continue;
    seen.add(port);
    ports.push({
      port,
      processName:
        typeof record["processName"] === "string" && record["processName"].trim() !== ""
          ? record["processName"]
          : null,
    });
  }
  return ports.toSorted((left, right) => left.port - right.port);
};

const samePorts = (left: ReadonlyArray<CloudThreadPort>, right: ReadonlyArray<CloudThreadPort>) =>
  left.length === right.length &&
  left.every(
    (port, index) =>
      port.port === right[index]?.port && port.processName === right[index]?.processName,
  );

export class CloudThreadPorts extends Context.Service<
  CloudThreadPorts,
  {
    /** Replaces the ports a thread's sandbox host reported; empty clears them. */
    readonly setPorts: (threadId: ThreadId, ports: ReadonlyArray<unknown>) => Effect.Effect<void>;
    readonly clear: (threadId: ThreadId) => Effect.Effect<void>;
    readonly snapshot: Effect.Effect<ReadonlyArray<DiscoveredLocalServer>>;
    readonly subscribe: (
      listener: () => Effect.Effect<void>,
    ) => Effect.Effect<void, never, Scope.Scope>;
  }
>()("t3/sandbox/CloudThreadPorts") {}

const make = Effect.gen(function* CloudThreadPortsMake() {
  const proxy = yield* CloudPreviewProxy.CloudPreviewProxy;
  const state = yield* Ref.make<State>(emptyState);
  const listeners = yield* Ref.make<ReadonlySet<() => Effect.Effect<void>>>(new Set());
  const lock = yield* Semaphore.make(1);

  const notify = Effect.gen(function* () {
    const active = yield* Ref.get(listeners);
    yield* Effect.forEach(active, (listener) => listener(), { discard: true });
  });

  const reconcile = Effect.fn("CloudThreadPorts.reconcile")(function* (
    threadId: ThreadId,
    ports: ReadonlyArray<CloudThreadPort>,
  ) {
    const current = yield* Ref.get(state);
    const previous = current.ports.get(threadKey(threadId)) ?? [];
    if (samePorts(previous, ports) && current.ports.has(threadKey(threadId))) return;
    const previousByPort = new Map(previous.map((port) => [port.port, port]));
    const nextServers = new Map(current.servers);
    for (const previousPort of previousByPort.values()) {
      if (ports.some((port) => port.port === previousPort.port)) continue;
      const key = `${threadKey(threadId)}\u0000${previousPort.port}`;
      nextServers.delete(key);
      yield* proxy.release({ threadId, port: previousPort.port });
    }
    for (const port of ports) {
      const key = `${threadKey(threadId)}\u0000${port.port}`;
      const ensured = yield* proxy.ensure({ threadId, port: port.port });
      if (ensured._tag === "Some") {
        nextServers.set(key, ensured.value);
      } else {
        nextServers.delete(key);
      }
    }
    yield* Ref.set(state, {
      ports: new Map(current.ports).set(threadKey(threadId), ports),
      servers: nextServers,
    });
    yield* notify;
  });

  const setPorts: CloudThreadPorts["Service"]["setPorts"] = Effect.fn("CloudThreadPorts.setPorts")(
    function* (threadId, ports) {
      yield* lock.withPermits(1)(reconcile(threadId, decodePorts(ports)));
    },
  );

  const clear: CloudThreadPorts["Service"]["clear"] = Effect.fn("CloudThreadPorts.clear")(
    function* (threadId) {
      yield* lock.withPermits(1)(reconcile(threadId, []));
    },
  );

  const snapshot: CloudThreadPorts["Service"]["snapshot"] = Ref.get(state).pipe(
    Effect.map((current) =>
      [...current.servers.values()].toSorted(
        (left, right) =>
          left.port - right.port ||
          (left.agent?.threadId ?? "").localeCompare(right.agent?.threadId ?? ""),
      ),
    ),
    Effect.withSpan("CloudThreadPorts.snapshot"),
  );

  const subscribe: CloudThreadPorts["Service"]["subscribe"] = (listener) =>
    Effect.acquireRelease(
      Ref.update(listeners, (current) => new Set(current).add(listener)),
      () =>
        Ref.update(listeners, (current) => {
          const next = new Set(current);
          next.delete(listener);
          return next;
        }),
    );

  return CloudThreadPorts.of({ setPorts, clear, snapshot, subscribe });
});

export const layer = Layer.effect(CloudThreadPorts, make);
