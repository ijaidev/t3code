import type {
  DiscoveredLocalServer,
  SandboxPreviewUnavailable,
  SandboxPreviewUnavailableReason,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Semaphore from "effect/Semaphore";
import type * as Scope from "effect/Scope";

import * as CloudPreviewProxy from "./CloudPreviewProxy.ts";

export interface CloudThreadPort {
  readonly port: number;
  readonly processName: string | null;
}

interface ThreadState {
  readonly threadId: ThreadId;
  readonly ports: ReadonlyArray<CloudThreadPort>;
  readonly servers: ReadonlyMap<number, DiscoveredLocalServer>;
  readonly reason: SandboxPreviewUnavailableReason | null;
}

interface State {
  readonly threads: ReadonlyMap<string, ThreadState>;
}

export interface CloudThreadPortsSnapshot {
  readonly servers: ReadonlyArray<DiscoveredLocalServer>;
  readonly unavailable: ReadonlyArray<SandboxPreviewUnavailable>;
}

const emptyState: State = { threads: new Map() };

const threadKey = (threadId: ThreadId) => `${threadId}`;

const decodePorts = (input: ReadonlyArray<unknown>): ReadonlyArray<CloudThreadPort> => {
  const seen = new Set<number>();
  const ports: CloudThreadPort[] = [];
  for (const value of input) {
    if (typeof value !== "object" || value === null) continue;
    const record = value as Readonly<Record<string, unknown>>;
    const port = Number(record["port"]);
    if (!Number.isInteger(port) || port <= 0 || port >= 65_536) continue;
    if (seen.has(port)) continue;
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

const settled = (thread: ThreadState) =>
  thread.reason === "provider-no-private-ports" ||
  thread.ports.length === 0 ||
  thread.ports.every((port) => thread.servers.has(port.port));

export class CloudThreadPorts extends Context.Service<
  CloudThreadPorts,
  {
    /** Replaces the ports a thread's sandbox host reported; empty clears them. */
    readonly setPorts: (threadId: ThreadId, ports: ReadonlyArray<unknown>) => Effect.Effect<void>;
    readonly clear: (threadId: ThreadId) => Effect.Effect<void>;
    readonly snapshot: Effect.Effect<CloudThreadPortsSnapshot>;
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
    const key = threadKey(threadId);
    const current = yield* Ref.get(state);
    const previous = current.threads.get(key);
    if (previous !== undefined && samePorts(previous.ports, ports) && settled(previous)) {
      return false;
    }

    const nextServers = new Map(previous?.servers ?? []);
    for (const port of nextServers.keys()) {
      if (ports.some((reported) => reported.port === port)) continue;
      nextServers.delete(port);
      yield* proxy.release({ threadId, port });
    }
    let reason: SandboxPreviewUnavailableReason | null = null;
    for (const port of ports) {
      if (nextServers.has(port.port)) continue;
      const ensured = yield* proxy.ensure({ threadId, port: port.port });
      if (ensured._tag === "ready") {
        nextServers.set(port.port, ensured.server);
      } else {
        reason = ensured.reason;
      }
    }
    const next: ThreadState = { threadId, ports, servers: nextServers, reason };
    yield* Ref.set(state, { threads: new Map(current.threads).set(key, next) });
    return true;
  });

  const retryMissing = Effect.fn("CloudThreadPorts.retryMissing")(function* () {
    const current = yield* Ref.get(state);
    for (const thread of current.threads.values()) {
      if (settled(thread)) continue;
      yield* reconcile(thread.threadId, thread.ports);
    }
  });

  const setPorts: CloudThreadPorts["Service"]["setPorts"] = Effect.fn("CloudThreadPorts.setPorts")(
    function* (threadId, ports) {
      const reserved = new Set(yield* proxy.reservedPorts(threadId));
      const reported = decodePorts(ports).filter((port) => !reserved.has(port.port));
      const changed = yield* lock.withPermits(1)(reconcile(threadId, reported));
      if (changed) yield* notify;
    },
  );

  const clear: CloudThreadPorts["Service"]["clear"] = Effect.fn("CloudThreadPorts.clear")(
    function* (threadId) {
      const changed = yield* lock.withPermits(1)(reconcile(threadId, []));
      if (changed) yield* notify;
    },
  );

  const snapshot: CloudThreadPorts["Service"]["snapshot"] = Effect.gen(function* () {
    yield* lock.withPermits(1)(retryMissing());
    const current = yield* Ref.get(state);
    return {
      servers: [...current.threads.values()]
        .flatMap((thread) => [...thread.servers.values()])
        .toSorted(
          (left, right) =>
            left.port - right.port ||
            (left.agent?.threadId ?? "").localeCompare(right.agent?.threadId ?? ""),
        ),
      unavailable: [...current.threads.values()].flatMap((thread) =>
        thread.reason === null || thread.ports.length === 0
          ? []
          : [{ threadId: thread.threadId, reason: thread.reason }],
      ),
    } satisfies CloudThreadPortsSnapshot;
  }).pipe(Effect.withSpan("CloudThreadPorts.snapshot"));

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
