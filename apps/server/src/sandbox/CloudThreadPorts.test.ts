import { type DiscoveredLocalServer, ThreadId } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";

import * as CloudPreviewProxy from "./CloudPreviewProxy.ts";
import * as CloudThreadPorts from "./CloudThreadPorts.ts";

const threadId = ThreadId.make("thread-cloud-ports");

interface ProxyState {
  readonly ensured: Ref.Ref<ReadonlyArray<number>>;
  readonly released: Ref.Ref<ReadonlyArray<number>>;
}

const proxyLayer = (state: ProxyState, supported = true) =>
  Layer.succeed(
    CloudPreviewProxy.CloudPreviewProxy,
    CloudPreviewProxy.CloudPreviewProxy.of({
      ensure: ({ threadId, port }) =>
        Effect.gen(function* () {
          yield* Ref.update(state.ensured, (ports) => [...ports, port]);
          return supported
            ? Option.some({
                host: "localhost",
                port,
                url: `http://localhost:${10_000 + port}`,
                processName: null,
                pid: null,
                terminal: null,
                agent: { threadId },
                sandboxPort: true as const,
              } satisfies DiscoveredLocalServer)
            : Option.none();
        }),
      release: ({ port }) => Ref.update(state.released, (ports) => [...ports, port]),
      releaseThread: () => Effect.void,
    }),
  );

const testLayer = (state: ProxyState, supported = true) =>
  CloudThreadPorts.layer.pipe(Layer.provide(proxyLayer(state, supported)));

describe("cloud thread ports", () => {
  it.effect("turns host-reported ports into previewable thread entries", () =>
    Effect.gen(function* () {
      const ensured = yield* Ref.make<ReadonlyArray<number>>([]);
      const released = yield* Ref.make<ReadonlyArray<number>>([]);
      const state = { ensured, released };
      return yield* Effect.gen(function* () {
        const ports = yield* CloudThreadPorts.CloudThreadPorts;
        yield* ports.setPorts(threadId, [
          { port: 5173 },
          { port: 49983 },
          { port: 3000, processName: "node" },
          { port: -1 },
        ]);
        const listed = yield* ports.snapshot;
        expect(listed.map((server) => server.port)).toEqual([3000, 5173]);
        expect(listed[0]?.sandboxPort).toBe(true);
        expect(listed[0]?.agent?.threadId).toBe(threadId);
        expect(listed[0]?.url).toBe("http://localhost:13000");
        expect(yield* Ref.get(ensured)).toEqual([3000, 5173]);

        yield* ports.setPorts(threadId, [{ port: 3000, processName: "node" }]);
        expect((yield* ports.snapshot).map((server) => server.port)).toEqual([3000]);
        expect(yield* Ref.get(released)).toEqual([5173]);

        yield* ports.clear(threadId);
        expect(yield* ports.snapshot).toEqual([]);
        expect(yield* Ref.get(released)).toEqual([5173, 3000]);
      }).pipe(Effect.provide(testLayer(state)));
    }),
  );

  it.effect("notifies subscribers when a thread's ports change", () =>
    Effect.gen(function* () {
      const state = {
        ensured: yield* Ref.make<ReadonlyArray<number>>([]),
        released: yield* Ref.make<ReadonlyArray<number>>([]),
      };
      return yield* Effect.gen(function* () {
        const ports = yield* CloudThreadPorts.CloudThreadPorts;
        let changes = 0;
        yield* ports.subscribe(() => Effect.sync(() => (changes += 1)));
        yield* ports.setPorts(threadId, [{ port: 8080 }]);
        expect(changes).toBe(1);
        yield* ports.setPorts(threadId, [{ port: 8080 }]);
        expect(changes).toBe(1);
      }).pipe(Effect.scoped, Effect.provide(testLayer(state)));
    }),
  );

  it.effect("omits ports the provider cannot preview privately", () =>
    Effect.gen(function* () {
      const state = {
        ensured: yield* Ref.make<ReadonlyArray<number>>([]),
        released: yield* Ref.make<ReadonlyArray<number>>([]),
      };
      return yield* Effect.gen(function* () {
        const ports = yield* CloudThreadPorts.CloudThreadPorts;
        yield* ports.setPorts(threadId, [{ port: 5173 }]);
        expect(yield* ports.snapshot).toEqual([]);
      }).pipe(Effect.provide(testLayer(state, false)));
    }),
  );
});
