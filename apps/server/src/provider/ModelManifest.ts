import type { ProviderDriverKind } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import { HttpClient, HttpClientResponse } from "effect/http";

import { writeFileStringAtomically } from "@t3tools/shared/atomicWrite";
import {
  BUNDLED_MODEL_MANIFEST,
  decodeManifestCache,
  encodeManifestCache,
  ModelManifestSchema,
  resolveProviderCatalog,
  type ModelManifestData,
} from "@t3tools/provider-core/server/modelManifest";
import * as ModelCatalog from "@t3tools/provider-core/server/ModelCatalog";
import { ServerConfig } from "../config.ts";
import * as ServerSettings from "../serverSettings.ts";
import { resolveProviderCompatibility } from "./providerCompatibility.ts";

const decodeManifest = Schema.decodeUnknownEffect(ModelManifestSchema);

export {
  BUNDLED_MODEL_MANIFEST,
  encodeManifestCache,
  ModelManifestSchema,
  resolveProviderCatalog,
  type ModelManifestData,
};

const MODEL_MANIFEST_URL =
  "https://raw.githubusercontent.com/pingdotgg/t3code/main/apps/server/src/provider/model-manifest.json";

const MANIFEST_TTL_MS = 60 * 60 * 1000;

/** GitHub's raw CDN caches about this long, so a shorter window buys nothing. */
const MANIFEST_ON_DEMAND_TTL_MS = 5 * 60 * 1000;

/** Back off failed fetches so offline health checks do not each wait on the network. */
const MANIFEST_RETRY_MS = 5 * 60 * 1000;

const FETCH_TIMEOUT_MS = 10_000;

/** A manifest with no policy for this driver and release falls back to the bundled one. */
export const providerCompatibility = (
  manifest: ModelManifestData,
  driver: ProviderDriverKind,
  version: string | null,
) =>
  resolveProviderCompatibility(manifest.compatibility, driver, version) ??
  resolveProviderCompatibility(BUNDLED_MODEL_MANIFEST.compatibility, driver, version);

function manifestUpdatedAtMs(manifest: ModelManifestData): number {
  if (manifest.updatedAt === undefined) return 0;
  const parsed = Date.parse(manifest.updatedAt);
  return Number.isNaN(parsed) ? 0 : parsed;
}

export class ModelManifest extends Context.Service<
  ModelManifest,
  {
    /**  Snapshot classification reads this, so it never waits on the network. */
    readonly current: Effect.Effect<ModelManifestData>;
    readonly refresh: Effect.Effect<ModelManifestData>;
    readonly forceRefresh: Effect.Effect<ModelManifestData>;
    /** For a user looking at the model list: tolerates far less staleness than `refresh`. */
    readonly refreshOnDemand: Effect.Effect<ModelManifestData>;
    /** Process-shared fetches must survive the triggering instance's teardown. */
    readonly refreshInBackground: Effect.Effect<void>;
  }
>()("t3/provider/ModelManifest") {}

const BundledOnlyModelManifest: ModelManifest["Service"] = {
  current: Effect.succeed(BUNDLED_MODEL_MANIFEST),
  refresh: Effect.succeed(BUNDLED_MODEL_MANIFEST),
  forceRefresh: Effect.succeed(BUNDLED_MODEL_MANIFEST),
  refreshOnDemand: Effect.succeed(BUNDLED_MODEL_MANIFEST),
  refreshInBackground: Effect.void,
};

const layerBundledOnly = Layer.succeed(ModelManifest, BundledOnlyModelManifest);

export const make = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const config = yield* ServerConfig;
  const settingsService = yield* ServerSettings.ServerSettingsService;
  const httpClient = yield* HttpClient.HttpClient;
  const serviceScope = yield* Effect.scope;

  const cachePath = path.join(config.stateDir, "model-manifest.json");
  let manifest = BUNDLED_MODEL_MANIFEST;
  let fetchedAtMs: number | null = null;
  let lastAttemptMs: number | null = null;
  const refreshSemaphore = yield* Semaphore.make(1);

  // Only refresh takes the fetch semaphore; current must not wait on network I/O.
  const ensureDiskCacheLoaded = yield* Effect.cached(
    Effect.gen(function* () {
      const fromDisk = yield* fileSystem.readFileString(cachePath).pipe(
        Effect.flatMap((raw) => decodeManifestCache(raw)),
        Effect.catchCause(() => Effect.succeed(null)),
      );
      if (fromDisk === null) return;
      if (manifestUpdatedAtMs(BUNDLED_MODEL_MANIFEST) > manifestUpdatedAtMs(fromDisk.manifest)) {
        return;
      }
      manifest = fromDisk.manifest;
      fetchedAtMs = fromDisk.fetchedAtMs;
    }),
  );

  const refresh = Effect.fn("ModelManifest.refresh")(function* (
    force = false,
    ttlMs = MANIFEST_TTL_MS,
  ) {
    yield* ensureDiskCacheLoaded;
    const now = yield* Clock.currentTimeMillis;
    // Expire future timestamps after clock rollback; disk caches cross restarts, ruling out monotonic time.
    const isWithin = (sinceMs: number | null, windowMs: number) =>
      sinceMs !== null && now >= sinceMs && now - sinceMs < windowMs;
    if (!force && isWithin(fetchedAtMs, ttlMs)) return manifest;
    if (!force && isWithin(lastAttemptMs, MANIFEST_RETRY_MS)) return manifest;

    const settings = yield* settingsService.getSettings.pipe(
      Effect.catchCause(() => Effect.succeed(null)),
    );
    if (settings !== null && !settings.enableProviderUpdateChecks) return manifest;

    lastAttemptMs = now;
    const fetched = yield* httpClient.get(MODEL_MANIFEST_URL).pipe(
      Effect.flatMap(HttpClientResponse.filterStatusOk),
      Effect.flatMap((response) => response.json),
      Effect.flatMap((json) => decodeManifest(json)),
      Effect.timeout(FETCH_TIMEOUT_MS),
      Effect.catchCause(() => Effect.succeed(null)),
    );
    // Reject stale CDN edits just like stale disk caches to preserve bundled version gates.
    if (fetched === null || manifestUpdatedAtMs(fetched) < manifestUpdatedAtMs(manifest)) {
      return manifest;
    }

    manifest = fetched;
    fetchedAtMs = now;
    yield* encodeManifestCache({ fetchedAtMs: now, manifest: fetched }).pipe(
      Effect.flatMap((contents) => writeFileStringAtomically({ filePath: cachePath, contents })),
      Effect.provideService(FileSystem.FileSystem, fileSystem),
      Effect.provideService(Path.Path, path),
      Effect.ignoreCause,
    );
    return manifest;
  });

  const guardedRefresh = refreshSemaphore.withPermits(1)(refresh());

  return ModelManifest.of({
    current: ensureDiskCacheLoaded.pipe(Effect.map(() => manifest)),
    refresh: guardedRefresh,
    forceRefresh: refreshSemaphore.withPermits(1)(refresh(true)),
    refreshOnDemand: refreshSemaphore.withPermits(1)(refresh(false, MANIFEST_ON_DEMAND_TTL_MS)),
    refreshInBackground: Effect.forkIn(guardedRefresh, serviceScope).pipe(Effect.asVoid),
  });
});

export const layer = Layer.effect(ModelManifest, make);

export const layerTest = Layer.suspend(() =>
  layerModelCatalog.pipe(Layer.provideMerge(layerBundledOnly)),
);

export const layerModelCatalog = Layer.effect(
  ModelCatalog.ModelCatalog,
  Effect.gen(function* () {
    const manifest = yield* ModelManifest;
    return ModelCatalog.ModelCatalog.of({
      current: Effect.fn("ModelCatalog.current")(function* (driverKind: ProviderDriverKind) {
        const data = yield* manifest.current;
        const nowMs = yield* Clock.currentTimeMillis;
        return resolveProviderCatalog(data, driverKind, nowMs) ?? undefined;
      }),
      bundled: (driverKind) =>
        resolveProviderCatalog(BUNDLED_MODEL_MANIFEST, driverKind) ?? undefined,
      refreshInBackground: manifest.refreshInBackground,
    });
  }),
);
