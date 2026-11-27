import type { ServerSettings } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Stream from "effect/Stream";

import { noCompatibilityPolicy, type ProviderCompatibilityLookup } from "./compatibility.ts";
import * as ProviderHost from "./ProviderHost.ts";

export interface SandboxProviderHostOptions {
  readonly paths: ProviderHost.ProviderHostPaths;
  /**
   * Sent by the controlling environment over the authenticated host connection.
   * It is the sandbox's only settings source, so a provider never reads the
   * controller's user data directory.
   */
  readonly settings: ServerSettings;
  readonly compatibility?: ProviderCompatibilityLookup;
  readonly shouldRunBackgroundWork?: boolean;
}

export interface SandboxProviderHost {
  readonly host: ProviderHost.ProviderHost["Service"];
  /** Drops every provider credential the host holds. Run when the sandbox stops. */
  readonly clearCredentials: Effect.Effect<void>;
}

const credentialKey = (namespace: string, bindingId: string) => `${namespace}:${bindingId}`;

/** A ProviderHost for a provider process that runs beside the workspace in a sandbox. */
export const make = Effect.fn("SandboxProviderHost.make")(function* (
  options: SandboxProviderHostOptions,
): Effect.fn.Return<SandboxProviderHost, never, FileSystem.FileSystem | Path.Path> {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  for (const directory of [
    options.paths.baseDir,
    options.paths.stateDir,
    options.paths.providerStatusCacheDir,
    options.paths.attachmentsDir,
  ]) {
    yield* fileSystem.makeDirectory(directory, { recursive: true }).pipe(Effect.orDie);
  }

  const credentials = new Map<string, Uint8Array>();
  const attachmentsRoot = path.resolve(options.paths.attachmentsDir);
  const settings = options.settings;

  const host = ProviderHost.ProviderHost.of({
    compatibility: options.compatibility ?? noCompatibilityPolicy,
    paths: options.paths,
    settings: {
      get: Effect.succeed(settings),
      withSnapshot: (use) => use(settings),
      changes: Stream.empty,
      subscribe: Effect.succeed(Stream.empty),
    },
    shouldRunBackgroundWork: Effect.fn("ProviderHost.shouldRunBackgroundWork")(function () {
      return Effect.succeed(options.shouldRunBackgroundWork ?? true);
    }),
    resolveAttachmentPath: (attachment) => {
      const candidate = path.resolve(attachmentsRoot, attachment.id);
      const relative = path.relative(attachmentsRoot, candidate);
      if (relative === "" || relative.startsWith("..") || path.isAbsolute(relative)) {
        return null;
      }
      return candidate;
    },
    credentials: Effect.fn("ProviderHost.credentials")(function (namespace, bindingId) {
      const key = credentialKey(namespace, bindingId);
      return Effect.succeed({
        binding: { owner: "t3", key },
        get: Effect.sync(() => Option.fromUndefinedOr(credentials.get(key))),
        set: (value: Uint8Array) =>
          Effect.sync(() => {
            credentials.set(key, value);
          }),
        remove: Effect.sync(() => {
          credentials.delete(key);
        }),
      });
    }),
  });

  return {
    host,
    clearCredentials: Effect.sync(() => {
      credentials.clear();
    }),
  };
});
