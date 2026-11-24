import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import { repositoryPathFromRemoteUrl } from "@t3tools/source-control-core/server/SourceControlProvider";

import * as ServerConfig from "../config.ts";
import { sourceControlDefinitions } from "../sourceControl/sourceControlDefinitions.ts";
import * as SourceControlProviderRegistry from "../sourceControl/SourceControlProviderRegistry.ts";

/** A transient credential for one sandbox Git command; it is never stored in the sandbox. */
export interface CloudRepositoryCredential {
  readonly username: string;
  readonly token: string;
  readonly url: string;
}

/**
 * Resolves the connected source-control account's credential for a remote. A remote with no
 * connected provider or credential is cloned without one, which works for public repositories.
 */
export class CloudRepositoryCredentialResolver extends Context.Service<
  CloudRepositoryCredentialResolver,
  {
    readonly resolve: (
      repositoryUrl: string,
    ) => Effect.Effect<Option.Option<CloudRepositoryCredential>>;
  }
>()("t3/sandbox/CloudRepositoryCredential/CloudRepositoryCredentialResolver") {}

const make = Effect.gen(function* () {
  const config = yield* ServerConfig.ServerConfig;
  const providers = yield* SourceControlProviderRegistry.SourceControlProviderRegistry;

  const resolve: CloudRepositoryCredentialResolver["Service"]["resolve"] = (repositoryUrl) => {
    const provider = sourceControlDefinitions.recognizeRemote(repositoryUrl);
    if (provider === null) return Effect.succeedNone;
    return Effect.gen(function* () {
      const handle = yield* providers.resolveHandle({
        cwd: config.cwd,
        context: { provider, remoteName: "origin", remoteUrl: repositoryUrl },
      });
      const credential = handle.provider.repositoryCredential;
      if (credential === undefined) return Option.none<CloudRepositoryCredential>();
      const repository = repositoryPathFromRemoteUrl(repositoryUrl);
      const resolved = yield* credential({
        cwd: config.cwd,
        ...(handle.context === null ? {} : { context: handle.context }),
        ...(repository === null ? {} : { repository }),
      });
      return Option.some({
        username: resolved.username,
        token: resolved.token,
        url: repositoryUrl,
      });
    }).pipe(
      // A public repository needs no credential; a private one reports the clone's own failure.
      Effect.catchCause((cause) =>
        Effect.logWarning("Cloud repository credential unavailable", {
          cause: String(cause),
        }).pipe(Effect.as(Option.none<CloudRepositoryCredential>())),
      ),
    );
  };

  return CloudRepositoryCredentialResolver.of({ resolve });
});

export const layer = Layer.effect(CloudRepositoryCredentialResolver, make);
