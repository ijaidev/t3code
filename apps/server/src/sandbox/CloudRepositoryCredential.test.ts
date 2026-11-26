import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import type * as SourceControlProvider from "@t3tools/source-control-core/server/SourceControlProvider";

import * as ServerConfig from "../config.ts";
import * as SourceControlProviderRegistry from "../sourceControl/SourceControlProviderRegistry.ts";
import * as CloudRepositoryCredential from "./CloudRepositoryCredential.ts";

const provider = {
  repositoryCredential: (input: { readonly repository?: string }) =>
    Effect.succeed({
      username: "x-access-token",
      token: `token-for-${input.repository ?? "unknown"}`,
    }),
} as unknown as SourceControlProvider.SourceControlProvider["Service"];

const layer = CloudRepositoryCredential.layer.pipe(
  Layer.provideMerge(
    Layer.succeed(SourceControlProviderRegistry.SourceControlProviderRegistry, {
      resolveHandle: () =>
        Effect.succeed({
          provider,
          context: {
            provider: {
              kind: "github" as never,
              name: "GitHub",
              baseUrl: "https://github.com",
            },
            remoteName: "origin",
            remoteUrl: "https://github.com/acme/private.git",
          },
        }),
    } as unknown as SourceControlProviderRegistry.SourceControlProviderRegistry["Service"]),
  ),
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-cloud-credential-" })),
  Layer.provideMerge(NodeServices.layer),
);

describe("cloud repository credentials", () => {
  it.effect("scopes the credential to the remote's repository", () => {
    const scenario = Effect.gen(function* () {
      const resolver = yield* CloudRepositoryCredential.CloudRepositoryCredentialResolver;
      const credential = yield* resolver.resolve("https://github.com/acme/private.git");
      expect(Option.isSome(credential)).toBe(true);
      if (Option.isSome(credential)) {
        expect(credential.value).toEqual({
          username: "x-access-token",
          token: "token-for-acme/private",
          url: "https://github.com/acme/private.git",
        });
      }
    }).pipe(Effect.provide(layer));
    return scenario;
  });
});
