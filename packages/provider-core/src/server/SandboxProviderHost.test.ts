import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import * as SandboxProviderHost from "./SandboxProviderHost.ts";

const makePaths = (root: string) => ({
  cwd: root,
  baseDir: `${root}/.t3/host`,
  stateDir: `${root}/.t3/host/state`,
  providerStatusCacheDir: `${root}/.t3/host/cache`,
  attachmentsDir: `${root}/.t3/host/attachments`,
});

describe("sandbox provider host attachments", () => {
  it.effect("finds an attachment stored with its original extension", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fileSystem.makeTempDirectory({ prefix: "t3-sandbox-host-" });
      const paths = makePaths(root);
      yield* fileSystem.makeDirectory(paths.attachmentsDir, { recursive: true });
      yield* fileSystem.writeFileString(`${paths.attachmentsDir}/image-1.png`, "png");
      const host = yield* SandboxProviderHost.make({
        paths,
        settings: {} as never,
      });
      expect(host.host.resolveAttachmentPath({ id: "image-1", type: "image" } as never)).toBe(
        path.join(paths.attachmentsDir, "image-1.png"),
      );
      expect(
        host.host.resolveAttachmentPath({ id: "../escape", type: "file" } as never),
      ).toBeNull();
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});
