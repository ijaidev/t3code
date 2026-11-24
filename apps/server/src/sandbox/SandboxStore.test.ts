import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";

import * as ServerConfig from "../config.ts";
import * as SandboxStore from "./SandboxStore.ts";

it.effect("refuses to start from an unreadable state file and leaves it untouched", () =>
  Effect.gen(function* () {
    const config = yield* ServerConfig.ServerConfig;
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const statePath = path.join(config.stateDir, "sandboxes.json");
    const corrupt = "{not-json";
    yield* fileSystem.writeFileString(statePath, corrupt);

    const built = yield* Effect.exit(Layer.build(SandboxStore.layer));

    expect(Exit.isFailure(built)).toBe(true);
    expect(yield* fileSystem.readFileString(statePath)).toBe(corrupt);
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        ServerConfig.layerTest(process.cwd(), { prefix: "t3-sandbox-store-test-" }).pipe(
          Layer.provide(NodeServices.layer),
        ),
        NodeServices.layer,
      ),
    ),
  ),
);
