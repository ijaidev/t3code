import {
  DEFAULT_SERVER_SETTINGS,
  ProviderDriverKind,
  ProviderInstanceId,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";

import { createProviderAdapterHostAdapter } from "./ProviderAdapterHostAdapters.ts";

const claudeDriver = ProviderDriverKind.make("claudeAgent");

describe("sandbox provider adapter host adapters", () => {
  it.effect("builds the real Claude adapter from sandbox paths", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const root = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-claude-host-" });
      const paths = {
        cwd: `${root}/workspace`,
        baseDir: `${root}/host`,
        stateDir: `${root}/host/state`,
        providerStatusCacheDir: `${root}/host/cache`,
        attachmentsDir: `${root}/attachments`,
      };

      const created = yield* createProviderAdapterHostAdapter({
        driver: claudeDriver,
        instanceId: ProviderInstanceId.make("claude-agent"),
        displayName: "Claude",
        accentColor: undefined,
        config: { binaryPath: `${root}/bin/claude`, homePath: `${root}/home` },
        settings: DEFAULT_SERVER_SETTINGS,
        paths,
      });

      expect(created.adapter.driver).toBe(claudeDriver);
      expect(created.adapter.instanceId).toBe(ProviderInstanceId.make("claude-agent"));
      expect(yield* created.onStop).toBeUndefined();
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("refuses a driver the host cannot run", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const root = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-codex-host-" });
      const error = yield* createProviderAdapterHostAdapter({
        driver: ProviderDriverKind.make("codex"),
        instanceId: ProviderInstanceId.make("codex"),
        displayName: undefined,
        accentColor: undefined,
        config: {},
        settings: DEFAULT_SERVER_SETTINGS,
        paths: {
          cwd: root,
          baseDir: `${root}/host`,
          stateDir: `${root}/host/state`,
          providerStatusCacheDir: `${root}/host/cache`,
          attachmentsDir: `${root}/attachments`,
        },
      }).pipe(Effect.flip);

      expect(error.code).toBe("unsupported_driver");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
