import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";

import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as FileSystem from "effect/FileSystem";

import * as ServerConfig from "../config.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as GitManager from "./GitManager.ts";
import * as GitWorkflowService from "./GitWorkflowService.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as VcsDriverRegistry from "../vcs/VcsDriverRegistry.ts";
import * as CloudThreadGit from "../sandbox/CloudThreadGit.ts";

it.layer(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))("GitWorkflowService", (it) => {
  it.effect("finds a name among the refs of every namespace", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const cwd = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-ref-named-" });
      const workflow = yield* GitWorkflowService.GitWorkflowService;
      const driver = yield* GitVcsDriver.GitVcsDriver;
      for (const args of [
        ["init", "-b", "main"],
        ["-c", "user.email=t@t", "-c", "user.name=t", "commit", "--allow-empty", "-m", "init"],
        ["update-ref", "refs/remotes/upstream/feature/topic", "HEAD"],
        ["symbolic-ref", "refs/remotes/origin/alias", "refs/heads/main"],
        ["update-ref", "refs/mirror/origin/mirrored", "HEAD"],
      ]) {
        yield* driver.execute({ operation: "test.setupRepo", cwd, args });
      }
      const found = yield* Effect.forEach(
        ["main", "feature/topic", "alias", "mirrored", "t3/renamed", "feature/*"],
        (refName) => workflow.hasRefNamed({ cwd, refName }),
      );
      assert.deepEqual(found, [true, true, true, true, false, false]);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        GitWorkflowService.layer.pipe(
          Layer.provide(
            Layer.mock(VcsDriverRegistry.VcsDriverRegistry)({
              resolve: () => Effect.succeed({ kind: "git" } as VcsDriverRegistry.VcsDriverHandle),
            }),
          ),
          Layer.provide(Layer.mock(GitManager.GitManager)({})),
          Layer.provideMerge(GitVcsDriver.layer),
          Layer.provideMerge(VcsProcess.layer),
          Layer.provideMerge(
            ServerConfig.layerTest(process.cwd(), { prefix: "t3-ref-named-" }).pipe(
              Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
            ),
          ),
          Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
        ),
      ),
    ),
  );

  it.effect("reads no HEAD path from a removed worktree without spawning Git", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const parent = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-removed-wt-" });
      const removed = `${parent}/removed`;
      const workflow = yield* GitWorkflowService.GitWorkflowService;
      assert.strictEqual(yield* workflow.headPath(removed), null);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        GitWorkflowService.layer.pipe(
          Layer.provide(Layer.mock(VcsDriverRegistry.VcsDriverRegistry)({})),
          Layer.provide(Layer.mock(GitManager.GitManager)({})),
          Layer.provide(
            Layer.mock(GitVcsDriver.GitVcsDriver)({
              execute: (input) => Effect.die(`spawned Git in ${input.cwd}`),
            }),
          ),
        ),
      ),
    ),
  );
});

it.effect("serializes cloud branch-changing actions per workspace", () => {
  const cwd = "/home/t3/.t3/threads/thread-cloud-lock";
  return Effect.gen(function* () {
    const active = yield* Ref.make(0);
    const maxActive = yield* Ref.make(0);
    const run = () =>
      Effect.gen(function* () {
        const current = yield* Ref.updateAndGet(active, (value) => value + 1);
        yield* Ref.update(maxActive, (value) => Math.max(value, current));
        yield* Effect.yieldNow;
        yield* Ref.update(active, (value) => value - 1);
        return {} as never;
      });
    const cloud = Layer.succeed(CloudThreadGit.CloudThreadGit, {
      forCwd: () => Effect.succeedSome({ threadId: "thread-cloud-lock" }),
      runStackedAction: run,
      createRef: run,
      switchRef: run,
    } as unknown as CloudThreadGit.CloudThreadGit["Service"]);
    const layer = GitWorkflowService.layer.pipe(
      Layer.provide(Layer.mock(VcsDriverRegistry.VcsDriverRegistry)({})),
      Layer.provide(Layer.mock(GitManager.GitManager)({})),
      Layer.provide(cloud),
      Layer.provideMerge(GitVcsDriver.layer),
      Layer.provideMerge(VcsProcess.layer),
      Layer.provideMerge(NodeCrypto.layer),
      Layer.provide(
        ServerConfig.layerTest(process.cwd(), { prefix: "t3-git-workflow-cloud-lock-" }),
      ),
      Layer.provide(NodeServices.layer),
    );
    return yield* Effect.gen(function* () {
      const workflow = yield* GitWorkflowService.GitWorkflowService;
      yield* Effect.all(
        [
          workflow.runStackedAction({ actionId: "a", cwd, action: "commit" } as never),
          workflow.runStackedAction({ actionId: "b", cwd, action: "commit" } as never),
        ],
        { concurrency: 2 },
      );
      assert.strictEqual(yield* Ref.get(maxActive), 1);
    }).pipe(Effect.provide(layer));
  });
});
