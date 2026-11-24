import {
  SandboxInstanceId,
  SandboxTestSandbox,
  type SandboxTestSandbox as SandboxTestSandboxRecord,
} from "@t3tools/contracts";
import { writeFileStringAtomically } from "@t3tools/shared/atomicWrite";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";

import * as ServerConfig from "../config.ts";

const SandboxStoreState = Schema.Struct({
  version: Schema.Literal(1),
  sandboxes: Schema.Record(SandboxInstanceId, SandboxTestSandbox),
});
type SandboxStoreState = typeof SandboxStoreState.Type;

const decodeStore = Schema.decodeUnknownEffect(Schema.fromJsonString(SandboxStoreState));
const encodeStore = Schema.encodeSync(Schema.fromJsonString(SandboxStoreState));

export class SandboxStoreError extends Schema.TaggedError<
  SandboxStoreError,
  { readonly brand: unique symbol }
>()("SandboxStoreError", {
  operation: Schema.Literals(["read", "write"]),
  path: Schema.String,
  cause: Schema.Defect(),
}) {
  override get message(): string {
    return `Failed to ${this.operation} the sandbox state file.`;
  }
}

const emptyState: SandboxStoreState = { version: 1, sandboxes: {} };

export class SandboxStore extends Context.Service<
  SandboxStore,
  {
    readonly path: string;
    readonly list: Effect.Effect<ReadonlyArray<SandboxTestSandboxRecord>>;
    readonly get: (id: SandboxInstanceId) => Effect.Effect<Option.Option<SandboxTestSandboxRecord>>;
    readonly upsert: (sandbox: SandboxTestSandboxRecord) => Effect.Effect<void, SandboxStoreError>;
    readonly remove: (id: SandboxInstanceId) => Effect.Effect<void, SandboxStoreError>;
  }
>()("t3/sandbox/SandboxStore") {}

const make = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const { stateDir } = yield* ServerConfig.ServerConfig;
  const storePath = path.join(stateDir, "sandboxes.json");
  const lock = yield* Semaphore.make(1);

  const write = (state: SandboxStoreState) =>
    writeFileStringAtomically({
      filePath: storePath,
      contents: `${encodeStore(state)}\n`,
    }).pipe(
      Effect.provideService(FileSystem.FileSystem, fileSystem),
      Effect.provideService(Path.Path, path),
      Effect.mapError(
        (cause) => new SandboxStoreError({ operation: "write", path: storePath, cause }),
      ),
    );

  const load = Effect.gen(function* () {
    if (!(yield* fileSystem.exists(storePath))) return emptyState;
    const contents = yield* fileSystem
      .readFileString(storePath)
      .pipe(
        Effect.mapError(
          (cause) => new SandboxStoreError({ operation: "read", path: storePath, cause }),
        ),
      );
    // Never replace an unreadable file with empty state: it may be the only list of billable sandboxes.
    return yield* decodeStore(contents).pipe(
      Effect.mapError(
        (cause) => new SandboxStoreError({ operation: "read", path: storePath, cause }),
      ),
    );
  });

  const state = yield* Ref.make(yield* load);

  const list = Ref.get(state).pipe(Effect.map((current) => Object.values(current.sandboxes)));
  const get = (id: SandboxInstanceId) =>
    Ref.get(state).pipe(Effect.map((current) => Option.fromUndefinedOr(current.sandboxes[id])));

  const mutate = (
    update: (current: SandboxStoreState) => SandboxStoreState,
  ): Effect.Effect<void, SandboxStoreError> =>
    lock.withPermits(1)(
      Effect.gen(function* () {
        const next = update(yield* Ref.get(state));
        yield* write(next);
        yield* Ref.set(state, next);
      }),
    );

  const upsert = (sandbox: SandboxTestSandboxRecord) =>
    mutate((current) => ({
      version: 1,
      sandboxes: { ...current.sandboxes, [sandbox.id]: sandbox },
    }));

  const remove = (id: SandboxInstanceId) =>
    mutate((current) => {
      if (!Object.hasOwn(current.sandboxes, id)) return current;
      const { [id]: _removed, ...sandboxes } = current.sandboxes;
      return { version: 1, sandboxes };
    });

  return SandboxStore.of({ path: storePath, list, get, upsert, remove });
});

export const layer = Layer.effect(SandboxStore, make);
