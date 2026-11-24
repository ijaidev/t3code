import {
  IsoDateTime,
  SandboxInstanceId,
  SandboxProviderAccountId,
  SandboxProviderKind,
  ThreadId,
  TrimmedNonEmptyString,
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

/** What the controlling server remembers about one cloud thread after a restart. */
export const CloudThreadRecord = Schema.Struct({
  threadId: ThreadId,
  sandboxId: SandboxInstanceId,
  accountId: SandboxProviderAccountId,
  providerKind: SandboxProviderKind,
  /** The public URL the sandbox cloned from. */
  repositoryUrl: TrimmedNonEmptyString,
  branch: TrimmedNonEmptyString,
  baseRef: TrimmedNonEmptyString,
  /** The checkout inside the sandbox, never a path on the controlling machine. */
  workspacePath: TrimmedNonEmptyString,
  /** The sandbox user's home, where attachments and the host script live. */
  homePath: TrimmedNonEmptyString,
  status: Schema.Literals(["provisioning", "ready", "failed", "lost"]),
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
  lastError: Schema.optionalKey(Schema.String),
});
export type CloudThreadRecord = typeof CloudThreadRecord.Type;

const CloudThreadStoreState = Schema.Struct({
  version: Schema.Literal(1),
  threads: Schema.Record(ThreadId, CloudThreadRecord),
});
type CloudThreadStoreState = typeof CloudThreadStoreState.Type;

const decodeStore = Schema.decodeUnknownEffect(Schema.fromJsonString(CloudThreadStoreState));
const encodeStore = Schema.encodeSync(Schema.fromJsonString(CloudThreadStoreState));

export class CloudThreadStoreError extends Schema.TaggedError<
  CloudThreadStoreError,
  { readonly brand: unique symbol }
>()("CloudThreadStoreError", {
  operation: Schema.Literals(["read", "write"]),
  path: Schema.String,
  cause: Schema.Defect(),
}) {
  override get message(): string {
    return `Failed to ${this.operation} cloud thread state.`;
  }
}

const emptyState: CloudThreadStoreState = { version: 1, threads: {} };

export class CloudThreadStore extends Context.Service<
  CloudThreadStore,
  {
    readonly path: string;
    readonly list: Effect.Effect<ReadonlyArray<CloudThreadRecord>>;
    readonly get: (threadId: ThreadId) => Effect.Effect<Option.Option<CloudThreadRecord>>;
    readonly upsert: (record: CloudThreadRecord) => Effect.Effect<void, CloudThreadStoreError>;
    readonly remove: (threadId: ThreadId) => Effect.Effect<void, CloudThreadStoreError>;
  }
>()("t3/sandbox/CloudThreadStore") {}

const make = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const { stateDir } = yield* ServerConfig.ServerConfig;
  const storePath = path.join(stateDir, "cloud-threads.json");
  const lock = yield* Semaphore.make(1);

  const write = (state: CloudThreadStoreState) =>
    writeFileStringAtomically({
      filePath: storePath,
      contents: `${encodeStore(state)}\n`,
    }).pipe(
      Effect.provideService(FileSystem.FileSystem, fileSystem),
      Effect.provideService(Path.Path, path),
      Effect.mapError(
        (cause) => new CloudThreadStoreError({ operation: "write", path: storePath, cause }),
      ),
    );

  const load = Effect.gen(function* () {
    if (!(yield* fileSystem.exists(storePath))) return emptyState;
    const contents = yield* fileSystem
      .readFileString(storePath)
      .pipe(
        Effect.mapError(
          (cause) => new CloudThreadStoreError({ operation: "read", path: storePath, cause }),
        ),
      );
    // A corrupt file may be the only map from thread to billable sandbox; never overwrite it.
    return yield* decodeStore(contents).pipe(
      Effect.mapError(
        (cause) => new CloudThreadStoreError({ operation: "read", path: storePath, cause }),
      ),
    );
  });

  const state = yield* Ref.make(yield* load);

  const list = Ref.get(state).pipe(Effect.map((current) => Object.values(current.threads)));
  const get = (threadId: ThreadId) =>
    Ref.get(state).pipe(Effect.map((current) => Option.fromUndefinedOr(current.threads[threadId])));

  const mutate = (
    update: (current: CloudThreadStoreState) => CloudThreadStoreState,
  ): Effect.Effect<void, CloudThreadStoreError> =>
    lock.withPermits(1)(
      Effect.gen(function* () {
        const next = update(yield* Ref.get(state));
        yield* write(next);
        yield* Ref.set(state, next);
      }),
    );

  const upsert = (record: CloudThreadRecord) =>
    mutate((current) => ({
      version: 1,
      threads: { ...current.threads, [record.threadId]: record },
    }));

  const remove = (threadId: ThreadId) =>
    mutate((current) => {
      if (!Object.hasOwn(current.threads, threadId)) return current;
      const { [threadId]: _removed, ...threads } = current.threads;
      return { version: 1, threads };
    });

  return CloudThreadStore.of({ path: storePath, list, get, upsert, remove });
});

export const layer = Layer.effect(CloudThreadStore, make);
