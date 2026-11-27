import {
  type ChatAttachment,
  type ModelSelection,
  ProviderDriverKind,
  type ProviderSessionId,
  type ServerSettings,
  type ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import type {
  ProviderAdapterV2,
  ProviderAdapterV2Error,
  ProviderAdapterV2ResumeTurnInput,
  ProviderAdapterV2RuntimePolicy,
  ProviderAdapterV2SessionRuntime,
  ProviderAdapterV2TurnInput,
} from "@t3tools/provider-core/server/ProviderAdapter";
import { ProviderAdapterProtocolError } from "@t3tools/provider-core/server/ProviderAdapter";

import { attachmentRelativePath, resolveAttachmentPath } from "../attachmentStore.ts";
import * as ServerConfig from "../config.ts";
import * as CloudProviderHost from "./CloudProviderHost.ts";
import * as CloudThreads from "./CloudThreads.ts";

const OPENCODE_DRIVER = ProviderDriverKind.make("opencode");

export interface CloudProviderRuntimeOpenInput {
  readonly threadId: ThreadId;
  readonly providerSessionId: ProviderSessionId;
  readonly modelSelection: ModelSelection;
  readonly runtimePolicy: ProviderAdapterV2RuntimePolicy;
  readonly environment: ReadonlyArray<{ readonly name: string; readonly value: string }>;
  /** The provider instance's own settings, decoded by the controller. */
  readonly config: unknown;
  readonly settings: ServerSettings;
}

export class CloudProviderRuntime extends Context.Service<
  CloudProviderRuntime,
  {
    readonly supports: (driver: ProviderDriverKind) => boolean;
    readonly openSession: (
      input: CloudProviderRuntimeOpenInput,
      base: ProviderAdapterV2["Service"],
    ) => Effect.Effect<ProviderAdapterV2SessionRuntime, ProviderAdapterV2Error, Scope.Scope>;
  }
>()("t3/sandbox/CloudProviderRuntime") {}

const make = Effect.gen(function* () {
  const cloudThreads = yield* CloudThreads.CloudThreads;
  const fileSystem = yield* FileSystem.FileSystem;
  const { attachmentsDir } = yield* ServerConfig.ServerConfig;

  const supports = (driver: ProviderDriverKind) => driver === OPENCODE_DRIVER;

  const openSession: CloudProviderRuntime["Service"]["openSession"] = Effect.fn(
    "CloudProviderRuntime.openSession",
  )(function* (input, base) {
    if (!supports(base.driver)) {
      return yield* new ProviderAdapterProtocolError({
        driver: base.driver,
        detail: `Cloud threads cannot run the ${base.driver} provider yet.`,
      });
    }
    const record = yield* cloudThreads.getRecord(input.threadId);
    if (Option.isNone(record)) {
      return yield* new ProviderAdapterProtocolError({
        driver: base.driver,
        detail: "No cloud sandbox is recorded for this thread.",
      });
    }
    const runtimeDir = CloudProviderHost.cloudProviderHostRuntimeDir(record.value.homePath);
    const host = yield* cloudThreads
      .openProviderHost(input.threadId, {
        driver: base.driver,
        instanceId: input.modelSelection.instanceId,
        config: CloudProviderHost.cloudOpenCodeHostConfig(input.config, runtimeDir),
        settings: input.settings,
        environment: input.environment,
      })
      .pipe(
        Effect.mapError(
          (error) =>
            new ProviderAdapterProtocolError({
              driver: base.driver,
              detail: error.detail,
              cause: error,
            }),
        ),
      );
    yield* Effect.addFinalizer(() =>
      Effect.all([host.close, cloudThreads.endTurn(input.threadId)]).pipe(Effect.ignore),
    );
    const runtime = yield* host.adapter.openSession({
      threadId: input.threadId,
      providerSessionId: input.providerSessionId,
      modelSelection: input.modelSelection,
      runtimePolicy: input.runtimePolicy,
    });

    const stageAttachments = Effect.fn("CloudProviderRuntime.stageAttachments")(function* (
      attachments: ReadonlyArray<ChatAttachment>,
    ) {
      for (const attachment of attachments) {
        const relativePath = attachmentRelativePath(attachment);
        if (relativePath === null) continue;
        const localPath = resolveAttachmentPath({ attachmentsDir, attachment });
        if (localPath === null) continue;
        const bytes = yield* fileSystem
          .readFile(localPath)
          .pipe(Effect.catchCause(() => Effect.succeed(new Uint8Array())));
        if (bytes.length === 0) continue;
        yield* cloudThreads.writeAttachment(
          input.threadId,
          `${host.paths.attachmentsDir}/${relativePath}`,
          bytes,
        );
      }
    });

    const beginTurn = cloudThreads.beginTurn(input.threadId);
    const startTurn = (turnInput: ProviderAdapterV2TurnInput) =>
      beginTurn.pipe(
        Effect.andThen(stageAttachments(turnInput.message.attachments)),
        Effect.andThen(runtime.startTurn(turnInput)),
        Effect.onError(() => cloudThreads.endTurn(input.threadId)),
      );
    const resumeNativeTurn = runtime.resumeTurn;
    const resumeTurn =
      resumeNativeTurn === undefined
        ? undefined
        : (turnInput: ProviderAdapterV2ResumeTurnInput) =>
            beginTurn.pipe(
              Effect.andThen(resumeNativeTurn(turnInput)),
              Effect.onError(() => cloudThreads.endTurn(input.threadId)),
            );

    return {
      ...runtime,
      events: runtime.events.pipe(
        Stream.tap((event) =>
          event.type === "turn.terminal" ? cloudThreads.endTurn(input.threadId) : Effect.void,
        ),
      ),
      startTurn,
      ...(runtime.compactThread === undefined ? {} : { compactThread: startTurn }),
      ...(resumeTurn === undefined ? {} : { resumeTurn }),
      steerTurn: (turnInput) =>
        stageAttachments(turnInput.message.attachments).pipe(
          Effect.andThen(runtime.steerTurn(turnInput)),
        ),
    } satisfies ProviderAdapterV2SessionRuntime;
  });

  return CloudProviderRuntime.of({ supports, openSession });
});

export const layer = Layer.effect(CloudProviderRuntime, make);
