import { ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";

import * as PreviewManager from "../preview/Manager.ts";
import * as CloudThreads from "./CloudThreads.ts";

/**
 * An open preview holds its cloud sandbox awake and a closed tab releases it. The bridge lives
 * above both services so sibling layer composition cannot leave it with no preview manager.
 */
export const layer = Layer.effectDiscard(
  Effect.gen(function* () {
    const cloudThreads = yield* CloudThreads.CloudThreads;
    const previews = yield* PreviewManager.PreviewManager;
    yield* previews.events.pipe(
      Stream.runForEach((event) => {
        const threadId = ThreadId.make(event.threadId);
        return (
          event.type === "opened"
            ? cloudThreads.holdPreview(threadId, event.tabId)
            : event.type === "closed"
              ? cloudThreads.releasePreview(threadId, event.tabId)
              : Effect.void
        ).pipe(Effect.ignore);
      }),
      Effect.ignore,
      Effect.forkScoped,
    );
  }),
);
