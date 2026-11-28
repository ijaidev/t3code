import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";

const EXTRACTED_TEXT_EXTENSIONS = new Set([
  "docx",
  "xlsx",
  "pptx",
  "odt",
  "ods",
  "odp",
  "rtf",
  "epub",
  "ipynb",
]);

/** Returns the sidecar text extracted for a document, when one already exists. */
export const resolveExtractedTextPath = Effect.fn("DocumentText.resolveExtractedTextPath")(
  function* (path: string) {
    const extension = path.split(".").at(-1)?.toLowerCase();
    if (!extension || !EXTRACTED_TEXT_EXTENSIONS.has(extension)) return;
    const fs = yield* FileSystem.FileSystem;
    return yield* fs.exists(path + ".txt").pipe(
      Effect.map((exists) => (exists ? path + ".txt" : undefined)),
      Effect.orElseSucceed(() => undefined),
    );
  },
);
