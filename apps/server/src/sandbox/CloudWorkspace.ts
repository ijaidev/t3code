// @effect-diagnostics nodeBuiltinImport:off -- cloud workspace paths are POSIX paths inside the sandbox, never host paths.
import * as NodeCrypto from "node:crypto";
import * as NodePath from "node:path";

import {
  type FilesystemGetMetadataInput,
  type FilesystemGetMetadataResult,
  type ProjectListEntriesInput,
  type ProjectListEntriesResult,
  type ProjectListIgnoredFilesInput,
  type ProjectListIgnoredFilesResult,
  type ProjectSearchContentsInput,
  type ProjectSearchContentsResult,
  type ProjectSearchEntriesInput,
  type ProjectSearchEntriesResult,
  type ProjectReadFileInput,
  type ProjectReadFileResult,
  type ProjectWriteFileInput,
  type ProjectWriteFileResult,
  type ThreadId,
} from "@t3tools/contracts";
import { isProjectFaviconPath } from "@t3tools/shared/projectFavicon";
import { normalizeSearchQuery, scoreQueryMatch } from "@t3tools/shared/searchRanking";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import * as KnownWorkspaces from "../workspace/KnownWorkspaces.ts";
import * as WorkspaceEntries from "../workspace/WorkspaceEntries.ts";
import * as WorkspaceFileSystem from "../workspace/WorkspaceFileSystem.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import * as WorkspaceSearchIndex from "../workspace/WorkspaceSearchIndex.ts";
import * as CloudThreads from "./CloudThreads.ts";

const READ_FILE_MAX_BYTES = 1024 * 1024;
const SEARCH_OUTPUT_MAX_BYTES = 8 * 1024 * 1024;
const CONTENT_SEARCH_MAX_MATCHES_PER_FILE = 100;
const CONTENT_LINE_MAX_LENGTH = 512;
const IGNORED_FILES_LIMIT = 200;

const shellQuote = (value: string) => `'${value.replaceAll("'", `'\\''`)}'`;

const WORD_CHARACTER = /[\p{Letter}\p{Mark}\p{Number}_]/u;

const codePointAt = (line: string, index: number): string | undefined => {
  const codePoint = line.codePointAt(index);
  return codePoint === undefined ? undefined : String.fromCodePoint(codePoint);
};

const codePointBefore = (line: string, index: number): string | undefined => {
  if (index <= 0) return undefined;
  const previousCodeUnit = line.charCodeAt(index - 1);
  const previousIndex =
    previousCodeUnit >= 0xdc00 && previousCodeUnit <= 0xdfff ? index - 2 : index - 1;
  return codePointAt(line, previousIndex);
};

const isWholeWordRange = (
  line: string,
  range: { readonly start: number; readonly end: number },
): boolean => {
  if (range.end <= range.start) return false;
  const isWord = (character: string | undefined) =>
    character !== undefined && WORD_CHARACTER.test(character);
  const leftIsBoundary =
    range.start === 0 ||
    !isWord(codePointBefore(line, range.start)) ||
    !isWord(codePointAt(line, range.start));
  const rightIsBoundary =
    range.end >= line.length ||
    !isWord(codePointAt(line, range.end)) ||
    !isWord(codePointBefore(line, range.end));
  return leftIsBoundary && rightIsBoundary;
};

const escapeRegExp = (input: string) => input.replace(/[\\^$.*+?()[\]{}|/]/g, "\\$&");

const mapContentMatchRanges = (
  line: string,
  byteRanges: ReadonlyArray<readonly [number, number]>,
): Array<{ readonly start: number; readonly end: number }> => {
  const lineBytes = Buffer.from(line);
  const toStringIndex = (byteOffset: number) => lineBytes.subarray(0, byteOffset).toString().length;
  return byteRanges.map(([startByte, endByte]) => ({
    start: toStringIndex(startByte),
    end: toStringIndex(endByte),
  }));
};

const locateRegexMatches = (
  line: string,
  matches: { readonly column: number; readonly texts: ReadonlyArray<string> } | undefined,
): Array<readonly [number, number]> => {
  if (!matches) return [];
  const lineBytes = Buffer.from(line);
  const ranges: Array<readonly [number, number]> = [];
  let start = matches.column - 1;
  for (const text of matches.texts) {
    const textBytes = Buffer.from(text);
    if (ranges.length > 0) start = lineBytes.indexOf(textBytes, ranges.at(-1)![1]);
    if (start < 0) break;
    ranges.push([start, start + textBytes.length]);
  }
  return ranges;
};

const parseGitGrepRecords = (
  stdout: string,
  fieldCount: number,
  truncated: boolean,
): Array<Array<string>> => {
  const lines = stdout.split("\n");
  if (truncated || lines.at(-1) === "") lines.pop();
  const records: Array<Array<string>> = [];
  for (const line of lines) {
    const fields = line.split("\0");
    if (fields.length < fieldCount) continue;
    records.push([...fields.slice(0, fieldCount - 1), fields.slice(fieldCount - 1).join("\0")]);
  }
  return records;
};

const firstLine = (value: string) => value.trim().split("\n")[0] ?? value.trim();

const isInside = (root: string, target: string) =>
  target === root || target.startsWith(root.endsWith("/") ? root : `${root}/`);

const relativePathSegments = (value: string) => value.replaceAll("\\", "/").split("/");

const hasUnsafeSegment = (value: string) =>
  relativePathSegments(value).some((segment) => segment === ".." || segment === ".git");

const normalizeRelativePath = (value: string): string | null => {
  if (value === "" || value.includes("\0")) return null;
  const normalized = value.replaceAll("\\", "/");
  if (normalized.startsWith("/") || normalized.startsWith("~")) return null;
  if (hasUnsafeSegment(normalized)) return null;
  return normalized;
};

const isCloudPath = (workspacePath: string, value: string) => {
  const normalized = NodePath.posix.normalize(value);
  return isInside(workspacePath, normalized);
};

interface CloudWorkspaceRecord {
  readonly threadId: ThreadId;
  readonly workspacePath: string;
}

interface WorkspaceExec {
  readonly threadId: ThreadId;
  readonly command: string;
  readonly cwd: string;
}

export class CloudWorkspace extends Context.Service<
  CloudWorkspace,
  {
    readonly forWorkspace: (cwd: string) => Effect.Effect<Option.Option<CloudWorkspaceRecord>>;
    readonly forPath: (path: string) => Effect.Effect<Option.Option<CloudWorkspaceRecord>>;
    readonly readFile: (
      input: ProjectReadFileInput,
    ) => Effect.Effect<
      ProjectReadFileResult,
      WorkspaceFileSystem.WorkspaceFileSystemError | WorkspacePaths.WorkspacePathOutsideRootError
    >;
    readonly writeFile: (
      input: ProjectWriteFileInput,
    ) => Effect.Effect<
      ProjectWriteFileResult,
      | WorkspaceFileSystem.WorkspaceFileSystemError
      | WorkspacePaths.WorkspacePathOutsideRootError
      | KnownWorkspaces.UnknownWorkspaceError
    >;
    readonly getMetadata: (
      input: FilesystemGetMetadataInput,
    ) => Effect.Effect<FilesystemGetMetadataResult>;
    readonly list: (
      input: ProjectListEntriesInput,
    ) => Effect.Effect<ProjectListEntriesResult, WorkspaceEntries.WorkspaceEntriesError>;
    readonly search: (
      input: ProjectSearchEntriesInput,
    ) => Effect.Effect<ProjectSearchEntriesResult, WorkspaceEntries.WorkspaceEntriesError>;
    readonly searchContents: (
      input: ProjectSearchContentsInput,
    ) => Effect.Effect<ProjectSearchContentsResult, WorkspaceEntries.WorkspaceEntriesError>;
    readonly listIgnoredFiles: (
      input: ProjectListIgnoredFilesInput,
    ) => Effect.Effect<ProjectListIgnoredFilesResult, WorkspaceEntries.WorkspaceEntriesError>;
  }
>()("t3/sandbox/CloudWorkspace") {}

const make = Effect.gen(function* () {
  const cloudThreads = yield* CloudThreads.CloudThreads;

  const recordFor = Effect.fn("CloudWorkspace.recordFor")(function* (cwd: string) {
    return yield* cloudThreads.forWorkspace(cwd).pipe(Effect.orElseSucceed(() => Option.none()));
  });

  const forWorkspace: CloudWorkspace["Service"]["forWorkspace"] = (cwd) =>
    recordFor(cwd).pipe(
      Effect.map((record) =>
        Option.map(record, (value) => ({
          threadId: value.threadId,
          workspacePath: value.workspacePath,
        })),
      ),
    );

  const forPath: CloudWorkspace["Service"]["forPath"] = Effect.fn("CloudWorkspace.forPath")(
    function* (path) {
      const normalized = NodePath.posix.normalize(path);
      const segments = normalized.split("/").filter(Boolean);
      for (let index = segments.length; index > 0; index -= 1) {
        const candidate = `/${segments.slice(0, index).join("/")}`;
        const record = yield* recordFor(candidate);
        if (Option.isSome(record)) {
          return Option.some({
            threadId: record.value.threadId,
            workspacePath: record.value.workspacePath,
          });
        }
      }
      return Option.none<CloudWorkspaceRecord>();
    },
  );

  const requireRecord = Effect.fn("CloudWorkspace.requireRecord")(function* (
    cwd: string,
    operation: "read" | "write" | "list" | "search",
  ) {
    const record = yield* recordFor(cwd);
    if (Option.isSome(record)) return record.value;
    return yield* new WorkspacePaths.WorkspacePathOutsideRootError({
      workspaceRoot: cwd,
      relativePath: operation,
    });
  });

  const fileOperationError = (
    input: { readonly cwd: string; readonly relativePath: string },
    operation: WorkspaceFileSystem.WorkspaceFileSystemOperationError["operation"],
    operationPath: string,
    cause: unknown,
  ) =>
    new WorkspaceFileSystem.WorkspaceFileSystemOperationError({
      workspaceRoot: input.cwd,
      relativePath: input.relativePath,
      resolvedPath: operationPath,
      operationPath,
      operation,
      cause,
    });

  const entryError = (
    input: {
      readonly cwd: string;
      readonly partialPath?: string | undefined;
      readonly directoryPath?: string | undefined;
    },
    parentPath: string,
    cause: unknown,
  ) =>
    new WorkspaceEntries.WorkspaceEntriesReadDirectoryError({
      cwd: input.cwd,
      partialPath: input.partialPath ?? input.directoryPath ?? "",
      parentPath,
      cause,
    });

  const exec = (input: WorkspaceExec, timeoutMs = 30_000) =>
    cloudThreads.exec({
      threadId: input.threadId,
      command: input.command,
      cwd: input.cwd,
      timeoutMs,
    });

  const searchError = (
    input: { readonly cwd: string; readonly query: string; readonly limit: number },
    reason: string,
    cause?: unknown,
  ) =>
    new WorkspaceSearchIndex.WorkspaceSearchIndexSearchFailed({
      cwd: input.cwd,
      queryLength: input.query.length,
      pageSize: input.limit,
      reason,
      ...(cause === undefined ? {} : { cause }),
    });

  const targetFor = (cwd: string, relativePath: string) =>
    NodePath.posix.resolve(cwd, relativePath);

  const readFile: CloudWorkspace["Service"]["readFile"] = Effect.fn("CloudWorkspace.readFile")(
    function* (input) {
      const record = yield* requireRecord(input.cwd, "read");
      const relativePath = input.relativePath;
      const target = relativePath.startsWith("/")
        ? NodePath.posix.normalize(relativePath)
        : (() => {
            const normalized = normalizeRelativePath(relativePath);
            return normalized === null ? null : targetFor(input.cwd, normalized);
          })();
      if (
        target === null ||
        hasUnsafeSegment(target) ||
        !isCloudPath(record.workspacePath, target)
      ) {
        return yield* new WorkspacePaths.WorkspacePathOutsideRootError({
          workspaceRoot: input.cwd,
          relativePath: input.relativePath,
        });
      }

      const script = [
        "set -eu",
        'root=$(realpath -e -- "$1") || exit 6',
        'target="$2"',
        '[ -e "$target" ] || exit 3',
        'real=$(realpath -e -- "$target") || exit 6',
        'case "$real" in "$root"|"$root"/*) ;; *) exit 2 ;; esac',
        '[ -f "$real" ] || exit 5',
        'size=$(stat -c %s -- "$real") || exit 6',
        'head -c 1048576 -- "$real" | base64 | tr -d "\\n"',
        'printf "\\n%s" "$size"',
      ].join("\n");
      const result = yield* exec({
        threadId: record.threadId,
        cwd: record.workspacePath,
        command: [
          "sh",
          "-c",
          shellQuote(script),
          "sh",
          shellQuote(record.workspacePath),
          shellQuote(target),
        ].join(" "),
      }).pipe(Effect.mapError((error) => fileOperationError(input, "read", target, error)));

      if (result.exitCode === 3) {
        if (input.allowMissing === true) {
          return {
            relativePath: input.relativePath,
            contents: "",
            byteLength: 0,
            truncated: false,
            exists: false,
          };
        }
        return yield* fileOperationError(
          input,
          "realpath-target",
          target,
          new Error("File is missing."),
        );
      }
      if (result.exitCode === 2) {
        return yield* new WorkspaceFileSystem.WorkspaceFilePathEscapeError({
          workspaceRoot: input.cwd,
          relativePath: input.relativePath,
          resolvedWorkspaceRoot: record.workspacePath,
          resolvedPath: target,
        });
      }
      if (result.exitCode === 5) {
        return yield* new WorkspaceFileSystem.WorkspacePathNotFileError({
          workspaceRoot: input.cwd,
          relativePath: input.relativePath,
          resolvedPath: target,
        });
      }
      if (result.exitCode !== 0) {
        return yield* fileOperationError(
          input,
          "read",
          target,
          new Error(result.stderr.trim() || "The sandbox could not read the file."),
        );
      }

      const newline = result.stdout.lastIndexOf("\n");
      const encoded = newline === -1 ? "" : result.stdout.slice(0, newline);
      const size = Number(
        (newline === -1 ? result.stdout : result.stdout.slice(newline + 1)).trim(),
      );
      if (!Number.isFinite(size) || size < 0) {
        return yield* fileOperationError(
          input,
          "stat",
          target,
          new Error("The sandbox returned an unreadable file size."),
        );
      }
      const bytes = Buffer.from(encoded, "base64");
      const truncated = size > READ_FILE_MAX_BYTES;
      if (bytes.includes(0)) {
        return yield* new WorkspaceFileSystem.WorkspaceBinaryFileError({
          workspaceRoot: input.cwd,
          relativePath: input.relativePath,
          resolvedPath: target,
        });
      }
      const contents = yield* Effect.try({
        try: () =>
          new TextDecoder("utf-8", { fatal: true }).decode(bytes, {
            stream: truncated,
          }),
        catch: () =>
          new WorkspaceFileSystem.WorkspaceBinaryFileError({
            workspaceRoot: input.cwd,
            relativePath: input.relativePath,
            resolvedPath: target,
          }),
      });
      return {
        relativePath: input.relativePath,
        contents,
        byteLength: size,
        truncated,
      };
    },
  );

  const writeFile: CloudWorkspace["Service"]["writeFile"] = Effect.fn("CloudWorkspace.writeFile")(
    function* (input) {
      const record = yield* requireRecord(input.cwd, "write");
      const relativePath = normalizeRelativePath(input.relativePath);
      if (relativePath === null) {
        return yield* new WorkspacePaths.WorkspacePathOutsideRootError({
          workspaceRoot: input.cwd,
          relativePath: input.relativePath,
        });
      }
      const target = targetFor(input.cwd, relativePath);
      if (!isCloudPath(record.workspacePath, target)) {
        return yield* new WorkspacePaths.WorkspacePathOutsideRootError({
          workspaceRoot: input.cwd,
          relativePath: input.relativePath,
        });
      }

      const prepareScript = [
        "set -eu",
        'root=$(realpath -e -- "$1") || exit 6',
        'target="$2"',
        'parent=$(dirname -- "$target")',
        'mkdir -p -- "$parent" || exit 6',
        'realparent=$(realpath -e -- "$parent") || exit 6',
        'case "$realparent" in "$root"|"$root"/*) ;; *) exit 2 ;; esac',
        'if [ -e "$target" ]; then',
        '  realtarget=$(realpath -e -- "$target") || exit 6',
        '  case "$realtarget" in "$root"|"$root"/*) ;; *) exit 2 ;; esac',
        '  [ -f "$realtarget" ] || exit 5',
        '  head -c 3 -- "$realtarget" | base64 | tr -d "\\n"',
        "fi",
        'printf "\\n%s" "$realparent"',
      ].join("\n");
      const prepared = yield* exec({
        threadId: record.threadId,
        cwd: record.workspacePath,
        command: [
          "sh",
          "-c",
          shellQuote(prepareScript),
          "sh",
          shellQuote(record.workspacePath),
          shellQuote(target),
        ].join(" "),
      }).pipe(
        Effect.mapError((error) => fileOperationError(input, "make-directory", target, error)),
      );
      if (prepared.exitCode === 2) {
        return yield* new WorkspaceFileSystem.WorkspaceFilePathEscapeError({
          workspaceRoot: input.cwd,
          relativePath: input.relativePath,
          resolvedWorkspaceRoot: record.workspacePath,
          resolvedPath: target,
        });
      }
      if (prepared.exitCode === 5) {
        return yield* new WorkspaceFileSystem.WorkspacePathNotFileError({
          workspaceRoot: input.cwd,
          relativePath: input.relativePath,
          resolvedPath: target,
        });
      }
      if (prepared.exitCode !== 0) {
        return yield* fileOperationError(
          input,
          "make-directory",
          target,
          new Error(prepared.stderr.trim() || "The sandbox could not prepare the write."),
        );
      }
      const newline = prepared.stdout.lastIndexOf("\n");
      const existingHeader = Buffer.from(
        newline === -1 ? "" : prepared.stdout.slice(0, newline),
        "base64",
      );
      const parentPath = (
        newline === -1 ? prepared.stdout : prepared.stdout.slice(newline + 1)
      ).trim();
      if (parentPath === "") {
        return yield* fileOperationError(
          input,
          "make-directory",
          target,
          new Error("The sandbox returned an unreadable folder path."),
        );
      }

      const keepsBom =
        existingHeader.length === 3 &&
        existingHeader[0] === 0xef &&
        existingHeader[1] === 0xbb &&
        existingHeader[2] === 0xbf &&
        !input.contents.startsWith("\uFEFF");
      const body = new TextEncoder().encode(input.contents);
      const bytes = keepsBom ? Buffer.concat([Buffer.from([0xef, 0xbb, 0xbf]), body]) : body;
      const tempPath = NodePath.posix.join(parentPath, `.t3-write-${NodeCrypto.randomUUID()}.tmp`);
      const written = yield* cloudThreads
        .writeAttachment(record.threadId, tempPath, bytes)
        .pipe(Effect.mapError((error) => fileOperationError(input, "write-file", tempPath, error)));
      if (!written) {
        return yield* fileOperationError(
          input,
          "write-file",
          tempPath,
          new Error("The sandbox rejected the write."),
        );
      }

      const finalizeScript = [
        "set -eu",
        'target="$1"',
        'temp="$2"',
        'expected="$3"',
        'realparent=$(realpath -e -- "$(dirname -- "$target")") || exit 6',
        '[ "$realparent" = "$expected" ] || exit 2',
        '[ -d "$target" ] && exit 5',
        "trap 'rm -f -- \"$temp\"' EXIT",
        'mv -f -- "$temp" "$target"',
      ].join("\n");
      const finalized = yield* exec({
        threadId: record.threadId,
        cwd: record.workspacePath,
        command: [
          "sh",
          "-c",
          shellQuote(finalizeScript),
          "sh",
          shellQuote(target),
          shellQuote(tempPath),
          shellQuote(parentPath),
        ].join(" "),
      }).pipe(Effect.mapError((error) => fileOperationError(input, "write-file", target, error)));
      if (finalized.exitCode === 2) {
        return yield* new WorkspaceFileSystem.WorkspaceFilePathEscapeError({
          workspaceRoot: input.cwd,
          relativePath: input.relativePath,
          resolvedWorkspaceRoot: record.workspacePath,
          resolvedPath: target,
        });
      }
      if (finalized.exitCode === 5) {
        return yield* new WorkspaceFileSystem.WorkspacePathNotFileError({
          workspaceRoot: input.cwd,
          relativePath: input.relativePath,
          resolvedPath: target,
        });
      }
      if (finalized.exitCode !== 0) {
        return yield* fileOperationError(
          input,
          "write-file",
          target,
          new Error(finalized.stderr.trim() || "The sandbox could not write the file."),
        );
      }
      return { relativePath: input.relativePath };
    },
  );

  const list: CloudWorkspace["Service"]["list"] = Effect.fn("CloudWorkspace.list")(
    function* (input) {
      const record = yield* requireRecord(input.cwd, "list").pipe(
        Effect.mapError((error) =>
          entryError({ cwd: input.cwd, partialPath: input.directoryPath ?? "" }, input.cwd, error),
        ),
      );
      const directoryPath = input.directoryPath ?? "";
      const normalized =
        directoryPath === ""
          ? ""
          : (normalizeRelativePath(directoryPath)?.replace(/\/+$/, "") ?? null);
      if (normalized === null) {
        return yield* entryError(
          { cwd: input.cwd, partialPath: directoryPath },
          NodePath.posix.resolve(input.cwd, directoryPath),
          new Error("Directory must stay inside the sandbox checkout."),
        );
      }
      const target = targetFor(input.cwd, normalized === "" ? "." : normalized);
      const script = [
        "set -eu",
        'root=$(realpath -e -- "$1") || exit 6',
        'target="$2"',
        '[ -d "$target" ] || exit 3',
        'real=$(realpath -e -- "$target") || exit 6',
        'case "$real" in "$root"|"$root"/*) ;; *) exit 2 ;; esac',
        'find "$real" -mindepth 1 -maxdepth 1 \\( -type f -o -type d \\) -printf "%y\\t%f\\0"',
      ].join("\n");
      const result = yield* exec({
        threadId: record.threadId,
        cwd: record.workspacePath,
        command: [
          "sh",
          "-c",
          shellQuote(script),
          "sh",
          shellQuote(record.workspacePath),
          shellQuote(target),
        ].join(" "),
      }).pipe(Effect.mapError((error) => entryError(input, target, error)));
      if (result.exitCode === 2) {
        return yield* entryError(
          input,
          target,
          new Error("Directory must stay inside the sandbox checkout."),
        );
      }
      if (result.exitCode === 3) {
        return yield* entryError(
          input,
          target,
          Object.assign(new Error("Directory does not exist."), { code: "ENOENT" }),
        );
      }
      if (result.exitCode !== 0) {
        return yield* entryError(
          input,
          target,
          new Error(result.stderr.trim() || "The sandbox could not list the directory."),
        );
      }
      const entries = result.stdout
        .split("\0")
        .flatMap((recordLine): Array<{ path: string; kind: "file" | "directory" }> => {
          if (recordLine === "") return [];
          const separator = recordLine.indexOf("\t");
          if (separator === -1) return [];
          const kind = recordLine.slice(0, separator);
          const name = recordLine.slice(separator + 1);
          if (name === "" || name === ".git" || name.includes("/")) return [];
          if (kind !== "f" && kind !== "d") return [];
          return [
            {
              path: normalized === "" ? name : `${normalized}/${name}`,
              kind: kind === "d" ? "directory" : "file",
            },
          ];
        })
        .toSorted((left, right) => left.path.localeCompare(right.path));
      return { entries, truncated: false };
    },
  );

  const listFilePaths = Effect.fn("CloudWorkspace.listFilePaths")(function* (
    record: CloudWorkspaceRecord,
  ) {
    const script = [
      "set -eu",
      'root=$(realpath -e -- "$1") || exit 6',
      'cd -- "$root"',
      "if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then",
      "  git ls-files --cached --others --exclude-standard -z",
      "else",
      "  find . -type f -not -path './.git/*' -printf '%P\\0'",
      "fi",
    ].join("\n");
    const result = yield* exec({
      threadId: record.threadId,
      cwd: record.workspacePath,
      command: ["sh", "-c", shellQuote(script), "sh", shellQuote(record.workspacePath)].join(" "),
    }).pipe(
      Effect.mapError(
        (error) =>
          new WorkspaceEntries.WorkspaceEntriesReadDirectoryError({
            cwd: record.workspacePath,
            partialPath: "",
            parentPath: record.workspacePath,
            cause: error,
          }),
      ),
    );
    if (result.exitCode !== 0) {
      return yield* new WorkspaceEntries.WorkspaceEntriesReadDirectoryError({
        cwd: record.workspacePath,
        partialPath: "",
        parentPath: record.workspacePath,
        cause: new Error(result.stderr.trim() || "The sandbox could not list the workspace files."),
      });
    }
    const truncated = result.stdout.length >= SEARCH_OUTPUT_MAX_BYTES;
    const paths = result.stdout.split("\0").filter((path) => path !== "");
    if (truncated && paths.length > 0) paths.pop();
    return paths;
  });

  const search: CloudWorkspace["Service"]["search"] = Effect.fn("CloudWorkspace.search")(
    function* (input) {
      const record = yield* requireRecord(input.cwd, "search").pipe(
        Effect.mapError((error) =>
          searchError(input, "This workspace is not a cloud thread checkout.", error),
        ),
      );
      const paths = yield* listFilePaths(record);
      const entries = new Map<string, { path: string; kind: "file" | "directory" }>();
      for (const path of paths) {
        if (path === "" || hasUnsafeSegment(path)) continue;
        entries.set(path, { path, kind: "file" });
        const segments = path.split("/");
        for (let index = 1; index < segments.length; index += 1) {
          const directory = segments.slice(0, index).join("/");
          if (!entries.has(directory))
            entries.set(directory, { path: directory, kind: "directory" });
        }
      }

      const query = normalizeSearchQuery(input.query, { trimLeadingPattern: /^[@./]+/ });
      const ranked: Array<{ entry: { path: string; kind: "file" | "directory" }; score: number }> =
        [];
      for (const entry of entries.values()) {
        if (input.kind !== undefined && entry.kind !== input.kind) continue;
        if (input.imageOnly === true && !isProjectFaviconPath(entry.path)) continue;
        const score =
          query === ""
            ? 0
            : scoreQueryMatch({
                value: entry.path.toLowerCase(),
                query,
                exactBase: 0,
                prefixBase: 8,
                boundaryBase: 16,
                includesBase: 24,
                fuzzyBase: 32,
                boundaryMarkers: ["/", "-", "_", "."],
              });
        if (score === null) continue;
        ranked.push({ entry, score });
      }
      ranked.sort(
        (left, right) =>
          left.score - right.score ||
          left.entry.path.length - right.entry.path.length ||
          left.entry.path.localeCompare(right.entry.path),
      );
      return {
        entries: ranked.slice(0, input.limit).map(({ entry }) => entry),
        truncated: ranked.length > input.limit,
      };
    },
  );

  const searchContents: CloudWorkspace["Service"]["searchContents"] = Effect.fn(
    "CloudWorkspace.searchContents",
  )(function* (input) {
    const record = yield* requireRecord(input.cwd, "search").pipe(
      Effect.mapError((error) =>
        searchError(input, "This workspace is not a cloud thread checkout.", error),
      ),
    );
    const runGitGrep = (regexMode: boolean, onlyMatching: boolean) =>
      exec(
        {
          threadId: record.threadId,
          cwd: record.workspacePath,
          command: [
            "set -u",
            `cd -- ${shellQuote(record.workspacePath)}`,
            [
              "git",
              "-c",
              "core.quotePath=false",
              "-c",
              "core.fsmonitor=false",
              "grep",
              "--untracked",
              "--no-color",
              "-I",
              "-n",
              "-z",
              "-m",
              String(CONTENT_SEARCH_MAX_MATCHES_PER_FILE),
              ...(onlyMatching ? ["-o", "--column"] : []),
              ...(input.caseSensitive ? [] : ["-i"]),
              regexMode ? "-P" : "-F",
              "-e",
              shellQuote(input.query),
            ].join(" "),
          ].join("\n"),
        },
        30_000,
      ).pipe(Effect.mapError((error) => new Error(error.detail)));

    let regexMode = input.useRegex;
    let regexFallbackError: string | undefined;
    let result = yield* runGitGrep(regexMode, false).pipe(Effect.result);
    if (result._tag === "Failure") {
      return yield* searchError(
        input,
        "The sandbox could not search the workspace.",
        result.failure,
      );
    }
    if (
      regexMode &&
      result.success.exitCode === 128 &&
      /USE_LIBPCRE|Perl-compatible/i.test(result.success.stderr)
    ) {
      regexFallbackError = firstLine(result.success.stderr) || "Invalid regex";
      regexMode = false;
      result = yield* runGitGrep(regexMode, false).pipe(Effect.result);
    }
    if (result._tag === "Failure" || result.success.exitCode > 1) {
      return yield* searchError(
        input,
        "The sandbox could not search the workspace.",
        result._tag === "Failure"
          ? result.failure
          : new Error(firstLine(result.success.stderr) || "git grep failed."),
      );
    }

    const regexMatches = new Map<string, { readonly column: number; readonly texts: string[] }>();
    let rangesTruncated = false;
    if (regexMode) {
      const ranges = yield* runGitGrep(regexMode, true).pipe(Effect.result);
      if (ranges._tag === "Failure" || ranges.success.exitCode > 1) {
        return yield* searchError(
          input,
          "The sandbox could not locate the matches.",
          ranges._tag === "Failure"
            ? ranges.failure
            : new Error(firstLine(ranges.success.stderr) || "git grep ranges failed."),
        );
      }
      if (ranges.success.exitCode === 0) {
        for (const recordLine of parseGitGrepRecords(ranges.success.stdout, 4, false)) {
          const [relativePath, lineNumber, column, text] = recordLine as [
            string,
            string,
            string,
            string,
          ];
          const key = `${relativePath}\0${lineNumber}`;
          const lineMatches = regexMatches.get(key);
          if (lineMatches) lineMatches.texts.push(text);
          else regexMatches.set(key, { column: Number(column), texts: [text] });
        }
      }
    }

    const literal = regexMode
      ? undefined
      : new RegExp(escapeRegExp(input.query), input.caseSensitive ? "gu" : "giu");
    const matches: Array<ProjectSearchContentsResult["matches"][number]> = [];
    for (const recordLine of parseGitGrepRecords(result.success.stdout, 3, false)) {
      const [relativePath, lineNumber, rawLine] = recordLine as [string, string, string];
      if (hasUnsafeSegment(relativePath)) continue;
      const key = `${relativePath}\0${lineNumber}`;
      if (rangesTruncated && !regexMatches.has(key)) break;
      const lineContent = (rawLine.endsWith("\r") ? rawLine.slice(0, -1) : rawLine).slice(
        0,
        CONTENT_LINE_MAX_LENGTH,
      );
      const matchRanges = (
        literal
          ? Array.from(lineContent.matchAll(literal), (match) => ({
              start: match.index,
              end: match.index + match[0].length,
            }))
          : mapContentMatchRanges(
              lineContent,
              locateRegexMatches(lineContent, regexMatches.get(key)),
            )
      ).filter(
        (range) =>
          range.end <= lineContent.length &&
          (!input.wholeWord || isWholeWordRange(lineContent, range)),
      );
      if (input.wholeWord && matchRanges.length === 0) continue;
      matches.push({
        path: relativePath,
        lineNumber: Number(lineNumber),
        lineContent,
        matchRanges,
      });
      if (matches.length > input.limit) break;
    }
    return {
      matches: matches.slice(0, input.limit),
      truncated: matches.length > input.limit,
      ...(regexFallbackError === undefined ? {} : { regexFallbackError }),
    };
  });

  const listIgnoredFiles: CloudWorkspace["Service"]["listIgnoredFiles"] = Effect.fn(
    "CloudWorkspace.listIgnoredFiles",
  )(function* (input) {
    const record = yield* requireRecord(input.cwd, "list").pipe(
      Effect.mapError(
        (error) =>
          new WorkspaceEntries.WorkspaceEntriesReadDirectoryError({
            cwd: input.cwd,
            partialPath: "",
            parentPath: input.cwd,
            cause: error,
          }),
      ),
    );
    const script = [
      "set -eu",
      'root=$(realpath -e -- "$1") || exit 6',
      'cd -- "$root"',
      "if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then exit 0; fi",
      "git ls-files --others --ignored --exclude-standard --directory --no-empty-directory -z",
    ].join("\n");
    const result = yield* exec({
      threadId: record.threadId,
      cwd: record.workspacePath,
      command: ["sh", "-c", shellQuote(script), "sh", shellQuote(record.workspacePath)].join(" "),
    }).pipe(
      Effect.mapError(
        (error) =>
          new WorkspaceEntries.WorkspaceEntriesReadDirectoryError({
            cwd: input.cwd,
            partialPath: "",
            parentPath: record.workspacePath,
            cause: error,
          }),
      ),
    );
    if (result.exitCode !== 0) {
      return yield* new WorkspaceEntries.WorkspaceEntriesReadDirectoryError({
        cwd: input.cwd,
        partialPath: "",
        parentPath: record.workspacePath,
        cause: new Error(firstLine(result.stderr) || "git ls-files failed."),
      });
    }
    const query = input.query?.toLowerCase() ?? "";
    const paths = result.stdout
      .split("\0")
      .filter(
        (path) =>
          path.length > 0 &&
          !path.endsWith("/") &&
          !hasUnsafeSegment(path) &&
          path.toLowerCase().includes(query),
      )
      .toSorted();
    const limited = paths.slice(0, IGNORED_FILES_LIMIT);
    if (limited.length === 0) return { files: [], truncated: paths.length > IGNORED_FILES_LIMIT };
    const statScript = [
      "set -eu",
      'cd -- "$1"',
      "shift",
      "for path do",
      '  if [ -f "$path" ]; then',
      '    printf "%s\\t%s\\0" "$(stat -c %s -- "$path")" "$path"',
      "  fi",
      "done",
    ].join("\n");
    const statResult = yield* exec({
      threadId: record.threadId,
      cwd: record.workspacePath,
      command: [
        "sh",
        "-c",
        shellQuote(statScript),
        "sh",
        shellQuote(record.workspacePath),
        ...limited.map(shellQuote),
      ].join(" "),
    }).pipe(
      Effect.mapError(
        (error) =>
          new WorkspaceEntries.WorkspaceEntriesReadDirectoryError({
            cwd: input.cwd,
            partialPath: "",
            parentPath: record.workspacePath,
            cause: error,
          }),
      ),
    );
    if (statResult.exitCode !== 0) {
      return yield* new WorkspaceEntries.WorkspaceEntriesReadDirectoryError({
        cwd: input.cwd,
        partialPath: "",
        parentPath: record.workspacePath,
        cause: new Error(firstLine(statResult.stderr) || "stat failed."),
      });
    }
    const files = statResult.stdout.split("\0").flatMap((line) => {
      const separator = line.indexOf("\t");
      if (separator === -1) return [];
      const byteLength = Number(line.slice(0, separator));
      const path = line.slice(separator + 1);
      return Number.isFinite(byteLength) && byteLength >= 0 && path !== ""
        ? [{ path, byteLength }]
        : [];
    });
    return { files, truncated: paths.length > IGNORED_FILES_LIMIT };
  });

  const getMetadata: CloudWorkspace["Service"]["getMetadata"] = Effect.fn(
    "CloudWorkspace.getMetadata",
  )(function* (input) {
    const entries = yield* Effect.forEach(input.paths, (path) =>
      Effect.gen(function* () {
        const record = yield* forPath(path);
        if (Option.isNone(record)) return null;
        const target = NodePath.posix.normalize(path);
        if (!isCloudPath(record.value.workspacePath, target)) return null;
        const script = [
          "set -eu",
          'root=$(realpath -e -- "$1") || exit 6',
          'target="$2"',
          '[ -e "$target" ] || exit 3',
          'real=$(realpath -e -- "$target") || exit 6',
          'case "$real" in "$root"|"$root"/*) ;; *) exit 2 ;; esac',
          'if [ -d "$real" ]; then printf "d"; exit 0; fi',
          '[ -f "$real" ] || exit 4',
          'size=$(stat -c %s -- "$real") || exit 6',
          'printf "f\\t%s" "$size"',
        ].join("\n");
        const result = yield* exec({
          threadId: record.value.threadId,
          cwd: record.value.workspacePath,
          command: [
            "sh",
            "-c",
            shellQuote(script),
            "sh",
            shellQuote(record.value.workspacePath),
            shellQuote(target),
          ].join(" "),
        }).pipe(Effect.orElseSucceed(() => ({ exitCode: 1, stdout: "", stderr: "" })));
        if (result.exitCode !== 0) return null;
        if (result.stdout.trim() === "d") return { kind: "directory" } as const;
        const [kind, size] = result.stdout.trim().split("\t");
        if (kind !== "f") return null;
        const byteLength = Number(size);
        return {
          kind: "file" as const,
          ...(Number.isFinite(byteLength) && byteLength >= 0 ? { byteLength } : {}),
        };
      }),
    );
    return { entries };
  });

  return CloudWorkspace.of({
    forWorkspace,
    forPath,
    readFile,
    writeFile,
    getMetadata,
    list,
    search,
    searchContents,
    listIgnoredFiles,
  });
});

export const layer = Layer.effect(CloudWorkspace, make);
