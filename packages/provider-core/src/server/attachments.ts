// @effect-diagnostics nodeBuiltinImport:off -- attachment ids use the platform's path rules.
import * as NodeCrypto from "node:crypto";
import * as NodePath from "node:path";

import type { ChatAttachment } from "@t3tools/contracts";

import { resolveAttachmentRelativePath } from "./attachmentPaths.ts";
import { inferImageExtension } from "./imageMime.ts";

export const ATTACHMENT_ID_THREAD_SEGMENT_MAX_CHARS = 80;
const ATTACHMENT_ID_FILE_EXTENSION_PATTERN = "[a-z0-9]{1,10}";

export const PENDING_ATTACHMENT_THREAD_SEGMENT = "pending";

export function toSafeThreadAttachmentSegment(threadId: string): string | null {
  const segment = threadId
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/gi, "-")
    .replace(/-+/g, "-")
    .replace(/^[-_]+|[-_]+$/g, "")
    .slice(0, ATTACHMENT_ID_THREAD_SEGMENT_MAX_CHARS)
    .replace(/[-_]+$/g, "");
  if (segment.length === 0) {
    return null;
  }
  return segment === PENDING_ATTACHMENT_THREAD_SEGMENT ? "_pending" : segment;
}

export function attachmentFileExtension(fileName: string): string {
  const extension = NodePath.extname(fileName).toLowerCase();
  // .part is reserved for pending uploads; the stale-upload sweep would delete stored files.
  if (extension === ".part" || !/^\.[a-z0-9]{1,10}$/.test(extension)) {
    return ".bin";
  }
  return extension;
}

function attachmentIdExtensionSuffix(extension: string | undefined): string {
  if (!extension) {
    return "";
  }
  const normalized = extension.replace(/^\./, "").toLowerCase();
  return new RegExp(`^${ATTACHMENT_ID_FILE_EXTENSION_PATTERN}$`).test(normalized)
    ? `-${normalized}`
    : "-bin";
}

export function createAttachmentId(threadId: string, extension?: string): string | null {
  const threadSegment = toSafeThreadAttachmentSegment(threadId);
  if (!threadSegment) {
    return null;
  }
  return `${threadSegment}-${NodeCrypto.randomUUID()}${attachmentIdExtensionSuffix(extension)}`;
}

export function attachmentRelativePath(attachment: ChatAttachment): string | null {
  switch (attachment.type) {
    case "image": {
      const extension = inferImageExtension({
        mimeType: attachment.mimeType,
        fileName: attachment.name,
      });
      return `${attachment.id}${extension}`;
    }
    case "file":
      return `${attachment.id}${attachmentFileExtension(attachment.name)}`;
    default:
      return null;
  }
}

export function resolveAttachmentPath(input: {
  readonly attachmentsDir: string;
  readonly attachment: ChatAttachment;
}): string | null {
  const relativePath = attachmentRelativePath(input.attachment);
  if (!relativePath) {
    return null;
  }
  return resolveAttachmentRelativePath({
    attachmentsDir: input.attachmentsDir,
    relativePath,
  });
}
