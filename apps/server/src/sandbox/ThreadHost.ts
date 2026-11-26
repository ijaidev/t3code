import { TrimmedNonEmptyString } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

export const THREAD_HOST_PROTOCOL_VERSION = "2";
const THREAD_HOST_REQUIRED_CAPABILITIES = ["exec", "filesystem", "git", "ports"] as const;

// POSIX shell keeps the first host bootstrap free of a runtime the template may not preinstall.
export const THREAD_HOST_BOOTSTRAP_COMMAND = [
  "set -eu",
  'mkdir -p "$HOME/.t3/thread-host"',
  `cat > "$HOME/.t3/thread-host/thread-host" <<'T3_THREAD_HOST'`,
  "#!/bin/sh",
  "set -eu",
  `printf '%s\\n' '{"kind":"t3-thread-host","protocolVersion":"${THREAD_HOST_PROTOCOL_VERSION}","capabilities":["exec","filesystem","git","ports"]}'`,
  "T3_THREAD_HOST",
  'chmod +x "$HOME/.t3/thread-host/thread-host"',
  '"$HOME/.t3/thread-host/thread-host"',
].join("\n");

const ThreadHostHandshake = Schema.Struct({
  kind: Schema.Literal("t3-thread-host"),
  protocolVersion: TrimmedNonEmptyString,
  capabilities: Schema.Array(TrimmedNonEmptyString),
});
export type ThreadHostHandshake = typeof ThreadHostHandshake.Type;

const decodeHandshake = Schema.decodeUnknownOption(Schema.fromJsonString(ThreadHostHandshake));

export class ThreadHostHandshakeError extends Schema.TaggedError<
  ThreadHostHandshakeError,
  { readonly brand: unique symbol }
>()("ThreadHostHandshakeError", {
  stage: Schema.Literals(["output", "version", "capabilities"]),
  detail: Schema.String,
}) {
  override get message(): string {
    return this.detail;
  }
}

/** Fails closed: an unknown version or a missing required capability is not a ready host. */
export const parseThreadHostHandshake = (
  stdout: string,
): Effect.Effect<ThreadHostHandshake, ThreadHostHandshakeError> => {
  const parsed = decodeHandshake(stdout.trim());
  if (Option.isNone(parsed)) {
    return Effect.fail(
      new ThreadHostHandshakeError({
        stage: "output",
        detail: "The sandbox thread host did not return a readable handshake.",
      }),
    );
  }
  const handshake = parsed.value;
  if (handshake.protocolVersion !== THREAD_HOST_PROTOCOL_VERSION) {
    return Effect.fail(
      new ThreadHostHandshakeError({
        stage: "version",
        detail: `The sandbox thread host speaks protocol ${handshake.protocolVersion}; this server needs ${THREAD_HOST_PROTOCOL_VERSION}.`,
      }),
    );
  }
  const missing = THREAD_HOST_REQUIRED_CAPABILITIES.filter(
    (capability) => !handshake.capabilities.includes(capability),
  );
  if (missing.length > 0) {
    return Effect.fail(
      new ThreadHostHandshakeError({
        stage: "capabilities",
        detail: `The sandbox thread host is missing required capabilities: ${missing.join(", ")}.`,
      }),
    );
  }
  return Effect.succeed(handshake);
};
