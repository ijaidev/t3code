import type { SandboxProviderCapabilities, SandboxProviderKind } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export const SandboxProviderOperation = Schema.Literals([
  "validate-credential",
  "create",
  "find-created",
  "get",
  "pause",
  "resume",
  "delete",
  "exec",
]);
export type SandboxProviderOperation = typeof SandboxProviderOperation.Type;

/** A provider failure with no credential or request payload in it. */
export class SandboxProviderError extends Schema.TaggedError<
  SandboxProviderError,
  { readonly brand: unique symbol }
>()("SandboxProviderError", {
  providerKind: Schema.String,
  operation: SandboxProviderOperation,
  detail: Schema.String,
  statusCode: Schema.optionalKey(Schema.Int),
}) {
  override get message(): string {
    return this.detail;
  }
}

/** Everything a driver needs about one connected account, with its credential resolved. */
export interface SandboxProviderAccount {
  readonly id: string;
  readonly kind: SandboxProviderKind;
  readonly label: string;
  readonly apiKey: string;
  readonly template: string;
}

export interface SandboxProviderCreateInput {
  readonly account: SandboxProviderAccount;
  readonly instanceId: string;
  readonly template: string;
  readonly timeoutMs: number;
}

export interface SandboxProviderExecInput {
  readonly account: SandboxProviderAccount;
  readonly providerSandboxId: string;
  readonly command: string;
  readonly cwd?: string;
  readonly timeoutMs?: number;
}

export interface SandboxProviderExecResult {
  readonly exitCode: number;
  readonly stdout: string;
  readonly stderr: string;
}

export interface SandboxProviderWriteFileInput {
  readonly account: SandboxProviderAccount;
  readonly providerSandboxId: string;
  readonly path: string;
  readonly data: string | Uint8Array;
}

export interface SandboxProviderPortAccessInput {
  readonly account: SandboxProviderAccount;
  readonly providerSandboxId: string;
  readonly port: number;
}

/** A private HTTP endpoint for one sandbox port. The token stays on the controlling server. */
export interface SandboxProviderPortAccess {
  readonly url: string;
  readonly headers: Readonly<Record<string, string>>;
}

export interface SandboxProviderStartProcessInput {
  readonly account: SandboxProviderAccount;
  readonly providerSandboxId: string;
  readonly command: string;
  readonly cwd?: string;
  readonly envs?: Readonly<Record<string, string>>;
  readonly onStdout: (data: string) => void;
  readonly onStderr: (data: string) => void;
}

export interface SandboxProviderProcess {
  readonly pid: number;
  readonly sendStdin: (data: string) => Effect.Effect<void, SandboxProviderError>;
  readonly kill: Effect.Effect<void, SandboxProviderError>;
  readonly wait: Effect.Effect<{ readonly exitCode: number }, SandboxProviderError>;
}

export interface SandboxProviderFoundSandbox {
  readonly providerSandboxId: string;
  readonly status: "running" | "paused";
}

export interface SandboxProviderDriver {
  readonly kind: SandboxProviderKind;
  readonly label: string;
  readonly capabilities: SandboxProviderCapabilities;
  readonly validateCredential: (
    account: SandboxProviderAccount,
  ) => Effect.Effect<void, SandboxProviderError>;
  readonly create: (
    input: SandboxProviderCreateInput,
  ) => Effect.Effect<{ readonly providerSandboxId: string }, SandboxProviderError>;
  /** Finds sandboxes a create may have made despite failing; metadata is the durable key. */
  readonly findCreated: (
    account: SandboxProviderAccount,
    instanceId: string,
  ) => Effect.Effect<ReadonlyArray<SandboxProviderFoundSandbox>, SandboxProviderError>;
  readonly get: (
    account: SandboxProviderAccount,
    providerSandboxId: string,
  ) => Effect.Effect<{ readonly status: "running" | "paused" }, SandboxProviderError>;
  readonly pause: (
    account: SandboxProviderAccount,
    providerSandboxId: string,
  ) => Effect.Effect<void, SandboxProviderError>;
  readonly resume: (
    account: SandboxProviderAccount,
    providerSandboxId: string,
  ) => Effect.Effect<void, SandboxProviderError>;
  readonly delete: (
    account: SandboxProviderAccount,
    providerSandboxId: string,
  ) => Effect.Effect<void, SandboxProviderError>;
  readonly exec: (
    input: SandboxProviderExecInput,
  ) => Effect.Effect<SandboxProviderExecResult, SandboxProviderError>;
  /** Uploads bytes; cloud threads use it for the host script and attachments. */
  readonly writeFile?: (
    input: SandboxProviderWriteFileInput,
  ) => Effect.Effect<void, SandboxProviderError>;
  /** Starts a long-lived stdio process; only drivers that support interactive work expose it. */
  readonly startProcess?: (
    input: SandboxProviderStartProcessInput,
  ) => Effect.Effect<SandboxProviderProcess, SandboxProviderError>;
  /**
   * Resolves a private HTTP endpoint for a port. Drivers without it cannot show sandbox previews;
   * the controlling server proxies requests so provider URLs never reach a client.
   */
  readonly portAccess?: (
    input: SandboxProviderPortAccessInput,
  ) => Effect.Effect<SandboxProviderPortAccess, SandboxProviderError>;
}
