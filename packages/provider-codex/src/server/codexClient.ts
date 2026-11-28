import { codexModelFamily } from "@t3tools/shared/model";
import type * as CodexSchema from "effect-codex-app-server/schema";

const CODEX_CLIENT_NAME = "T3 Code";

export function codexDefaultReasoningEffortOverride(model: string): "medium" | undefined {
  return codexModelFamily(model) === "gpt-6-astra" ? "medium" : undefined;
}

/** Set originator at spawn; the pre-initialize refresh otherwise overwrites shared cache with plain-CLI models. */
export const codexAppServerEnvironment = (
  environment: NodeJS.ProcessEnv | undefined,
): NodeJS.ProcessEnv => ({
  CODEX_INTERNAL_ORIGINATOR_OVERRIDE: CODEX_CLIENT_NAME,
  ...environment,
});

export function buildCodexInitializeParams(clientVersion: string): CodexSchema.V1InitializeParams {
  return {
    clientInfo: {
      name: CODEX_CLIENT_NAME,
      title: CODEX_CLIENT_NAME,
      version: clientVersion,
    },
    capabilities: {
      experimentalApi: true,
    },
  };
}
