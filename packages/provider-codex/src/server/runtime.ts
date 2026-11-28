import type { CodexSettings } from "@t3tools/contracts";

/** The config and environment a managed Codex instance resolves for its adapter. */
export interface CodexEffectiveRuntime {
  readonly config: CodexSettings;
  readonly environment: NodeJS.ProcessEnv;
  readonly revision: string;
}
