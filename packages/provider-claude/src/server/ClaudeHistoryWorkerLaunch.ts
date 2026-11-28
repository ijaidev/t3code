/** Arguments that start the Claude history worker in each server layout. */
export interface ClaudeHistoryWorkerLaunchInput {
  readonly fromSource: boolean;
  readonly executable: boolean;
  readonly modulePath: string;
  readonly sourceEntryPath: string;
  readonly controllerEntryPath: string;
  readonly controllerEntryExists: boolean;
}

export const claudeHistoryWorkerArguments = (input: ClaudeHistoryWorkerLaunchInput): string[] => {
  if (input.executable) return ["__claude-history"];
  if (input.fromSource) return ["--experimental-strip-types", input.sourceEntryPath];
  // The controller bundle splits adapters into chunks; only bin.mjs dispatches the subcommand.
  return input.controllerEntryExists
    ? [input.controllerEntryPath, "__claude-history"]
    : [input.modulePath, "__claude-history"];
};
