import type * as Effect from "effect/Effect";

/** Environment variable that ties a listening agent process to its T3 thread. */
export const AGENT_THREAD_ENV = "T3CODE_THREAD_ID";

export interface AgentCommandStopWatcher {
  /** The variable the CLI sets on each command it starts, naming that command. */
  readonly variable: string;
  readonly onStop: (command: string) => Effect.Effect<void>;
  /** The signal never reached the command, so it is still running or ended by itself. */
  readonly onStopAbandoned: (command: string) => Effect.Effect<void>;
}
