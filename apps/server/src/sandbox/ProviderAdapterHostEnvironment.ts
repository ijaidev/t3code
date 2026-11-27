/**
 * The host process needs its control variables, but the agent and every tool it
 * spawns must not inherit them. A tool that prints its environment could
 * otherwise drive the adapter host over the controller's connection.
 */
export const sandboxProviderEnvironment = (environment: NodeJS.ProcessEnv): NodeJS.ProcessEnv => {
  const filtered: NodeJS.ProcessEnv = {};
  for (const [name, value] of Object.entries(environment)) {
    if (name.startsWith("T3_PROVIDER_HOST_")) continue;
    filtered[name] = value;
  }
  return filtered;
};
