import * as Context from "effect/Context";

export const ProviderProcesses = Context.Reference<{
  readonly register?: (pid: number) => () => void;
  readonly hasOtherLiveOwner: boolean;
}>("t3/process/ProviderProcesses", { defaultValue: () => ({ hasOtherLiveOwner: false }) });
