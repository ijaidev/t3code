import {
  type ProviderInstanceId,
  type SandboxError,
  type SandboxListCloudTargetsResult,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import * as ProviderInstanceRegistry from "../provider/ProviderInstanceRegistry.ts";
import {
  type CloudHarnessCapability,
  type CloudTargetRemote,
  cloudHarnessCapability,
  legacySandboxCloudTargets,
  sandboxCloudTargets,
} from "./CloudTargets.ts";
import * as SandboxAccounts from "./SandboxAccounts.ts";

export class CloudTargetList extends Context.Service<
  CloudTargetList,
  {
    readonly list: (input: {
      readonly remote: CloudTargetRemote;
      readonly instanceId?: ProviderInstanceId;
      /** New clients decode every reason; older ones get a reason their union knows. */
      readonly extendedReasons?: boolean;
    }) => Effect.Effect<SandboxListCloudTargetsResult, SandboxError>;
  }
>()("t3/sandbox/CloudTargetList") {}

const make = Effect.gen(function* () {
  const accounts = yield* SandboxAccounts.SandboxAccounts;
  const instances = yield* ProviderInstanceRegistry.ProviderInstanceRegistry;

  const harnessFor = Effect.fn("CloudTargetList.harnessFor")(function* (
    instanceId: ProviderInstanceId,
  ) {
    const instance = yield* instances.getInstance(instanceId);
    if (instance === undefined) {
      return { supported: false, credentialSupported: false } satisfies CloudHarnessCapability;
    }
    const environment =
      instances.getResolvedEnvironment === undefined
        ? []
        : ((yield* instances.getResolvedEnvironment(instanceId)) ?? []);
    return cloudHarnessCapability({
      driver: instance.driverKind,
      config: Option.getOrUndefined(yield* instances.getInstanceConfig(instanceId)),
      environment,
    });
  });

  const list: CloudTargetList["Service"]["list"] = Effect.fn("CloudTargetList.list")(
    function* (input) {
      const providers = yield* accounts.list;
      const harness =
        input.instanceId === undefined ? undefined : yield* harnessFor(input.instanceId);
      const targets = sandboxCloudTargets({
        remote: input.remote,
        accounts: providers.accounts,
        providers: providers.providers,
        ...(harness === undefined ? {} : { harness }),
      });
      return {
        targets: input.extendedReasons === true ? targets : legacySandboxCloudTargets(targets),
      };
    },
  );

  return CloudTargetList.of({ list });
});

export const layer = Layer.effect(CloudTargetList, make);
