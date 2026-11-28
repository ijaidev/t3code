import { type SandboxProviderKind, type SandboxProviderSummary } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Layer from "effect/Layer";

import { makeDaytonaSandboxProvider } from "./DaytonaSandboxProvider.ts";
import { makeE2BSandboxProvider } from "./E2BSandboxProvider.ts";
import type { SandboxProviderDriver } from "./SandboxProvider.ts";

export class SandboxProviderRegistry extends Context.Service<
  SandboxProviderRegistry,
  {
    readonly list: ReadonlyArray<SandboxProviderSummary>;
    readonly find: (kind: SandboxProviderKind) => SandboxProviderDriver | undefined;
  }
>()("t3/sandbox/SandboxProviderRegistry") {}

const make = (drivers: ReadonlyArray<SandboxProviderDriver>) =>
  SandboxProviderRegistry.of({
    list: drivers.map((driver) => ({
      kind: driver.kind,
      label: driver.label,
      capabilities: {
        ...driver.capabilities,
        // The declared flag is a claim; the host methods are what actually run a harness.
        harness:
          driver.startProcess !== undefined &&
          driver.portAccess !== undefined &&
          driver.writeFile !== undefined,
      },
    })),
    find: (kind) => drivers.find((driver) => driver.kind === kind),
  });

export const layerFromDrivers = (drivers: ReadonlyArray<SandboxProviderDriver>) =>
  Layer.succeed(SandboxProviderRegistry, make(drivers));

export const layer = layerFromDrivers([makeE2BSandboxProvider(), makeDaytonaSandboxProvider()]);
