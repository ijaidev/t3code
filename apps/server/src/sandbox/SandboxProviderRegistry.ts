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
    list: drivers.map(({ kind, label, capabilities }) => ({ kind, label, capabilities })),
    find: (kind) => drivers.find((driver) => driver.kind === kind),
  });

export const layerFromDrivers = (drivers: ReadonlyArray<SandboxProviderDriver>) =>
  Layer.succeed(SandboxProviderRegistry, make(drivers));

export const layer = layerFromDrivers([makeE2BSandboxProvider(), makeDaytonaSandboxProvider()]);
