import { createFileRoute } from "@tanstack/react-router";

import { SandboxProvidersSettingsPanel } from "../components/settings/SandboxProvidersSettings";

export const Route = createFileRoute("/_chat/settings/sandboxes")({
  component: SandboxProvidersSettingsPanel,
});
