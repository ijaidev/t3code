import { EnvironmentId, ThreadId, type CloudSandboxState } from "@t3tools/contracts";
import { expect, it, vi } from "vite-plus/test";
import { page } from "vite-plus/test/browser";

const harness = vi.hoisted(() => ({
  state: null as CloudSandboxState | null,
  recreate: vi.fn(async () => ({ _tag: "Success" as const, value: {} })),
}));

vi.mock("../../state/server", async () => {
  const { Atom } = await import("effect/reactivity");
  const { AsyncResult } = await import("effect/reactivity");
  const permission = Atom.make(true);
  return {
    serverEnvironment: {
      cloudSandboxState: () => Atom.make(AsyncResult.success(harness.state)),
      recreateCloudSandbox: { permissionAtom: () => permission, run: harness.recreate },
    },
  } as never;
});

vi.mock("../../state/use-atom-command", () => ({
  useAtomCommand: (command: { readonly run?: unknown }) =>
    command.run ?? (async () => ({ _tag: "Success", value: {} })),
}));

import { render } from "../../../test/render";
import { CloudWorkspaceLostBanner } from "./CloudWorkspaceLostBanner";

const threadId = ThreadId.make("thread-cloud-banner");
const environmentId = EnvironmentId.make("env");

const sandbox = (overrides: Partial<CloudSandboxState> = {}): CloudSandboxState => ({
  threadId,
  accountId: "e2b-work" as CloudSandboxState["accountId"],
  providerKind: "e2b" as CloudSandboxState["providerKind"],
  providerLabel: "E2B",
  status: "lost",
  sandboxId: null,
  template: "base",
  size: null,
  region: null,
  createdAt: "2026-11-24T10:00:00.000Z",
  lastActiveAt: "2026-11-24T10:05:00.000Z",
  sleepingSince: null,
  canSleep: false,
  canWake: false,
  canDelete: false,
  canRecreate: true,
  lossReason: "external",
  ...overrides,
});

const banner = () => <CloudWorkspaceLostBanner environmentId={environmentId} threadId={threadId} />;

it("stays hidden while the cloud workspace is usable", async () => {
  harness.state = sandbox({ status: "running", canRecreate: false });
  await using screen = await render(banner());
  expect(screen.container.textContent).not.toContain("Cloud workspace lost");
});

it("names an external loss and sends the recreate action", async () => {
  harness.state = sandbox();
  harness.recreate.mockClear();
  await using _screen = await render(banner());
  await expect.element(page.getByText(/deleted outside T3 Code/)).toBeVisible();
  await page.getByRole("button", { name: "Recreate sandbox" }).click();
  await expect.element(page.getByText(/Unpushed work from the lost sandbox is gone/)).toBeVisible();
  const confirm = page
    .getByRole("alertdialog")
    .getByRole("button", { name: "Recreate sandbox" })
    .element();
  (confirm as HTMLElement).click();
  await vi.waitFor(() =>
    expect(harness.recreate).toHaveBeenCalledWith({
      environmentId,
      input: { threadId },
    }),
  );
});

it("uses neutral copy when an older record has no loss reason", async () => {
  const { lossReason: _lossReason, ...withoutReason } = sandbox();
  harness.state = withoutReason;
  await using _screen = await render(banner());
  await expect.element(page.getByText(/This cloud workspace is gone/)).toBeVisible();
});
