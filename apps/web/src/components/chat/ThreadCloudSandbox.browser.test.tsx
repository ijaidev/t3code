import { EnvironmentId, ThreadId, type CloudSandboxState } from "@t3tools/contracts";
import { expect, it, vi } from "vite-plus/test";
import { page } from "vite-plus/test/browser";

const harness = vi.hoisted(() => ({
  state: null as CloudSandboxState | null,
  sleep: vi.fn(async () => ({ _tag: "Success" as const, value: {} })),
  wake: vi.fn(async () => ({ _tag: "Success" as const, value: {} })),
  previewDelete: vi.fn(async () => ({ _tag: "Success" as const, value: {} })),
  deleteSandbox: vi.fn(async () => ({ _tag: "Success" as const, value: {} })),
  recreateSandbox: vi.fn(async () => ({ _tag: "Success" as const, value: {} })),
}));

vi.mock("../../state/server", async () => {
  const { Atom } = await import("effect/reactivity");
  const { AsyncResult } = await import("effect/reactivity");
  const permission = Atom.make(true);
  return {
    serverEnvironment: {
      cloudSandboxState: () => Atom.make(AsyncResult.success(harness.state)),
      sleepCloudSandbox: { permissionAtom: () => permission, run: harness.sleep },
      wakeCloudSandbox: { permissionAtom: () => permission, run: harness.wake },
      previewCloudSandboxDelete: {
        permissionAtom: () => permission,
        run: harness.previewDelete,
      },
      deleteCloudSandbox: { permissionAtom: () => permission, run: harness.deleteSandbox },
      recreateCloudSandbox: { permissionAtom: () => permission, run: harness.recreateSandbox },
    },
  } as never;
});

vi.mock("../../state/use-atom-command", () => ({
  useAtomCommand: (command: { readonly run?: unknown }) =>
    command.run ?? (async () => ({ _tag: "Success", value: {} })),
}));

import { render } from "../../../test/render";
import { ThreadCloudSandbox } from "./ThreadCloudSandbox";

const threadId = ThreadId.make("thread-cloud");
const environmentId = EnvironmentId.make("env");

const sandbox = (overrides: Partial<CloudSandboxState> = {}): CloudSandboxState => ({
  threadId,
  accountId: "e2b-work" as CloudSandboxState["accountId"],
  providerKind: "e2b" as CloudSandboxState["providerKind"],
  providerLabel: "E2B",
  status: "running",
  sandboxId: "sandbox-1" as CloudSandboxState["sandboxId"],
  template: "base",
  size: { id: "small", label: "Small", cpuCount: 2, memoryMB: 1024 },
  region: { id: "us", label: "United States" },
  createdAt: "2026-11-24T10:00:00.000Z",
  lastActiveAt: "2026-11-24T10:05:00.000Z",
  sleepingSince: null,
  canSleep: true,
  canWake: false,
  canDelete: true,
  canRecreate: false,
  ...overrides,
});

const panel = () => <ThreadCloudSandbox environmentId={environmentId} threadId={threadId} />;

it("shows the provider, size, region, age, and sleep action", async () => {
  harness.state = sandbox();
  await using _screen = await render(panel());
  await expect.element(page.getByText("E2B")).toBeVisible();
  await expect.element(page.getByText("Running")).toBeVisible();
  await expect.element(page.getByText(/Small · United States · Up for/)).toBeVisible();
  await expect.element(page.getByRole("button", { name: "Sleep" })).toBeVisible();
  await expect.element(page.getByRole("button", { name: "Delete sandbox" })).toBeVisible();
});

it("hides sleep with a reason on a provider without pause", async () => {
  harness.state = sandbox({ canSleep: false, sleepDisabledReason: "provider-no-sleep" });
  await using screen = await render(panel());
  await expect.element(page.getByText("This provider cannot pause sandboxes.")).toBeVisible();
  const sleepButtons = [...screen.container.querySelectorAll("button")].filter((button) =>
    button.textContent?.includes("Sleep"),
  );
  expect(sleepButtons).toHaveLength(0);
  await expect.element(page.getByRole("button", { name: "Delete sandbox" })).toBeVisible();
});

it("confirms deletion and names unpushed work", async () => {
  harness.state = sandbox();
  harness.previewDelete.mockResolvedValueOnce({
    _tag: "Success",
    value: {
      state: sandbox(),
      risk: { hasUnpushedChanges: true, changedFiles: 2, unpushedCommits: 1 },
    },
  });
  harness.deleteSandbox.mockClear();
  await using _screen = await render(panel());
  await page.getByRole("button", { name: "Delete sandbox" }).click();
  await expect
    .element(page.getByText(/This sandbox has 2 changed files · 1 unpushed commit/))
    .toBeVisible();
  const confirm = page
    .getByRole("alertdialog")
    .getByRole("button", { name: "Delete sandbox" })
    .element();
  (confirm as HTMLElement).click();
  await vi.waitFor(() =>
    expect(harness.deleteSandbox).toHaveBeenCalledWith({
      environmentId,
      input: { threadId, acknowledgedUnpushedChanges: true },
    }),
  );
});

it("warns that unpushed work is gone before recreating a lost sandbox", async () => {
  harness.state = sandbox({
    status: "lost",
    sandboxId: null,
    canSleep: false,
    canWake: false,
    canDelete: false,
    canRecreate: true,
    lossReason: "external",
  });
  harness.recreateSandbox.mockClear();
  await using _screen = await render(panel());
  await expect
    .element(page.getByText(/Unpushed work is gone; the transcript is still here/))
    .toBeVisible();
  await page.getByRole("button", { name: "Recreate sandbox" }).click();
  await expect.element(page.getByText(/Unpushed work from the lost sandbox is gone/)).toBeVisible();
  const confirm = page
    .getByRole("alertdialog")
    .getByRole("button", { name: "Recreate sandbox" })
    .element();
  (confirm as HTMLElement).click();
  await vi.waitFor(() =>
    expect(harness.recreateSandbox).toHaveBeenCalledWith({
      environmentId,
      input: { threadId },
    }),
  );
  expect(harness.deleteSandbox).not.toHaveBeenCalled();
});

it("uses deleted copy after an explicit sandbox delete", async () => {
  harness.state = sandbox({
    status: "lost",
    sandboxId: null,
    canSleep: false,
    canWake: false,
    canDelete: false,
    canRecreate: true,
    lossReason: "deleted",
  });
  await using screen = await render(panel());
  await expect.element(page.getByText(/This sandbox was deleted\./)).toBeVisible();
  expect(screen.container.textContent).not.toContain("deleted outside T3 Code");
});
