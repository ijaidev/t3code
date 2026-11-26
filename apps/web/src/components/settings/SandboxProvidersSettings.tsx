import type {
  EnvironmentId,
  SandboxProviderAccountConfig,
  SandboxProviderAccountId,
  SandboxProviderCapabilities,
  SandboxProviderSummary,
  SandboxTestSandbox,
} from "@t3tools/contracts";
import { useAtomValue } from "@effect/atom-react";
import { CloudIcon, PlayIcon, PlusIcon, RefreshCwIcon, Trash2Icon } from "lucide-react";
import { useState } from "react";
import { msg, T, useGT, useMessages, Var } from "gt-react";

import { useEnvironmentSettings } from "../../hooks/useSettings";
import { formatEnvironmentQueryError, useEnvironmentQuery } from "../../state/query";
import { serverEnvironment } from "../../state/server";
import { useAtomCommand } from "../../state/use-atom-command";
import {
  AlertDialog,
  AlertDialogClose,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogPopup,
  AlertDialogTitle,
} from "../ui/alert-dialog";
import { Button } from "../ui/button";
import {
  Dialog,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogPanel,
  DialogPopup,
  DialogTitle,
} from "../ui/dialog";
import { Input } from "../ui/input";
import { Label } from "../ui/label";
import { Spinner } from "../ui/spinner";
import { toastManager } from "../ui/toast";
import { SecretInput } from "./SecretInput";
import { useSettingsScope } from "./SettingsScopeContext";
import { SettingsPageContainer, SettingsRow, SettingsSection } from "./settingsLayout";
import { useSearchableSetting } from "./settingsSearch";
import type { MessageResolver } from "../../i18n/messages";

const PROVIDER_STATUS_LABELS: Readonly<Record<SandboxTestSandbox["status"], string>> = {
  provisioning: msg("Provisioning"),
  running: msg("Running"),
  paused: msg("Paused"),
  deleting: msg("Deleting"),
};

const STATUS_CLASSNAMES: Readonly<Record<SandboxTestSandbox["status"], string>> = {
  provisioning: "text-warning",
  running: "text-success",
  paused: "text-muted-foreground",
  deleting: "text-muted-foreground",
};

const CAPABILITY_LABELS = {
  create: msg("create"),
  exec: msg("exec"),
  pauseResume: msg("pause/resume"),
  snapshots: msg("snapshots"),
  privatePorts: msg("private ports"),
};

const capabilitySummary = (
  capabilities: SandboxProviderCapabilities,
  resolve: MessageResolver,
): string => {
  const values = [
    capabilities.create ? CAPABILITY_LABELS.create : null,
    capabilities.exec ? CAPABILITY_LABELS.exec : null,
    capabilities.pauseResume ? CAPABILITY_LABELS.pauseResume : null,
    capabilities.snapshots ? CAPABILITY_LABELS.snapshots : null,
    capabilities.privatePorts ? CAPABILITY_LABELS.privatePorts : null,
  ].flatMap((value) => (value === null ? [] : [resolve(value)]));
  return values.join(" · ");
};

export function SandboxProvidersSettingsPanel() {
  const { scope, environment } = useSettingsScope();
  const setting = useSearchableSetting();
  if (environment === null || environment.connection.phase !== "connected") {
    return (
      <SettingsPageContainer>
        <SettingsSection {...setting("sandbox-providers")}>
          <p className="px-4 py-3 text-sm text-muted-foreground">
            <T>Connect an environment to manage the sandbox providers it can use.</T>
          </p>
        </SettingsSection>
      </SettingsPageContainer>
    );
  }
  if (environment.serverConfig?.sandboxProviders !== true) {
    return (
      <SettingsPageContainer>
        <SettingsSection {...setting("sandbox-providers")}>
          <p className="px-4 py-3 text-sm text-muted-foreground">
            <T>
              Update T3 Code on <Var>{environment.label}</Var> to manage its sandbox providers.
            </T>
          </p>
        </SettingsSection>
      </SettingsPageContainer>
    );
  }
  return (
    <EnvironmentSandboxProviders
      key={environment.environmentId}
      environmentId={environment.environmentId}
      environmentLabel={environment.label}
      projectScoped={scope.kind === "project" || scope.kind === "checkout"}
    />
  );
}

function EnvironmentSandboxProviders(props: {
  readonly environmentId: EnvironmentId;
  readonly environmentLabel: string;
  readonly projectScoped: boolean;
}) {
  const gt = useGT();
  const messages = useMessages();
  const { environmentId, environmentLabel, projectScoped } = props;
  const setting = useSearchableSetting();
  const settings = useEnvironmentSettings(environmentId);
  const canManage = useAtomValue(
    serverEnvironment.sandboxConnectAccount.permissionAtom(environmentId),
  );
  const providersQuery = useEnvironmentQuery(
    serverEnvironment.sandboxProviders({ environmentId, input: {} }),
    messages,
  );
  const sandboxesQuery = useEnvironmentQuery(
    serverEnvironment.sandboxTestSandboxes({ environmentId, input: {} }),
    messages,
  );
  const connectAccount = useAtomCommand(serverEnvironment.sandboxConnectAccount, {
    reportFailure: false,
  });
  const testAccount = useAtomCommand(serverEnvironment.sandboxTestAccount, {
    reportFailure: false,
  });
  const disconnectAccount = useAtomCommand(serverEnvironment.sandboxDisconnectAccount, {
    reportFailure: false,
  });
  const createSandbox = useAtomCommand(serverEnvironment.sandboxCreateTestSandbox, {
    reportFailure: false,
  });
  const pauseSandbox = useAtomCommand(serverEnvironment.sandboxPauseTestSandbox, {
    reportFailure: false,
  });
  const resumeSandbox = useAtomCommand(serverEnvironment.sandboxResumeTestSandbox, {
    reportFailure: false,
  });
  const deleteSandbox = useAtomCommand(serverEnvironment.sandboxDeleteTestSandbox, {
    reportFailure: false,
  });
  const checkSandbox = useAtomCommand(serverEnvironment.sandboxCheckTestSandbox, {
    reportFailure: false,
  });
  const updateSettings = useAtomCommand(serverEnvironment.updateSettings, {
    reportFailure: false,
  });
  const [connecting, setConnecting] = useState<SandboxProviderSummary | null>(null);
  const [disconnecting, setDisconnecting] = useState<SandboxProviderAccountId | null>(null);
  const [deleting, setDeleting] = useState<SandboxTestSandbox | null>(null);
  const [provisioningAccount, setProvisioningAccount] = useState<SandboxProviderAccountId | null>(
    null,
  );
  const [busy, setBusy] = useState(false);
  const accounts = Object.entries(settings.sandboxProviders) as Array<
    [SandboxProviderAccountId, SandboxProviderAccountConfig]
  >;
  const sandboxes = sandboxesQuery.data?.sandboxes ?? [];
  const providers = providersQuery.data?.providers ?? [];
  const readOnly = projectScoped || !canManage;
  const accountLabel = (accountId: SandboxProviderAccountId) => {
    const entry = accounts.find(([id]) => id === accountId);
    return entry?.[1].label ?? accountId;
  };

  const reportFailure = (cause: Parameters<typeof formatEnvironmentQueryError>[0]) => {
    toastManager.add({
      type: "error",
      title: gt("The sandbox request failed"),
      description: formatEnvironmentQueryError(cause, messages),
    });
  };

  const refreshSandboxes = () => sandboxesQuery.refresh();

  const run = async (
    action: () => Promise<
      | { readonly _tag: "Success" }
      | {
          readonly _tag: "Failure";
          readonly cause: Parameters<typeof formatEnvironmentQueryError>[0];
        }
    >,
    success: string,
  ) => {
    setBusy(true);
    const result = await action();
    setBusy(false);
    if (result._tag === "Failure") {
      reportFailure(result.cause);
      return false;
    }
    toastManager.add({ type: "success", title: success });
    refreshSandboxes();
    return true;
  };

  return (
    <SettingsPageContainer>
      <SettingsSection {...setting("sandbox-providers")}>
        {providersQuery.error !== null ? (
          <p className="px-4 py-3 text-sm text-destructive">{providersQuery.error}</p>
        ) : null}
        {providers.length === 0 && providersQuery.isPending ? (
          <SettingsRow title={<T>Loading sandbox providers…</T>} />
        ) : null}
        {providers.map((provider) => (
          <SettingsRow
            key={provider.kind}
            title={provider.label}
            description={gt(
              "{provider} sandboxes for cloud work. The API key stays on {environment}.",
              {
                provider: provider.label,
                environment: environmentLabel,
              },
            )}
            status={capabilitySummary(provider.capabilities, messages)}
            control={
              <Button
                size="xs"
                variant="outline"
                disabled={readOnly}
                onClick={() => setConnecting(provider)}
              >
                <PlusIcon className="size-3" aria-hidden />
                <T>Connect</T>
              </Button>
            }
          />
        ))}
      </SettingsSection>

      <SettingsSection
        title={gt("Accounts")}
        headerAction={
          <span className="text-xs text-muted-foreground">
            {gt("{count} connected", { count: accounts.length })}
          </span>
        }
      >
        {accounts.length === 0 ? (
          <SettingsRow
            title={gt("No sandbox accounts")}
            description={gt("Connect a provider to create a test sandbox.")}
          />
        ) : null}
        {accounts.map(([id, account]) => (
          <SettingsRow
            key={id}
            title={account.label}
            description={gt("{kind} · template {template}", {
              kind: account.kind,
              template: account.template,
            })}
            control={
              <div className="flex flex-wrap items-center justify-end gap-1.5">
                <Button
                  size="xs"
                  variant="ghost"
                  disabled={readOnly || busy}
                  onClick={() =>
                    void run(
                      () => testAccount({ environmentId, input: { accountId: id } }),
                      gt("{label} connection works.", { label: account.label }),
                    )
                  }
                >
                  <T>Test</T>
                </Button>
                <Button
                  size="xs"
                  variant="outline"
                  disabled={readOnly || busy}
                  onClick={() => {
                    setProvisioningAccount(id);
                    void run(
                      () => createSandbox({ environmentId, input: { accountId: id } }),
                      gt("Test sandbox created on {account}.", { account: account.label }),
                    ).finally(() => setProvisioningAccount(null));
                  }}
                >
                  <T>Create test sandbox</T>
                </Button>
                <Button
                  size="icon-sm"
                  variant="ghost-muted"
                  aria-label={gt("Disconnect {label}", { label: account.label })}
                  disabled={readOnly || busy}
                  onClick={() => setDisconnecting(id)}
                >
                  <Trash2Icon className="size-3.5" />
                </Button>
              </div>
            }
          />
        ))}
      </SettingsSection>

      <SettingsSection title={gt("Lifecycle")}>
        <SettingsRow
          title={gt("Sleep after")}
          description={gt(
            "Idle minutes before a cloud sandbox sleeps. A running turn or open preview keeps it awake.",
          )}
          control={
            <div className="flex items-center gap-1.5">
              <Input
                key={settings.cloudSandboxIdleMinutes}
                type="number"
                min={1}
                max={1440}
                defaultValue={settings.cloudSandboxIdleMinutes}
                disabled={readOnly}
                className="w-20"
                aria-label={gt("Cloud sandbox idle minutes")}
                onBlur={(event) => {
                  const value = Number.parseInt(event.target.value, 10);
                  if (!Number.isFinite(value) || value < 1 || value > 1440) {
                    event.target.value = String(settings.cloudSandboxIdleMinutes);
                    return;
                  }
                  if (value === settings.cloudSandboxIdleMinutes) return;
                  void updateSettings({
                    environmentId,
                    input: { patch: { cloudSandboxIdleMinutes: value } },
                  }).then((result) => {
                    if (result._tag === "Failure") {
                      reportFailure(result.cause);
                      event.target.value = String(settings.cloudSandboxIdleMinutes);
                    }
                  });
                }}
                onKeyDown={(event) => {
                  if (event.key === "Enter") event.currentTarget.blur();
                }}
              />
              <span className="text-xs text-muted-foreground">{gt("minutes")}</span>
            </div>
          }
        />
      </SettingsSection>

      <SettingsSection
        title={gt("Test sandboxes")}
        headerAction={
          <Button
            size="xs"
            variant="ghost"
            disabled={sandboxesQuery.isPending}
            onClick={refreshSandboxes}
          >
            <RefreshCwIcon
              className={sandboxesQuery.isPending ? "size-3 animate-spin" : "size-3"}
            />
            <T>Refresh</T>
          </Button>
        }
      >
        {sandboxesQuery.error !== null ? (
          <p className="px-4 py-3 text-sm text-destructive">{sandboxesQuery.error}</p>
        ) : null}
        {provisioningAccount !== null ? (
          <SettingsRow
            title={
              <span className="inline-flex items-center gap-2">
                <CloudIcon className="size-3.5 text-muted-foreground" aria-hidden />
                <span>{accountLabel(provisioningAccount)}</span>
                <span className="text-warning">
                  {messages(PROVIDER_STATUS_LABELS.provisioning)}
                </span>
              </span>
            }
            description={gt("Provisioning the sandbox on {account}…", {
              account: accountLabel(provisioningAccount),
            })}
            control={<Spinner className="size-3.5" />}
          />
        ) : null}
        {sandboxes.length === 0 && provisioningAccount === null && !sandboxesQuery.isPending ? (
          <SettingsRow
            title={gt("No test sandboxes")}
            description={gt(
              "Create one from an account to check the provider and the sandbox thread host.",
            )}
          />
        ) : null}
        {sandboxes.map((sandbox) => {
          const health = sandbox.lastHealth;
          return (
            <SettingsRow
              key={sandbox.id}
              title={
                <span className="inline-flex items-center gap-2">
                  <CloudIcon className="size-3.5 text-muted-foreground" aria-hidden />
                  <span>{accountLabel(sandbox.accountId)}</span>
                  <span className={STATUS_CLASSNAMES[sandbox.status]}>
                    {messages(PROVIDER_STATUS_LABELS[sandbox.status])}
                  </span>
                </span>
              }
              description={
                health?.status === "ready"
                  ? gt("Thread host ready · protocol {version}", {
                      version: health.protocolVersion ?? "?",
                    })
                  : sandbox.lastError !== undefined
                    ? sandbox.lastError
                    : gt("{kind} sandbox · {status}", {
                        kind: sandbox.providerKind,
                        status: sandbox.status,
                      })
              }
              control={
                <div className="flex flex-wrap items-center justify-end gap-1.5">
                  {sandbox.status === "running" ? (
                    <>
                      <Button
                        size="xs"
                        variant="outline"
                        disabled={readOnly || busy}
                        onClick={() =>
                          void run(
                            () =>
                              checkSandbox({
                                environmentId,
                                input: { sandboxId: sandbox.id },
                              }),
                            gt("Thread host ready."),
                          )
                        }
                      >
                        <PlayIcon className="size-3" aria-hidden />
                        <T>Health check</T>
                      </Button>
                      <Button
                        size="xs"
                        variant="ghost"
                        disabled={readOnly || busy}
                        onClick={() =>
                          void run(
                            () =>
                              pauseSandbox({
                                environmentId,
                                input: { sandboxId: sandbox.id },
                              }),
                            gt("Sandbox paused."),
                          )
                        }
                      >
                        <T>Pause</T>
                      </Button>
                    </>
                  ) : null}
                  {sandbox.status === "paused" ? (
                    <Button
                      size="xs"
                      variant="outline"
                      disabled={readOnly || busy}
                      onClick={() =>
                        void run(
                          () =>
                            resumeSandbox({
                              environmentId,
                              input: { sandboxId: sandbox.id },
                            }),
                          gt("Sandbox resumed."),
                        )
                      }
                    >
                      <T>Resume</T>
                    </Button>
                  ) : null}
                  {sandbox.status === "provisioning" || sandbox.status === "deleting" ? (
                    <Spinner className="size-3.5" />
                  ) : null}
                  <Button
                    size="icon-sm"
                    variant="ghost-muted"
                    aria-label={gt("Delete test sandbox")}
                    disabled={readOnly || busy}
                    onClick={() => setDeleting(sandbox)}
                  >
                    <Trash2Icon className="size-3.5" />
                  </Button>
                </div>
              }
            />
          );
        })}
      </SettingsSection>

      {connecting !== null ? (
        <ConnectSandboxAccountDialog
          provider={connecting}
          environmentLabel={environmentLabel}
          canManage={!readOnly}
          onClose={() => setConnecting(null)}
          onConnect={async (label, apiKey, template) => {
            const result = await connectAccount({
              environmentId,
              input: {
                config: {
                  kind: connecting.kind,
                  label,
                  apiKey,
                  template,
                  enabled: true,
                },
              },
            });
            if (result._tag === "Failure") {
              return formatEnvironmentQueryError(result.cause, messages);
            }
            toastManager.add({
              type: "success",
              title: gt("{label} connected.", { label }),
            });
            setConnecting(null);
            return null;
          }}
        />
      ) : null}

      {disconnecting !== null ? (
        <AlertDialog open onOpenChange={(open) => !open && setDisconnecting(null)}>
          <AlertDialogPopup>
            <AlertDialogHeader>
              <AlertDialogTitle>
                <T>Disconnect this sandbox account?</T>
              </AlertDialogTitle>
              <AlertDialogDescription>
                <T>
                  Its saved API key and test sandboxes are deleted. Any future cloud thread target
                  from it disappears.
                </T>
              </AlertDialogDescription>
            </AlertDialogHeader>
            <AlertDialogFooter>
              <AlertDialogClose render={<Button variant="outline" />}>
                <T>Cancel</T>
              </AlertDialogClose>
              <Button
                variant="destructive"
                disabled={busy}
                onClick={() => {
                  const id = disconnecting;
                  void run(
                    () => disconnectAccount({ environmentId, input: { accountId: id } }),
                    gt("Sandbox account disconnected."),
                  ).then((ok) => {
                    if (ok) setDisconnecting(null);
                  });
                }}
              >
                <T>Disconnect</T>
              </Button>
            </AlertDialogFooter>
          </AlertDialogPopup>
        </AlertDialog>
      ) : null}

      {deleting !== null ? (
        <AlertDialog open onOpenChange={(open) => !open && setDeleting(null)}>
          <AlertDialogPopup>
            <AlertDialogHeader>
              <AlertDialogTitle>
                <T>Delete this test sandbox?</T>
              </AlertDialogTitle>
              <AlertDialogDescription>
                <T>
                  This stops provider billing for it. The thread host and everything in the sandbox
                  are removed.
                </T>
              </AlertDialogDescription>
            </AlertDialogHeader>
            <AlertDialogFooter>
              <AlertDialogClose render={<Button variant="outline" />}>
                <T>Cancel</T>
              </AlertDialogClose>
              <Button
                variant="destructive"
                disabled={busy}
                onClick={() => {
                  const sandbox = deleting;
                  void run(
                    () =>
                      deleteSandbox({
                        environmentId,
                        input: { sandboxId: sandbox.id },
                      }),
                    gt("Test sandbox deleted."),
                  ).then((ok) => {
                    if (ok) setDeleting(null);
                  });
                }}
              >
                <T>Delete</T>
              </Button>
            </AlertDialogFooter>
          </AlertDialogPopup>
        </AlertDialog>
      ) : null}
    </SettingsPageContainer>
  );
}

function ConnectSandboxAccountDialog(props: {
  readonly provider: SandboxProviderSummary;
  readonly environmentLabel: string;
  readonly canManage: boolean;
  readonly onClose: () => void;
  readonly onConnect: (label: string, apiKey: string, template: string) => Promise<string | null>;
}) {
  const gt = useGT();
  const [label, setLabel] = useState(props.provider.label);
  const [apiKey, setApiKey] = useState("");
  const [template, setTemplate] = useState("base");
  const [submitted, setSubmitted] = useState(false);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const labelError =
    submitted && label.trim().length === 0 ? gt("Enter a name for this account.") : null;
  const keyError =
    submitted && apiKey.trim().length === 0 ? gt("Enter the provider API key.") : null;
  const templateError =
    submitted && template.trim().length === 0 ? gt("Enter the template to start from.") : null;
  const save = async () => {
    setSubmitted(true);
    setError(null);
    if (labelError !== null || keyError !== null || templateError !== null) return;
    setPending(true);
    const failure = await props.onConnect(label.trim(), apiKey.trim(), template.trim());
    setPending(false);
    if (failure !== null) setError(failure);
  };
  return (
    <Dialog
      open
      onOpenChange={(open) => {
        if (!open) props.onClose();
      }}
    >
      <DialogPopup className="max-w-md">
        <DialogHeader>
          <DialogTitle>{gt("Connect {provider}", { provider: props.provider.label })}</DialogTitle>
          <DialogDescription>
            <T>
              The API key is stored on <Var>{props.environmentLabel}</Var> and never sent to a
              sandbox or a client.
            </T>
          </DialogDescription>
        </DialogHeader>
        <DialogPanel>
          <form
            id="sandbox-account-form"
            className="grid gap-4"
            onSubmit={(event) => {
              event.preventDefault();
              void save();
            }}
          >
            <div className="grid gap-1.5">
              <Label htmlFor="sandbox-account-label">
                <T>Name</T>
              </Label>
              <Input
                id="sandbox-account-label"
                value={label}
                maxLength={64}
                disabled={!props.canManage || pending}
                onChange={(event) => setLabel(event.target.value)}
                aria-invalid={labelError !== null}
                autoFocus
              />
              {labelError !== null ? (
                <p role="alert" className="text-xs text-destructive">
                  {labelError}
                </p>
              ) : null}
            </div>
            <div className="grid gap-1.5">
              <Label htmlFor="sandbox-account-key">
                <T>API key</T>
              </Label>
              <SecretInput
                id="sandbox-account-key"
                value={apiKey}
                disabled={!props.canManage || pending}
                onChange={(event) => setApiKey(event.target.value)}
                aria-invalid={keyError !== null}
              />
              {keyError !== null ? (
                <p role="alert" className="text-xs text-destructive">
                  {keyError}
                </p>
              ) : null}
            </div>
            <div className="grid gap-1.5">
              <Label htmlFor="sandbox-account-template">
                <T>Template</T>
              </Label>
              <Input
                id="sandbox-account-template"
                value={template}
                maxLength={128}
                disabled={!props.canManage || pending}
                onChange={(event) => setTemplate(event.target.value)}
                aria-invalid={templateError !== null}
              />
              <p className="text-xs text-muted-foreground">
                <T>The provider's project image or template to start a sandbox from.</T>
              </p>
              {templateError !== null ? (
                <p role="alert" className="text-xs text-destructive">
                  {templateError}
                </p>
              ) : null}
            </div>
            {error !== null ? (
              <p role="alert" className="text-sm text-destructive">
                {error}
              </p>
            ) : null}
          </form>
        </DialogPanel>
        <DialogFooter>
          <Button variant="outline" disabled={pending} onClick={props.onClose}>
            <T>Cancel</T>
          </Button>
          <Button type="submit" form="sandbox-account-form" disabled={!props.canManage || pending}>
            {pending ? <Spinner className="size-3.5" /> : null}
            <T>Connect</T>
          </Button>
        </DialogFooter>
      </DialogPopup>
    </Dialog>
  );
}
