import {
  type CloudSandboxDeletePreview,
  type CloudSandboxSleepDisabledReason,
  type CloudSandboxStatus,
  type EnvironmentId,
  type ThreadId,
} from "@t3tools/contracts";
import { useAtomValue } from "@effect/atom-react";
import { AsyncResult } from "effect/reactivity";
import { msg, useGT, useMessages } from "gt-react";
import { CloudIcon, MoonIcon, Trash2Icon } from "lucide-react";
import { useState } from "react";

import { formatElapsedDurationLabel } from "../../timestampFormat";
import { formatEnvironmentQueryError } from "../../state/query";
import { serverEnvironment } from "../../state/server";
import { useAtomCommand } from "../../state/use-atom-command";
import { useNowMinute } from "../../hooks/useNowMinute";
import type { MessageResolver } from "../../i18n/messages";
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
import { Spinner } from "../ui/spinner";
import { toastManager } from "../ui/toast";
import { ThreadDetailsSection } from "./ThreadDetailsSection";

const STATUS_LABELS: Readonly<Record<CloudSandboxStatus, string>> = {
  provisioning: msg("Provisioning"),
  waking: msg("Waking"),
  recovering: msg("Recovering"),
  running: msg("Running"),
  sleeping: msg("Sleeping"),
  deleting: msg("Deleting"),
  lost: msg("Workspace lost"),
  failed: msg("Failed"),
};

const SLEEP_REASONS: Readonly<Record<CloudSandboxSleepDisabledReason, string>> = {
  "provider-no-sleep": msg("This provider cannot pause sandboxes."),
  "provider-unavailable": msg("The provider account is unavailable."),
  "active-turn": msg("This sandbox stays awake while its turn runs."),
  "open-preview": msg("This sandbox stays awake while a preview is open."),
  "not-running": msg("The sandbox is not running."),
};

const riskSummary = (preview: CloudSandboxDeletePreview, resolve: MessageResolver) =>
  [
    preview.risk.changedFiles > 0
      ? resolve(
          msg("{count, plural, one {{count} changed file} other {{count} changed files}}", {
            count: preview.risk.changedFiles,
          }),
        )
      : null,
    preview.risk.unpushedCommits > 0
      ? resolve(
          msg("{count, plural, one {{count} unpushed commit} other {{count} unpushed commits}}", {
            count: preview.risk.unpushedCommits,
          }),
        )
      : null,
  ]
    .filter((part): part is string => part !== null)
    .join(" · ");

export function ThreadCloudSandbox(props: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
}) {
  const gt = useGT();
  const messages = useMessages();
  const nowMs = Date.parse(`${useNowMinute()}:00Z`);
  const state = useAtomValue(
    serverEnvironment.cloudSandboxState({
      environmentId: props.environmentId,
      input: { threadId: props.threadId },
    }),
  );
  const canOperate = useAtomValue(
    serverEnvironment.deleteCloudSandbox.permissionAtom(props.environmentId),
  );
  const sleep = useAtomCommand(serverEnvironment.sleepCloudSandbox, {
    reportFailure: true,
  });
  const wake = useAtomCommand(serverEnvironment.wakeCloudSandbox, {
    reportFailure: true,
  });
  const previewDelete = useAtomCommand(serverEnvironment.previewCloudSandboxDelete, {
    reportFailure: false,
  });
  const deleteSandbox = useAtomCommand(serverEnvironment.deleteCloudSandbox, {
    reportFailure: false,
  });
  const recreateSandbox = useAtomCommand(serverEnvironment.recreateCloudSandbox, {
    reportFailure: false,
  });
  const [preview, setPreview] = useState<CloudSandboxDeletePreview | null>(null);
  const [recreating, setRecreating] = useState(false);
  const [busy, setBusy] = useState(false);

  if (!AsyncResult.isSuccess(state)) return null;

  const sandbox = state.value;
  const showDelete = sandbox.canDelete;
  const showRecreate = sandbox.canRecreate;
  const sleepUnavailableOnProvider =
    sandbox.sleepDisabledReason === "provider-no-sleep" ||
    sandbox.sleepDisabledReason === "provider-unavailable";
  const showSleep = sandbox.status === "running" && !sleepUnavailableOnProvider;
  const showWake = sandbox.status === "sleeping" && sandbox.canWake;

  const reportFailure = (cause: Parameters<typeof formatEnvironmentQueryError>[0]) => {
    toastManager.add({
      type: "error",
      title: gt("The sandbox request failed"),
      description: formatEnvironmentQueryError(cause, messages),
    });
  };

  const requestDelete = async () => {
    setBusy(true);
    const result = await previewDelete({
      environmentId: props.environmentId,
      input: { threadId: props.threadId },
    });
    setBusy(false);
    if (result._tag === "Failure") {
      reportFailure(result.cause);
      return;
    }
    setPreview(result.value);
  };

  const confirmDelete = async () => {
    if (preview === null) return;
    setBusy(true);
    const result = await deleteSandbox({
      environmentId: props.environmentId,
      input: {
        threadId: props.threadId,
        acknowledgedUnpushedChanges: preview.risk.hasUnpushedChanges,
      },
    });
    setBusy(false);
    if (result._tag === "Failure") {
      reportFailure(result.cause);
      return;
    }
    setPreview(null);
  };

  const confirmRecreate = async () => {
    setBusy(true);
    const result = await recreateSandbox({
      environmentId: props.environmentId,
      input: { threadId: props.threadId },
    });
    setBusy(false);
    if (result._tag === "Failure") {
      reportFailure(result.cause);
      return;
    }
    setRecreating(false);
  };

  const detail = [
    sandbox.size?.label ?? null,
    sandbox.region?.label ?? null,
    gt("Up for {duration}", {
      duration: formatElapsedDurationLabel(sandbox.createdAt, nowMs, messages),
    }),
  ]
    .filter((part): part is string => part !== null)
    .join(" · ");
  const lossCopy =
    sandbox.lossReason === "deleted"
      ? gt("This sandbox was deleted. Unpushed work is gone; the transcript is still here.")
      : sandbox.lossReason === "external"
        ? gt(
            "The sandbox was deleted outside T3 Code. Unpushed work is gone; the transcript is still here.",
          )
        : gt("This cloud workspace is gone. Unpushed work is lost; the transcript is still here.");

  return (
    <>
      <ThreadDetailsSection
        headingId="thread-details-cloud-sandbox-heading"
        title={gt("Cloud sandbox")}
        showHeading={false}
      >
        <div className="flex flex-col gap-1.5 px-2.5 text-xs">
          <div className="flex min-w-0 items-center gap-2">
            <CloudIcon className="size-3.5 shrink-0 text-muted-foreground" aria-hidden />
            <span className="min-w-0 truncate font-medium">{sandbox.providerLabel}</span>
            <span className="shrink-0 text-muted-foreground">
              {messages(STATUS_LABELS[sandbox.status])}
            </span>
          </div>
          <div className="truncate text-muted-foreground">{detail}</div>
          {sandbox.sleepDisabledReason !== undefined && sandbox.status === "running" ? (
            <div className="text-muted-foreground">
              {messages(SLEEP_REASONS[sandbox.sleepDisabledReason])}
            </div>
          ) : null}
          {sandbox.status === "lost" ? (
            <div className="text-muted-foreground">{lossCopy}</div>
          ) : null}
          {sandbox.lastError !== undefined ? (
            <div className="text-destructive">{sandbox.lastError}</div>
          ) : null}
          {showSleep || showWake || showDelete || showRecreate ? (
            <div className="flex items-center gap-1 pt-0.5">
              {showRecreate ? (
                <Button
                  size="xs"
                  variant="outline"
                  disabled={!canOperate || busy}
                  onClick={() => setRecreating(true)}
                >
                  <CloudIcon className="size-3" aria-hidden />
                  {gt("Recreate sandbox")}
                </Button>
              ) : null}
              {showSleep ? (
                <Button
                  size="xs"
                  variant="outline"
                  disabled={!canOperate || busy || !sandbox.canSleep}
                  onClick={() =>
                    void sleep({
                      environmentId: props.environmentId,
                      input: { threadId: props.threadId },
                    })
                  }
                >
                  <MoonIcon className="size-3" aria-hidden />
                  {gt("Sleep")}
                </Button>
              ) : null}
              {showWake ? (
                <Button
                  size="xs"
                  variant="outline"
                  disabled={!canOperate || busy}
                  onClick={() =>
                    void wake({
                      environmentId: props.environmentId,
                      input: { threadId: props.threadId },
                    })
                  }
                >
                  <CloudIcon className="size-3" aria-hidden />
                  {gt("Wake")}
                </Button>
              ) : null}
              {showDelete ? (
                <Button
                  size="xs"
                  variant="ghost-muted"
                  disabled={!canOperate || busy}
                  onClick={() => void requestDelete()}
                >
                  {busy ? (
                    <Spinner className="size-3" />
                  ) : (
                    <Trash2Icon className="size-3" aria-hidden />
                  )}
                  {gt("Delete sandbox")}
                </Button>
              ) : null}
            </div>
          ) : null}
        </div>
      </ThreadDetailsSection>

      {preview !== null ? (
        <AlertDialog open onOpenChange={(open) => !open && !busy && setPreview(null)}>
          <AlertDialogPopup>
            <AlertDialogHeader>
              <AlertDialogTitle>{gt("Delete this cloud sandbox?")}</AlertDialogTitle>
              <AlertDialogDescription>
                {preview.risk.hasUnpushedChanges
                  ? gt(
                      "This sandbox has {changes}. Deleting it permanently discards them. The thread transcript stays.",
                      { changes: riskSummary(preview, messages) },
                    )
                  : gt(
                      "This stops provider billing and removes the sandbox workspace. The thread transcript stays.",
                    )}
              </AlertDialogDescription>
            </AlertDialogHeader>
            <AlertDialogFooter>
              <AlertDialogClose render={<Button variant="outline" />} disabled={busy}>
                {gt("Cancel")}
              </AlertDialogClose>
              <Button variant="destructive" disabled={busy} onClick={() => void confirmDelete()}>
                {busy ? <Spinner className="size-3.5" /> : null}
                {gt("Delete sandbox")}
              </Button>
            </AlertDialogFooter>
          </AlertDialogPopup>
        </AlertDialog>
      ) : null}

      {recreating ? (
        <AlertDialog open onOpenChange={(open) => !open && !busy && setRecreating(false)}>
          <AlertDialogPopup>
            <AlertDialogHeader>
              <AlertDialogTitle>{gt("Recreate this cloud sandbox?")}</AlertDialogTitle>
              <AlertDialogDescription>
                {gt(
                  "This clones the thread's branch into a new sandbox. Unpushed work from the lost sandbox is gone.",
                )}
              </AlertDialogDescription>
            </AlertDialogHeader>
            <AlertDialogFooter>
              <AlertDialogClose render={<Button variant="outline" />} disabled={busy}>
                {gt("Cancel")}
              </AlertDialogClose>
              <Button disabled={busy} onClick={() => void confirmRecreate()}>
                {busy ? <Spinner className="size-3.5" /> : null}
                {gt("Recreate sandbox")}
              </Button>
            </AlertDialogFooter>
          </AlertDialogPopup>
        </AlertDialog>
      ) : null}
    </>
  );
}
