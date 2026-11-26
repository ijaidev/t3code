import {
  type CloudSandboxSleepDisabledReason,
  type CloudSandboxStatus,
  type EnvironmentId,
  type ThreadId,
} from "@t3tools/contracts";
import { useAtomValue } from "@effect/atom-react";
import { AsyncResult } from "effect/reactivity";
import { msg, useGT, useMessages } from "gt-react-native";

import * as Dialog from "../../components/ui/Dialog";
import type { AppBarMenu, AppBarMenuItem } from "../../components/ui/AppBar.types";
import { useNowMinuteMs } from "../../lib/clock";
import { formatEnvironmentQueryError } from "../../state/query";
import { serverEnvironment } from "../../state/server";
import { useAtomCommand } from "../../state/use-atom-command";

const STATUS_LABELS: Readonly<Record<CloudSandboxStatus, string>> = {
  provisioning: msg("Provisioning"),
  waking: msg("Waking"),
  running: msg("Running"),
  sleeping: msg("Sleeping"),
  deleting: msg("Deleting"),
  lost: msg("Workspace lost"),
  failed: msg("Failed"),
};

const SLEEP_REASONS: Readonly<Record<CloudSandboxSleepDisabledReason, string>> = {
  "provider-no-sleep": msg("This provider cannot pause sandboxes."),
  "provider-unavailable": msg("The provider account is unavailable."),
  "active-turn": msg("Awake while its turn runs."),
  "open-preview": msg("Awake while a preview is open."),
  "not-running": msg("The sandbox is not running."),
};

const minutesLabel = (minutes: number) =>
  minutes < 60
    ? msg("{count, plural, one {{count}m} other {{count}m}}", { count: minutes })
    : msg("{count, plural, one {{count}h} other {{count}h}}", {
        count: Math.floor(minutes / 60),
      });

export function useCloudSandboxMenu(props: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
}): { readonly subtitle: string | null; readonly menu: AppBarMenu | null } {
  const gt = useGT();
  const messages = useMessages();
  const nowMs = useNowMinuteMs();
  const state = useAtomValue(
    serverEnvironment.cloudSandboxState({
      environmentId: props.environmentId,
      input: { threadId: props.threadId },
    }),
  );
  const canOperate = useAtomValue(
    serverEnvironment.deleteCloudSandbox.permissionAtom(props.environmentId),
  );
  const sleep = useAtomCommand(serverEnvironment.sleepCloudSandbox);
  const wake = useAtomCommand(serverEnvironment.wakeCloudSandbox);
  const previewDelete = useAtomCommand(serverEnvironment.previewCloudSandboxDelete, {
    reportFailure: false,
  });
  const deleteSandbox = useAtomCommand(serverEnvironment.deleteCloudSandbox, {
    reportFailure: false,
  });

  if (!AsyncResult.isSuccess(state)) return { subtitle: null, menu: null };
  const sandbox = state.value;
  const status = messages(STATUS_LABELS[sandbox.status]);
  const detail = [
    sandbox.providerLabel,
    sandbox.size?.label ?? null,
    sandbox.region?.label ?? null,
    gt("Up {duration}", {
      duration: messages(
        minutesLabel(Math.max(0, Math.floor((nowMs - Date.parse(sandbox.createdAt)) / 60_000))),
      ),
    }),
  ]
    .filter((part): part is string => part !== null)
    .join(" · ");

  const reportFailure = (cause: Parameters<typeof formatEnvironmentQueryError>[0]) => {
    Dialog.alert(gt("The sandbox request failed"), formatEnvironmentQueryError(cause, messages));
  };

  const requestDelete = async () => {
    const preview = await previewDelete({
      environmentId: props.environmentId,
      input: { threadId: props.threadId },
    });
    if (preview._tag === "Failure") {
      reportFailure(preview.cause);
      return;
    }
    const changes = [
      preview.value.risk.changedFiles > 0
        ? messages(
            msg("{count, plural, one {{count} changed file} other {{count} changed files}}", {
              count: preview.value.risk.changedFiles,
            }),
          )
        : null,
      preview.value.risk.unpushedCommits > 0
        ? messages(
            msg("{count, plural, one {{count} unpushed commit} other {{count} unpushed commits}}", {
              count: preview.value.risk.unpushedCommits,
            }),
          )
        : null,
    ]
      .filter((part): part is string => part !== null)
      .join(" · ");
    Dialog.confirm({
      title: gt("Delete this cloud sandbox?"),
      message: preview.value.risk.hasUnpushedChanges
        ? gt(
            "This sandbox has {changes}. Deleting it permanently discards them. The thread transcript stays.",
            { changes },
          )
        : gt(
            "This stops provider billing and removes the sandbox workspace. The thread transcript stays.",
          ),
      confirmText: gt("Delete sandbox"),
      destructive: true,
      onConfirm: () => {
        void deleteSandbox({
          environmentId: props.environmentId,
          input: {
            threadId: props.threadId,
            acknowledgedUnpushedChanges: preview.value.risk.hasUnpushedChanges,
          },
        }).then((result) => {
          if (result._tag === "Failure") reportFailure(result.cause);
        });
      },
    });
  };

  const items: Array<AppBarMenuItem> = [
    {
      id: "cloud-sandbox-detail",
      title: detail,
      icon: "cloud",
      disabled: true,
      onPress: () => {},
    },
  ];
  if (sandbox.status === "running") {
    const providerCannotSleep =
      sandbox.sleepDisabledReason === "provider-no-sleep" ||
      sandbox.sleepDisabledReason === "provider-unavailable";
    items.push({
      id: "cloud-sandbox-sleep",
      title: providerCannotSleep ? gt("Sleep unavailable") : gt("Sleep sandbox"),
      icon: "moon",
      disabled: providerCannotSleep || !canOperate || !sandbox.canSleep,
      ...(sandbox.sleepDisabledReason === undefined
        ? {}
        : { subtitle: messages(SLEEP_REASONS[sandbox.sleepDisabledReason]) }),
      onPress: () => {
        if (providerCannotSleep) return;
        void sleep({
          environmentId: props.environmentId,
          input: { threadId: props.threadId },
        });
      },
    });
  }
  if (sandbox.status === "sleeping" && sandbox.canWake) {
    items.push({
      id: "cloud-sandbox-wake",
      title: gt("Wake sandbox"),
      icon: "cloud",
      disabled: !canOperate,
      onPress: () => {
        void wake({
          environmentId: props.environmentId,
          input: { threadId: props.threadId },
        });
      },
    });
  }
  if (sandbox.canDelete) {
    items.push({
      id: "cloud-sandbox-delete",
      title: gt("Delete sandbox"),
      icon: "trash",
      destructive: true,
      disabled: !canOperate,
      onPress: () => void requestDelete(),
    });
  }
  if (items.length === 1 && sandbox.status !== "running" && sandbox.status !== "sleeping") {
    return { subtitle: gt("Cloud · {status}", { status }), menu: null };
  }

  return {
    subtitle: gt("Cloud · {status}", { status }),
    menu: {
      title: gt("Cloud sandbox"),
      icon: "cloud",
      status,
      items,
    },
  };
}
