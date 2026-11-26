import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { useAtomValue } from "@effect/atom-react";
import { AsyncResult } from "effect/reactivity";
import { useGT, useMessages } from "gt-react";
import { CloudIcon } from "lucide-react";
import { useState } from "react";

import { formatEnvironmentQueryError } from "../../state/query";
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
import { Alert, AlertAction, AlertDescription, AlertTitle } from "../ui/alert";
import { Button } from "../ui/button";
import { Spinner } from "../ui/spinner";
import { toastManager } from "../ui/toast";

/** A lost cloud workspace stays visible above the transcript until it is recreated. */
export function CloudWorkspaceLostBanner(props: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
}) {
  const gt = useGT();
  const messages = useMessages();
  const state = useAtomValue(
    serverEnvironment.cloudSandboxState({
      environmentId: props.environmentId,
      input: { threadId: props.threadId },
    }),
  );
  const canOperate = useAtomValue(
    serverEnvironment.recreateCloudSandbox.permissionAtom(props.environmentId),
  );
  const recreateSandbox = useAtomCommand(serverEnvironment.recreateCloudSandbox, {
    reportFailure: false,
  });
  const [confirming, setConfirming] = useState(false);
  const [busy, setBusy] = useState(false);

  if (!AsyncResult.isSuccess(state) || state.value.status !== "lost" || !state.value.canRecreate) {
    return null;
  }

  const confirm = async () => {
    setBusy(true);
    const result = await recreateSandbox({
      environmentId: props.environmentId,
      input: { threadId: props.threadId },
    });
    setBusy(false);
    if (result._tag === "Failure") {
      toastManager.add({
        type: "error",
        title: gt("The sandbox could not be recreated"),
        description: formatEnvironmentQueryError(result.cause, messages),
      });
      return;
    }
    setConfirming(false);
  };

  return (
    <>
      <div className="pointer-events-auto mx-auto w-fit max-w-[min(48rem,calc(100%-2rem))] pt-3">
        <Alert variant="warning" surface="glass" controlAlignment="first-line">
          <CloudIcon />
          <AlertDescription>
            <AlertTitle>{gt("Cloud workspace lost")}</AlertTitle>
            <p>
              {gt(
                "The sandbox was deleted outside T3 Code. Unpushed work is gone; the transcript is still here.",
              )}
            </p>
          </AlertDescription>
          <AlertAction>
            <Button
              size="sm"
              variant="outline"
              disabled={!canOperate || busy}
              onClick={() => setConfirming(true)}
            >
              {gt("Recreate sandbox")}
            </Button>
          </AlertAction>
        </Alert>
      </div>

      {confirming ? (
        <AlertDialog open onOpenChange={(open) => !open && !busy && setConfirming(false)}>
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
              <Button disabled={busy} onClick={() => void confirm()}>
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
