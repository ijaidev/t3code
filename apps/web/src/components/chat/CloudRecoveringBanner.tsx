import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { useAtomValue } from "@effect/atom-react";
import { AsyncResult } from "effect/reactivity";
import { useGT } from "gt-react";
import { CloudIcon } from "lucide-react";

import { serverEnvironment } from "../../state/server";
import { Alert, AlertDescription, AlertTitle } from "../ui/alert";

/** A restarted controller reattaches active cloud threads before their turns settle. */
export function CloudRecoveringBanner(props: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
}) {
  const gt = useGT();
  const state = useAtomValue(
    serverEnvironment.cloudSandboxState({
      environmentId: props.environmentId,
      input: { threadId: props.threadId },
    }),
  );

  if (!AsyncResult.isSuccess(state) || state.value.status !== "recovering") return null;

  return (
    <div className="pointer-events-auto mx-auto w-fit max-w-[min(48rem,calc(100%-2rem))] pt-3">
      <Alert variant="info" surface="glass" controlAlignment="first-line">
        <CloudIcon />
        <AlertDescription>
          <AlertTitle>{gt("Reconnecting cloud sandbox")}</AlertTitle>
          <p>{gt("The thread continues when its native session reattaches.")}</p>
        </AlertDescription>
      </Alert>
    </div>
  );
}
