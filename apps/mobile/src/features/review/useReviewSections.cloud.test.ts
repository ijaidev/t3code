import { EnvironmentId } from "@t3tools/contracts";
import { expect, it } from "vite-plus/test";

import { reviewDiffPreviewRequest } from "./reviewDiffPreviewRequest";

it("builds a cloud thread's Git review request from the sandbox workspace", () => {
  expect(
    reviewDiffPreviewRequest(
      EnvironmentId.make("environment"),
      "/home/t3/.t3/threads/thread-cloud",
      "branch-range",
    ),
  ).toEqual({
    environmentId: "environment",
    input: {
      cwd: "/home/t3/.t3/threads/thread-cloud",
      sourceKind: "branch-range",
    },
  });
});
