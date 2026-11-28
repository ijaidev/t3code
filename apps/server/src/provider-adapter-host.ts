import {
  runClaudeHistoryWorkerIfRequested,
  runProviderAdapterHostMain,
} from "./sandbox/ProviderAdapterHostMain.ts";

if (!(await runClaudeHistoryWorkerIfRequested())) {
  await runProviderAdapterHostMain();
}
