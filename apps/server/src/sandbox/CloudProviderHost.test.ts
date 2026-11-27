import { DEFAULT_SERVER_SETTINGS } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";

import * as CloudProviderHost from "./CloudProviderHost.ts";

describe("cloud provider host", () => {
  it("installs the pinned server and OpenCode runtimes into a private prefix", () => {
    const command = CloudProviderHost.cloudProviderHostInstallCommand({
      hostDir: "/home/t3/.t3/provider-host",
      opencodeVersion: "2.4.2",
      nodeVersion: "22.16.0",
    });

    expect(command).toContain("prefix='/home/t3/.t3/provider-host/runtime'");
    expect(command).toContain('npm install --global --prefix "$prefix"');
    expect(command).toContain("'node@22.16.0'");
    expect(command).toContain("'@opencode/cli@2.4.2'");
    expect(command).toContain('test -x "$prefix/bin/node"');
    expect(command).toContain('test -x "$prefix/bin/opencode"');
  });

  it("replaces only the host named by its own pid file", () => {
    const command = CloudProviderHost.cloudProviderHostStartCommand({
      hostDir: "/home/t3/.t3/provider-host",
      bundlePath: "/home/t3/.t3/provider-host/provider-adapter-host.mjs",
    });

    expect(command).toContain("pid_file='/home/t3/.t3/provider-host/host.pid'");
    expect(command).toContain('kill -0 "$pid"');
    expect(command).toContain("cmdline");
    expect(command).toContain("__provider-adapter-host");
    expect(command).toContain('echo $$ > "$pid_file"');
    expect(command).toContain(
      "exec '/home/t3/.t3/provider-host/runtime/bin/node' '/home/t3/.t3/provider-host/provider-adapter-host.mjs' __provider-adapter-host",
    );
  });

  it("keeps provider environment out of the host control channel", () => {
    const environment = CloudProviderHost.cloudProviderHostEnvironment({
      token: "host-token",
      port: 8790,
      build: "t3-provider-adapter-host-v1",
      runtimeDir: "/home/t3/.t3/provider-host/runtime",
      environment: [
        { name: "OPENAI_API_KEY", value: "sk-test" },
        { name: "T3_PROVIDER_HOST_TOKEN", value: "attacker" },
      ],
    });

    expect(environment).toEqual({
      OPENAI_API_KEY: "sk-test",
      PATH: "/home/t3/.t3/provider-host/runtime/bin:/usr/local/bin:/usr/bin:/bin",
      T3_PROVIDER_HOST_TOKEN: "host-token",
      T3_PROVIDER_HOST_PORT: "8790",
      T3_PROVIDER_HOST_BIND: "0.0.0.0",
      T3_PROVIDER_HOST_BUILD: "t3-provider-adapter-host-v1",
    });
  });

  it("parses the listening line and upgrades preview URLs to WebSocket", () => {
    expect(
      CloudProviderHost.parseCloudProviderHostListening(
        JSON.stringify({ type: "t3-provider-adapter-host", state: "listening", port: 9000 }),
      ),
    ).toEqual({ port: 9000 });
    expect(CloudProviderHost.parseCloudProviderHostListening('{"type":"ready"}')).toBeNull();
    expect(CloudProviderHost.providerHostWebSocketUrl("https://8790-sbx.e2b.app")).toBe(
      "wss://8790-sbx.e2b.app/",
    );
  });

  it("binds OpenCode to the sandbox runtime instead of a controller path", () => {
    expect(
      CloudProviderHost.cloudOpenCodeHostConfig(
        {
          binaryPath: "/usr/local/bin/opencode",
          serverUrl: "http://127.0.0.1:4096",
          customModels: [],
        },
        "/home/t3/.t3/provider-host/runtime",
      ),
    ).toEqual({
      binaryPath: "/home/t3/.t3/provider-host/runtime/bin/opencode",
      serverUrl: "",
      serverPassword: "",
      customModels: [],
    });
  });

  it("caps and redacts the public host failure detail", () => {
    expect(
      CloudProviderHost.providerHostFailureDetail(
        `failed with sk-live-secret and e2b_another_secret ${"x".repeat(500)}`,
      ),
    ).toMatch(/^failed with \[redacted\] and \[redacted\]/);
    expect(
      CloudProviderHost.providerHostFailureDetail(`failed with sk-live-secret ${"x".repeat(500)}`)
        .length,
    ).toBeLessThanOrEqual(400);
    expect(CloudProviderHost.providerHostFailureDetail(undefined)).toBe(
      "The sandbox provider host stopped before it was ready.",
    );
  });

  it("sends only non-secret settings to the sandbox host", () => {
    const narrowed = CloudProviderHost.cloudProviderHostSettings({
      ...DEFAULT_SERVER_SETTINGS,
      enableProviderUpdateChecks: false,
      sandboxProviders: {
        "e2b-work": {
          kind: "e2b",
          label: "E2B",
          apiKey: "e2b_super_secret",
          template: "base",
          enabled: true,
        },
      },
      cloudSpeechCredentials: {
        provider: "openai",
        apiKey: "sk-speech-secret",
      },
      providerInstances: {
        opencode: {
          driver: "opencode",
          config: { serverPassword: "sk-provider-secret" },
        },
      },
    } as never);

    expect(narrowed.enableProviderUpdateChecks).toBe(false);
    expect(narrowed.sandboxProviders).toEqual({});
    expect(narrowed.providerInstances).toEqual({});
    expect(JSON.stringify(narrowed)).not.toContain("e2b_super_secret");
    expect(JSON.stringify(narrowed)).not.toContain("sk-speech-secret");
  });

  it("keeps host state and attachments under the sandbox home", () => {
    expect(
      CloudProviderHost.cloudProviderHostPaths("/home/t3", "/home/t3/.t3/threads/thread-1"),
    ).toEqual({
      cwd: "/home/t3/.t3/threads/thread-1",
      baseDir: "/home/t3/.t3/provider-host",
      stateDir: "/home/t3/.t3/provider-host/state",
      providerStatusCacheDir: "/home/t3/.t3/provider-host/cache",
      attachmentsDir: "/home/t3/.t3/attachments",
    });
  });
});
