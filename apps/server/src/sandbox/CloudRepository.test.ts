import { describe, expect, it } from "@effect/vitest";

import { publicCloneUrl } from "./CloudRepository.ts";

describe("cloud repository remotes", () => {
  it("keeps a public HTTPS clone URL without credentials", () => {
    expect(publicCloneUrl("https://github.com/acme/public.git")).toEqual({
      type: "url",
      url: "https://github.com/acme/public.git",
    });
  });

  it("rejects an HTTPS remote that embeds a token", () => {
    expect(
      publicCloneUrl("https://x-access-token:ghp_supersecret@github.com/acme/private.git"),
    ).toEqual({ type: "credentials" });
  });

  it("turns an SSH remote into a public HTTPS clone URL", () => {
    expect(publicCloneUrl("git@github.com:acme/public.git")).toEqual({
      type: "url",
      url: "https://github.com/acme/public.git",
    });
  });

  it("rejects a non-git SSH user", () => {
    expect(publicCloneUrl("x-access-token@github.com:acme/private.git")).toEqual({
      type: "credentials",
    });
  });
});
