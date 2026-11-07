import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";
import { Miniflare } from "miniflare";

const root = fileURLToPath(new URL("..", import.meta.url));
const anonymousHash = "2f183a4e64493af3f377f745eda502363cd3e7ef6e4d266d444758de0a85fcc8";

function jsonFrame(type, payload) {
  const bytes = new TextEncoder().encode(JSON.stringify(payload));
  const frame = new Uint8Array(6 + bytes.length);
  frame.set([0x04, type]);
  new DataView(frame.buffer).setUint32(2, bytes.length);
  frame.set(bytes, 6);
  return frame;
}

function inbox(socket) {
  const queue = [];
  const waiting = [];
  socket.addEventListener("message", (event) => {
    const frame = new Uint8Array(event.data);
    const resolve = waiting.shift();
    if (resolve) resolve(frame);
    else queue.push(frame);
  });
  return async (type) => {
    for (;;) {
      const frame = queue.length
        ? queue.shift()
        : await new Promise((resolve, reject) => {
            const timer = setTimeout(() => reject(new Error(`timed out waiting for message type ${type}`)), 5000);
            waiting.push((value) => {
              clearTimeout(timer);
              resolve(value);
            });
          });
      if (frame[1] === type) return frame;
    }
  };
}

test("reserved hostnames block other accounts and forced temporary mode rotates them", { timeout: 30000 }, async (context) => {
  const bundle = await build({
    absWorkingDir: root,
    entryPoints: ["src/index.ts"],
    bundle: true,
    format: "esm",
    platform: "browser",
    external: ["cloudflare:workers"],
    write: false,
  });
  const password = randomBytes(20).toString("hex");
  const worker = new Miniflare({
    modules: true,
    script: bundle.outputFiles[0].text,
    compatibilityDate: "2025-04-13",
    durableObjects: {
      RELAY: { className: "Relay", useSQLite: true },
      TOKEN: { className: "Token", useSQLite: true },
      ZONE: { className: "Zone", useSQLite: true },
    },
    d1Databases: { DB: "reservations-test" },
    bindings: {
      ADMIN_USERNAME: "admin",
      ADMIN_PASSWORD: password,
      WEB_ZONE: "example.com",
      BASE_HOST: "localhost",
    },
    outboundService: () => new Response("colo=TEST\n"),
  });
  context.after(async () => worker.dispose());

  const login = await worker.dispatchFetch("http://localhost/admin?login", {
    method: "POST",
    redirect: "manual",
    body: new URLSearchParams({ username: "admin", password }),
  });
  assert.equal(login.status, 303);
  const session = login.headers.get("set-cookie")?.split(";", 1)[0];
  assert.ok(session?.startsWith("ls_session="));
  const headers = { Cookie: session };

  const settingsPage = await worker.dispatchFetch("http://localhost/admin?tab=settings", { headers });
  const settingsHtml = await settingsPage.text();
  const csrf = settingsHtml.match(/name="csrf" value="([^"]+)"/)?.[1];
  assert.ok(csrf);

  const allowCustomHosts = await worker.dispatchFetch("http://localhost/admin/action", {
    method: "POST",
    redirect: "manual",
    headers,
    body: new URLSearchParams({
      csrf,
      action: "saveSettings",
      scope: "global",
      allowAnonymous: "1",
      allowCustomHost: "1",
    }),
  });
  assert.equal(allowCustomHosts.status, 303);

  const created = await worker.dispatchFetch("http://localhost/admin/action", {
    method: "POST",
    redirect: "manual",
    headers,
    body: new URLSearchParams({
      csrf,
      action: "createUser",
      username: "reserved-owner",
      password: randomBytes(20).toString("hex"),
    }),
  });
  assert.equal(created.status, 303);
  const userId = new URL(created.headers.get("location")).searchParams.get("user");
  assert.ok(userId);

  const reserved = await worker.dispatchFetch("http://localhost/admin/action", {
    method: "POST",
    redirect: "manual",
    headers,
    body: new URLSearchParams({
      csrf,
      action: "createReservation",
      host: "keep.example.com",
      userId,
      note: "owned by support",
    }),
  });
  assert.equal(reserved.status, 303);
  const db = await worker.getD1Database("DB");
  const row = await db.prepare("SELECT host, user_id FROM reservations WHERE host = ?").bind("keep.example.com").first();
  assert.equal(row?.user_id, userId);

  async function openWebProvider(host, token = anonymousHash) {
    const response = await worker.dispatchFetch(
      `http://localhost/socket?token=${encodeURIComponent(token)}&reverse=true&web=1`,
      { headers: { Upgrade: "websocket" } },
    );
    const provider = response.webSocket;
    assert.ok(provider);
    const next = inbox(provider);
    provider.accept();
    await next(0x02);
    provider.send(jsonFrame(0x12, { host, backends: [{ url: "http://localhost:8080" }] }));
    const registration = JSON.parse(new TextDecoder().decode((await next(0x13)).slice(6)));
    provider.close();
    return registration;
  }

  const blocked = await openWebProvider("keep.example.com");
  assert.equal(blocked.success, false);
  assert.match(blocked.error, /reserved/i);

  const tokenCreated = await worker.dispatchFetch("http://localhost/admin/action", {
    method: "POST",
    redirect: "manual",
    headers,
    body: new URLSearchParams({ csrf, action: "createToken", userId, label: "reserved owner" }),
  });
  assert.equal(tokenCreated.status, 303);
  const flashCookie = tokenCreated.headers.get("set-cookie")?.split(";", 1)[0];
  const tokenPage = await worker.dispatchFetch(new URL(tokenCreated.headers.get("location"), "http://localhost"), {
    headers: { Cookie: `${session}; ${flashCookie}` },
  });
  const rawToken = (await tokenPage.text()).match(/id="new-token">([^<]+)</)?.[1];
  assert.ok(rawToken);

  const claimed = await openWebProvider("keep.example.com", rawToken);
  assert.equal(claimed.success, true);
  assert.equal(claimed.host, "keep.example.com");
  assert.equal(claimed.temporary, false);

  // The provider socket is now closed, but a web relay keeps its published
  // hostname for 24 hours so a restart does not surrender it.
  const heldSite = await db.prepare("SELECT host FROM sites WHERE host = ?").bind("keep.example.com").first();
  assert.equal(heldSite?.host, "keep.example.com");

  const released = await worker.dispatchFetch("http://localhost/admin/action", {
    method: "POST",
    redirect: "manual",
    headers,
    body: new URLSearchParams({ csrf, action: "deleteReservation", host: "keep.example.com" }),
  });
  assert.equal(released.status, 303);

  const forced = await worker.dispatchFetch("http://localhost/admin/action", {
    method: "POST",
    redirect: "manual",
    headers,
    body: new URLSearchParams({
      csrf,
      action: "saveSettings",
      scope: "global",
      allowAnonymous: "1",
      allowCustomHost: "1",
      forceTemporaryHost: "1",
      autoRotateSeconds: "3600",
    }),
  });
  assert.equal(forced.status, 303);

  const rotated = await openWebProvider("custom.example.com");
  assert.equal(rotated.success, true);
  assert.notEqual(rotated.host, "custom.example.com");
  assert.equal(rotated.temporary, true);

  const reservationsPage = await worker.dispatchFetch("http://localhost/admin?tab=reservations", { headers });
  const reservationsHtml = await reservationsPage.text();
  assert.match(reservationsHtml, /Reservations/);
  assert.match(reservationsHtml, /Reserve a hostname/);
});
