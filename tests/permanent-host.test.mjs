import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";
import { Miniflare } from "miniflare";

const root = fileURLToPath(new URL("..", import.meta.url));

function jsonFrame(type, payload) {
  const bytes = new TextEncoder().encode(JSON.stringify(payload));
  const frame = new Uint8Array(6 + bytes.length);
  frame.set([0x04, type]);
  new DataView(frame.buffer).setUint32(2, bytes.length);
  frame.set(bytes, 6);
  return frame;
}

function framePayload(frame) {
  return JSON.parse(new TextDecoder().decode(frame.slice(6)));
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
  return async (type, timeoutMs = 5000) => {
    for (;;) {
      const frame = queue.length
        ? queue.shift()
        : await new Promise((resolve, reject) => {
            const timer = setTimeout(
              () => reject(new Error(`timed out waiting for message type ${type}`)),
              timeoutMs,
            );
            waiting.push((value) => {
              clearTimeout(timer);
              resolve(value);
            });
          });
      if (frame[1] === type) return frame;
    }
  };
}

async function openProvider(worker, token) {
  const response = await worker.dispatchFetch(
    `http://localhost/socket?token=${encodeURIComponent(token)}&reverse=true&web=1`,
    { headers: { Upgrade: "websocket" } },
  );
  const socket = response.webSocket;
  assert.ok(socket);
  const next = inbox(socket);
  socket.accept();
  await next(0x02);
  return { socket, next };
}

async function register(provider, host) {
  provider.socket.send(jsonFrame(0x12, { host, backends: [{ url: "http://localhost:8080" }] }));
  return framePayload(await provider.next(0x13));
}

async function createAccountToken(worker, headers, csrf, username, { allowCustomHost }) {
  const created = await worker.dispatchFetch("http://localhost/admin/action", {
    method: "POST",
    redirect: "manual",
    headers,
    body: new URLSearchParams({ csrf, action: "createUser", username, password: randomBytes(16).toString("hex") }),
  });
  assert.equal(created.status, 303);
  const userId = new URL(created.headers.get("location")).searchParams.get("user");
  assert.ok(userId);

  const policy = await worker.dispatchFetch("http://localhost/admin/action", {
    method: "POST",
    redirect: "manual",
    headers,
    body: new URLSearchParams({
      csrf,
      action: "saveSettings",
      scope: "user",
      userId,
      ...(allowCustomHost ? { allowCustomHost: "1" } : {}),
    }),
  });
  assert.equal(policy.status, 303);

  const tokenCreated = await worker.dispatchFetch("http://localhost/admin/action", {
    method: "POST",
    redirect: "manual",
    headers,
    body: new URLSearchParams({ csrf, action: "createToken", userId, label: "permanent" }),
  });
  assert.equal(tokenCreated.status, 303);
  const flashCookie = tokenCreated.headers.get("set-cookie")?.split(";", 1)[0];
  const tokenPage = await worker.dispatchFetch(
    new URL(tokenCreated.headers.get("location"), "http://localhost"),
    { headers: { Cookie: `${headers.Cookie}; ${flashCookie}` } },
  );
  const token = (await tokenPage.text()).match(/id="new-token">([^<]+)</)?.[1];
  assert.ok(token);
  return { userId, token };
}

test("the permanent-host grant is per account and survives forced temporary mode", { timeout: 30000 }, async (context) => {
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
    d1Databases: { DB: "permanent-host-test" },
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
  const session = login.headers.get("set-cookie")?.split(";", 1)[0];
  assert.ok(session);
  const headers = { Cookie: session };
  const settingsHtml = await (await worker.dispatchFetch("http://localhost/admin?tab=settings", { headers })).text();
  const csrf = settingsHtml.match(/name="csrf" value="([^"]+)"/)?.[1];
  assert.ok(csrf);

  const granted = await createAccountToken(worker, headers, csrf, "granted", { allowCustomHost: true });
  const denied = await createAccountToken(worker, headers, csrf, "denied", { allowCustomHost: false });

  const userHtml = await (
    await worker.dispatchFetch(`http://localhost/admin?tab=users&user=${granted.userId}&modal=policy`, { headers })
  ).text();
  assert.ok(userHtml.includes("Allow permanent custom hostnames"), "the per-user policy must name the merged grant");
  assert.ok(
    userHtml.includes('name="allowCustomHost" value="1" checked'),
    "a granted account must show the permission checked",
  );
  const deniedHtml = await (
    await worker.dispatchFetch(`http://localhost/admin?tab=users&user=${denied.userId}&modal=policy`, { headers })
  ).text();
  assert.ok(
    !deniedHtml.includes('name="allowCustomHost" value="1" checked'),
    "an ungranted account must show the permission unchecked by default",
  );

  // Without forced temporary mode an ungranted account is refused a custom host.
  const deniedProvider = await openProvider(worker, denied.token);
  const refused = await register(deniedProvider, "blog.example.com");
  assert.equal(refused.success, false, JSON.stringify(refused));

  // The granted account publishes its own hostname as a permanent site.
  const grantedProvider = await openProvider(worker, granted.token);
  const published = await register(grantedProvider, "blog.example.com");
  assert.equal(published.success, true, JSON.stringify(published));
  assert.equal(published.host, "blog.example.com");
  assert.equal(published.temporary, false);
  assert.equal(published.expiresAt, undefined);

  // A bare label is the supported request: the relay composes it under its
  // own zone, so the account never names a domain itself.
  const prefixProvider = await openProvider(worker, granted.token);
  const prefixed = await register(prefixProvider, "notes");
  assert.equal(prefixed.success, true, JSON.stringify(prefixed));
  assert.equal(prefixed.host, "notes.example.com");
  assert.equal(prefixed.temporary, false);

  const badLabel = await register(prefixProvider, "bad_label");
  assert.equal(badLabel.success, false, JSON.stringify(badLabel));
  assert.match(badLabel.error, /single label/);

  const db = await worker.getD1Database("DB");
  const row = await db.prepare("SELECT temporary, expires_at FROM sites WHERE host = ?").bind("blog.example.com").first();
  assert.equal(row.temporary, 0);
  assert.equal(row.expires_at, null);

  // Turn on the global force. The grant is the explicit exception, so the
  // granted account keeps its hostname while the other one is rewritten.
  const saved = await worker.dispatchFetch("http://localhost/admin/action", {
    method: "POST",
    redirect: "manual",
    headers,
    body: new URLSearchParams({ csrf, action: "saveSettings", scope: "global", forceTemporaryHost: "1" }),
  });
  assert.equal(saved.status, 303);

  const stillGranted = await openProvider(worker, granted.token);
  const kept = await register(stillGranted, "blog.example.com");
  assert.equal(kept.success, true, JSON.stringify(kept));
  assert.equal(kept.host, "blog.example.com");
  assert.equal(kept.temporary, false);

  const forced = await openProvider(worker, denied.token);
  const rewritten = await register(forced, "writer.example.com");
  assert.equal(rewritten.success, true, JSON.stringify(rewritten));
  assert.equal(rewritten.temporary, true);
  assert.notEqual(rewritten.host, "writer.example.com");
  assert.ok(rewritten.host.endsWith(".example.com"));
});
