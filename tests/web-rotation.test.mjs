import assert from "node:assert/strict";
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

async function openProvider(worker, token, backends = [{ url: "http://localhost:8080" }]) {
  const response = await worker.dispatchFetch(
    `http://localhost/socket?token=${encodeURIComponent(token)}&reverse=true&web=1`,
    { headers: { Upgrade: "websocket" } },
  );
  const socket = response.webSocket;
  assert.ok(socket);
  const next = inbox(socket);
  socket.accept();
  await next(0x02);
  return { socket, next, backends };
}

async function register(provider, host, rotate = false) {
  provider.socket.send(jsonFrame(0x12, {
    host,
    backends: provider.backends,
    rotate,
  }));
  return framePayload(await provider.next(0x13));
}

async function createUserToken(worker, headers, csrf, username, password) {
  const created = await worker.dispatchFetch("http://localhost/admin/action", {
    method: "POST",
    redirect: "manual",
    headers,
    body: new URLSearchParams({ csrf, action: "createUser", username, password }),
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
      allowCustomHost: "1",
      autoRotateSeconds: "8",
    }),
  });
  assert.equal(policy.status, 303);

  const tokenCreated = await worker.dispatchFetch("http://localhost/admin/action", {
    method: "POST",
    redirect: "manual",
    headers,
    body: new URLSearchParams({ csrf, action: "createToken", userId, label: "rotation" }),
  });
  assert.equal(tokenCreated.status, 303);
  const flashCookie = tokenCreated.headers.get("set-cookie")?.split(";", 1)[0];
  const tokenPage = await worker.dispatchFetch(
    new URL(tokenCreated.headers.get("location"), "http://localhost"),
    { headers: { Cookie: `${headers.Cookie}; ${flashCookie}` } },
  );
  const token = (await tokenPage.text()).match(/id="new-token">([^<]+)</)?.[1];
  assert.ok(token);
  return token;
}

test("the relay alarm rotates every provider onto one replacement host", { timeout: 30000 }, async (context) => {
  const bundle = await build({
    absWorkingDir: root,
    entryPoints: ["src/index.ts"],
    bundle: true,
    format: "esm",
    platform: "browser",
    external: ["cloudflare:workers"],
    write: false,
  });
  const worker = new Miniflare({
    modules: true,
    script: bundle.outputFiles[0].text,
    compatibilityDate: "2025-04-13",
    durableObjects: {
      RELAY: { className: "Relay", useSQLite: true },
      TOKEN: { className: "Token", useSQLite: true },
    },
    d1Databases: { DB: "web-rotation-test" },
    bindings: {
      ADMIN_USERNAME: "admin",
      ADMIN_PASSWORD: "test-password-rotation",
      WEB_ZONE: "example.com",
      BASE_HOST: "localhost",
      WEB_ROTATION_LEAD_MS: "1000",
    },
    outboundService: () => new Response("colo=TEST\n"),
  });
  context.after(async () => worker.dispose());

  const login = await worker.dispatchFetch("http://localhost/admin?login", {
    method: "POST",
    redirect: "manual",
    body: new URLSearchParams({ username: "admin", password: "test-password-rotation" }),
  });
  const session = login.headers.get("set-cookie")?.split(";", 1)[0];
  assert.ok(session);
  const headers = { Cookie: session };
  const settingsHtml = await (await worker.dispatchFetch("http://localhost/admin?tab=settings", { headers })).text();
  const csrf = settingsHtml.match(/name="csrf" value="([^"]+)"/)?.[1];
  assert.ok(csrf);
  const saved = await worker.dispatchFetch("http://localhost/admin/action", {
    method: "POST",
    redirect: "manual",
    headers,
    body: new URLSearchParams({
      csrf,
      action: "saveSettings",
      scope: "global",
      allowAnonymous: "1",
      allowCustomHost: "1",
      autoRotateSeconds: "8",
    }),
  });
  assert.equal(saved.status, 303);

  const token = await createUserToken(worker, headers, csrf, "rotation-user", "rotation-password");
  const secondToken = await createUserToken(worker, headers, csrf, "intruder", "intruder-password");

  const first = await openProvider(worker, token);
  const published = await register(first, "");
  assert.equal(published.success, true, JSON.stringify(published));
  assert.equal(published.temporary, true);
  assert.ok(published.host.endsWith(".example.com"));

  const second = await openProvider(worker, token);
  const joined = await register(second, published.host);
  assert.equal(joined.success, true, JSON.stringify(joined));
  assert.equal(joined.host, published.host);

  // No provider asks for a rotation: the relay's alarm has to do it, and both
  // providers must be told about the same replacement.
  const [firstBroadcast, secondBroadcast] = await Promise.all([
    first.next(0x18, 12000).then(framePayload),
    second.next(0x18, 12000).then(framePayload),
  ]);
  assert.equal(firstBroadcast.host, secondBroadcast.host);
  assert.notEqual(firstBroadcast.host, published.host);
  assert.equal(firstBroadcast.url, `https://${firstBroadcast.host}`);

  // A client timer firing after the relay already rotated gets the current
  // host back instead of allocating another one.
  const lateNudge = await register(first, firstBroadcast.host, true);
  assert.equal(lateNudge.success, true, JSON.stringify(lateNudge));
  assert.equal(lateNudge.host, firstBroadcast.host);

  // A provider that missed the broadcast and reconnects with the deprecated
  // hostname is carried onto the shared successor.
  const reconnected = await openProvider(worker, token);
  const followed = await register(reconnected, published.host);
  assert.equal(followed.success, true, JSON.stringify(followed));
  assert.equal(followed.host, firstBroadcast.host);

  // A different relay may not claim any hostname in the rotation chain.
  const intruder = await openProvider(worker, secondToken);
  const stolen = await register(intruder, published.host);
  assert.equal(stolen.success, false);
  assert.match(stolen.error || "", /retired/);

  const db = await worker.getD1Database("DB");
  const oldSite = await db.prepare("SELECT host FROM sites WHERE host = ?").bind(published.host).first();
  assert.equal(oldSite, null);
  const retired = await db
    .prepare("SELECT relay_id, successor_host FROM retired_hosts WHERE host = ?")
    .bind(published.host)
    .first();
  assert.equal(retired?.successor_host, firstBroadcast.host);
  assert.ok(retired.relay_id);
  const currentSite = await db
    .prepare("SELECT host, relay_id, owner_id, rotate_seconds FROM sites WHERE host = ?")
    .bind(firstBroadcast.host)
    .first();
  assert.equal(currentSite?.host, firstBroadcast.host);
  assert.ok(currentSite.rotate_seconds > 0);

  // A missed alarm leaves an expired temporary host behind. Another account
  // must not be able to rotate it into its own site just by registering it.
  const expiredHost = "expired-hijack.example.com";
  const intruderUser = await db
    .prepare("SELECT id FROM users WHERE username = ?")
    .bind("intruder")
    .first();
  assert.ok(intruderUser?.id);
  await db
    .prepare(
      `INSERT INTO sites (
         host, relay_id, owner_id, token_hash, temporary, expires_at, rotate_seconds,
         backends_json, backend_count, insecure, anonymous, quota_bytes, quota_requests,
         created_at, updated_at, last_seen_at
       ) VALUES (?, ?, ?, NULL, 1, ?, 8, ?, 1, 0, 0, 0, 0, ?, ?, NULL)`,
    )
    .bind(
      expiredHost,
      currentSite.relay_id,
      intruderUser.id,
      Date.now() - 1_000,
      JSON.stringify(["http://localhost:8080"]),
      Date.now(),
      Date.now(),
    )
    .run();
  const hijacked = await register(first, expiredHost);
  assert.equal(hijacked.success, false, JSON.stringify(hijacked));
  assert.match(hijacked.error || "", /owned by another user/);
  const expiredSite = await db
    .prepare("SELECT relay_id, owner_id FROM sites WHERE host = ?")
    .bind(expiredHost)
    .first();
  assert.equal(expiredSite?.relay_id, currentSite.relay_id);
  assert.equal(expiredSite?.owner_id, intruderUser.id);

  first.socket.close();
  second.socket.close();
  reconnected.socket.close();
  intruder.socket.close();
});
