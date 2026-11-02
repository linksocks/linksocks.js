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

test("all providers sharing a temporary host rotate to one replacement", { timeout: 30000 }, async (context) => {
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
      autoRotateSeconds: "4",
    }),
  });
  assert.equal(saved.status, 303);

  const created = await worker.dispatchFetch("http://localhost/admin/action", {
    method: "POST",
    redirect: "manual",
    headers,
    body: new URLSearchParams({
      csrf,
      action: "createUser",
      username: "rotation-user",
      password: "rotation-password",
    }),
  });
  assert.equal(created.status, 303);
  const userId = new URL(created.headers.get("location")).searchParams.get("user");
  assert.ok(userId);
  const userPolicy = await worker.dispatchFetch("http://localhost/admin/action", {
    method: "POST",
    redirect: "manual",
    headers,
    body: new URLSearchParams({
      csrf,
      action: "saveSettings",
      scope: "user",
      userId,
      allowCustomHost: "1",
      autoRotateSeconds: "4",
    }),
  });
  assert.equal(userPolicy.status, 303);
  const tokenCreated = await worker.dispatchFetch("http://localhost/admin/action", {
    method: "POST",
    redirect: "manual",
    headers,
    body: new URLSearchParams({ csrf, action: "createToken", userId, label: "rotation" }),
  });
  assert.equal(tokenCreated.status, 303);
  const flashCookie = tokenCreated.headers.get("set-cookie")?.split(";", 1)[0];
  const tokenPage = await worker.dispatchFetch(new URL(tokenCreated.headers.get("location"), "http://localhost"), {
    headers: { Cookie: `${session}; ${flashCookie}` },
  });
  const token = (await tokenPage.text()).match(/id="new-token">([^<]+)</)?.[1];
  assert.ok(token);

  const first = await openProvider(worker, token);
  const published = await register(first, "");
  assert.equal(published.success, true, JSON.stringify(published));
  assert.equal(published.temporary, true);
  assert.ok(published.host.endsWith(".example.com"));

  const second = await openProvider(worker, token);
  const joined = await register(second, published.host);
  assert.equal(joined.success, true, JSON.stringify(joined));
  assert.equal(joined.host, published.host);

  first.socket.send(jsonFrame(0x12, {
    host: "",
    backends: first.backends,
    rotate: true,
  }));
  const firstRotation = framePayload(await first.next(0x13));
  const broadcast = framePayload(await second.next(0x18));
  assert.equal(firstRotation.success, true, JSON.stringify(firstRotation));
  assert.equal(firstRotation.temporary, true);
  assert.notEqual(firstRotation.host, published.host);
  assert.equal(broadcast.host, firstRotation.host);
  assert.equal(broadcast.url, firstRotation.url);

  // Both providers now race their own timers. The relay rotates once and
  // hands the loser the same hostname instead of allocating a second one.
  first.socket.send(jsonFrame(0x12, {
    host: firstRotation.host,
    backends: first.backends,
    rotate: true,
  }));
  second.socket.send(jsonFrame(0x12, {
    host: firstRotation.host,
    backends: second.backends,
    rotate: true,
  }));
  const [firstAgain, secondAgain] = await Promise.all([
    first.next(0x13).then(framePayload),
    second.next(0x13).then(framePayload),
  ]);
  assert.equal(firstAgain.success, true, JSON.stringify(firstAgain));
  assert.equal(secondAgain.success, true, JSON.stringify(secondAgain));
  assert.equal(firstAgain.host, secondAgain.host);
  assert.notEqual(firstAgain.host, firstRotation.host);

  // A provider that reconnects with the old host follows the recorded
  // rotation rather than registering a fresh domain of its own.
  const reconnected = await openProvider(worker, token);
  const followed = await register(reconnected, firstRotation.host);
  assert.equal(followed.success, true);
  assert.equal(followed.host, firstAgain.host);

  const db = await worker.getD1Database("DB");
  const oldSite = await db.prepare("SELECT host FROM sites WHERE host = ?").bind(published.host).first();
  assert.equal(oldSite, null);
  const rotatedSite = await db.prepare("SELECT host, rotate_seconds FROM sites WHERE host = ?").bind(firstAgain.host).first();
  assert.equal(rotatedSite?.host, firstAgain.host);
  assert.ok(rotatedSite.rotate_seconds > 0);

  first.socket.close();
  second.socket.close();
  reconnected.socket.close();
});
