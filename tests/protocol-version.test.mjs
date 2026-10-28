import assert from "node:assert/strict";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";
import { Miniflare } from "miniflare";

const projectRoot = fileURLToPath(new URL("..", import.meta.url));
const anonymousHash = "2f183a4e64493af3f377f745eda502363cd3e7ef6e4d266d444758de0a85fcc8";

test("feature minimums are distinct and reject incompatible major versions", async () => {
  const bundle = await build({
    absWorkingDir: projectRoot,
    entryPoints: ["src/feature-version.ts"],
    bundle: true,
    format: "esm",
    write: false,
  });
  const feature = await import(`data:text/javascript;base64,${Buffer.from(bundle.outputFiles[0].text).toString("base64")}`);
  assert.equal(feature.MIN_VERSION_DIRECT, 0x01);
  assert.equal(feature.MIN_VERSION_MIGRATION, 0x02);
  assert.equal(feature.MIN_VERSION_WEB, 0x03);
  assert.equal(feature.supportsFeatureVersion(0x02, feature.MIN_VERSION_WEB), false);
  assert.equal(feature.supportsFeatureVersion(0x03, feature.MIN_VERSION_WEB), true);
  assert.equal(feature.supportsFeatureVersion(0x04, feature.MIN_VERSION_WEB), true);
  assert.equal(feature.supportsFeatureVersion(0x10, feature.MIN_VERSION_DIRECT), false);
});

function logFrame(version) {
  const payload = new TextEncoder().encode(JSON.stringify({ level: "info", msg: "version probe" }));
  const frame = new Uint8Array(6 + payload.length);
  frame[0] = version;
  frame[1] = 0x09;
  new DataView(frame.buffer).setUint32(2, payload.length);
  frame.set(payload, 6);
  return frame;
}

function authFrame(version) {
  const token = new TextEncoder().encode(anonymousHash);
  const frame = new Uint8Array(2 + 1 + token.length + 1 + 16);
  frame[0] = version;
  frame[1] = 0x01;
  frame[2] = token.length;
  frame.set(token, 3);
  frame[3 + token.length] = 1;
  return frame;
}

function webRegisterFrame(version) {
  const payload = new TextEncoder().encode(JSON.stringify({ backends: [{ url: "http://localhost:8080" }] }));
  const frame = new Uint8Array(6 + payload.length);
  frame[0] = version;
  frame[1] = 0x12;
  new DataView(frame.buffer).setUint32(2, payload.length);
  frame.set(payload, 6);
  return frame;
}

function webCancelFrame(channelId) {
  const payload = new TextEncoder().encode(JSON.stringify({ channel_id: channelId, reason: "origin cancelled" }));
  const frame = new Uint8Array(6 + payload.length);
  frame[0] = 0x03;
  frame[1] = 0x17;
  new DataView(frame.buffer).setUint32(2, payload.length);
  frame.set(payload, 6);
  return frame;
}

function messageInbox(socket) {
  const queued = [];
  const waiting = [];
  socket.addEventListener("message", (event) => {
    const frame = new Uint8Array(event.data);
    const waiter = waiting.shift();
    if (waiter) waiter(frame);
    else queued.push(frame);
  });
  return async () => {
    if (queued.length) return queued.shift();
    return await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("timed out waiting for WebSocket message")), 5000);
      waiting.push((frame) => {
        clearTimeout(timer);
        resolve(frame);
      });
    });
  };
}

test("worker checks versions and ends cancelled web requests", { timeout: 20000 }, async (context) => {
  const bundle = await build({
    absWorkingDir: projectRoot,
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
    d1Databases: { DB: "linksocks-test" },
    bindings: { WEB_ZONE: "example.com", BASE_HOST: "localhost", ADMIN_USERNAME: "admin" },
    outboundService: () => new Response("colo=TEST\n"),
  });
  context.after(async () => worker.dispose());

  const response = await worker.dispatchFetch(`http://localhost/socket?token=${anonymousHash}&reverse=true`, {
    headers: { Upgrade: "websocket" },
  });
  assert.equal(response.status, 101);
  const socket = response.webSocket;
  assert.ok(socket);
  const nextMessage = messageInbox(socket);
  socket.accept();

  const auth = await nextMessage();
  assert.equal(auth[0], 0x03);
  assert.equal(auth[1], 0x02);

  socket.send(logFrame(0x01));
  let warning;
  for (let index = 0; index < 5; index++) {
    const frame = await nextMessage();
    if (frame[1] !== 0x09) continue;
    const message = JSON.parse(new TextDecoder().decode(frame.slice(6)));
    if (message.level === "warn" && message.msg.includes("Protocol minor version mismatch")) {
      warning = message;
      break;
    }
  }
  assert.ok(warning);
  assert.match(warning.msg, /peer=0x01/);

  socket.send(authFrame(0x01));
  const secondAuth = await nextMessage();
  assert.equal(secondAuth[1], 0x02, "the same version should not produce another warning");

  const closed = new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("worker did not close incompatible connection")), 5000);
    socket.addEventListener("close", (event) => {
      clearTimeout(timer);
      resolve(event);
    }, { once: true });
  });
  socket.send(logFrame(0x10));
  const close = await closed;
  assert.equal(close.code, 1011);

  const secondResponse = await worker.dispatchFetch(`http://localhost/socket?token=${anonymousHash}&reverse=true`, {
    headers: { Upgrade: "websocket" },
  });
  assert.equal(secondResponse.status, 101);
  const secondSocket = secondResponse.webSocket;
  assert.ok(secondSocket);
  const secondInbox = messageInbox(secondSocket);
  secondSocket.accept();
  assert.equal((await secondInbox())[1], 0x02);
  const secondClosed = new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("worker accepted an outdated Web message")), 5000);
    secondSocket.addEventListener("close", (event) => {
      clearTimeout(timer);
      resolve(event);
    }, { once: true });
  });
  secondSocket.send(webRegisterFrame(0x02));
  assert.equal((await secondClosed).code, 1008);

  const webResponse = await worker.dispatchFetch(`http://localhost/socket?token=${anonymousHash}&reverse=true&web=1`, {
    headers: { Upgrade: "websocket" },
  });
  assert.equal(webResponse.status, 101);
  const webSocket = webResponse.webSocket;
  assert.ok(webSocket);
  const webInbox = messageInbox(webSocket);
  webSocket.accept();
  assert.equal((await webInbox())[0], 0x03);
  webSocket.send(webRegisterFrame(0x03));
  let registration;
  for (let index = 0; index < 5; index++) {
    const frame = await webInbox();
    if (frame[1] === 0x13) {
      registration = JSON.parse(new TextDecoder().decode(frame.slice(6)));
      break;
    }
  }
  assert.equal(registration?.success, true);
  assert.match(registration.host, /\.example\.com$/);

  const db = await worker.getD1Database("DB");
  const site = await db.prepare("SELECT relay_id FROM sites WHERE host = ?").bind(registration.host).first();
  assert.ok(site?.relay_id);
  const namespace = await worker.getDurableObjectNamespace("RELAY");
  const relay = namespace.get(namespace.idFromString(site.relay_id));
  const httpResponse = relay.fetch("http://relay.test/web", {
    headers: { "X-LS-Site-Host": registration.host },
  });
  let request;
  for (let index = 0; index < 5; index++) {
    const frame = await webInbox();
    if (frame[1] === 0x14) {
      request = JSON.parse(new TextDecoder().decode(frame.slice(6)));
      break;
    }
  }
  assert.ok(request?.channel_id);
  webSocket.send(webCancelFrame(request.channel_id));
  let timeout;
  try {
    const result = await Promise.race([
      httpResponse,
      new Promise((_, reject) => { timeout = setTimeout(() => reject(new Error("cancelled web request did not finish")), 2000); }),
    ]);
    assert.equal(result.status, 504);
  } finally {
    clearTimeout(timeout);
  }
  webSocket.close();
});