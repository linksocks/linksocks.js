import assert from "node:assert/strict";
import { createHash, randomBytes } from "node:crypto";
import http from "node:http";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";
import { Miniflare } from "miniflare";

const root = fileURLToPath(new URL("..", import.meta.url));
const anonymousHash = "2f183a4e64493af3f377f745eda502363cd3e7ef6e4d266d444758de0a85fcc8";

function jsonFrame(type, payload) {
  const bytes = new TextEncoder().encode(JSON.stringify(payload));
  const frame = new Uint8Array(6 + bytes.length);
  frame.set([0x03, type]);
  new DataView(frame.buffer).setUint32(2, bytes.length);
  frame.set(bytes, 6);
  return frame;
}

function webDataFrame(channelId, data) {
  const frame = new Uint8Array(23 + data.length);
  frame.set([0x03, 0x16]);
  frame.set(Buffer.from(channelId.replaceAll("-", ""), "hex"), 2);
  frame[18] = 1;
  new DataView(frame.buffer).setUint32(19, data.length);
  frame.set(data, 23);
  return frame;
}

function connectorFrame(channel, token) {
  const bytes = new TextEncoder().encode(token);
  const frame = new Uint8Array(20 + bytes.length);
  frame.set([0x03, 0x07]);
  frame.set(channel, 2);
  frame[18] = bytes.length;
  frame.set(bytes, 19);
  frame[19 + bytes.length] = 1;
  return frame;
}

function connectFrame(channel) {
  const address = new TextEncoder().encode("127.0.0.1");
  const frame = new Uint8Array(22 + address.length);
  frame.set([0x03, 0x03, 0x01]);
  frame.set(channel, 3);
  frame[19] = address.length;
  frame.set(address, 20);
  frame.set([0x1f, 0x90], 20 + address.length);
  return frame;
}

function connectResponseFrame(channel) {
  const frame = new Uint8Array(19);
  frame.set([0x03, 0x05, 0x01]);
  frame.set(channel, 3);
  return frame;
}

function networkDataFrame(channel, data) {
  const frame = new Uint8Array(24 + data.length);
  frame.set([0x03, 0x04, 0x01]);
  frame.set(channel, 3);
  new DataView(frame.buffer).setUint32(20, data.length);
  frame.set(data, 24);
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
      const frame = queue.length ? queue.shift() : await new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error("timed out waiting for a protocol message")), 5000);
        waiting.push((value) => { clearTimeout(timer); resolve(value); });
      });
      if (frame[1] === type) return frame;
    }
  };
}

function siteRequest(workerURL, host) {
  return new Promise((resolve, reject) => {
    const request = http.get({
      hostname: workerURL.hostname,
      port: workerURL.port,
      path: "/large",
      headers: { Host: host },
    }, (response) => {
      let bytes = 0;
      let finished = false;
      const done = () => {
        if (finished) return;
        finished = true;
        resolve({ status: response.statusCode, bytes });
      };
      response.on("data", (chunk) => { bytes += chunk.length; });
      response.on("end", done);
      response.on("aborted", done);
      response.on("error", done);
    });
    request.setTimeout(5000, () => request.destroy(new Error("site request timed out")));
    request.on("error", reject);
  });
}

test("a single Web response cannot exceed the byte quota", { timeout: 20000 }, async (context) => {
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
    },
    d1Databases: { DB: "quota-test" },
    bindings: { WEB_ZONE: "example.com", BASE_HOST: "localhost", ADMIN_USERNAME: "admin", ADMIN_PASSWORD: password },
    outboundService: () => new Response("colo=TEST\n"),
  });
  context.after(async () => worker.dispose());

  const login = await worker.dispatchFetch("http://localhost/admin?login", {
    method: "POST", redirect: "manual", body: new URLSearchParams({ username: "admin", password }),
  });
  assert.equal(login.status, 303);
  const session = login.headers.get("set-cookie")?.split(";", 1)[0];
  assert.ok(session?.startsWith("ls_session="));
  const settings = await worker.dispatchFetch("http://localhost/admin?tab=settings", { headers: { Cookie: session } });
  const csrf = (await settings.text()).match(/name="csrf" value="([^"]+)"/)?.[1];
  assert.ok(csrf);
  const saved = await worker.dispatchFetch("http://localhost/admin/action", {
    method: "POST",
    redirect: "manual",
    headers: { Cookie: session },
    body: new URLSearchParams({
      csrf, action: "saveSettings", scope: "global", allowAnonymous: "1", allowUserSettings: "1",
      quotaBytes: "1024", quotaRequests: "0",
    }),
  });
  assert.equal(saved.status, 303);

  const response = await worker.dispatchFetch(`http://localhost/socket?token=${anonymousHash}&reverse=true&web=1`, {
    headers: { Upgrade: "websocket" },
  });
  assert.equal(response.status, 101);
  const socket = response.webSocket;
  assert.ok(socket);
  const next = inbox(socket);
  socket.accept();
  assert.equal((await next(0x02))[2], 1);
  socket.send(jsonFrame(0x12, { backends: [{ url: "http://localhost:8080" }] }));
  const registration = JSON.parse(new TextDecoder().decode((await next(0x13)).slice(6)));
  assert.equal(registration.success, true);

  const resultPromise = siteRequest(await worker.ready, registration.host);
  const request = JSON.parse(new TextDecoder().decode((await next(0x14)).slice(6)));
  socket.send(jsonFrame(0x15, { channel_id: request.channel_id, status: 200, headers: [], body_length: 65536 }));
  socket.send(webDataFrame(request.channel_id, new Uint8Array(65536)));
  const result = await resultPromise;
  assert.equal(result.status, 429, `known 65536-byte response should be rejected before headers`);
  socket.close();
});

test("anonymous SOCKS and Web traffic share the same byte quota", { timeout: 20000 }, async (context) => {
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
    },
    d1Databases: { DB: "shared-quota-test" },
    bindings: { WEB_ZONE: "example.com", BASE_HOST: "localhost", ADMIN_USERNAME: "admin", ADMIN_PASSWORD: password },
    outboundService: () => new Response("colo=TEST\n"),
  });
  context.after(async () => worker.dispose());
  const login = await worker.dispatchFetch("http://localhost/admin?login", {
    method: "POST", redirect: "manual", body: new URLSearchParams({ username: "admin", password }),
  });
  const session = login.headers.get("set-cookie")?.split(";", 1)[0];
  assert.ok(session?.startsWith("ls_session="));
  const settings = await worker.dispatchFetch("http://localhost/admin?tab=settings", { headers: { Cookie: session } });
  const csrf = (await settings.text()).match(/name="csrf" value="([^"]+)"/)?.[1];
  assert.ok(csrf);
  const saved = await worker.dispatchFetch("http://localhost/admin/action", {
    method: "POST", redirect: "manual", headers: { Cookie: session },
    body: new URLSearchParams({
      csrf, action: "saveSettings", scope: "global", allowAnonymous: "1", allowUserSettings: "1",
      quotaBytes: "1024", quotaRequests: "0",
    }),
  });
  assert.equal(saved.status, 303);

  const providerToken = randomBytes(24).toString("hex");
  const connectorToken = randomBytes(24).toString("hex");
  const hash = (value) => createHash("sha256").update(value).digest("hex");
  async function open(token, reverse, web = false) {
    const response = await worker.dispatchFetch(`http://localhost/socket?token=${hash(token)}&reverse=${reverse}&web=${web ? 1 : 0}`, {
      headers: { Upgrade: "websocket" },
    });
    assert.equal(response.status, 101);
    const socket = response.webSocket;
    assert.ok(socket);
    const next = inbox(socket);
    socket.accept();
    assert.equal((await next(0x02))[2], 1);
    return { socket, next };
  }

  const network = await open(providerToken, true);
  network.socket.send(connectorFrame(randomBytes(16), connectorToken));
  assert.equal((await network.next(0x08))[18], 1);
  const connector = await open(connectorToken, false);
  const networkChannel = randomBytes(16);
  connector.socket.send(connectFrame(networkChannel));
  await network.next(0x03);
  network.socket.send(connectResponseFrame(networkChannel));
  assert.equal((await connector.next(0x05))[2], 1);
  connector.socket.send(networkDataFrame(networkChannel, new Uint8Array(1000)));
  assert.equal((await network.next(0x04)).length, 1024);

  const web = await open(providerToken, true, true);
  web.socket.send(jsonFrame(0x12, { backends: [{ url: "http://localhost:8080" }] }));
  const registration = JSON.parse(new TextDecoder().decode((await web.next(0x13)).slice(6)));
  assert.equal(registration.success, true);
  const resultPromise = siteRequest(await worker.ready, registration.host);
  const request = JSON.parse(new TextDecoder().decode((await web.next(0x14)).slice(6)));
  web.socket.send(jsonFrame(0x15, { channel_id: request.channel_id, status: 200, headers: [], body_length: 128 }));
  web.socket.send(webDataFrame(request.channel_id, new Uint8Array(128)));
  const result = await resultPromise;
  assert.equal(result.status, 429, `1000 SOCKS bytes should leave insufficient quota for a 128-byte response`);

  connector.socket.send(networkDataFrame(networkChannel, new Uint8Array(24)));
  assert.equal((await network.next(0x04)).length, 48);
  connector.socket.send(networkDataFrame(networkChannel, new Uint8Array(1)));
  assert.equal((await connector.next(0x06))[1], 0x06);
  assert.equal((await siteRequest(await worker.ready, registration.host)).status, 429);
  connector.socket.close();
  web.socket.close();
  network.socket.close();
});