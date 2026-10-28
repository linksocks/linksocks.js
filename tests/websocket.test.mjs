import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import http from "node:http";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";
import { Miniflare } from "miniflare";
import WebSocket, { WebSocketServer } from "ws";

const root = fileURLToPath(new URL("..", import.meta.url));
const anonymousHash = "2f183a4e64493af3f377f745eda502363cd3e7ef6e4d266d444758de0a85fcc8";

test("WebSocket protocol frames preserve binary data", async () => {
  const bundle = await build({ absWorkingDir: root, entryPoints: ["src/message.ts"], bundle: true, format: "esm", write: false });
  const protocol = await import(`data:text/javascript;base64,${Buffer.from(bundle.outputFiles[0].text).toString("base64")}`);
  const channelId = "00112233-4455-6677-8899-aabbccddeeff";
  const encoded = protocol.packMessage({ channelId, opcode: 2, data: new Uint8Array([0, 1, 255]), getType: () => "websocket_frame" });
  assert.deepEqual([...encoded.slice(0, 2)], [0x04, 0x19]);
  const decoded = protocol.parseMessage(encoded);
  assert.equal(decoded.channelId, channelId);
  assert.equal(decoded.opcode, 2);
  assert.deepEqual([...decoded.data], [0, 1, 255]);
  assert.throws(() => protocol.parseMessage(encoded.slice(0, -1)), /length/);
});

function jsonFrame(type, payload) {
  const bytes = new TextEncoder().encode(JSON.stringify(payload));
  const frame = new Uint8Array(6 + bytes.length);
  frame.set([0x04, type]);
  new DataView(frame.buffer).setUint32(2, bytes.length);
  frame.set(bytes, 6);
  return frame;
}

function socketFrame(channelId, opcode, data) {
  const frame = new Uint8Array(23 + data.length);
  frame.set([0x04, 0x19]);
  frame.set(Buffer.from(channelId.replaceAll("-", ""), "hex"), 2);
  frame[18] = opcode;
  new DataView(frame.buffer).setUint32(19, data.length);
  frame.set(data, 23);
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
        const timer = setTimeout(() => reject(new Error(`timed out waiting for message type ${type}`)), 5000);
        waiting.push((value) => { clearTimeout(timer); resolve(value); });
      });
      if (frame[1] === type) return frame;
    }
  };
}

function nextBrowserMessage(socket) {
  return new Promise((resolve, reject) => {
    socket.once("message", (data, isBinary) => resolve({ data: Buffer.from(data), isBinary }));
    socket.once("error", reject);
  });
}

test("website WebSocket upgrades and forwards text and binary frames", { timeout: 20000 }, async (context) => {
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
    d1Databases: { DB: "websocket-test" },
    bindings: { WEB_ZONE: "example.com", BASE_HOST: "localhost" },
    outboundService: () => new Response("colo=TEST\n"),
  });
  context.after(async () => worker.dispose());
  const response = await worker.dispatchFetch(`http://localhost/socket?token=${anonymousHash}&reverse=true&web=1`, {
    headers: { Upgrade: "websocket" },
  });
  const provider = response.webSocket;
  assert.ok(provider);
  const next = inbox(provider);
  provider.accept();
  assert.equal((await next(0x02))[2], 1);
  provider.send(jsonFrame(0x12, { backends: [{ url: "http://localhost:8080" }] }));
  const registration = JSON.parse(new TextDecoder().decode((await next(0x13)).slice(6)));
  assert.equal(registration.success, true);

  const workerURL = await worker.ready;
  const browser = new WebSocket(`ws://${workerURL.host}/chat?room=one`, "chat", {
    headers: { Host: registration.host, Origin: "https://app.example.com" },
  });
  context.after(() => browser.terminate());
  const opened = new Promise((resolve, reject) => {
    browser.once("open", resolve);
    browser.once("error", reject);
    browser.once("unexpected-response", (_request, result) => {
      result.resume();
      reject(new Error(`unexpected WebSocket response: ${result.statusCode}`));
    });
  });
  const first = await Promise.race([
    next(0x14).then((frame) => ({ frame })),
    opened.catch((error) => ({ error })),
  ]);
  if (first.error) throw first.error;
  const request = JSON.parse(new TextDecoder().decode(first.frame.slice(6)));
  assert.equal(request.websocket, true);
  assert.equal(request.path, "/chat?room=one");
  assert.ok(request.headers.some(([name, value]) => name.toLowerCase() === "sec-websocket-protocol" && value === "chat"));
  provider.send(jsonFrame(0x15, {
    channel_id: request.channel_id,
    status: 101,
    headers: [["sec-websocket-protocol", "chat"]],
  }));
  await opened;
  assert.equal(browser.protocol, "chat");

  browser.send("hello");
  let frame = await next(0x19);
  assert.equal(frame[18], 1);
  assert.equal(Buffer.from(frame.slice(23)).toString(), "hello");
  browser.send(Buffer.from([0, 1, 255]));
  frame = await next(0x19);
  assert.equal(frame[18], 2);
  assert.deepEqual(Buffer.from(frame.slice(23)), Buffer.from([0, 1, 255]));

  let message = nextBrowserMessage(browser);
  provider.send(socketFrame(request.channel_id, 1, Buffer.from("world")));
  assert.deepEqual(await message, { data: Buffer.from("world"), isBinary: false });
  message = nextBrowserMessage(browser);
  provider.send(socketFrame(request.channel_id, 2, Buffer.from([255, 0, 2])));
  assert.deepEqual(await message, { data: Buffer.from([255, 0, 2]), isBinary: true });
  browser.close(1000, "done");
  const cancelled = JSON.parse(new TextDecoder().decode((await next(0x17)).slice(6)));
  assert.equal(cancelled.channel_id, request.channel_id);
  provider.close();
});

test("real Go Web provider proxies a WebSocket origin", { timeout: 20000, skip: !process.env.LINKSOCKS_TEST_BIN }, async (context) => {
  const origin = http.createServer();
  const sockets = new WebSocketServer({ server: origin, handleProtocols: (protocols) => protocols.has("chat") ? "chat" : false });
  let originPath = "";
  sockets.on("connection", (socket, request) => {
    originPath = request.url;
    socket.on("message", (data, isBinary) => socket.send(data, { binary: isBinary }));
  });
  await new Promise((resolve) => origin.listen(0, "127.0.0.1", resolve));
  const bundle = await build({
    absWorkingDir: root, entryPoints: ["src/index.ts"], bundle: true, format: "esm", platform: "browser",
    external: ["cloudflare:workers"], write: false,
  });
  const worker = new Miniflare({
    modules: true,
    script: bundle.outputFiles[0].text,
    compatibilityDate: "2025-04-13",
    durableObjects: {
      RELAY: { className: "Relay", useSQLite: true },
      TOKEN: { className: "Token", useSQLite: true },
    },
    d1Databases: { DB: "websocket-go-test" },
    bindings: { WEB_ZONE: "example.com", BASE_HOST: "localhost" },
    outboundService: () => new Response("colo=TEST\n"),
  });
  const workerURL = await worker.ready;
  const child = spawn(process.env.LINKSOCKS_TEST_BIN, [
    "web", "-u", `ws://${workerURL.host}`, "-l", `http://127.0.0.1:${origin.address().port}/base`, "--no-reconnect",
  ], {
    stdio: ["ignore", "pipe", "pipe"],
    env: { ...process.env, NO_COLOR: "1", HTTP_PROXY: "", HTTPS_PROXY: "", ALL_PROXY: "", http_proxy: "", https_proxy: "", all_proxy: "" },
  });
  let browser;
  context.after(async () => {
    if (browser) browser.terminate();
    if (child.exitCode === null) child.kill("SIGTERM");
    await worker.dispose();
    if (child.exitCode === null) child.kill("SIGKILL");
    for (const socket of sockets.clients) socket.terminate();
    await new Promise((resolve) => origin.close(resolve));
  });
  const host = await new Promise((resolve, reject) => {
    let output = "";
    const timer = setTimeout(() => reject(new Error("Go provider did not register")), 10000);
    const onData = (chunk) => {
      output += chunk.toString();
      const match = output.match(/Web site published[^\n]*\bhost=([a-z0-9.-]+\.example\.com)/i);
      if (match) { clearTimeout(timer); resolve(match[1]); }
    };
    child.stdout.on("data", onData);
    child.stderr.on("data", onData);
    child.once("exit", (code) => { clearTimeout(timer); reject(new Error(`Go provider exited: ${code}`)); });
  });
  browser = new WebSocket(`ws://${workerURL.host}/chat?room=one`, "chat", { headers: { Host: host } });
  await new Promise((resolve, reject) => { browser.once("open", resolve); browser.once("error", reject); });
  assert.equal(browser.protocol, "chat");
  assert.equal(originPath, "/base/chat?room=one");
  let message = nextBrowserMessage(browser);
  browser.send("hello");
  assert.deepEqual(await message, { data: Buffer.from("hello"), isBinary: false });
  message = nextBrowserMessage(browser);
  browser.send(Buffer.from([0, 1, 255]));
  assert.deepEqual(await message, { data: Buffer.from([0, 1, 255]), isBinary: true });
});