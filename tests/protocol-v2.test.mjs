import assert from "node:assert/strict";
import { createHash, randomBytes } from "node:crypto";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";
import { Miniflare } from "miniflare";

import { channelOpenFrame, sealedFrame } from "./v2-frames.mjs";

const projectRoot = fileURLToPath(new URL("..", import.meta.url));

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

async function nextType(inbox, type) {
  for (let index = 0; index < 12; index++) {
    const frame = await inbox();
    if (frame[1] === type) return frame;
  }
  throw new Error(`did not receive message type ${type}`);
}

function connectorFrame(version, channel, token) {
  const tokenBytes = new TextEncoder().encode(token);
  const frame = new Uint8Array(2 + 16 + 1 + tokenBytes.length + 1);
  frame.set([version, 0x07]);
  frame.set(channel, 2);
  frame[18] = tokenBytes.length;
  frame.set(tokenBytes, 19);
  frame[19 + tokenBytes.length] = 1;
  return frame;
}

async function newWorker(context, name) {
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
      ZONE: { className: "Zone", useSQLite: true },
    },
    d1Databases: { DB: name },
    bindings: { WEB_ZONE: "example.com", BASE_HOST: "localhost" },
    outboundService: () => new Response("colo=TEST\n"),
  });
  context.after(async () => worker.dispose());
  return worker;
}

function hex(bytes) {
  return Buffer.from(bytes).toString("hex");
}

test("V2 peers exchange sealed frames the relay only routes", { timeout: 20000 }, async (context) => {
  const worker = await newWorker(context, "protocol-v2");
  const providerToken = randomBytes(24).toString("hex");
  const connectorToken = randomBytes(24).toString("hex");
  const hash = (value) => createHash("sha256").update(value).digest("hex");

  async function open(token, reverse, version) {
    const versionParam = version ? `&version=0x${version.toString(16).padStart(2, "0")}` : "";
    const response = await worker.dispatchFetch(
      `http://localhost/socket?token=${hash(token)}&reverse=${reverse}${versionParam}`,
      { headers: { Upgrade: "websocket" } },
    );
    assert.equal(response.status, 101);
    const socket = response.webSocket;
    assert.ok(socket);
    const inbox = messageInbox(socket);
    socket.accept();
    const auth = await nextType(inbox, 0x02);
    assert.equal(auth[2], 1);
    return { socket, inbox, auth };
  }

  // Provider registers a connector token so the connector side can resolve
  // this relay.
  const provider = await open(providerToken, true, 0x20);
  assert.equal(provider.auth[0], 0x20, "relay responses are stamped with the provider generation");
  provider.socket.send(connectorFrame(0x20, randomBytes(16), connectorToken));
  const registration = await nextType(provider.inbox, 0x08);
  assert.equal(registration[18], 1);
  assert.equal(registration[0], 0x20);

  const connector = await open(connectorToken, false, 0x20);
  // The relay's own partners broadcast must speak the connection's generation.
  const partners = await nextType(connector.inbox, 0x0a);
  assert.equal(partners[0], 0x20, "relay-generated messages are stamped with the peer generation");

  const channelId = randomBytes(16);
  connector.socket.send(channelOpenFrame(0x20, channelId, true));
  const opened = await nextType(provider.inbox, 0x1a);
  assert.equal(opened[0], 0x20);
  assert.equal(hex(opened.slice(2, 18)), hex(channelId));
  assert.equal(opened[18], 1, "resume flag is forwarded");

  // Sealed payloads cross the relay byte for byte; the relay never parses
  // them, so an arbitrary binary marker survives both directions.
  const marker = new Uint8Array([0x00, 0xff, 0x53, 0x45, 0x41, 0x4c, 0x45, 0x44, 0x10, 0x00]);
  connector.socket.send(sealedFrame(0x20, channelId, marker));
  const toProvider = await nextType(provider.inbox, 0x1b);
  assert.equal(toProvider[0], 0x20);
  assert.equal(hex(toProvider.slice(2, 18)), hex(channelId));
  assert.equal(hex(toProvider.slice(22)), hex(marker));

  provider.socket.send(sealedFrame(0x20, channelId, new Uint8Array([0xaa, 0xbb, 0xcc])));
  const toConnector = await nextType(connector.inbox, 0x1b);
  assert.equal(hex(toConnector.slice(22)), "aabbcc");

  connector.socket.close();
  provider.socket.close();
});

test("legacy connections cannot use sealed channels", { timeout: 20000 }, async (context) => {
  const worker = await newWorker(context, "protocol-v2-legacy");
  const providerToken = randomBytes(24).toString("hex");
  const connectorToken = randomBytes(24).toString("hex");
  const hash = (value) => createHash("sha256").update(value).digest("hex");

  async function open(token, reverse) {
    const response = await worker.dispatchFetch(
      `http://localhost/socket?token=${hash(token)}&reverse=${reverse}`,
      { headers: { Upgrade: "websocket" } },
    );
    assert.equal(response.status, 101);
    const socket = response.webSocket;
    assert.ok(socket);
    const inbox = messageInbox(socket);
    socket.accept();
    const auth = await nextType(inbox, 0x02);
    assert.equal(auth[2], 1);
    return { socket, inbox, auth };
  }

  const provider = await open(providerToken, true);
  assert.equal(provider.auth[0], 0x04, "legacy connections keep the native generation");
  provider.socket.send(connectorFrame(0x20, randomBytes(16), connectorToken));
  await nextType(provider.inbox, 0x08);

  const connector = await open(connectorToken, false);
  const closed = new Promise((resolve) => {
    connector.socket.addEventListener("close", (event) => resolve(event));
  });

  // A generation-0 connection has no business sending sealed channel frames;
  // the relay must refuse loudly instead of downgrading.
  connector.socket.send(channelOpenFrame(0x04, randomBytes(16), false));
  const event = await Promise.race([
    closed,
    new Promise((_, reject) => setTimeout(() => reject(new Error("relay did not close legacy sealed-channel attempt")), 5000)),
  ]);
  assert.equal(event.code, 1008);

  provider.socket.close();
});
