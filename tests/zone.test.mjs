import assert from "node:assert/strict";
import { randomBytes, webcrypto } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";
import { Miniflare } from "miniflare";

const root = fileURLToPath(new URL("..", import.meta.url));
const hex = (bytes) => Buffer.from(bytes).toString("hex");

function inbox(socket) {
  const queue = [];
  let wake;
  socket.addEventListener("message", ({ data }) => {
    queue.push(typeof data === "string" ? JSON.parse(data) : Buffer.from(data));
    wake?.();
  });
  const closed = new Promise((resolve) => socket.addEventListener("close", resolve, { once: true }));
  return {
    queue,
    closed,
    async next(type, timeout = 5000) {
      const matches = typeof type === "function" ? type : (message) => message.type === type;
      const deadline = Date.now() + timeout;
      for (;;) {
        const index = queue.findIndex(matches);
        if (index !== -1) return queue.splice(index, 1)[0];
        const remaining = deadline - Date.now();
        if (remaining <= 0) throw new Error(`Timed out waiting for ${type}`);
        await new Promise((resolve, reject) => {
          const timer = setTimeout(() => {
            wake = undefined;
            reject(new Error(`Timed out waiting for ${type}`));
          }, remaining);
          wake = () => { clearTimeout(timer); wake = undefined; resolve(); };
        });
      }
    },
  };
}

async function identity() {
  const pair = await webcrypto.subtle.generateKey("Ed25519", true, ["sign", "verify"]);
  return { pair, identity: hex(await webcrypto.subtle.exportKey("raw", pair.publicKey)) };
}

function memberMac(last) {
  return Buffer.from([2, 0, 0, 0, 0, last]);
}

function ethernetPayload(sourceMac, destinationMac, size = 46) {
  const frame = Buffer.alloc(14 + size, 0xa5);
  destinationMac.copy(frame, 0);
  sourceMac.copy(frame, 6);
  frame.writeUInt16BE(0x0800, 12);
  return frame;
}

function frameMessage(sourceMac, destinationMac, sequence = 1n, frame) {
  const header = Buffer.alloc(22);
  header[0] = 1;
  header[1] = 2;
  sourceMac.copy(header, 2);
  destinationMac.copy(header, 8);
  header.writeBigUInt64BE(sequence, 14);
  return Buffer.concat([header, frame || ethernetPayload(sourceMac, destinationMac)]);
}

function ipBytes(address) {
  return Buffer.from(address.split(".").map(Number));
}

function openMessage(id, sourceIP, destinationIP, port) {
  const message = Buffer.alloc(16);
  message[0] = 2;
  message[1] = 1;
  message.writeUInt32BE(id, 2);
  ipBytes(sourceIP).copy(message, 6);
  ipBytes(destinationIP).copy(message, 10);
  message.writeUInt16BE(port, 14);
  return message;
}

function acceptMessage(id) {
  const message = Buffer.alloc(6);
  message[0] = 2;
  message[1] = 2;
  message.writeUInt32BE(id, 2);
  return message;
}

function closeMessage(id) {
  const message = Buffer.alloc(6);
  message[0] = 2;
  message[1] = 5;
  message.writeUInt32BE(id, 2);
  return message;
}

function resetMessage(id, code = 1) {
  const message = Buffer.alloc(8);
  message[0] = 2;
  message[1] = 6;
  message.writeUInt32BE(id, 2);
  message.writeUInt16BE(code, 6);
  return message;
}

function sealed(payload) {
  const data = Buffer.from(payload);
  return data.byteLength >= 17 ? data : Buffer.concat([data, Buffer.alloc(17 - data.byteLength)]);
}

function dataMessage(id, sequence, payload = Buffer.from("payload")) {
  const message = Buffer.alloc(14);
  message[0] = 2;
  message[1] = 4;
  message.writeUInt32BE(id, 2);
  message.writeBigUInt64BE(sequence, 6);
  return Buffer.concat([message, sealed(payload)]);
}

function datagramMessage(sourceIP, destinationIP, sourcePort, destinationPort, payload = Buffer.from("ping")) {
  const message = Buffer.alloc(21);
  message[0] = 3;
  ipBytes(sourceIP).copy(message, 1);
  ipBytes(destinationIP).copy(message, 5);
  message.writeUInt16BE(sourcePort, 9);
  message.writeUInt16BE(destinationPort, 11);
  return Buffer.concat([message, sealed(payload)]);
}

function streamFields(buffer) {
  return {
    op: buffer[1],
    id: buffer.readUInt32BE(2),
    rest: buffer.subarray(6),
  };
}

async function waitFor(read, predicate) {
  for (let attempt = 0; attempt < 100; attempt++) {
    const value = await read();
    if (predicate(value)) return value;
    await delay(10);
  }
  throw new Error("Condition did not become true");
}

test("zone coordinator authenticates identities and preserves isolated leases", { timeout: 60000 }, async (context) => {
  const bundle = await build({
    absWorkingDir: root, entryPoints: ["src/index.ts"], bundle: true, format: "esm", platform: "browser",
    external: ["cloudflare:workers"], write: false,
  });
  const worker = new Miniflare({
    name: "zone-test",
    modules: true,
    script: bundle.outputFiles[0].text,
    compatibilityDate: "2025-04-13",
    durableObjects: {
      RELAY: { className: "Relay", useSQLite: true },
      TOKEN: { className: "Token", useSQLite: true },
      ZONE: { className: "Zone", useSQLite: true },
    },
    d1Databases: { DB: "zones-test" },
    bindings: { WEB_ZONE: "example.com", BASE_HOST: "localhost" },
    outboundService: () => new Response("colo=TEST\n"),
  });
  context.after(() => worker.dispose());
  const db = await worker.getD1Database("DB");
  const origin = "http://localhost";
  const initial = await worker.dispatchFetch(`${origin}/api/zones/public`);
  assert.deepEqual(await initial.json(), { zones: [] });
  for (const [id, admin] of [["admin", 1], ["alice", 0], ["bob", 0]]) {
    await db.prepare("INSERT INTO users (id, username, password_hash, is_admin, disabled, created_at) VALUES (?, ?, '', ?, 0, ?)")
      .bind(id, id, admin, Date.now()).run();
    await db.prepare("INSERT INTO sessions (id, user_id, csrf, created_at, expires_at) VALUES (?, ?, ?, ?, ?)")
      .bind(`session-${id}`, id, `csrf-${id}`, Date.now(), Date.now() + 60000).run();
  }

  function api(path, method = "GET", actor, body, extra = {}) {
    return worker.dispatchFetch(`${origin}${path}`, {
      method,
      headers: {
        ...(actor ? { Cookie: `ls_session=session-${actor}`, "X-CSRF-Token": `csrf-${actor}` } : {}),
        ...(body ? { "Content-Type": "application/json" } : {}),
        ...extra,
      },
      ...(body ? { body: JSON.stringify(body) } : {}),
    });
  }

  async function create(config = {}, actor = "alice") {
    const response = await api("/api/zones", "POST", actor, { name: "Test network", cidr: "10.88.0.0/24", max_members: 8, ...config });
    assert.equal(response.status, 201, await response.clone().text());
    return (await response.json()).zone;
  }

  async function info(zone) {
    const response = await api(`/api/zones/${zone.id}/info`);
    assert.equal(response.status, 200);
    return response.json();
  }

  async function open(zone) {
    const response = await worker.dispatchFetch(`${origin}/zone/${zone.id}/socket`, { headers: { Upgrade: "websocket" } });
    assert.equal(response.status, 101);
    const socket = response.webSocket;
    assert.ok(socket);
    const messages = inbox(socket);
    socket.accept();
    const challenge = (await messages.next("challenge")).challenge;
    assert.match(challenge, /^[0-9a-f]{64}$/);
    return { socket, messages, challenge };
  }

  async function signed(zone, connection, who, overrides = {}) {
    const packetKey = await webcrypto.subtle.generateKey("X25519", true, ["deriveBits"]);
    const message = {
      type: "join", action: "join", identity: who.identity, key: hex(await webcrypto.subtle.exportKey("raw", packetKey.publicKey)),
      session: randomBytes(16).toString("hex"), name: "Test machine", ...overrides,
    };
    const text = `linksocks-zone-v1\n${zone.id}\n${connection.challenge}\n${message.action}\n${message.key}\n${message.session}\n${message.name}`;
    message.signature = hex(await webcrypto.subtle.sign("Ed25519", who.pair.privateKey, new TextEncoder().encode(text)));
    return message;
  }

  async function join(zone, who, overrides = {}) {
    const connection = await open(zone);
    const message = await signed(zone, connection, who, overrides);
    connection.socket.send(JSON.stringify(message));
    const roster = await connection.messages.next("members");
    return { ...connection, message, roster, ip: roster.self };
  }

  async function rejected(zone, who, mutate, pattern, overrides = {}) {
    const connection = await open(zone);
    const message = await signed(zone, connection, who, overrides);
    mutate(message);
    connection.socket.send(JSON.stringify(message));
    assert.match((await connection.messages.next("error")).error, pattern);
    assert.equal((await connection.messages.closed).code, 1008);
  }

  await context.test("management checks authentication, CSRF, ownership, visibility and private subnet limits", async () => {
    assert.equal((await api("/api/zones")).status, 401);
    assert.equal((await api("/api/zones", "POST", "alice", {}, { "X-CSRF-Token": "wrong" })).status, 403);
    for (const config of [
      { cidr: "8.8.0.0/16" }, { cidr: "172.15.0.0/16" }, { cidr: "192.169.0.0/16" },
      { cidr: "10.0.0.0/15" }, { cidr: "10.0.0.0/31" }, { cidr: "10.0.0.0/30", max_members: 3 },
      { cidr: "10.0.0.999/24" }, { max_members: 1 }, { max_members: 257 }, { max_members: 2.5 },
      { max_members: "4" }, { name: "" }, { name: "a\nb" }, { name: "x".repeat(65) },
      { visibility: "secret" }, { visibility: 1 },
    ]) {
      assert.equal((await api("/api/zones", "POST", "alice", { name: "Invalid", cidr: "10.0.0.0/24", max_members: 8, ...config })).status, 400);
    }
    const refused = await api("/api/zones", "POST", "alice", { name: "Public attempt", cidr: "10.0.0.0/24", max_members: 8, visibility: "public" });
    assert.equal(refused.status, 403);
    assert.match((await refused.json()).error, /administrator/i);
    const alice = await create({ cidr: "192.168.4.9/24" });
    assert.equal(alice.cidr, "192.168.4.0/24");
    assert.equal(alice.visibility, "private");
    assert.match(alice.id, /^[0-9a-f-]{36}$/);
    const bob = await create({ cidr: "172.16.0.0/16" }, "bob");
    const listed = await create({ name: "Public office", visibility: "public" }, "admin");
    assert.equal(listed.visibility, "public");
    const aliceZones = await (await api("/api/zones", "GET", "alice")).json();
    assert.deepEqual(aliceZones.zones.map((zone) => zone.id), [alice.id]);
    const adminZones = await (await api("/api/zones", "GET", "admin")).json();
    assert.deepEqual(adminZones.zones.map((zone) => zone.id), [listed.id]);
    const publicZones = await (await api("/api/zones/public")).json();
    assert.deepEqual(publicZones.zones.map((zone) => zone.id), [listed.id]);
    assert.deepEqual(Object.keys(publicZones.zones[0]).sort(), ["cidr", "created_at", "id", "max_members", "name", "owner_id", "visibility"]);
    const infoResult = await info(alice);
    assert.equal(infoResult.zone.id, alice.id);
    assert.deepEqual(infoResult.members, []);
    assert.deepEqual(infoResult.settings, { welcome: "", default_action: "allow", rules: [] });
    assert.equal((await api(`/api/zones/${alice.id}`, "DELETE", "bob")).status, 403);
    assert.equal((await api(`/api/zones/${alice.id}`, "DELETE", "alice", undefined, { "X-CSRF-Token": "wrong" })).status, 403);
    assert.equal((await api(`/api/zones/${alice.id}`, "DELETE", "alice")).status, 200);
    assert.equal((await api(`/api/zones/${bob.id}`, "DELETE", "admin")).status, 200);
    assert.equal((await api(`/api/zones/${listed.id}`, "DELETE", "admin")).status, 200);
  });

  await context.test("owners kick members, which disconnects and releases their lease", async () => {
    const zone = await create();
    const [alice, bob] = await Promise.all([identity(), identity()]);
    const a = await join(zone, alice, { name: "Alice" });
    const b = await join(zone, bob, { name: "Bob" });
    assert.equal((await api(`/api/zones/${zone.id}/members/${bob.identity}/kick`, "POST", "bob")).status, 403);
    assert.equal((await api(`/api/zones/${zone.id}/members/${bob.identity}/kick`, "POST", "alice", undefined, { "X-CSRF-Token": "wrong" })).status, 403);
    assert.equal((await api(`/api/zones/${zone.id}/members/${"f".repeat(64)}/kick`, "POST", "alice")).status, 404);
    assert.equal((await api(`/api/zones/${zone.id}/members/${bob.identity}/kick`, "POST", "alice")).status, 200);
    assert.match((await b.messages.next("error")).error, /removed from the zone/i);
    assert.equal((await b.messages.closed).code, 4003);
    const roster = await a.messages.next((message) => message.type === "members" && message.peers.length === 0);
    assert.deepEqual(roster.peers, []);
    assert.deepEqual((await info(zone)).members.map((member) => member.identity), [alice.identity]);
    const again = await join(zone, bob, { name: "Bob again" });
    assert.equal(again.ip, b.ip);
    assert.equal((await api(`/api/zones/${zone.id}/members/${alice.identity}/kick`, "POST", "admin")).status, 200);
    assert.equal((await a.messages.closed).code, 4003);
    assert.deepEqual((await info(zone)).members.map((member) => member.identity), [bob.identity]);
    await api(`/api/zones/${zone.id}`, "DELETE", "alice");
    assert.equal((await again.messages.closed).code, 4001);
  });

  await context.test("zone owners publish welcome text and firewall rules to online members", async () => {
    const zone = await create();
    const a = await join(zone, await identity(), { name: "Alice" });
    const rules = [
      { src: "10.88.0.0/24", src_port: "any", proto: "tcp", dst: "any", dst_port: "443", action: "allow" },
      { src: "any", src_port: "any", proto: "udp", dst: "10.88.0.2", dst_port: "1000-2000", action: "deny" },
    ];
    const settings = { welcome: "Welcome to the office zone.\nKeep it tidy.", default_action: "deny", rules };
    assert.equal((await api(`/api/zones/${zone.id}/settings`, "POST", "bob", settings)).status, 403);
    assert.equal((await api(`/api/zones/${zone.id}/settings`, "POST", "alice", settings, { "X-CSRF-Token": "wrong" })).status, 403);
    for (const invalid of [
      { welcome: "x".repeat(513) },
      { welcome: "ok", default_action: "maybe" },
      { rules: "nope" },
      { rules: Array.from({ length: 65 }, () => ({ src: "any", dst: "any", proto: "tcp", action: "allow" })) },
      { rules: [{ src: "10.88.0.0/33", dst: "any", proto: "tcp", action: "allow" }] },
      { rules: [{ src: "any", dst: "any", proto: "sctp", action: "allow" }] },
      { rules: [{ src: "any", dst: "any", proto: "tcp", action: "block" }] },
      { rules: [{ src: "any", src_port: "70000", proto: "tcp", dst: "any", action: "allow" }] },
      { rules: [{ src: "any", src_port: "2000-1000", proto: "tcp", dst: "any", action: "allow" }] },
      { rules: [{ src: "any", src_port: "80", proto: "icmp", dst: "any", action: "allow" }] },
    ]) {
      const response = await api(`/api/zones/${zone.id}/settings`, "POST", "alice", invalid);
      assert.equal(response.status, 400, JSON.stringify(invalid));
    }
    const saved = await api(`/api/zones/${zone.id}/settings`, "POST", "alice", settings);
    assert.equal(saved.status, 200);
    const normalized = [
      { src: "10.88.0.0/24", src_port: "any", proto: "tcp", dst: "any", dst_port: 443, action: "allow" },
      { src: "any", src_port: "any", proto: "udp", dst: "10.88.0.2", dst_port: { from: 1000, to: 2000 }, action: "deny" },
    ];
    const updated = await a.messages.next((message) => message.type === "members" && message.settings?.welcome !== "");
    assert.equal(updated.settings.welcome, settings.welcome);
    assert.equal(updated.settings.default_action, "deny");
    assert.deepEqual(updated.settings.rules, normalized);
    const persisted = await info(zone);
    assert.deepEqual(persisted.settings, updated.settings);
    await worker.unsafeEvictDurableObject("zone-test", "Zone", { name: zone.id, webSockets: "hibernate" });
    assert.deepEqual((await info(zone)).settings, updated.settings);
    await api(`/api/zones/${zone.id}`, "DELETE", "alice");
    assert.equal((await a.messages.closed).code, 4001);
  });

  await context.test("concurrent allocation, rejoin, duplicate identity, exhaustion and both leave forms", async () => {
    const zone = await create({ cidr: "10.88.0.0/30", max_members: 2 });
    const [alice, bob, carol] = await Promise.all([identity(), identity(), identity()]);
    const [a, b] = await Promise.all([join(zone, alice, { name: "Alice" }), join(zone, bob, { name: "Bob" })]);
    assert.deepEqual([a.ip, b.ip].sort(), ["10.88.0.1", "10.88.0.2"]);
    const roster = a.roster.peers.length === 1 ? a.roster : await a.messages.next((message) => message.type === "members" && message.peers.length === 1);
    assert.deepEqual(roster.zone, { id: zone.id, name: zone.name, cidr: zone.cidr, mtu: 1500, visibility: "private" });
    assert.deepEqual(roster.settings, { welcome: "", default_action: "allow", rules: [] });
    assert.deepEqual(roster.peers, [{ identity: bob.identity, key: b.message.key, session: b.message.session, name: "Bob", ip: b.ip }]);
    await rejected(zone, alice, () => {}, /already online/i);
    await rejected(zone, carol, () => {}, /full/i);
    a.socket.close(1000, "Restart");
    await a.messages.closed;
    await waitFor(() => info(zone), (value) => value.members.some((member) => member.identity === alice.identity && !member.online));
    await rejected(zone, carol, () => {}, /full/i);
    const reconnect = await join(zone, alice, { name: "Alice restarted" });
    assert.equal(reconnect.ip, a.ip);
    assert.notEqual(reconnect.message.key, a.message.key);
    const rotated = await b.messages.next((message) => message.type === "members" && message.peers[0]?.key === reconnect.message.key);
    assert.equal(rotated.peers[0].session, reconnect.message.session);
    assert.equal(rotated.peers[0].name, "Alice restarted");
    b.socket.close(1000, "Offline");
    await b.messages.closed;
    await waitFor(() => info(zone), (value) => value.members.some((member) => member.identity === bob.identity && !member.online));
    const leaving = await open(zone);
    leaving.socket.send(JSON.stringify(await signed(zone, leaving, bob, { action: "leave" })));
    await leaving.messages.next("left");
    assert.equal((await leaving.messages.closed).code, 4001);
    const c = await join(zone, carol);
    assert.equal(c.ip, b.ip);
    c.socket.send(JSON.stringify({ type: "leave" }));
    await c.messages.next("left");
    assert.equal((await c.messages.closed).code, 4001);
    assert.equal((await info(zone)).members.length, 1);
    const removeActive = await open(zone);
    removeActive.socket.send(JSON.stringify(await signed(zone, removeActive, alice, { action: "leave" })));
    await removeActive.messages.next("left");
    assert.equal((await reconnect.messages.closed).code, 4001);
    assert.equal((await removeActive.messages.closed).code, 4001);
    assert.deepEqual((await info(zone)).members, []);
    assert.equal((await api(`/api/zones/${zone.id}`, "DELETE", "alice")).status, 200);
  });

  await context.test("signatures cover challenge, action, key, session and exact machine name", async () => {
    const zone = await create();
    const who = await identity();
    for (const change of [
      (message) => { message.signature = "00".repeat(64); },
      (message) => { message.key = randomBytes(32).toString("hex"); },
      (message) => { message.session = randomBytes(16).toString("hex"); },
      (message) => { message.name = "Different machine"; },
      (message) => { message.action = "leave"; },
    ]) await rejected(zone, who, change, /signature/i);
    const first = await open(zone);
    const captured = await signed(zone, first, who);
    first.socket.close(1000, "Restart");
    await first.messages.closed;
    const replay = await open(zone);
    replay.socket.send(JSON.stringify(captured));
    assert.match((await replay.messages.next("error")).error, /signature/i);
    await replay.messages.closed;
    assert.deepEqual((await info(zone)).members, []);
    const active = await join(zone, who);
    active.socket.send(JSON.stringify({ ...active.message, name: "Unsigned change" }));
    assert.match((await active.messages.next("error")).error, /unexpected/i);
    assert.equal((await active.messages.closed).code, 1008);
    assert.equal((await info(zone)).members[0].name, "Test machine");
    const oversized = await open(zone);
    oversized.socket.send("x".repeat(2049));
    assert.match((await oversized.messages.next("error")).error, /too large/i);
    assert.equal((await oversized.messages.closed).code, 1009);
    assert.equal((await api(`/api/zones/${zone.id}`, "DELETE", "alice")).status, 200);
  });

  await context.test("join fields and zone paths require lowercase hex", async () => {
    const zone = await create();
    const who = await identity();
    const packetKey = await webcrypto.subtle.generateKey("X25519", true, ["deriveBits"]);
    const key = hex(await webcrypto.subtle.exportKey("raw", packetKey.publicKey));
    for (const overrides of [
      { identity: who.identity.toUpperCase() },
      { key: key.toUpperCase() },
      { session: "AB".repeat(16) },
    ]) await rejected(zone, who, () => {}, /invalid join/i, overrides);
    await rejected(zone, who, (message) => { message.signature = message.signature.toUpperCase(); }, /invalid join/i);
    const response = await worker.dispatchFetch(`${origin}/zone/${zone.id.toUpperCase()}/socket`, { headers: { Upgrade: "websocket" } });
    assert.equal(response.status, 404);
    assert.deepEqual((await info(zone)).members, []);
    await api(`/api/zones/${zone.id}`, "DELETE", "alice");
  });

  await context.test("signed low-order X25519 keys never acquire leases or enter peer rosters", async () => {
    const zone = await create({ cidr: "10.88.0.0/30", max_members: 2 });
    const healthy = await join(zone, await identity());
    const attacker = await identity();
    const invalidKeys = ["00".repeat(32), `01${"00".repeat(31)}`, `ec${"ff".repeat(30)}7f`];
    for (const key of invalidKeys) {
      await rejected(zone, attacker, () => {}, /invalid X25519 public key/i, { key, name: "Invalid peer" });
      assert.deepEqual((await info(zone)).members, [{ identity: healthy.message.identity, name: healthy.message.name, ip: healthy.ip, online: true }]);
      healthy.socket.send(JSON.stringify({ type: "ping" }));
      await healthy.messages.next("pong");
      assert.equal(healthy.messages.queue.some((message) => message.type === "members" && message.peers.some((peer) => peer.identity === attacker.identity || invalidKeys.includes(peer.key))), false);
      assert.equal(healthy.messages.queue.some((message) => message.type === "error"), false);
    }
    const second = await join(zone, await identity());
    assert.equal(second.ip, "10.88.0.2");
    const roster = await healthy.messages.next((message) => message.type === "members" && message.peers.length === 1);
    assert.equal(roster.peers[0].identity, second.message.identity);
    assert.equal(roster.peers[0].key, second.message.key);
    await api(`/api/zones/${zone.id}`, "DELETE", "alice");
    assert.equal((await healthy.messages.closed).code, 4001);
    assert.equal((await second.messages.closed).code, 4001);
  });

  await context.test("L2 frames learn sender MACs, flood group traffic and drop spoofed sources", async () => {
    const zone = await create({ max_members: 16 });
    const otherZone = await create();
    const [a, outsider] = await Promise.all([join(zone, await identity()), join(otherZone, await identity())]);
    const bWho = await identity();
    const b = await join(zone, bWho);
    const c = await join(zone, await identity());
    await a.messages.next((message) => message.type === "members" && message.peers.length === 2);
    const aMac = memberMac(1);
    const bMac = memberMac(2);
    const cMac = memberMac(3);
    const broadcast = Buffer.from([0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
    const groupMac = Buffer.from([0x01, 0x00, 0x5e, 0x00, 0x00, 0x01]);

    const greeting = frameMessage(bMac, aMac, 1n);
    b.socket.send(greeting);
    assert.deepEqual(await a.messages.next(Buffer.isBuffer), greeting);
    assert.deepEqual(await c.messages.next(Buffer.isBuffer), greeting);
    const unicast = frameMessage(aMac, bMac, 2n);
    a.socket.send(unicast);
    assert.deepEqual(await b.messages.next(Buffer.isBuffer), unicast);
    const broadcastFrame = frameMessage(aMac, broadcast, 3n);
    a.socket.send(broadcastFrame);
    assert.deepEqual(await b.messages.next(Buffer.isBuffer), broadcastFrame);
    assert.deepEqual(await c.messages.next(Buffer.isBuffer), broadcastFrame);
    const groupFrame = frameMessage(bMac, groupMac, 4n);
    b.socket.send(groupFrame);
    const receivedGroup = await c.messages.next(Buffer.isBuffer);
    assert.deepEqual(receivedGroup, groupFrame);
    assert.deepEqual(await a.messages.next(Buffer.isBuffer), groupFrame);

    const spoofed = frameMessage(aMac, bMac, 5n);
    c.socket.send(spoofed);
    c.socket.send(JSON.stringify({ type: "ping" }));
    assert.deepEqual(await c.messages.next("pong"), { type: "pong" });
    assert.equal(b.messages.queue.filter(Buffer.isBuffer).length, 0);
    assert.equal(a.messages.queue.filter(Buffer.isBuffer).length, 0);

    b.socket.close(1000, "Offline");
    await b.messages.closed;
    await waitFor(() => info(zone), (value) => value.members.some((member) => member.identity === bWho.identity && !member.online));
    const whileOffline = frameMessage(aMac, bMac, 6n);
    a.socket.send(whileOffline);
    assert.deepEqual(await c.messages.next(Buffer.isBuffer), whileOffline);

    for (const [mutate, code] of [
      [(bytes) => { bytes[1] = 1; return bytes; }, 1008],
      [(bytes) => { bytes[2] = 1; return bytes; }, 1008],
      [(bytes) => bytes.subarray(0, 40), 1008],
      [(bytes) => Buffer.concat([bytes, Buffer.alloc(4096)]), 1009],
    ]) {
      const peer = await join(zone, await identity());
      peer.socket.send(mutate(frameMessage(memberMac(9), cMac)));
      assert.match((await peer.messages.next("error")).error, /frame/i);
      assert.equal((await peer.messages.closed).code, code);
    }
    assert.equal(c.messages.queue.filter(Buffer.isBuffer).length, 0);
    for (const peer of [outsider]) {
      peer.socket.send(JSON.stringify({ type: "ping" }));
      await peer.messages.next("pong");
      assert.equal(peer.messages.queue.filter(Buffer.isBuffer).length, 0);
    }
    await api(`/api/zones/${zone.id}`, "DELETE", "alice");
    await api(`/api/zones/${otherZone.id}`, "DELETE", "alice");
    assert.equal((await a.messages.closed).code, 4001);
    assert.equal((await outsider.messages.closed).code, 4001);
    assert.equal((await c.messages.closed).code, 4001);
  });

  await context.test("streams route OPEN, ACCEPT, DATA and half-close, and enforce zone rules", async () => {
    const zone = await create({ max_members: 8 });
    const a = await join(zone, await identity(), { name: "Alice" });
    const b = await join(zone, await identity(), { name: "Bob" });
    await a.messages.next((message) => message.type === "members" && message.peers.length === 1);

    a.socket.send(openMessage(7, a.ip, b.ip, 8080));
    const open = await b.messages.next(Buffer.isBuffer);
    assert.deepEqual(streamFields(open), { op: 1, id: 7, rest: Buffer.concat([ipBytes(a.ip), ipBytes(b.ip), Buffer.from([0x1f, 0x90])]) });
    b.socket.send(acceptMessage(7));
    assert.deepEqual(streamFields(await a.messages.next(Buffer.isBuffer)), { op: 2, id: 7, rest: Buffer.alloc(0) });
    const upstream = dataMessage(7, 1n, Buffer.from("hello bob"));
    b.socket.send(upstream);
    assert.deepEqual(await a.messages.next(Buffer.isBuffer), upstream);
    const downstream = dataMessage(7, 1n, Buffer.from("hello alice"));
    a.socket.send(downstream);
    assert.deepEqual(await b.messages.next(Buffer.isBuffer), downstream);
    b.socket.send(closeMessage(7));
    assert.deepEqual(streamFields(await a.messages.next(Buffer.isBuffer)), { op: 5, id: 7, rest: Buffer.alloc(0) });
    const afterHalfClose = dataMessage(7, 2n, Buffer.from("still open"));
    a.socket.send(afterHalfClose);
    assert.deepEqual(await b.messages.next(Buffer.isBuffer), afterHalfClose);
    a.socket.send(closeMessage(7));
    assert.deepEqual(streamFields(await b.messages.next(Buffer.isBuffer)), { op: 5, id: 7, rest: Buffer.alloc(0) });
    a.socket.send(dataMessage(7, 3n));
    assert.deepEqual(streamFields(await a.messages.next(Buffer.isBuffer)), { op: 6, id: 7, rest: Buffer.from([0, 1]) });

    a.socket.send(openMessage(9, a.ip, "10.88.0.200", 80));
    const unreachable = streamFields(await a.messages.next(Buffer.isBuffer));
    assert.equal(unreachable.op, 3);
    assert.equal(unreachable.rest.readUInt16BE(0), 1);
    a.socket.send(openMessage(10, a.ip, a.ip, 80));
    assert.deepEqual(streamFields(await a.messages.next(Buffer.isBuffer)), { op: 6, id: 10, rest: Buffer.from([0, 1]) });
    a.socket.send(openMessage(11, b.ip, b.ip, 80));
    assert.match((await a.messages.next("error")).error, /source/i);
    assert.equal((await a.messages.closed).code, 1008);

    const again = await join(zone, await identity(), { name: "Alice again" });
    const allow = await api(`/api/zones/${zone.id}/settings`, "POST", "alice", {
      welcome: "", default_action: "deny",
      rules: [{ src: "any", src_port: "any", proto: "tcp", dst: "any", dst_port: "8080", action: "allow" }],
    });
    assert.equal(allow.status, 200);
    await b.messages.next((message) => message.type === "members" && message.settings?.default_action === "deny");
    again.socket.send(openMessage(12, again.ip, b.ip, 8080));
    const allowed = await b.messages.next(Buffer.isBuffer);
    assert.deepEqual(streamFields(allowed), { op: 1, id: 12, rest: Buffer.concat([ipBytes(again.ip), ipBytes(b.ip), Buffer.from([0x1f, 0x90])]) });
    again.socket.send(openMessage(13, again.ip, b.ip, 22));
    const blocked = streamFields(await again.messages.next(Buffer.isBuffer));
    assert.equal(blocked.op, 3);
    assert.equal(blocked.rest.readUInt16BE(0), 2);
    assert.deepEqual(again.messages.queue.filter(Buffer.isBuffer).length, 0);

    b.socket.close(1000, "Offline");
    await b.messages.closed;
    assert.deepEqual(streamFields(await again.messages.next(Buffer.isBuffer)), { op: 6, id: 12, rest: Buffer.from([0, 1]) });
    await api(`/api/zones/${zone.id}`, "DELETE", "alice");
    assert.equal((await again.messages.closed).code, 4001);
  });

  await context.test("datagrams route by port and honour firewall rules", async () => {
    const zone = await create({ max_members: 8 });
    const a = await join(zone, await identity(), { name: "Alice" });
    const b = await join(zone, await identity(), { name: "Bob" });
    await a.messages.next((message) => message.type === "members" && message.peers.length === 1);
    const datagram = datagramMessage(a.ip, b.ip, 40000, 5353, Buffer.from("query"));
    a.socket.send(datagram);
    assert.deepEqual(await b.messages.next(Buffer.isBuffer), datagram);
    const reply = datagramMessage(b.ip, a.ip, 5353, 40000, Buffer.from("answer"));
    b.socket.send(reply);
    assert.deepEqual(await a.messages.next(Buffer.isBuffer), reply);

    const deny = await api(`/api/zones/${zone.id}/settings`, "POST", "alice", {
      welcome: "", default_action: "allow",
      rules: [{ src: "any", src_port: "any", proto: "udp", dst: "any", dst_port: "5353", action: "deny" }],
    });
    assert.equal(deny.status, 200);
    await b.messages.next((message) => message.type === "members" && message.settings?.rules?.length === 1);
    a.socket.send(datagramMessage(a.ip, b.ip, 40001, 5353, Buffer.from("blocked")));
    a.socket.send(JSON.stringify({ type: "ping" }));
    assert.deepEqual(await a.messages.next("pong"), { type: "pong" });
    assert.equal(b.messages.queue.filter(Buffer.isBuffer).length, 0);

    a.socket.send(datagramMessage(b.ip, a.ip, 40000, 5353, Buffer.from("spoof")));
    assert.match((await a.messages.next("error")).error, /datagram source/i);
    assert.equal((await a.messages.closed).code, 1008);
    await api(`/api/zones/${zone.id}`, "DELETE", "alice");
    assert.equal((await b.messages.closed).code, 4001);
  });

  await context.test("hibernation restores joined peers and pending challenges without extending auth deadlines", { timeout: 30000 }, async () => {
    const zone = await create();
    const [a, b] = await Promise.all([join(zone, await identity()), join(zone, await identity())]);
    const who = await identity();
    const pending = await open(zone);
    const joinMessage = await signed(zone, pending, who);
    await worker.unsafeEvictDurableObject("zone-test", "Zone", { name: zone.id, webSockets: "hibernate" });
    pending.socket.send(JSON.stringify(joinMessage));
    const resumed = await pending.messages.next("members");
    assert.equal(resumed.peers.length, 2);
    assert.deepEqual(resumed.peers.find((peer) => peer.identity === a.message.identity), {
      identity: a.message.identity, key: a.message.key, session: a.message.session, name: a.message.name, ip: a.ip,
    });
    const frame = frameMessage(memberMac(21), memberMac(22), 1n);
    a.socket.send(frame);
    assert.deepEqual(await b.messages.next(Buffer.isBuffer), frame);
    await rejected(zone, who, () => {}, /already online/i);
    const expiring = await open(zone);
    await delay(1000);
    await worker.unsafeEvictDurableObject("zone-test", "Zone", { name: zone.id, webSockets: "hibernate" });
    assert.match((await expiring.messages.next("error", 11000)).error, /timed out/i);
    assert.equal((await expiring.messages.closed).code, 1008);
    await api(`/api/zones/${zone.id}`, "DELETE", "alice");
    assert.equal((await a.messages.closed).code, 4001);
  });

  await context.test("deleting a zone closes authenticated and pending sockets and clears durable state", async () => {
    const zone = await create();
    const a = await join(zone, await identity());
    const pending = await open(zone);
    assert.equal((await api(`/api/zones/${zone.id}`, "DELETE", "admin")).status, 200);
    assert.equal((await a.messages.closed).code, 4001);
    assert.equal((await pending.messages.closed).code, 4001);
    assert.equal((await api(`/api/zones/${zone.id}/info`)).status, 404);
    const response = await worker.dispatchFetch(`${origin}/zone/${zone.id}/socket`, { headers: { Upgrade: "websocket" } });
    assert.equal(response.status, 404);
    const namespace = await worker.getDurableObjectNamespace("ZONE");
    assert.deepEqual(await namespace.get(namespace.idFromName(zone.id)).getMembers(), []);
    const listed = await (await api("/api/zones/public")).json();
    assert.equal(listed.zones.some((item) => item.id === zone.id), false);
  });
});
