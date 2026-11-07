import { DurableObject } from "cloudflare:workers";
import { ensureSchema, getSessionContext, randomHex } from "./db";
import type { Env } from "./types";

const LEASE_TTL_MS = 24 * 60 * 60 * 1000;
const AUTH_TIMEOUT_MS = 10_000;
const MAX_CONTROL_BYTES = 2048;
const MAX_BINARY_BYTES = 64 * 1024;
const MAX_PENDING_SOCKETS = 32;
const MAX_STREAMS_PER_MEMBER = 256;
const KIND_FRAME = 1;
const KIND_STREAM = 2;
const KIND_DATAGRAM = 3;
const FRAME_VERSION = 2;
const FRAME_HEADER = 22;
const MIN_FRAME = 14;
const MAX_FRAME = 1514;
const TAG_BYTES = 16;
const STREAM_HEADER = 6;
const MAX_STREAM_DATA = 16384;
const MAX_STREAM_REASON = 64;
const DATAGRAM_HEADER = 21;
const MAX_DATAGRAM_PAYLOAD = 1472;
const OP_OPEN = 1;
const OP_ACCEPT = 2;
const OP_REFUSE = 3;
const OP_DATA = 4;
const OP_CLOSE = 5;
const OP_RESET = 6;
const REFUSE_UNREACHABLE = 1;
const REFUSE_NOT_ALLOWED = 2;
const ZONE_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const encoder = new TextEncoder();

export interface ZoneRow {
  id: string;
  name: string;
  cidr: string;
  max_members: number;
  owner_id: string;
  created_at: number;
  visibility: ZoneVisibility;
}

export type ZoneVisibility = "private" | "public";

export interface ZoneInput {
  name: string;
  cidr: string;
  max_members: number;
  visibility: ZoneVisibility;
}

export type ZoneProtocol = "any" | "tcp" | "udp" | "icmp" | "arp";
export type ZonePort = "any" | number | { from: number; to: number };

export interface ZoneRule {
  src: string;
  src_port: ZonePort;
  proto: ZoneProtocol;
  dst: string;
  dst_port: ZonePort;
  action: "allow" | "deny";
}

export interface ZoneSettings {
  welcome: string;
  default_action: "allow" | "deny";
  rules: ZoneRule[];
}

export const DEFAULT_ZONE_SETTINGS: ZoneSettings = { welcome: "", default_action: "allow", rules: [] };
const MAX_ZONE_RULES = 64;
const MAX_WELCOME_CHARS = 512;

export interface ZoneMember {
  identity: string;
  name: string;
  ip: string;
  online: boolean;
}

interface CompiledRule {
  src: { base: number; mask: number } | null;
  srcPort: { from: number; to: number } | null;
  proto: ZoneProtocol;
  dst: { base: number; mask: number } | null;
  dstPort: { from: number; to: number } | null;
  action: "allow" | "deny";
}

interface StreamRecord {
  id: number;
  a: string;
  b: string;
  closedA: boolean;
  closedB: boolean;
}

function parseRuleNetwork(value: string): { base: number; mask: number } | null {
  if (value === "any") return null;
  const [address, prefixText] = value.trim().split("/");
  const prefix = prefixText === undefined ? 32 : Number(prefixText);
  const mask = prefix === 0 ? 0 : (0xffffffff << (32 - prefix)) >>> 0;
  return { base: (ipv4Number(address) & mask) >>> 0, mask };
}

function portRange(port: ZonePort): { from: number; to: number } | null {
  if (port === "any") return null;
  if (typeof port === "number") return { from: port, to: port };
  return { from: port.from, to: port.to };
}

function compileSettings(settings: ZoneSettings): { rules: CompiledRule[]; defaultAllow: boolean } {
  return {
    defaultAllow: settings.default_action !== "deny",
    rules: (settings.rules || []).map((rule) => ({
      src: parseRuleNetwork(rule.src),
      srcPort: portRange(rule.src_port),
      proto: rule.proto,
      dst: parseRuleNetwork(rule.dst),
      dstPort: portRange(rule.dst_port),
      action: rule.action,
    })),
  };
}

function networkHolds(network: { base: number; mask: number } | null, address: number): boolean {
  if (!network) return true;
  return ((address & network.mask) >>> 0) === network.base;
}

function portHolds(range: { from: number; to: number } | null, port: number | null): boolean {
  if (!range) return true;
  return port !== null && port >= range.from && port <= range.to;
}

function ruleAllows(
  compiled: { rules: CompiledRule[]; defaultAllow: boolean },
  proto: "tcp" | "udp",
  srcIP: number,
  srcPort: number | null,
  dstIP: number,
  dstPort: number | null,
): boolean {
  for (const rule of compiled.rules) {
    if (rule.proto !== "any" && rule.proto !== proto) continue;
    if (!networkHolds(rule.src, srcIP)) continue;
    if (!networkHolds(rule.dst, dstIP)) continue;
    if (rule.proto === "any" || rule.proto === "tcp" || rule.proto === "udp") {
      if (!portHolds(rule.srcPort, srcPort)) continue;
      if (!portHolds(rule.dstPort, dstPort)) continue;
    }
    return rule.action === "allow";
  }
  return compiled.defaultAllow;
}

function macHex(bytes: Uint8Array, offset: number): string {
  let out = "";
  for (let index = offset; index < offset + 6; index++) out += bytes[index].toString(16).padStart(2, "0");
  return out;
}

function resetBytes(id: number, code: number): Uint8Array {
  const message = new Uint8Array(STREAM_HEADER + 2);
  message[0] = KIND_STREAM;
  message[1] = OP_RESET;
  new DataView(message.buffer).setUint32(2, id);
  new DataView(message.buffer).setUint16(6, code);
  return message;
}

function refuseBytes(id: number, code: number, reason: string): Uint8Array {
  const text = new TextEncoder().encode(reason.slice(0, MAX_STREAM_REASON));
  const message = new Uint8Array(STREAM_HEADER + 2 + text.byteLength);
  message[0] = KIND_STREAM;
  message[1] = OP_REFUSE;
  const view = new DataView(message.buffer);
  view.setUint32(2, id);
  view.setUint16(6, code);
  message.set(text, 8);
  return message;
}

interface Lease {
  identity: string;
  name: string;
  ip: string;
  expiresAt: number | null;
}

interface SocketAttachment {
  zoneId: string;
  challenge: string;
  deadline: number;
  phase: "pending" | "joined" | "closed";
  identity?: string;
  key?: string;
  session?: string;
  name?: string;
  ip?: string;
}

interface JoinMessage {
  type: "join";
  action: "join" | "leave";
  identity: string;
  key: string;
  session: string;
  name: string;
  signature: string;
}

function ipv4Number(value: string): number {
  return value.split(".").reduce((result, octet) => result * 256 + Number(octet), 0);
}

function ipv4String(value: number): string {
  return [value >>> 24, (value >>> 16) & 255, (value >>> 8) & 255, value & 255].join(".");
}

function validName(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0 && Array.from(value).length <= 64 && !/[\x00-\x1f\x7f]/.test(value);
}

export function validateZoneInput(input: unknown): ZoneInput {
  if (!input || typeof input !== "object") throw new Error("Invalid zone configuration");
  const { name, cidr, max_members, visibility } = input as Record<string, unknown>;
  if (!validName(name)) throw new Error("Zone name must contain 1-64 characters");
  if (visibility !== undefined && visibility !== "private" && visibility !== "public") {
    throw new Error("Zone visibility must be private or public");
  }
  if (typeof cidr !== "string") throw new Error("A private IPv4 CIDR is required");
  const match = /^((?:\d{1,3}\.){3}\d{1,3})\/(\d{1,2})$/.exec(cidr.trim());
  if (!match || !match[1].split(".").every((octet) => /^(0|[1-9]\d{0,2})$/.test(octet) && Number(octet) <= 255)) {
    throw new Error("Invalid IPv4 CIDR");
  }
  const prefix = Number(match[2]);
  if (prefix < 16 || prefix > 30) throw new Error("Zone prefix must be between /16 and /30");
  const size = 2 ** (32 - prefix);
  const network = Math.floor(ipv4Number(match[1]) / size) * size;
  const end = network + size - 1;
  const privateRanges = [[0x0a000000, 0x0affffff], [0xac100000, 0xac1fffff], [0xc0a80000, 0xc0a8ffff]];
  if (!privateRanges.some(([first, last]) => network >= first && end <= last)) {
    throw new Error("Zone network must be in an RFC1918 private address range");
  }
  if (!Number.isInteger(max_members) || Number(max_members) < 2 || Number(max_members) > 256 || Number(max_members) > size - 2) {
    throw new Error("Member limit must be 2-256 and fit the available addresses");
  }
  return {
    name: name.trim(),
    cidr: `${ipv4String(network)}/${prefix}`,
    max_members: Number(max_members),
    visibility: visibility === "public" ? "public" : "private",
  };
}

function validRuleAddress(value: unknown): value is string {
  if (value === "any") return true;
  if (typeof value !== "string") return false;
  const match = /^((?:\d{1,3}\.){3}\d{1,3})(?:\/(\d{1,2}))?$/.exec(value.trim());
  if (!match) return false;
  if (!match[1].split(".").every((octet) => /^(0|[1-9]\d{0,2})$/.test(octet) && Number(octet) <= 255)) return false;
  if (match[2] === undefined) return true;
  const prefix = Number(match[2]);
  return prefix >= 0 && prefix <= 32;
}

function parseRulePort(value: unknown): ZonePort {
  if (value === undefined || value === "any" || value === "") return "any";
  const text = String(value).trim();
  const single = /^(\d{1,5})$/.exec(text);
  if (single) {
    const port = Number(single[1]);
    if (port > 65535) throw new Error("Rule ports must be between 0 and 65535");
    return port;
  }
  const range = /^(\d{1,5})\s*-\s*(\d{1,5})$/.exec(text);
  if (range) {
    const from = Number(range[1]);
    const to = Number(range[2]);
    if (from > to || to > 65535) throw new Error("Rule port ranges must count up and stay below 65536");
    return { from, to };
  }
  throw new Error("Rule ports must be a number, a range like 1000-2000, or any");
}

export function validateZoneSettings(input: unknown): ZoneSettings {
  if (!input || typeof input !== "object") throw new Error("Invalid zone settings");
  const { welcome, default_action, rules } = input as Record<string, unknown>;
  const text = welcome === undefined || welcome === null ? "" : String(welcome);
  if (Array.from(text).length > MAX_WELCOME_CHARS) throw new Error(`Welcome message must stay under ${MAX_WELCOME_CHARS} characters`);
  if (/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/.test(text)) throw new Error("Welcome message contains control characters");
  if (default_action !== undefined && default_action !== "allow" && default_action !== "deny") {
    throw new Error("Default action must be allow or deny");
  }
  if (rules !== undefined && !Array.isArray(rules)) throw new Error("Firewall rules must be an array");
  const list = (rules as unknown[] | undefined) || [];
  if (list.length > MAX_ZONE_RULES) throw new Error(`A zone can hold up to ${MAX_ZONE_RULES} firewall rules`);
  const parsed = list.map((rule, index) => {
    if (!rule || typeof rule !== "object") throw new Error(`Rule ${index + 1} is not an object`);
    const { src, dst, proto, action, src_port, dst_port } = rule as Record<string, unknown>;
    if (!validRuleAddress(src)) throw new Error(`Rule ${index + 1} needs a source network or any`);
    if (!validRuleAddress(dst)) throw new Error(`Rule ${index + 1} needs a destination network or any`);
    if (proto !== "any" && proto !== "tcp" && proto !== "udp" && proto !== "icmp" && proto !== "arp") {
      throw new Error(`Rule ${index + 1} protocol must be any, tcp, udp, icmp or arp`);
    }
    if (action !== "allow" && action !== "deny") throw new Error(`Rule ${index + 1} action must be allow or deny`);
    if ((proto === "icmp" || proto === "arp") && (src_port !== undefined && src_port !== "any" || dst_port !== undefined && dst_port !== "any")) {
      throw new Error(`Rule ${index + 1} cannot use ports with ${proto}`);
    }
    return {
      src, dst, proto, action,
      src_port: parseRulePort(src_port),
      dst_port: parseRulePort(dst_port),
    } as ZoneRule;
  });
  return { welcome: text, default_action: default_action === "deny" ? "deny" : "allow", rules: parsed };
}

export async function listZones(env: Env, ownerId?: string, publicOnly = false): Promise<ZoneRow[]> {
  await ensureSchema(env);
  const query = ownerId
    ? env.DB.prepare("SELECT * FROM zones WHERE owner_id = ? ORDER BY created_at DESC, id").bind(ownerId)
    : publicOnly
      ? env.DB.prepare("SELECT * FROM zones WHERE visibility = 'public' ORDER BY created_at DESC, id")
      : env.DB.prepare("SELECT * FROM zones ORDER BY created_at DESC, id");
  return (await query.all<ZoneRow>()).results || [];
}

export async function getZone(env: Env, id: string): Promise<ZoneRow | null> {
  await ensureSchema(env);
  return env.DB.prepare("SELECT * FROM zones WHERE id = ?").bind(id).first<ZoneRow>();
}

export async function createZone(env: Env, ownerId: string, input: unknown): Promise<ZoneRow> {
  const config = validateZoneInput(input);
  await ensureSchema(env);
  const zone: ZoneRow = { id: crypto.randomUUID(), ...config, owner_id: ownerId, created_at: Date.now() };
  await env.DB.prepare("INSERT INTO zones (id, name, cidr, max_members, owner_id, created_at, visibility) VALUES (?, ?, ?, ?, ?, ?, ?)")
    .bind(zone.id, zone.name, zone.cidr, zone.max_members, zone.owner_id, zone.created_at, zone.visibility).run();
  return zone;
}

export async function deleteZone(env: Env, id: string): Promise<void> {
  await ensureSchema(env);
  await env.DB.prepare("DELETE FROM zones WHERE id = ?").bind(id).run();
  await env.ZONE.get(env.ZONE.idFromName(id)).remove();
}

function json(data: unknown, status = 200): Response {
  return Response.json(data, { status, headers: { "Cache-Control": "no-store", "X-Content-Type-Options": "nosniff" } });
}

export async function handleZones(request: Request, env: Env, url = new URL(request.url)): Promise<Response> {
  if (!env.DB || !env.ZONE) return json({ error: "Zone service is not configured" }, 503);
  const socketRoute = /^\/zone\/([^/]+)\/socket$/.exec(url.pathname);
  if (socketRoute) {
    if (!ZONE_ID.test(socketRoute[1])) return json({ error: "Zone not found" }, 404);
    return env.ZONE.get(env.ZONE.idFromName(socketRoute[1])).fetch(request);
  }
  await ensureSchema(env);
  if (url.pathname === "/api/zones/public" && request.method === "GET") {
    return json({ zones: await listZones(env, undefined, true) });
  }
  const infoRoute = /^\/api\/zones\/([^/]+)\/info$/.exec(url.pathname);
  if (infoRoute && request.method === "GET") {
    const zone = await getZone(env, infoRoute[1]);
    if (!zone) return json({ error: "Zone not found" }, 404);
    const state = env.ZONE.get(env.ZONE.idFromName(zone.id));
    const [members, settings] = await Promise.all([state.getMembers(), state.getSettings()]);
    return json({ zone, members, settings });
  }
  const settingsRoute = /^\/api\/zones\/([^/]+)\/settings$/.exec(url.pathname);
  const kickRoute = /^\/api\/zones\/([^/]+)\/members\/([0-9a-f]{64})\/kick$/.exec(url.pathname);
  const itemRoute = /^\/api\/zones\/([^/]+)$/.exec(url.pathname);
  if (url.pathname !== "/api/zones" && !itemRoute && !settingsRoute && !kickRoute) return json({ error: "Not found" }, 404);
  const context = await getSessionContext(env, request);
  if (!context) return json({ error: "Sign in required" }, 401);
  if (url.pathname === "/api/zones" && request.method === "GET") {
    return json({ zones: await listZones(env, context.user.id) });
  }
  if (settingsRoute && request.method === "POST") {
    const zone = await getZone(env, settingsRoute[1]);
    if (!zone) return json({ error: "Zone not found" }, 404);
    if (!context.user.is_admin && zone.owner_id !== context.user.id) return json({ error: "Forbidden" }, 403);
    if (request.headers.get("X-CSRF-Token") !== context.session.csrf) return json({ error: "Invalid CSRF token" }, 403);
    const body = await request.text();
    if (encoder.encode(body).byteLength > 32 * 1024) return json({ error: "Request is too large" }, 413);
    let settings: ZoneSettings;
    try {
      settings = validateZoneSettings(JSON.parse(body));
    } catch (error) {
      return json({ error: error instanceof SyntaxError ? "Invalid JSON" : (error as Error).message }, 400);
    }
    await env.ZONE.get(env.ZONE.idFromName(zone.id)).updateSettings(settings);
    return json({ settings });
  }
  if (kickRoute && request.method === "POST") {
    const zone = await getZone(env, kickRoute[1]);
    if (!zone) return json({ error: "Zone not found" }, 404);
    if (!context.user.is_admin && zone.owner_id !== context.user.id) return json({ error: "Forbidden" }, 403);
    if (request.headers.get("X-CSRF-Token") !== context.session.csrf) return json({ error: "Invalid CSRF token" }, 403);
    const removed = await env.ZONE.get(env.ZONE.idFromName(zone.id)).kick(kickRoute[2]);
    return removed ? json({ success: true }) : json({ error: "Member not found" }, 404);
  }
  if ((url.pathname === "/api/zones" && request.method === "POST") || (itemRoute && request.method === "DELETE")) {
    if (request.headers.get("X-CSRF-Token") !== context.session.csrf) return json({ error: "Invalid CSRF token" }, 403);
    if (itemRoute) {
      const zone = await getZone(env, itemRoute[1]);
      if (!zone) return json({ error: "Zone not found" }, 404);
      if (!context.user.is_admin && zone.owner_id !== context.user.id) return json({ error: "Forbidden" }, 403);
      await deleteZone(env, zone.id);
      return json({ success: true });
    }
    const body = await request.text();
    if (encoder.encode(body).byteLength > MAX_CONTROL_BYTES) return json({ error: "Request is too large" }, 413);
    let input: ZoneInput;
    try {
      input = validateZoneInput(JSON.parse(body));
    } catch (error) {
      return json({ error: error instanceof SyntaxError ? "Invalid JSON" : (error as Error).message }, 400);
    }
    if (input.visibility === "public" && !context.user.is_admin) {
      return json({ error: "Only administrators can create public zones. Create a private zone and share its ID." }, 403);
    }
    return json({ zone: await createZone(env, context.user.id, input) }, 201);
  }
  return json({ error: "Method not allowed" }, 405);
}

function hexBytes(hex: string): Uint8Array {
  return Uint8Array.from(hex.match(/../g)!, (byte) => parseInt(byte, 16));
}

async function isUsableX25519Key(hex: string): Promise<boolean> {
  try {
    const publicKey = await crypto.subtle.importKey("raw", hexBytes(hex), "X25519", false, []);
    const probe = await crypto.subtle.generateKey("X25519", false, ["deriveBits"]) as CryptoKeyPair;
    const parameters = { name: "X25519", public: publicKey };
    const shared = await crypto.subtle.deriveBits(parameters, probe.privateKey, 256);
    return new Uint8Array(shared).some((byte) => byte !== 0);
  } catch {
    return false;
  }
}

function isJoinMessage(message: Record<string, unknown>): message is Record<string, unknown> & JoinMessage {
  return message.type === "join" && (message.action === "join" || message.action === "leave") &&
    typeof message.identity === "string" && /^[0-9a-f]{64}$/.test(message.identity) &&
    typeof message.key === "string" && /^[0-9a-f]{64}$/.test(message.key) &&
    typeof message.session === "string" && /^[0-9a-f]{32}$/.test(message.session) &&
    validName(message.name) && typeof message.signature === "string" && /^[0-9a-f]{128}$/.test(message.signature);
}

export class Zone extends DurableObject<Env> {
  private state: DurableObjectState;
  private zone: ZoneRow | null = null;
  private zoneId: string | null = null;
  private leases = new Map<string, Lease>();
  private sockets = new Map<WebSocket, SocketAttachment>();
  private online = new Map<string, WebSocket>();
  private macOwners = new Map<string, string>();
  private streams = new Map<string, StreamRecord>();
  private settings: ZoneSettings = DEFAULT_ZONE_SETTINGS;
  private compiled = compileSettings(DEFAULT_ZONE_SETTINGS);
  private pending: Promise<void> = Promise.resolve();
  private deleted = false;

  constructor(state: DurableObjectState, env: Env) {
    super(state, env);
    this.state = state;
    state.blockConcurrencyWhile(async () => {
      this.zoneId = await state.storage.get<string>("zoneId") || null;
      const storedSettings = await state.storage.get<ZoneSettings>("settings");
      if (storedSettings) {
        this.settings = {
          welcome: typeof storedSettings.welcome === "string" ? storedSettings.welcome : "",
          default_action: storedSettings.default_action === "deny" ? "deny" : "allow",
          rules: Array.isArray(storedSettings.rules) ? storedSettings.rules : [],
        };
        this.compiled = compileSettings(this.settings);
      }
      const stored = await state.storage.list<Lease>({ prefix: "lease:" });
      for (const lease of stored.values()) this.leases.set(lease.identity, lease);
      for (const socket of state.getWebSockets()) {
        const meta = socket.deserializeAttachment() as SocketAttachment;
        if (!meta || meta.phase === "closed" || socket.readyState !== WebSocket.OPEN) continue;
        this.sockets.set(socket, meta);
        this.zoneId = meta.zoneId;
        if (meta.phase === "joined") this.online.set(meta.identity!, socket);
      }
      if (this.zoneId) {
        this.zone = await getZone(env, this.zoneId);
        if (!this.zone) {
          await this.clear();
          return;
        }
      }
      let recoveredLease = false;
      for (const lease of this.leases.values()) {
        if (!this.online.has(lease.identity) && lease.expiresAt === null) {
          lease.expiresAt = Date.now() + LEASE_TTL_MS;
          await this.saveLease(lease);
          recoveredLease = true;
        }
      }
      await this.expireLeases();
      if (recoveredLease) await this.scheduleAlarm();
    });
  }

  private serial<T>(work: () => Promise<T>): Promise<T> {
    const result = this.pending.then(work);
    this.pending = result.then(() => undefined, () => undefined);
    return result;
  }

  async fetch(request: Request): Promise<Response> {
    return this.serial(async () => {
      const match = /^\/zone\/([^/]+)\/socket$/.exec(new URL(request.url).pathname);
      if (!match || !ZONE_ID.test(match[1]) || this.deleted) return json({ error: "Zone not found" }, 404);
      if (request.method !== "GET" || request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
        return json({ error: "WebSocket upgrade required" }, 426);
      }
      this.zone = await getZone(this.env, match[1]);
      if (!this.zone) return json({ error: "Zone not found" }, 404);
      this.zoneId = this.zone.id;
      if ([...this.sockets.values()].filter((meta) => meta.phase === "pending").length >= MAX_PENDING_SOCKETS) {
        return json({ error: "Too many pending connections" }, 429);
      }
      await this.state.storage.put("zoneId", this.zoneId);
      const [client, server] = Object.values(new WebSocketPair());
      const meta: SocketAttachment = {
        zoneId: this.zoneId, challenge: randomHex(32), deadline: Date.now() + AUTH_TIMEOUT_MS, phase: "pending",
      };
      this.state.acceptWebSocket(server);
      server.serializeAttachment(meta);
      this.sockets.set(server, meta);
      this.send(server, { type: "challenge", challenge: meta.challenge });
      await this.scheduleAlarm();
      return new Response(null, { status: 101, webSocket: client });
    });
  }

  async getMembers(): Promise<ZoneMember[]> {
    return this.serial(async () => {
      await this.expireLeases();
      return [...this.leases.values()].map((lease) => ({
        identity: lease.identity, name: lease.name, ip: lease.ip, online: this.online.has(lease.identity),
      }));
    });
  }

  async getSettings(): Promise<ZoneSettings> {
    return this.serial(async () => this.settings);
  }

  async updateSettings(settings: ZoneSettings): Promise<ZoneSettings> {
    return this.serial(async () => {
      this.settings = settings;
      this.compiled = compileSettings(settings);
      await this.state.storage.put("settings", settings);
      await this.broadcastRoster();
      return this.settings;
    });
  }

  async kick(identity: string): Promise<boolean> {
    return this.serial(async () => {
      const lease = this.leases.get(identity);
      if (!lease) return false;
      const active = this.online.get(identity);
      if (active) {
        this.send(active, { type: "error", error: "Removed from the zone by its owner" });
        await this.detach(active);
        active.close(4003, "Removed from zone");
      }
      this.leases.delete(identity);
      await this.state.storage.delete(`lease:${identity}`);
      await this.broadcastRoster();
      await this.scheduleAlarm();
      return true;
    });
  }

  async remove(): Promise<void> {
    return this.serial(() => this.clear());
  }

  private async clear(): Promise<void> {
    this.deleted = true;
    for (const socket of this.state.getWebSockets()) {
      socket.serializeAttachment({ phase: "closed" });
      socket.close(4001, "Zone deleted");
    }
    this.online.clear();
    this.sockets.clear();
    this.leases.clear();
    this.macOwners.clear();
    this.streams.clear();
    this.zone = null;
    await this.state.storage.deleteAll();
    await this.state.storage.deleteAlarm();
  }

  async webSocketMessage(socket: WebSocket, message: string | ArrayBuffer): Promise<void> {
    return this.serial(async () => {
      const meta = this.sockets.get(socket);
      if (!meta || this.deleted) return;
      if (meta.phase === "pending" && Date.now() >= meta.deadline) {
        await this.fail(socket, "Authentication timed out");
        return;
      }
      if (typeof message !== "string") {
        if (meta.phase !== "joined") {
          await this.fail(socket, "Authentication required");
          return;
        }
        const bytes = new Uint8Array(message);
        if (bytes.byteLength === 0 || bytes.byteLength > MAX_BINARY_BYTES) {
          await this.fail(socket, "Invalid binary message", bytes.byteLength > MAX_BINARY_BYTES ? 1009 : 1008);
          return;
        }
        switch (bytes[0]) {
          case KIND_FRAME:
            await this.handleFrame(socket, meta, bytes);
            return;
          case KIND_STREAM:
            await this.handleStream(socket, meta, bytes);
            return;
          case KIND_DATAGRAM:
            await this.handleDatagram(socket, meta, bytes);
            return;
          default:
            await this.fail(socket, "Invalid binary message kind");
            return;
        }
      }
      if (message.length > MAX_CONTROL_BYTES || encoder.encode(message).byteLength > MAX_CONTROL_BYTES) {
        await this.fail(socket, "Control message is too large", 1009);
        return;
      }
      let control: Record<string, unknown>;
      try {
        control = JSON.parse(message);
        if (!control || typeof control !== "object" || Array.isArray(control)) throw new Error();
      } catch {
        await this.fail(socket, "Invalid control message");
        return;
      }
      if (control.type === "ping") {
        this.send(socket, { type: "pong" });
        return;
      }
      if (meta.phase === "joined") {
        if (control.type === "leave") await this.leave(meta.identity!, socket);
        else await this.fail(socket, "Unexpected control message");
        return;
      }
      if (!isJoinMessage(control)) {
        await this.fail(socket, "Invalid join message");
        return;
      }
      const signed = `linksocks-zone-v1\n${meta.zoneId}\n${meta.challenge}\n${control.action}\n${control.key}\n${control.session}\n${control.name}`;
      let valid = false;
      try {
        const key = await crypto.subtle.importKey("raw", hexBytes(control.identity), "Ed25519", false, ["verify"]);
        valid = await crypto.subtle.verify("Ed25519", key, hexBytes(control.signature), encoder.encode(signed));
      } catch {
        // Invalid public keys and signatures both fail authentication.
      }
      if (!valid || Date.now() >= meta.deadline) {
        await this.fail(socket, valid ? "Authentication timed out" : "Invalid identity signature");
        return;
      }
      const identity = control.identity;
      if (control.action === "leave") {
        await this.leave(identity, socket);
        return;
      }
      if (this.online.has(identity)) {
        await this.fail(socket, "Identity is already online");
        return;
      }
      const usableKey = await isUsableX25519Key(control.key);
      if (!usableKey || Date.now() >= meta.deadline) {
        await this.fail(socket, usableKey ? "Authentication timed out" : "Invalid X25519 public key");
        return;
      }
      await this.expireLeases();
      let lease = this.leases.get(identity);
      if (!lease) {
        if (this.leases.size >= this.zone!.max_members) {
          await this.fail(socket, "Zone is full");
          return;
        }
        const network = ipv4Number(this.zone!.cidr.split("/")[0]);
        const used = new Set([...this.leases.values()].map((item) => item.ip));
        let address = network + 1;
        while (used.has(ipv4String(address))) address++;
        lease = { identity, name: control.name, ip: ipv4String(address), expiresAt: null };
      }
      lease.name = control.name;
      lease.expiresAt = null;
      await this.saveLease(lease);
      this.leases.set(identity, lease);
      Object.assign(meta, { phase: "joined", identity, key: control.key, session: control.session, name: control.name, ip: lease.ip });
      socket.serializeAttachment(meta);
      this.online.set(identity, socket);
      await this.broadcastRoster();
      await this.scheduleAlarm();
    });
  }

  private findMemberByIP(ip: string, except?: WebSocket): WebSocket | null {
    for (const socket of this.online.values()) {
      if (socket === except) continue;
      if (this.sockets.get(socket)?.ip === ip) return socket;
    }
    return null;
  }

  private async deliver(socket: WebSocket, message: Uint8Array): Promise<boolean> {
    try {
      socket.send(message);
      return true;
    } catch {
      await this.detach(socket);
      await this.broadcastRoster();
      await this.scheduleAlarm();
      return false;
    }
  }

  private async handleFrame(socket: WebSocket, meta: SocketAttachment, bytes: Uint8Array): Promise<void> {
    const size = bytes.byteLength;
    if (bytes[1] !== FRAME_VERSION || (bytes[2] & 1) === 1 || size < FRAME_HEADER + MIN_FRAME + TAG_BYTES || size > FRAME_HEADER + MAX_FRAME + TAG_BYTES) {
      await this.fail(socket, "Invalid packet frame", size > FRAME_HEADER + MAX_FRAME + TAG_BYTES ? 1009 : 1008);
      return;
    }
    const sourceMac = macHex(bytes, 2);
    const destinationMac = macHex(bytes, 8);
    const owner = this.macOwners.get(sourceMac);
    if (owner && owner !== meta.identity && this.online.has(owner)) return;
    this.macOwners.set(sourceMac, meta.identity!);
    const peers = [...this.online.values()].filter((peer) => peer !== socket);
    if ((bytes[8] & 1) === 1) {
      for (const peer of peers) await this.deliver(peer, bytes);
      return;
    }
    const targetOwner = this.macOwners.get(destinationMac);
    if (targetOwner === meta.identity) return;
    const target = targetOwner ? this.online.get(targetOwner) : undefined;
    if (target) {
      await this.deliver(target, bytes);
      return;
    }
    for (const peer of peers) await this.deliver(peer, bytes);
  }

  private async handleStream(socket: WebSocket, meta: SocketAttachment, bytes: Uint8Array): Promise<void> {
    if (bytes.byteLength < STREAM_HEADER) {
      await this.fail(socket, "Invalid zone stream message");
      return;
    }
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const op = bytes[1];
    const id = view.getUint32(2);
    const identity = meta.identity!;
    if (op < OP_OPEN || op > OP_RESET || id === 0) {
      await this.fail(socket, "Invalid zone stream message");
      return;
    }
    if (op === OP_OPEN) {
      if (bytes.byteLength !== STREAM_HEADER + 10) {
        await this.fail(socket, "Invalid zone stream message");
        return;
      }
      const sourceIP = ipv4String(view.getUint32(6));
      const destinationIP = ipv4String(view.getUint32(10));
      const destinationPort = view.getUint16(14);
      if (sourceIP !== meta.ip) {
        await this.fail(socket, "Stream source does not match assigned address");
        return;
      }
      if (destinationIP === meta.ip) {
        this.send(socket, resetBytes(id, REFUSE_UNREACHABLE));
        return;
      }
      let count = 0;
      for (const record of this.streams.values()) if (record.a === identity) count++;
      if (count >= MAX_STREAMS_PER_MEMBER) {
        this.send(socket, refuseBytes(id, REFUSE_UNREACHABLE, "too many streams"));
        return;
      }
      if (this.streams.has(`${identity}|${id}`)) {
        this.send(socket, resetBytes(id, REFUSE_UNREACHABLE));
        return;
      }
      const rules = this.compiled;
      if (!ruleAllows(rules, "tcp", ipv4Number(meta.ip!), null, ipv4Number(destinationIP), destinationPort)) {
        this.send(socket, refuseBytes(id, REFUSE_NOT_ALLOWED, "blocked by zone rules"));
        return;
      }
      const target = this.findMemberByIP(destinationIP, socket);
      if (!target) {
        this.send(socket, refuseBytes(id, REFUSE_UNREACHABLE, "member is not online"));
        return;
      }
      const record: StreamRecord = { id, a: identity, b: this.sockets.get(target)!.identity!, closedA: false, closedB: false };
      this.streams.set(`${record.a}|${id}`, record);
      this.streams.set(`${record.b}|${id}`, record);
      if (!(await this.deliver(target, bytes))) this.send(socket, refuseBytes(id, REFUSE_UNREACHABLE, "member is not online"));
      return;
    }
    if (op === OP_DATA) {
      if (bytes.byteLength < STREAM_HEADER + 8 + TAG_BYTES + 1 || bytes.byteLength > STREAM_HEADER + 8 + MAX_STREAM_DATA + TAG_BYTES) {
        await this.fail(socket, "Invalid zone stream message", bytes.byteLength > STREAM_HEADER + 8 + MAX_STREAM_DATA + TAG_BYTES ? 1009 : 1008);
        return;
      }
    } else if (op === OP_REFUSE) {
      if (bytes.byteLength < STREAM_HEADER + 2 || bytes.byteLength > STREAM_HEADER + 2 + MAX_STREAM_REASON) {
        await this.fail(socket, "Invalid zone stream message");
        return;
      }
    } else if (bytes.byteLength !== STREAM_HEADER && !(op === OP_RESET && bytes.byteLength === STREAM_HEADER + 2)) {
      await this.fail(socket, "Invalid zone stream message");
      return;
    }
    const record = this.streams.get(`${identity}|${id}`);
    if (!record) {
      if (op === OP_DATA || op === OP_CLOSE || op === OP_ACCEPT) this.send(socket, resetBytes(id, REFUSE_UNREACHABLE));
      return;
    }
    const partnerIdentity = record.a === identity ? record.b : record.a;
    const partner = this.online.get(partnerIdentity);
    if (!partner) {
      this.dropStream(record);
      this.send(socket, resetBytes(id, REFUSE_UNREACHABLE));
      return;
    }
    if (op === OP_CLOSE) {
      if (identity === record.a) record.closedA = true;
      else record.closedB = true;
      if ((await this.deliver(partner, bytes)) && record.closedA && record.closedB) this.dropStream(record);
      return;
    }
    if (op === OP_RESET) {
      this.dropStream(record);
    }
    await this.deliver(partner, bytes);
  }

  private dropStream(record: StreamRecord): void {
    this.streams.delete(`${record.a}|${record.id}`);
    this.streams.delete(`${record.b}|${record.id}`);
  }

  private async resetStreamsOf(identity: string): Promise<void> {
    for (const record of [...new Set(this.streams.values())]) {
      if (record.a !== identity && record.b !== identity) continue;
      const partnerIdentity = record.a === identity ? record.b : record.a;
      const partner = this.online.get(partnerIdentity);
      this.dropStream(record);
      if (partner) this.send(partner, resetBytes(record.id, REFUSE_UNREACHABLE));
    }
  }

  private async handleDatagram(socket: WebSocket, meta: SocketAttachment, bytes: Uint8Array): Promise<void> {
    const size = bytes.byteLength;
    if (size < DATAGRAM_HEADER + TAG_BYTES || size > DATAGRAM_HEADER + MAX_DATAGRAM_PAYLOAD + TAG_BYTES) {
      await this.fail(socket, "Invalid zone datagram", size > DATAGRAM_HEADER + MAX_DATAGRAM_PAYLOAD + TAG_BYTES ? 1009 : 1008);
      return;
    }
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const sourceIP = ipv4String(view.getUint32(1));
    const destinationIP = ipv4String(view.getUint32(5));
    const sourcePort = view.getUint16(9);
    const destinationPort = view.getUint16(11);
    if (sourceIP !== meta.ip) {
      await this.fail(socket, "Datagram source does not match assigned address");
      return;
    }
    const target = this.findMemberByIP(destinationIP, socket);
    if (!target) return;
    if (!ruleAllows(this.compiled, "udp", ipv4Number(meta.ip!), sourcePort, ipv4Number(destinationIP), destinationPort)) return;
    await this.deliver(target, bytes);
  }

  async webSocketClose(socket: WebSocket, code: number, reason: string): Promise<void> {
    return this.serial(async () => {
      await this.detach(socket);
      socket.close(code === 1005 || code === 1006 ? 1000 : code, reason);
      await this.broadcastRoster();
      await this.scheduleAlarm();
    });
  }

  async webSocketError(socket: WebSocket): Promise<void> {
    return this.serial(async () => {
      await this.detach(socket);
      socket.close(1011, "Transport error");
      await this.broadcastRoster();
      await this.scheduleAlarm();
    });
  }

  async alarm(): Promise<void> {
    return this.serial(async () => {
      for (const [socket, meta] of this.sockets) {
        if (meta.phase === "pending" && meta.deadline <= Date.now()) {
          this.send(socket, { type: "error", error: "Authentication timed out" });
          await this.detach(socket);
          socket.close(1008, "Authentication timed out");
        }
      }
      await this.expireLeases();
      await this.scheduleAlarm();
    });
  }

  private saveLease(lease: Lease): Promise<void> {
    return this.state.storage.put(`lease:${lease.identity}`, lease);
  }

  private async expireLeases(): Promise<void> {
    for (const [identity, lease] of this.leases) {
      if (!this.online.has(identity) && lease.expiresAt !== null && lease.expiresAt <= Date.now()) {
        this.leases.delete(identity);
        await this.state.storage.delete(`lease:${identity}`);
      }
    }
  }

  private async scheduleAlarm(): Promise<void> {
    const deadlines = [...this.sockets.values()].filter((meta) => meta.phase === "pending").map((meta) => meta.deadline);
    for (const lease of this.leases.values()) if (lease.expiresAt !== null) deadlines.push(lease.expiresAt);
    if (deadlines.length) await this.state.storage.setAlarm(Math.min(...deadlines));
    else await this.state.storage.deleteAlarm();
  }

  private send(socket: WebSocket, message: unknown): boolean {
    try {
      if (message instanceof Uint8Array) socket.send(message);
      else socket.send(JSON.stringify(message));
      return true;
    } catch {
      return false;
    }
  }

  private async detach(socket: WebSocket): Promise<void> {
    const meta = this.sockets.get(socket);
    if (!meta) return;
    if (meta.phase === "joined" && meta.identity) {
      if (this.online.get(meta.identity) === socket) {
        this.online.delete(meta.identity);
        await this.resetStreamsOf(meta.identity);
        for (const [mac, owner] of this.macOwners) if (owner === meta.identity) this.macOwners.delete(mac);
        const lease = this.leases.get(meta.identity);
        if (lease) {
          lease.expiresAt = Date.now() + LEASE_TTL_MS;
          await this.saveLease(lease);
        }
      }
    }
    meta.phase = "closed";
    socket.serializeAttachment(meta);
    this.sockets.delete(socket);
  }

  private async fail(socket: WebSocket, error: string, code = 1008): Promise<void> {
    this.send(socket, { type: "error", error });
    await this.detach(socket);
    socket.close(code, error);
    await this.broadcastRoster();
    await this.scheduleAlarm();
  }

  private async leave(identity: string, requester: WebSocket): Promise<void> {
    const active = this.online.get(identity);
    for (const socket of new Set([requester, ...(active ? [active] : [])])) {
      if (socket === requester) this.send(socket, { type: "left" });
      await this.detach(socket);
      socket.close(4001, "Member removed");
    }
    this.leases.delete(identity);
    await this.state.storage.delete(`lease:${identity}`);
    await this.broadcastRoster();
    await this.scheduleAlarm();
  }

  private async broadcastRoster(): Promise<void> {
    if (!this.zone) return;
    let failed: WebSocket[];
    do {
      failed = [];
      const members = [...this.online.values()].map((socket) => ({ socket, meta: this.sockets.get(socket)! }));
      for (const { socket, meta } of members) {
        const delivered = this.send(socket, {
          type: "members",
          zone: { id: this.zone.id, name: this.zone.name, cidr: this.zone.cidr, mtu: 1500, visibility: this.zone.visibility },
          self: meta.ip,
          settings: this.settings,
          peers: members.filter((peer) => peer.socket !== socket).map(({ meta: peer }) => ({
            identity: peer.identity, key: peer.key, session: peer.session, name: peer.name, ip: peer.ip,
          })),
        });
        if (!delivered) failed.push(socket);
      }
      for (const socket of failed) await this.detach(socket);
    } while (failed.length);
  }
}
