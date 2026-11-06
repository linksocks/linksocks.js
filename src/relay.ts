import { DurableObject } from "cloudflare:workers";
import zxcvbn from "zxcvbn";

import type { Env, WebsocketMeta } from "./types";
import {
  type BaseMessage,
  type ConnectMessage,
  type ConnectorMessage,
  type ConnectorResponseMessage,
  type ConnectResponseMessage,
  type ChannelOpenMessage,
  type DataMessage,
  type DisconnectMessage,
  type AuthMessage,
  type PartnersMessage,
  type SealedMessage,
  type ChannelBindMessage,
  type ChannelMigrateMessage,
  type ChannelMigrateAckMessage,
  type ChannelDataAckMessage,
  type WebRegisterMessage,
  type WebRegisterResponseMessage,
  type WebRequestMessage,
  type WebResponseMessage,
  type WebDataMessage,
  type WebCancelMessage,
  type WebSocketFrameMessage,
  type WebRotateMessage,
  parseMessageWithVersion,
  packMessage,
  protocolMajor,
  isCurrentProtocolMinor,
  MessageType,
  PROTOCOL_VERSION,
  PROTOCOL_VERSION_V2,
  AuthResponseMessage,
  LogMessage,
} from "./message";
import { handleErrors } from "./common";
import { describeCloudflareColo } from "./colo";
import { MIN_VERSION_WEB, MIN_VERSION_WEBSOCKET, supportsFeatureVersion } from "./feature-version";
import { type Token } from "./token";
import {
  type Settings,
  type SiteRow,
  type UserRow,
  consumeQuota,
  deleteRetiredHostsByRelay,
  deleteSite,
  effectiveSettings,
  ensureSchema,
  findUserByTokenHash,
  getReservation,
  getRetiredHost,
  getSite,
  isSiteHostForZone,
  listReservations,
  listSites,
  listSitesByRelay,
  normalizeHost,
  quotaCanFit,
  randomHostLabel,
  rotateSiteRecord,
  sha256Hex,
  siteUrl,
  upsertSite,
} from "./db";
import { renderExpiredPage, renderErrorPage } from "./ui";

interface WebUsage {
  requests: number;
  bytes: number;
  lastSeenAt: number;
}

interface NetworkQuota {
  principal: string;
  bytes: number;
  requests: number;
}

interface WebRotationResult {
  host: string;
  url: string;
  expiresAt: number;
  rotateSeconds: number;
}

interface WebPending {
  host: string;
  ws: WebSocket;
  method: string;
  quotaPrincipal: string;
  quotaBytes: number;
  quotaRequests: number;
  controller: ReadableStreamDefaultController<Uint8Array> | null;
  responseReceived: boolean;
  resolve: (msg: WebResponseMessage) => void;
  reject: (err: Error) => void;
  timeout: ReturnType<typeof setTimeout> | null;
  bytes: number;
}

class WebQuotaExceededError extends Error {}

const MAX_WEB_BODY_BYTES = 16 * 1024 * 1024;
const WEB_DATA_CHUNK_BYTES = 32 * 1024;
const WEB_RESPONSE_TIMEOUT_MS = 30_000;
const WEB_USAGE_FLUSH_DELAY_MS = 5_000;
const SITE_CACHE_TTL_MS = 30_000;
const WEB_MESSAGE_TYPES = new Set<string>([
  MessageType.WebRegister,
  MessageType.WebRegisterResponse,
  MessageType.WebRequest,
  MessageType.WebResponse,
  MessageType.WebData,
  MessageType.WebCancel,
  MessageType.WebRotate,
  MessageType.WebSocketFrame,
]);

function isHopByHopHeader(name: string): boolean {
  const lower = name.toLowerCase();
  return (
    lower === "connection" ||
    lower === "keep-alive" ||
    lower === "proxy-authenticate" ||
    lower === "proxy-authorization" ||
    lower === "proxy-connection" ||
    lower === "te" ||
    lower === "trailer" ||
    lower === "transfer-encoding" ||
    lower === "upgrade" ||
    lower === "expect"
  );
}

function isInternalHeader(name: string): boolean {
  return name.toLowerCase().startsWith("x-ls-");
}

function utcDayKey(now = Date.now()): string {
  return new Date(now).toISOString().slice(0, 10);
}

function humanizeDuration(ms: number): string {
  if (ms >= 24 * 60 * 60 * 1000 && ms % (24 * 60 * 60 * 1000) === 0) {
    return `${ms / (24 * 60 * 60 * 1000)} day${ms === 24 * 60 * 60 * 1000 ? "" : "s"}`;
  }
  if (ms >= 60 * 60 * 1000 && ms % (60 * 60 * 1000) === 0) {
    return `${ms / (60 * 60 * 1000)} hour${ms === 60 * 60 * 1000 ? "" : "s"}`;
  }
  if (ms >= 60_000 && ms % 60_000 === 0) {
    return `${ms / 60_000} minute${ms === 60_000 ? "" : "s"}`;
  }
  return `${Math.max(1, Math.round(ms / 1000))} seconds`;
}


export class Relay extends DurableObject {
  private static readonly PROVIDER_DISCONNECT_GRACE_MS = 60_000;
  private static readonly WEB_RELAY_HOLD_MS = 24 * 60 * 60 * 1000;
  private static readonly WEB_FORCED_TEMP_HOLD_MS = 60_000;
  private static readonly CONNECTOR_SHUTDOWN_NOTICE_DELAY_MS = 100;
  private static readonly TRAFFIC_FLUSH_IDLE_MS = 60_000;
  private static readonly DEFAULT_CONNECTOR_WAIT_PROVIDER_MS = 5_000;
  // Rotate before the hostname expires so every provider has time to adopt the
  // replacement. The relay owns this timer; clients only receive WebRotate.
  private static readonly WEB_ROTATE_LEAD_MS = 120_000;

  private providerChannels: Map<string, WebSocket>;
  private connectorChannels: Map<string, WebSocket>;
  private providers: Set<WebSocket>;
  private connectors: Set<WebSocket>;
  private currentProviderIndex: number;

  private lastSyncAt: number = 0;

  private state: DurableObjectState;
  protected declare env: Env;
  private storage: DurableObjectStorage;
  private token: DurableObjectStub<Token>;

  private trafficAccumulator: number = 0;
  private lastTrafficReport: number = 0;
  private trafficFlushPromise: Promise<void> | null = null;

  private nextAlarmAt: number = 0;
  private alarmArmPromise: Promise<void> = Promise.resolve();

  private ownColo: string | null = null;

  // Channel creation counter – flushed together with traffic to avoid per-channel Token DO writes.
  private pendingChannelCreations: number = 0;

  // Web reverse proxy state.
  private webProviders: Map<string, WebSocket[]> = new Map();
  private webRoundRobin: Map<string, number> = new Map();
  private webPending: Map<string, WebPending> = new Map();
  private webDataQueues: Map<string, Promise<void>> = new Map();
  private networkQueues: Map<string, Promise<void>> = new Map();
  private webSocketClients: Map<string, WebSocket> = new Map();
  private webSocketProviders: Map<string, WebSocket> = new Map();
  private webUsage: Record<string, WebUsage> = {};
  private webUsageDay: string = "";
  private webUsageTimer: ReturnType<typeof setTimeout> | null = null;
  private siteCache: Map<string, { site: SiteRow | null; at: number }> = new Map();
  // Registration and rotation both read-then-write D1 site rows. A single
  // per-DO chain keeps those critical sections from interleaving.
  private siteMutationChain: Promise<void> = Promise.resolve();

  private async getOwnerTokenKey(): Promise<string> {
    const v = (await this.storage.get("ownerTokenKey")) as string | undefined;
    if (typeof v === "string" && v.trim()) return v.trim();
    return "";
  }

  private async setOwnerTokenKeyFromActualToken(actualToken: string): Promise<void> {
    const key = await this.toTokenKey(actualToken);
    if (!key) return;
    await this.storage.put("ownerTokenKey", key);
  }

  private scheduleAlarmAt(when: number): Promise<void> {
    const target = Math.max(1, Math.floor(when));
    const now = Date.now();
    if (this.nextAlarmAt > now && this.nextAlarmAt <= target) return this.alarmArmPromise;
    this.nextAlarmAt = target;

    this.alarmArmPromise = this.alarmArmPromise
      .then(() => this.storage.setAlarm(this.nextAlarmAt))
      .catch(() => this.storage.setAlarm(this.nextAlarmAt));
    return this.alarmArmPromise;
  }

  constructor(state: DurableObjectState, env: Env) {
    super(state, env);

    this.state = state;
    this.env = env;
    this.storage = state.storage;

    this.token = env.TOKEN.get(env.TOKEN.idFromName("main"));

    this.providers = new Set();
    this.connectors = new Set();
    this.currentProviderIndex = 0;

    this.providerChannels = new Map();
    this.connectorChannels = new Map();

    this.state.blockConcurrencyWhile(async () => {
      const alarm = await this.storage.getAlarm();
      if (typeof alarm === "number") this.nextAlarmAt = alarm;

      const trafficState = (await this.storage.get("trafficState")) as
        | { accumulator?: number; lastTrafficReport?: number; pendingChannels?: number }
        | undefined;

      if (trafficState && typeof trafficState === "object") {
        if (typeof trafficState.accumulator === "number") this.trafficAccumulator = trafficState.accumulator;
        if (typeof trafficState.lastTrafficReport === "number") this.lastTrafficReport = trafficState.lastTrafficReport;
        if (typeof trafficState.pendingChannels === "number") this.pendingChannelCreations = trafficState.pendingChannels;
      }

      const storedWebUsage = (await this.storage.get("webUsage")) as Record<string, WebUsage> | undefined;
      if (storedWebUsage && typeof storedWebUsage === "object") {
        this.webUsage = storedWebUsage;
      }

      const storedDay = (await this.storage.get("webUsageDay")) as string | undefined;
      this.webUsageDay = typeof storedDay === "string" ? storedDay : "";
      const today = utcDayKey();
      if (this.webUsageDay !== today) {
        this.webUsage = {};
        this.webUsageDay = today;
        await this.storage.put("webUsage", {});
        await this.storage.put("webUsageDay", today);
      }

      if (this.trafficAccumulator > 0 || this.pendingChannelCreations > 0) {
        await this.scheduleAlarmAt(Date.now() + Relay.TRAFFIC_FLUSH_IDLE_MS);
      }
    });

    // Setup traffic reporting alarm if not exists
    // We will use a simple interval check in webSocketMessage or similar, 
    // but for DO, we can just report periodically if there is activity.
    // Or just report on every N bytes to avoid too many RPC calls.
  }

  private async getOwnColo(): Promise<string> {
    if (this.ownColo) return this.ownColo;
    try {
      const resp = await fetch('https://1.1.1.1/cdn-cgi/trace', {
        signal: AbortSignal.timeout(3000),
      });
      const text = await resp.text();
      const match = text.match(/^colo=(.+)$/m);
      this.ownColo = match ? match[1].trim() : 'unknown';
    } catch {
      this.ownColo = 'unknown';
    }
    return this.ownColo;
  }

  private safeDeserializeAttachment(ws: WebSocket): WebsocketMeta {
    try {
      const meta = ws.deserializeAttachment() as WebsocketMeta;
      if (meta && typeof meta === "object") return meta;
    } catch {
      // Ignore
    }
    return {};
  }

  // socketProtocolVersion resolves the generation stamped on relay-generated
  // frames for a connection. Connections that never declared one speak the
  // native legacy generation.
  private socketProtocolVersion(ws: WebSocket): number {
    const version = this.safeDeserializeAttachment(ws).peerProtocolVersion;
    return typeof version === "number" && version > 0 ? version : PROTOCOL_VERSION;
  }

  // sendMessage writes a relay-generated message stamped with the generation
  // the target connection speaks. Raw forwarded frames bypass this and are
  // relayed byte for byte.
  private sendMessage(ws: WebSocket, msg: BaseMessage): void {
    ws.send(packMessage(msg, this.socketProtocolVersion(ws)));
  }

  private syncFromState(force: boolean = false) {
    const now = Date.now();
    if (!force && now - this.lastSyncAt < 2000) return;

    this.providers.clear();
    this.connectors.clear();
    this.providerChannels.clear();
    this.connectorChannels.clear();
    this.webProviders.clear();
    this.webSocketClients.clear();
    this.webSocketProviders.clear();

    const providerByChannel = new Map<string, WebSocket>();
    const connectorByChannel = new Map<string, WebSocket>();

    for (const ws of this.state.getWebSockets()) {
      const meta = this.safeDeserializeAttachment(ws);
      if (meta.webClient === true) {
        if (meta.webChannelId) this.webSocketClients.set(meta.webChannelId, ws);
        continue;
      }
      const isProvider = meta.isProvider === true;
      if (isProvider) {
        this.providers.add(ws);
      } else {
        this.connectors.add(ws);
      }

      if (meta.web === true) {
        for (const channelId of meta.webSocketChannels || []) {
          if (typeof channelId === "string" && channelId) this.webSocketProviders.set(channelId, ws);
        }
        const hosts = Array.isArray(meta.webHosts) ? meta.webHosts : [];
        for (const host of hosts) {
          if (typeof host !== "string" || !host) continue;
          const list = this.webProviders.get(host) || [];
          list.push(ws);
          this.webProviders.set(host, list);
        }
      }

      const channels = Array.isArray(meta.channels) ? meta.channels : [];
      for (const channelId of channels) {
        if (typeof channelId !== "string" || channelId.length === 0) continue;
        if (isProvider) providerByChannel.set(channelId, ws);
        else connectorByChannel.set(channelId, ws);
      }
    }

    for (const [channelId, provider] of providerByChannel.entries()) {
      this.providerChannels.set(channelId, provider);
    }
    for (const [channelId, connector] of connectorByChannel.entries()) {
      this.connectorChannels.set(channelId, connector);
    }

    if (this.currentProviderIndex >= this.providers.size) {
      this.currentProviderIndex = 0;
    }

    this.lastSyncAt = now;
  }

  private getActualProviderCount(): number {
    let count = 0;
    for (const ws of this.state.getWebSockets()) {
      const meta = this.safeDeserializeAttachment(ws);
      if (meta.isProvider === true) count++;
    }
    return count;
  }

  private getActualConnectorCount(): number {
    let count = 0;
    for (const ws of this.state.getWebSockets()) {
      const meta = this.safeDeserializeAttachment(ws);
      if (meta.isProvider !== true && meta.webClient !== true) count++;
    }
    return count;
  }

  private getActualProviderCountExcluding(exclude: WebSocket): number {
    let count = 0;
    for (const ws of this.state.getWebSockets()) {
      if (ws === exclude) continue;
      const meta = this.safeDeserializeAttachment(ws);
      if (meta.isProvider === true) count++;
    }
    return count;
  }

  private getActualConnectorCountExcluding(exclude: WebSocket): number {
    let count = 0;
    for (const ws of this.state.getWebSockets()) {
      if (ws === exclude) continue;
      const meta = this.safeDeserializeAttachment(ws);
      if (meta.isProvider !== true && meta.webClient !== true) count++;
    }
    return count;
  }

  async adminGetRuntimeInfo(): Promise<{ providerCount: number; connectorCount: number; channelCount: number }> {
    this.syncFromState(true);
    // Get actual WebSocket count from DO state (survives hibernation)
    const allWebSockets = this.state.getWebSockets();
    let actualProviderCount = 0;
    let actualConnectorCount = 0;
    
    for (const ws of allWebSockets) {
      const meta = this.safeDeserializeAttachment(ws);
      if (meta.webClient === true) continue;
      if (meta.isProvider === true) actualProviderCount++;
      else actualConnectorCount++;
    }
    
    // Best-effort: if we observe no providers (e.g. the close event was missed), ensure cleanup is scheduled.
    if (actualProviderCount === 0) {
      await this.ensureCleanupScheduled();
    }

    return {
      providerCount: actualProviderCount,
      connectorCount: actualConnectorCount,
      channelCount: this.providerChannels.size,
    };
  }

  async adminGetConnectionDetails(): Promise<{ providerIPs: string[]; connectorIPs: string[] }> {
    const providerIPs: string[] = [];
    const connectorIPs: string[] = [];

    for (const ws of this.state.getWebSockets()) {
      const meta = this.safeDeserializeAttachment(ws);
      if (meta.webClient === true) continue;
      const ip = meta.clientIp || "unknown";
      if (meta.isProvider === true) {
        providerIPs.push(ip);
      } else {
        connectorIPs.push(ip);
      }
    }

    return { providerIPs, connectorIPs };
  }

  private async ensureCleanupScheduled() {
    const providerDisconnectTime = (await this.storage.get('providerDisconnectTime')) as number | undefined;
    const graceMs = await this.getProviderDisconnectGraceMs();
    const now = Date.now();
    if (!providerDisconnectTime) {
      const hasNoWebSockets = this.state.getWebSockets().length === 0;
      const oldestCreatedAt = hasNoWebSockets
        ? await this.token.getRelayOldestCreatedAt(this.state.id.toString())
        : null;
      const cleanupStartAt = oldestCreatedAt && oldestCreatedAt < now ? oldestCreatedAt : now;
      await this.storage.put('providerDisconnectTime', cleanupStartAt);
      const elapsed = now - cleanupStartAt;
      if (elapsed >= graceMs) {
        // Grace already expired — perform cleanup directly instead of relying on an alarm.
        await this.performStaleRelayCleanup();
        return;
      }
      await this.scheduleAlarmAt(now + (graceMs - elapsed));
      return;
    }

    const elapsed = now - providerDisconnectTime;
    if (elapsed < graceMs) {
      await this.scheduleAlarmAt(now + (graceMs - elapsed));
      return;
    }

    // Grace already elapsed — clean up directly instead of scheduling another alarm
    // that may never fire (e.g. DO eviction or lost alarm).
    await this.performStaleRelayCleanup();
  }

  /**
   * Immediately terminate a stale relay: notify connectors, revoke tokens, clear storage.
   * Safe to call multiple times — idempotent once storage is cleared.
   */
  private async performStaleRelayCleanup(): Promise<void> {
    const graceMs = await this.getProviderDisconnectGraceMs();
    const notifiedConnectors = this.notifyConnectorsRelayTermination(graceMs);
    if (notifiedConnectors > 0) {
      await new Promise((resolve) => setTimeout(resolve, Relay.CONNECTOR_SHUTDOWN_NOTICE_DELAY_MS));
    }

    // Release the published hostnames and their rotation history before
    // revoking tokens. Reservations, if any, live in their own table and keep
    // the name bound to its owner.
    await this.withSiteMutation(async () => {
      await this.deleteRelaySites();
      if (this.env.DB) {
        try {
          await deleteRetiredHostsByRelay(this.env, this.state.id.toString());
        } catch (error) {
          console.error("cleanup: failed to delete retired host rows", error);
        }
      }
    });
    await this.deleteRelayRecords();

    // Clear local storage
    await this.storage.deleteAll();
    this.webUsage = {};
    this.webUsageDay = "";
    this.siteCache.clear();

    // Close all remaining connections
    this.adminDisconnectAll('All providers disconnected, relay terminated');
  }

  private async deleteRelaySites(): Promise<void> {
    if (!this.env.DB) return;
    const relayId = this.state.id.toString();
    try {
      const sites = await listSitesByRelay(this.env, relayId);
      for (const site of sites) {
        await deleteSite(this.env, site.host);
        this.siteCache.delete(site.host);
      }
    } catch (error) {
      console.error("cleanup: failed to delete site rows", error);
    }
  }

  adminDisconnectAll(reason: string = "Admin disconnect") {
    // Use the DO-managed websocket list so this also works after hibernation.
    for (const ws of this.state.getWebSockets()) {
      try {
        ws.close(1012, reason);
      } catch {
        // Ignore
      }
    }

    this.providerChannels.clear();
    this.connectorChannels.clear();
    this.providers.clear();
    this.connectors.clear();
  }

  async adminRevokeConnectorTokens() {
    await this.invalidateConnectorTokens();
  }

  private async sha256(tokenName: string): Promise<string> {
    const msgBuffer = new TextEncoder().encode(tokenName);
    const hashBuffer = await crypto.subtle.digest('SHA-256', msgBuffer);
    return Array.from(new Uint8Array(hashBuffer))
      .map(b => b.toString(16).padStart(2, '0'))
      .join('');
  }

  private isSha256Hex(value: string): boolean {
    return /^[0-9a-f]{64}$/i.test(value);
  }

  private async toTokenKey(tokenOrKey: string): Promise<string> {
    const v = (tokenOrKey || "").trim();
    if (!v) return "";
    if (this.isSha256Hex(v)) return v.toLowerCase();
    return await this.sha256(v);
  }

  private validateTokenComplexity(token: string): { valid: boolean; reason?: string } {
    if (token.length < 8) {
      return { valid: false, reason: "Token must be at least 8 characters" };
    }
    
    const result = zxcvbn(token);
    if (result.score < 2) {
      const feedback = result.feedback.warning || result.feedback.suggestions[0] || "Token is too weak";
      return { valid: false, reason: feedback };
    }
    
    return { valid: true };
  }

  private async saveToken(tokenOrKey: string) {
    const tokenKey = await this.toTokenKey(tokenOrKey);
    if (!tokenKey) return;

    const stored = (await this.storage.get('tokens')) as string[] | undefined;
    const existing = Array.isArray(stored) ? stored : [];

    // Fast path: if every stored entry is already a valid hash key and the
    // new key is already present, skip the write entirely.
    const allCanonical = existing.every((t) => this.isSha256Hex(t));
    if (allCanonical && existing.includes(tokenKey)) {
      const metadata = await this.token.getRelayMetadata(tokenKey);
      if (!metadata) {
        await this.token.setRelay(tokenKey, this.state.id.toString());
      } else if (!metadata.createdAt) {
        await this.token.updateRelayMetadata(tokenKey, { createdAt: Date.now() });
      }
      return;
    }

    // Slow path: migrate older versions that stored raw tokens to canonical keys.
    const keySet = new Set<string>();
    for (const t of existing) {
      const k = await this.toTokenKey(t);
      if (k) keySet.add(k);
    }
    keySet.add(tokenKey);

    await this.storage.put('tokens', Array.from(keySet));

    // Always ensure metadata exists with createdAt
    const metadata = await this.token.getRelayMetadata(tokenKey);
    if (!metadata) {
      await this.token.setRelay(tokenKey, this.state.id.toString());
    } else if (!metadata.createdAt) {
      await this.token.updateRelayMetadata(tokenKey, { createdAt: Date.now() });
    }
  }

  private async updateMetadata() {
    // Get actual WebSocket count from DO state (survives hibernation)
    const allWebSockets = this.state.getWebSockets();
    let actualProviderCount = 0;
    let actualConnectorCount = 0;

    for (const ws of allWebSockets) {
      try {
        const meta = ws.deserializeAttachment() as WebsocketMeta;
        if (meta.webClient === true) continue;
        if (meta.isProvider) {
          actualProviderCount++;
        } else {
          actualConnectorCount++;
        }
      } catch (e) {
        // Ignore deserialization errors
      }
    }

    // Single RPC to Token DO — it batches the update across all tokens in this relay
    // via one list() + one put() instead of N individual RPCs.
    await this.token.updateRelayCounts(
      this.state.id.toString(),
      actualProviderCount,
      actualConnectorCount,
    );
  }

  private flushTraffic(force: boolean = false): Promise<void> {
    if (this.trafficFlushPromise) return this.trafficFlushPromise;

    this.trafficFlushPromise = (async () => {
      try {
        while (true) {
          const now = Date.now();
          const bytesToReport = this.trafficAccumulator;
          const channelsToReport = this.pendingChannelCreations;

          if (!force) {
            const overSize = bytesToReport >= 1024 * 1024;
            const overTime = now - this.lastTrafficReport >= Relay.TRAFFIC_FLUSH_IDLE_MS;
            if (!overSize && !(overTime && (bytesToReport > 0 || channelsToReport > 0))) break;
          }

          if (bytesToReport <= 0 && channelsToReport <= 0) {
            this.lastTrafficReport = now;
            break;
          }

          // Snapshot and reset BEFORE awaiting to avoid losing bytes due to interleaving.
          this.trafficAccumulator = 0;
          this.pendingChannelCreations = 0;
          this.lastTrafficReport = now;

          try {
            await this.token.reportUsage(bytesToReport, channelsToReport);
          } catch (err) {
            // Restore so we can retry later.
            this.trafficAccumulator += bytesToReport;
            this.pendingChannelCreations += channelsToReport;
            throw err;
          }

          if (!force) break;
          if (this.trafficAccumulator <= 0 && this.pendingChannelCreations <= 0) break;
        }
      } finally {
        // Persist to avoid losing pending bytes/channels on eviction.
        // Skip when nothing pending — avoids a redundant row write per
        // force-flush on connection close when accumulator already drained.
        if (this.trafficAccumulator > 0 || this.pendingChannelCreations > 0) {
          await this.storage.put("trafficState", {
            accumulator: this.trafficAccumulator,
            lastTrafficReport: this.lastTrafficReport,
            pendingChannels: this.pendingChannelCreations,
          });
        } else {
          // Clear stale trafficState so the next DO instantiation does not
          // treat an old entry as pending work.
          await this.storage.delete("trafficState");
        }
      }
    })().finally(() => {
      this.trafficFlushPromise = null;
    });

    return this.trafficFlushPromise;
  }

  private reportTraffic(bytes: number) {
    const now = Date.now();
    if (this.lastTrafficReport === 0) this.lastTrafficReport = now;

    this.trafficAccumulator += bytes;

    // Flush in background when threshold is reached.
    if (this.trafficAccumulator >= 1024 * 1024 || now - this.lastTrafficReport >= Relay.TRAFFIC_FLUSH_IDLE_MS) {
      this.flushTraffic(false).catch(console.error);
    } else if (this.trafficAccumulator > 0) {
      this.scheduleAlarmAt(now + Relay.TRAFFIC_FLUSH_IDLE_MS).catch(console.error);
    }
  }

  async fetch(request: Request) {
    return await handleErrors(request, async () => {
      const url = new URL(request.url);

      if (url.pathname === "/web") {
        if (request.headers.get("Upgrade")?.toLowerCase() === "websocket") {
          return await this.handleWebSocketHttp(request);
        }
        return await this.handleWebHttp(request);
      }

      if (request.headers.get("Upgrade") === "websocket") {
        const pair = new WebSocketPair();
        const [client, server] = [pair[0], pair[1]];

        // Accept the WebSocket
        this.state.acceptWebSocket(server);

        // Check is provider according to request URL
        const isWebProvider = url.pathname === "/web-provider";
        const isProvider = url.pathname === "/provider" || isWebProvider;

        // Add to providers/connectors set
        if (isProvider) {
          this.providers.add(server);
          // If any provider connects, clear pending termination.
          await this.storage.delete('providerDisconnectTime');
        } else {
          this.connectors.add(server);
        }

        // Return the actual token for reconnection (especially for anonymous providers)
        const actualToken = url.searchParams.get("actualToken") || "";
        const baseHost = url.searchParams.get("baseHost") || "";

        // Save token and update metadata when connection is established
        if (actualToken) {
          await this.saveToken(actualToken);
          await this.updateMetadata();

          if (isProvider) {
            await this.setOwnerTokenKeyFromActualToken(actualToken);
          }
        }

        const clientIp = request.headers.get('CF-Connecting-IP') || request.headers.get('X-Forwarded-For')?.split(',')[0].trim() || 'unknown';

        // Updated clients declare the protocol generation they speak in the
        // query string (the Go client sends version=0x20). Seeding it here
        // lets even the very first relay response be stamped correctly.
        const versionParam = url.searchParams.get("version");
        let declaredVersion: number | undefined;
        if (versionParam) {
          const parsed = Number.parseInt(versionParam.replace(/^0x/i, ""), 16);
          if (Number.isFinite(parsed) && parsed > 0) {
            declaredVersion = parsed;
          }
        }

        // Save session data for hibernation
        server.serializeAttachment({
          isProvider,
          actualToken,
          clientIp,
          web: isWebProvider,
          baseHost,
          ...(declaredVersion !== undefined ? { peerProtocolVersion: declaredVersion } : {}),
        } satisfies WebsocketMeta);

        const currentProviders = this.getActualProviderCount();
        const currentConnectors = this.getActualConnectorCount();

        const response: AuthResponseMessage = {
          success: true,
          token: actualToken,
          getType: () => MessageType.AuthResponse,
        };
        this.sendMessage(server, response);

        const edgeColo = request.cf && request.cf.colo ? String(request.cf.colo) : 'unknown';
        const edgeColoDescription = describeCloudflareColo(edgeColo);
        const doColo = await this.getOwnColo();
        const doColoDescription = describeCloudflareColo(doColo);
        const clientCountry = request.headers.get('CF-IPCountry') || (request.cf && request.cf.country ? String(request.cf.country) : 'unknown');
        const providerLabel = currentProviders === 1 ? 'provider' : 'providers';
        const connectorLabel = currentConnectors === 1 ? 'connector' : 'connectors';
        let datacenterInfo: string;
        if (edgeColo === doColo) {
          datacenterInfo = `datacenter: ${edgeColoDescription}`;
        } else {
          datacenterInfo = `edge: ${edgeColoDescription}, relay: ${doColoDescription}`;
        }
        const log: LogMessage = {
          level: "info",
          msg: `Welcome to LinkSocks.js relay server. This server is running in ${datacenterInfo}. Your connection comes from ${clientCountry} (${clientIp}). After you connected, this relay group has ${currentProviders} ${providerLabel} and ${currentConnectors} ${connectorLabel}.`,
          getType: () => MessageType.Log,
        };
        this.sendMessage(server, log);

        // Send partners count after auth response
        if (isProvider) {
          // Send partners count to all connectors after new provider connected
          this.broadcastPartnersCountToConnectors();
        } else {
          // Send current providers count to new connector
          this.sendPartnersCountToConnector(server);
          // Send connectors count to all providers
          this.broadcastPartnersCountToProviders();
        }

        // Return client-side WebSocket
        return new Response(null, {
          status: 101,
          webSocket: client,
        });
      }
      return new Response("Method not allowed", { status: 405 });
    });
  }

  // Helper method to broadcast partners count to all connectors.
  // exclude: optional WebSocket to ignore while counting (e.g. a socket mid-close).
  private broadcastPartnersCountToConnectors(exclude?: WebSocket) {
    // Get actual provider count from DO state (survives hibernation)
    const actualProviderCount = exclude
      ? this.getActualProviderCountExcluding(exclude)
      : this.getActualProviderCount();
    
    const partnersMsg: PartnersMessage = {
      count: actualProviderCount,
      getType: () => MessageType.Partners,
    };
    for (const connector of this.connectors) {
      if (exclude && connector === exclude) continue;
      try {
        this.sendMessage(connector, partnersMsg);
      } catch (e) {
        // Ignore send errors
      }
    }
  }

  // Helper method to broadcast partners count to all providers.
  // exclude: optional WebSocket to ignore while counting (e.g. a socket mid-close).
  private broadcastPartnersCountToProviders(exclude?: WebSocket) {
    // Get actual connector count from DO state (survives hibernation)
    const actualConnectorCount = exclude
      ? this.getActualConnectorCountExcluding(exclude)
      : this.getActualConnectorCount();
    
    const partnersMsg: PartnersMessage = {
      count: actualConnectorCount,
      getType: () => MessageType.Partners,
    };
    for (const provider of this.providers) {
      if (exclude && provider === exclude) continue;
      try {
        this.sendMessage(provider, partnersMsg);
      } catch (e) {
        // Ignore send errors
      }
    }
  }

  // Helper method to send partners count to a specific connector
  private sendPartnersCountToConnector(connector: WebSocket) {
    // Get actual provider count from DO state (survives hibernation)
    const allWebSockets = this.state.getWebSockets();
    let actualProviderCount = 0;
    
    for (const ws of allWebSockets) {
      try {
        const meta = ws.deserializeAttachment() as WebsocketMeta;
        if (meta.isProvider) {
          actualProviderCount++;
        }
      } catch (e) {
        // Ignore
      }
    }
    
    const partnersMsg: PartnersMessage = {
      count: actualProviderCount,
      getType: () => MessageType.Partners,
    };
    try {
      this.sendMessage(connector, partnersMsg);
    } catch (e) {
      // Ignore send errors
    }
  }

  private getNextProvider(forceSync: boolean = false): WebSocket | null {
    this.syncFromState(forceSync);
    const providers = Array.from(this.providers).filter((ws) => this.safeDeserializeAttachment(ws).web !== true);
    if (providers.length === 0) return null;

    if (this.currentProviderIndex < 0 || this.currentProviderIndex >= providers.length) {
      this.currentProviderIndex = 0;
    }

    const ws = providers[this.currentProviderIndex];
    this.currentProviderIndex = (this.currentProviderIndex + 1) % providers.length;
    return ws;
  }

  private async waitForNextProvider(timeoutMs: number): Promise<WebSocket | null> {
    let provider = this.getNextProvider(true);
    if (provider || timeoutMs <= 0) {
      return provider;
    }

    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 100));
      provider = this.getNextProvider(true);
      if (provider) {
        return provider;
      }
    }

    return null;
  }

  // WebSocket event handlers
  async webSocketMessage(ws: WebSocket, message: ArrayBuffer | string) {
    try {
      this.syncFromState();
      const initialMeta = this.safeDeserializeAttachment(ws);
      if (initialMeta.webClient === true) {
        if (initialMeta.webChannelId) {
          await this.queueWebAction(initialMeta.webChannelId, () => this.forwardWebSocketClient(ws, message, initialMeta));
        }
        return;
      }
      const messageData =
        typeof message === "string"
          ? new TextEncoder().encode(message)
          : new Uint8Array(message);
      
      // Report traffic (approximate size)
      this.reportTraffic(messageData.byteLength);

      const { message: msg, version } = parseMessageWithVersion(messageData);
      const meta = this.safeDeserializeAttachment(ws);
      if (meta.peerProtocolVersion !== version) {
        // A connection must not change protocol generation mid-stream: a
        // generation-2 peer sending generation-0 frames (or vice versa) is
        // either broken or attempting a silent downgrade.
        if (typeof meta.peerProtocolVersion === "number" &&
            protocolMajor(meta.peerProtocolVersion) !== protocolMajor(version)) {
          ws.close(1008, "Connection changed protocol generation mid-stream");
          return;
        }
        meta.peerProtocolVersion = version;
        ws.serializeAttachment(meta);
        if (!isCurrentProtocolMinor(version)) {
          const knownMajor = protocolMajor(version);
          const knownMinor = knownMajor === protocolMajor(PROTOCOL_VERSION_V2)
            ? PROTOCOL_VERSION_V2 & 0x0f
            : PROTOCOL_VERSION & 0x0f;
          const baseline = (knownMajor << 4) | knownMinor;
          const warning = `Protocol minor version mismatch: peer=0x${version.toString(16).padStart(2, "0")} relay=0x${baseline.toString(16).padStart(2, "0")}; connection allowed but features may differ`;
          console.warn(warning);
          this.sendMessage(ws, {
            level: "warn",
            msg: warning,
            getType: () => MessageType.Log,
          } as LogMessage);
        }
      }
      const isProvider = meta.isProvider === true;
      if (WEB_MESSAGE_TYPES.has(msg.getType()) && !supportsFeatureVersion(version, MIN_VERSION_WEB)) {
        ws.close(1008, `Web messages require protocol 0x${MIN_VERSION_WEB.toString(16).padStart(2, "0")} or newer`);
        return;
      }
      if (msg.getType() === MessageType.WebSocketFrame && !supportsFeatureVersion(version, MIN_VERSION_WEBSOCKET)) {
        ws.close(1008, "WebSocket frames require protocol 0x04 or newer");
        return;
      }
      if (
        (msg.getType() === MessageType.ChannelOpen || msg.getType() === MessageType.Sealed) &&
        protocolMajor(version) !== protocolMajor(PROTOCOL_VERSION_V2)
      ) {
        ws.close(1008, "Sealed channels require protocol generation 2");
        return;
      }

      switch (msg.getType()) {
        case MessageType.Auth: {
          const authMsg = msg as AuthMessage;

          // sha256("anonymous") in lowercase hex
          const ANONYMOUS_TOKEN_HASH = "2f183a4e64493af3f377f745eda502363cd3e7ef6e4d266d444758de0a85fcc8";

          const actualToken = meta.actualToken || "";

          if (!actualToken) {
            const response: AuthResponseMessage = {
              success: false,
              error: "Missing actual token",
              getType: () => MessageType.AuthResponse,
            };
            this.sendMessage(ws, response);
            break;
          }

          // If client uses anonymous token hash, return the actual token bound to this websocket.
          if (authMsg.token === ANONYMOUS_TOKEN_HASH) {
            if (!isProvider) {
              const response: AuthResponseMessage = {
                success: false,
                error: "Anonymous token is only allowed for providers",
                getType: () => MessageType.AuthResponse,
              };
              this.sendMessage(ws, response);
              break;
            }
            const response: AuthResponseMessage = {
              success: true,
              token: actualToken,
              getType: () => MessageType.AuthResponse,
            };
            this.sendMessage(ws, response);
            if (isProvider) await this.markProviderOnline();
            break;
          }

          const actualTokenKey = await this.toTokenKey(actualToken);
          if (authMsg.token !== actualToken && (!actualTokenKey || authMsg.token !== actualTokenKey)) {
            const response: AuthResponseMessage = {
              success: false,
              error: "Invalid token",
              getType: () => MessageType.AuthResponse,
            };
            this.sendMessage(ws, response);
            break;
          }

          // For non-anonymous auth messages, we currently don't perform additional auth here.
          // The connection is already authenticated by the entry worker.
          const response: AuthResponseMessage = {
            success: true,
            token: actualToken,
            getType: () => MessageType.AuthResponse,
          };
          this.sendMessage(ws, response);
          if (isProvider) await this.markProviderOnline();
          break;
        }

        case MessageType.WebRegister: {
          if (meta.web === true && isProvider) {
            await this.handleWebRegister(ws, msg as WebRegisterMessage);
          }
          break;
        }

        case MessageType.WebResponse: {
          const response = msg as WebResponseMessage;
          await this.queueWebAction(response.channelId, () => this.handleWebResponse(response));
          break;
        }

        case MessageType.WebData: {
          const data = msg as WebDataMessage;
          await this.queueWebAction(data.channelId, () => this.handleWebData(data));
          break;
        }

        case MessageType.WebCancel: {
          const cancel = msg as WebCancelMessage;
          if (this.webSocketProviders.get(cancel.channelId) === ws) {
            this.closeWebSocketChannel(cancel.channelId, cancel.code || 1000, cancel.reason || "", false);
          } else {
            this.handleWebCancel(cancel);
          }
          break;
        }

        case MessageType.WebSocketFrame: {
          const frame = msg as WebSocketFrameMessage;
          await this.queueWebAction(frame.channelId, () => this.forwardWebSocketProvider(ws, frame));
          break;
        }

        case MessageType.ChannelOpen: {
          if (isProvider) {
            ws.close(1008, "Invalid connector token.");
            break;
          }
          // Protocol V2 channel request. The destination stays sealed inside
          // the channel, so the relay only binds a provider and forwards
          // frames from here on.
          const openMsg = msg as ChannelOpenMessage;
          const channelId = openMsg.channelId;

          // Bind and forward through the same per-channel action queue that
          // carries sealed frames: the peer's first sealed handshake frame
          // can already be in flight while this handler awaits a provider,
          // and it must not overtake the binding.
          await this.queueNetworkAction(channelId, async () => {
            // A resume must reach the provider the channel is already bound
            // to; only a fresh open picks a (possibly different) provider
            // and counts against the request quota.
            let provider = this.providerChannels.get(channelId) ?? null;
            if (provider && !this.providers.has(provider)) {
              // The bound provider is gone; drop the stale binding.
              this.disconnectChannel(channelId);
              provider = null;
            }

            if (!provider) {
              // Get round-robin provider
              provider = await this.waitForNextProvider(Relay.DEFAULT_CONNECTOR_WAIT_PROVIDER_MS);
              if (!provider) {
                const response: ConnectResponseMessage = {
                  success: false,
                  error: 'No available providers',
                  channelId,
                  getType: () => MessageType.ConnectResponse,
                };
                this.sendMessage(ws, response);
                return;
              }

              const quota = await this.getNetworkQuota(provider);
              if (quota && !await consumeQuota(this.env, quota.principal, 0, 1, quota.bytes, quota.requests)) {
                const response: ConnectResponseMessage = {
                  success: false,
                  error: "Quota exceeded",
                  channelId,
                  getType: () => MessageType.ConnectResponse,
                };
                this.sendMessage(ws, response);
                return;
              }

              this.providerChannels.set(channelId, provider);
              this.connectorChannels.set(channelId, ws);

              // Report channel creation (batched — flushed together with traffic)
              this.pendingChannelCreations++;

              // Update metadata for both WebSockets
              const providerMeta = this.safeDeserializeAttachment(provider);
              if (quota) {
                providerMeta.quotaPrincipal = quota.principal;
                providerMeta.quotaBytes = quota.bytes;
                providerMeta.quotaRequests = quota.requests;
              }
              providerMeta.channels = providerMeta.channels || [];
              providerMeta.channels.push(channelId);
              provider.serializeAttachment(providerMeta);

              const connectorMeta = ws.deserializeAttachment() as WebsocketMeta;
              connectorMeta.channels = connectorMeta.channels || [];
              connectorMeta.channels.push(channelId);
              ws.serializeAttachment(connectorMeta);
            }

            // Forward the open (raw bytes) to the provider.
            provider.send(message);
          });
          break;
        }

        case MessageType.Connect: {
          if (isProvider) {
            ws.close(1008, "Invalid connector token.");
            break;
          }
          // Only connectors can send Connect messages. Binding and forwarding
          // go through the per-channel action queue so the first Data frame
          // cannot overtake the bind while a provider is being awaited.
          const connectMsg = msg as ConnectMessage;
          await this.queueNetworkAction(connectMsg.channelId, async () => {
            // Get round-robin provider
            const provider = await this.waitForNextProvider(Relay.DEFAULT_CONNECTOR_WAIT_PROVIDER_MS);
            if (!provider) {
              const response: ConnectResponseMessage = {
                success: false,
                error: 'No available providers',
                channelId: connectMsg.channelId,
                getType: () => MessageType.ConnectResponse,
              };
              this.sendMessage(ws, response);
              return;
            }

            const quota = await this.getNetworkQuota(provider);
            if (quota && !await consumeQuota(this.env, quota.principal, 0, 1, quota.bytes, quota.requests)) {
              const response: ConnectResponseMessage = {
                success: false,
                error: "Quota exceeded",
                channelId: connectMsg.channelId,
                getType: () => MessageType.ConnectResponse,
              };
              this.sendMessage(ws, response);
              return;
            }

            // Create a new channel
            const channelId = connectMsg.channelId;
            this.providerChannels.set(channelId, provider);
            this.connectorChannels.set(channelId, ws);

            // Report channel creation (batched — flushed together with traffic)
            this.pendingChannelCreations++;

            // Update metadata for both WebSockets
            const providerMeta = this.safeDeserializeAttachment(provider);
            if (quota) {
              providerMeta.quotaPrincipal = quota.principal;
              providerMeta.quotaBytes = quota.bytes;
              providerMeta.quotaRequests = quota.requests;
            }
            providerMeta.channels = providerMeta.channels || [];
            providerMeta.channels.push(channelId);
            provider.serializeAttachment(providerMeta);

            const connectorMeta = ws.deserializeAttachment() as WebsocketMeta;
            connectorMeta.channels = connectorMeta.channels || [];
            connectorMeta.channels.push(channelId);
            ws.serializeAttachment(connectorMeta);

            // Forward the connect message to provider
            provider.send(message);
          });
          break;
        }

        case MessageType.Connector: {
          if (isProvider) {
            const connectorMsg = msg as ConnectorMessage;

            if (meta.actualToken) {
              await this.setOwnerTokenKeyFromActualToken(meta.actualToken);
            }

            const op = (connectorMsg.operation || "add").toLowerCase();

            // Best-effort: always bind connector tokens to the relay owner token, so admin UI can show them.
            let ownerKey = await this.getOwnerTokenKey();
            if (!ownerKey) {
              const tokens = (await this.storage.get("tokens")) as string[] | undefined;
              if (Array.isArray(tokens) && tokens.length > 0) {
                ownerKey = await this.toTokenKey(tokens[0]);
                if (ownerKey) {
                  await this.storage.put("ownerTokenKey", ownerKey);
                }
              }
            }

            if (op === "remove") {
              const connectorToken = (connectorMsg.connectorToken || "").trim();
              if (ownerKey && connectorToken) {
                await this.token.removeConnectorToken(ownerKey, connectorToken);
              }
              const connectorTokenKey = await this.toTokenKey(connectorToken);
              if (connectorTokenKey) {
                await this.token.deleteToken(connectorTokenKey);
              }

              const response: ConnectorResponseMessage = {
                success: true,
                channelId: connectorMsg.channelId,
                connectorToken,
                getType: () => MessageType.ConnectorResponse,
              };
              this.sendMessage(ws, response);
              break;
            }

            // Generate random token if not provided
            if (!connectorMsg.connectorToken) {
              const randomBytes = new Uint8Array(8);
              crypto.getRandomValues(randomBytes);
              connectorMsg.connectorToken = Array.from(randomBytes)
                .map(b => b.toString(16).padStart(2, '0'))
                .join('');
            } else {
              // Validate user-provided token complexity
              const validation = this.validateTokenComplexity(connectorMsg.connectorToken);
              if (!validation.valid) {
                const response: ConnectorResponseMessage = {
                  success: false,
                  error: validation.reason,
                  channelId: connectorMsg.channelId,
                  getType: () => MessageType.ConnectorResponse,
                };
                this.sendMessage(ws, response);
                break;
              }
            }

            const tokenHash = await this.toTokenKey(connectorMsg.connectorToken);
            
            // Check if connector token already exists
            const existingRelay = await this.token.getRelay(tokenHash);
            if (existingRelay && existingRelay !== this.state.id.toString()) {
              const response: ConnectorResponseMessage = {
                success: false,
                error: 'Connector token already exists, please choose a different one',
                channelId: connectorMsg.channelId,
                getType: () => MessageType.ConnectorResponse,
              };
              this.sendMessage(ws, response);
              break;
            }
            
            if (ownerKey) {
              await this.token.addConnectorToken(ownerKey, connectorMsg.connectorToken);
            }
            
            await this.token.setRelay(tokenHash, this.state.id.toString());
            
            // Store the token ID
            await this.saveToken(connectorMsg.connectorToken);

            // Update metadata after adding connector token
            await this.updateMetadata();

            // Notify the provider about successful connector registration
            const response: ConnectorResponseMessage = {
              success: true,
              channelId: connectorMsg.channelId,
              connectorToken: connectorMsg.connectorToken,
              getType: () => MessageType.ConnectorResponse,
            };

            this.sendMessage(ws, response);
          }
          break;
        }

        case MessageType.ConnectResponse: {
          if (isProvider) {
            // Only providers can send connect responses
            const responseMsg = msg as ConnectResponseMessage;
            const connector = this.connectorChannels.get(responseMsg.channelId);
            if (connector) {
              connector.send(message);
            }
          }
          break;
        }

        case MessageType.Data: {
          const dataMsg = msg as DataMessage;
          await this.queueNetworkAction(dataMsg.channelId, () => this.forwardNetworkData(ws, message, dataMsg, isProvider));
          break;
        }

        case MessageType.Sealed: {
          // Protocol V2 opaque payload: routed exactly like Data through the
          // per-channel action queue, but the relay never looks inside.
          const sealedMsg = msg as SealedMessage;
          await this.queueNetworkAction(sealedMsg.channelId, () =>
            this.forwardChannelPayload(ws, message, sealedMsg.channelId, sealedMsg.data.byteLength, isProvider));
          break;
        }

        case MessageType.Disconnect: {
          const disconnectMsg = msg as DisconnectMessage;
          const channelId = disconnectMsg.channelId;
          await this.queueNetworkAction(channelId, async () => {
            if (isProvider) {
              const connector = this.connectorChannels.get(channelId);
              if (connector) connector.send(message);
            } else {
              const provider = this.providerChannels.get(channelId);
              if (provider) provider.send(message);
            }
            this.disconnectChannel(channelId);
          });
          break;
        }

        case MessageType.ChannelBind: {
          const bindMsg = msg as ChannelBindMessage;
          const channelId = bindMsg.channelId;

          // Record the sender as the channel peer so later control messages
          // (migrate/data-ack) can be routed, mirroring the Go server.
          if (isProvider) {
            this.providerChannels.set(channelId, ws);
          } else {
            this.connectorChannels.set(channelId, ws);
          }

          const target = isProvider
            ? this.connectorChannels.get(channelId)
            : this.providerChannels.get(channelId);
          if (target && target !== ws) {
            target.send(message);
          }
          break;
        }

        case MessageType.ChannelMigrate:
        case MessageType.ChannelMigrateAck:
        case MessageType.ChannelDataAck: {
          const controlMsg = msg as
            | ChannelMigrateMessage
            | ChannelMigrateAckMessage
            | ChannelDataAckMessage;
          const channelId = controlMsg.channelId;

          const target = isProvider
            ? this.connectorChannels.get(channelId)
            : this.providerChannels.get(channelId);
          if (target && target !== ws) {
            target.send(message);
          }
          break;
        }

        case MessageType.DirectCapabilities:
        case MessageType.DirectRendezvous:
        case MessageType.DirectStatus: {
          if (isProvider) {
            for (const connector of this.connectors) {
              try {
                connector.send(message);
              } catch (e) {
                // Ignore send errors
              }
            }
          } else {
            for (const provider of this.providers) {
              try {
                provider.send(message);
              } catch (e) {
                // Ignore send errors
              }
            }
          }
          break;
        }
      }
    } catch (err) {
      console.error("Error handling message:", err);
      ws.close(1011, err.toString());
    }
  }

  /* ---------------------------------------------------------------------- */
  /* Web reverse proxy                                                      */
  /* ---------------------------------------------------------------------- */

  private async getNetworkQuota(provider: WebSocket): Promise<NetworkQuota | null> {
    if (!this.env.DB) return null;
    const meta = this.safeDeserializeAttachment(provider);
    if (!meta.actualToken) throw new Error("Missing provider token for quota enforcement");
    await ensureSchema(this.env);
    const tokenKey = await this.toTokenKey(meta.actualToken);
    const user = await findUserByTokenHash(this.env, tokenKey);
    const settings = await effectiveSettings(this.env, user);
    return {
      principal: user ? `user:${user.id}` : `anonymous:${tokenKey}`,
      bytes: settings.quotaBytes,
      requests: settings.quotaRequests,
    };
  }

  private async queueNetworkAction(channelId: string, action: () => Promise<void>): Promise<void> {
    const previous = this.networkQueues.get(channelId) || Promise.resolve();
    const current = previous.catch(() => undefined).then(action);
    this.networkQueues.set(channelId, current);
    try {
      await current;
    } finally {
      if (this.networkQueues.get(channelId) === current) this.networkQueues.delete(channelId);
    }
  }

  private async forwardNetworkData(ws: WebSocket, message: ArrayBuffer | string, data: DataMessage, isProvider: boolean): Promise<void> {
    await this.forwardChannelPayload(ws, message, data.channelId, data.data.byteLength, isProvider);
  }

  // forwardChannelPayload routes one channel payload frame to the peer and
  // applies per-provider byte quota. Both the legacy Data frame and the
  // protocol V2 Sealed frame use it; the relay never inspects the payload.
  private async forwardChannelPayload(
    ws: WebSocket,
    message: ArrayBuffer | string,
    channelId: string,
    payloadBytes: number,
    isProvider: boolean,
  ): Promise<void> {
    const provider = this.providerChannels.get(channelId);
    const connector = this.connectorChannels.get(channelId);
    if (!provider || !connector || (isProvider ? provider !== ws : connector !== ws)) return;

    let meta = this.safeDeserializeAttachment(provider);
    if (this.env.DB && !meta.quotaPrincipal) {
      const quota = await this.getNetworkQuota(provider);
      if (quota) {
        meta = this.safeDeserializeAttachment(provider);
        meta.quotaPrincipal = quota.principal;
        meta.quotaBytes = quota.bytes;
        meta.quotaRequests = quota.requests;
        provider.serializeAttachment(meta);
      }
    }
    if (meta.quotaPrincipal && this.env.DB && !await consumeQuota(this.env, meta.quotaPrincipal,
      payloadBytes, 0, meta.quotaBytes || 0, meta.quotaRequests || 0)) {
      const disconnect: DisconnectMessage = { channelId, getType: () => MessageType.Disconnect };
      try { this.sendMessage(provider, disconnect); } catch {}
      try { this.sendMessage(connector, disconnect); } catch {}
      this.disconnectChannel(channelId);
      return;
    }
    if (this.providerChannels.get(channelId) !== provider || this.connectorChannels.get(channelId) !== connector) return;
    (isProvider ? connector : provider).send(message);
  }

  private async handleWebRegister(ws: WebSocket, msg: WebRegisterMessage): Promise<void> {
    const meta = this.safeDeserializeAttachment(ws);
    const reply = (
      success: boolean,
      error?: string,
      extra: Partial<WebRegisterResponseMessage> = {},
    ) => {
      const response: WebRegisterResponseMessage = {
        success,
        error,
        ...extra,
        getType: () => MessageType.WebRegisterResponse,
      };
      try {
        this.sendMessage(ws, response);
      } catch {
        // The provider may have disconnected while registration was in flight.
      }
    };

    if (meta.web !== true) {
      reply(false, "This connection is not a web provider");
      return;
    }
    if (!this.env.DB) {
      reply(false, "Web proxy requires a D1 database binding named DB");
      return;
    }

    const release = await this.lockSiteMutation();
    try {
      await ensureSchema(this.env);
      const relayId = this.state.id.toString();
      const actualToken = (meta.actualToken || "").trim();
      let tokenHash = "";
      let user: UserRow | null = null;
      if (actualToken) {
        tokenHash = this.isSha256Hex(actualToken)
          ? actualToken.toLowerCase()
          : await this.sha256(actualToken);
        user = await findUserByTokenHash(this.env, tokenHash);
      }

      const settings: Settings = await effectiveSettings(this.env, user);
      if (!user && !settings.allowAnonymous) {
        reply(false, "Anonymous web providers are disabled on this server");
        return;
      }
      if (user && user.disabled) {
        reply(false, "This user account is disabled");
        return;
      }

      const rawBackends = Array.isArray(msg.backends) ? msg.backends : [];
      const backends: Array<{ url: string; weight?: number }> = [];
      const seenBackends = new Set<string>();
      for (const entry of rawBackends) {
        const value = typeof entry === "string" ? entry : entry?.url;
        if (typeof value !== "string" || !value.trim()) continue;
        let parsed: URL;
        try {
          parsed = new URL(value.trim());
        } catch {
          continue;
        }
        if (parsed.protocol !== "http:" && parsed.protocol !== "https:") continue;
        if (parsed.username || parsed.password) continue;
        const normalized = parsed.toString().replace(/\/+$/, "");
        if (seenBackends.has(normalized)) continue;
        seenBackends.add(normalized);
        backends.push({
          url: normalized,
          weight: typeof entry === "object" && Number.isFinite(entry?.weight) ? Number(entry.weight) : undefined,
        });
        if (backends.length >= settings.maxBackends) break;
      }
      if (backends.length === 0) {
        reply(false, "At least one http:// or https:// backend is required");
        return;
      }

      const zone = normalizeHost(this.env.WEB_ZONE || meta.baseHost || "");
      const now = Date.now();
      const currentHost = (Array.isArray(meta.webHosts) ? meta.webHosts : [])
        .map((value) => normalizeHost(value))
        .find((value) => value) || "";
      let requestedHost = normalizeHost(msg.host || "");
      const originalRequestedHost = requestedHost;

      // A reservation binds a hostname to one account even while no site exists.
      // It is invisible on the user side and only blocks other accounts.
      const reservation = requestedHost ? await getReservation(this.env, requestedHost) : null;
      const reservedClaim = Boolean(
        requestedHost && reservation && user && reservation.user_id === user.id,
      );
      if (requestedHost && reservation) {
        if (!user) {
          reply(false, "This hostname is reserved by an account");
          return;
        }
        if (reservation.user_id !== user.id) {
          reply(false, "This hostname is reserved by another account");
          return;
        }
      }

      // Forced temporary mode discards custom hostnames (and old hosts) on
      // every registration. The provider receives the new URL in the response
      // and in a log message so its operator can see the change.
      if (settings.forceTemporaryHost) {
        requestedHost = "";
      }

      // Every hostname a rotation leaves behind is a tombstone. Only the relay
      // that owns the chain may follow it to the current head; nobody can
      // register the deprecated hostname itself again.
      const retired = requestedHost ? await getRetiredHost(this.env, requestedHost) : null;
      if (retired) {
        if (retired.relay_id !== relayId) {
          reply(false, "This hostname has been retired by another relay");
          return;
        }
        const mappedSite = await getSite(this.env, retired.successor_host);
        requestedHost = mappedSite && mappedSite.relay_id === relayId ? mappedSite.host : "";
      }

      if (msg.rotate) {
        const rotationSourceHost = requestedHost || currentHost;
        const sourceSite = rotationSourceHost ? await getSite(this.env, rotationSourceHost) : null;

        if (
          sourceSite &&
          sourceSite.temporary === 1 &&
          sourceSite.rotate_seconds > 0 &&
          sourceSite.relay_id === relayId &&
          (!sourceSite.owner_id || sourceSite.owner_id === (user?.id || null))
        ) {
          // Older clients may still send a rotation request. Treat it as a
          // nudge: rotate only when the relay's own schedule says it is due,
          // otherwise hand back the current host.
          const lead = this.webRotationLeadMs(sourceSite.rotate_seconds);
          if (!sourceSite.expires_at || sourceSite.expires_at - now <= lead) {
            const rotated = await this.rotateTemporarySiteLocked(sourceSite, ws);
            if (rotated) {
              await this.rememberWebGrace(settings);
              reply(true, undefined, {
                host: rotated.host,
                url: rotated.url,
                temporary: true,
                expiresAt: rotated.expiresAt,
                rotateSeconds: rotated.rotateSeconds,
                backendCount: backends.length,
                owner: user?.username,
              });
              return;
            }
          }

          meta.webHosts = [sourceSite.host];
          meta.webBackendCount = backends.length;
          ws.serializeAttachment(meta);
          this.syncFromState(true);
          await this.rememberWebGrace(settings);
          reply(true, undefined, {
            host: sourceSite.host,
            url: siteUrl(sourceSite.host),
            temporary: true,
            expiresAt: sourceSite.expires_at ?? undefined,
            rotateSeconds: sourceSite.rotate_seconds,
            backendCount: backends.length,
            owner: user?.username,
          });
          return;
        }
      }

      let existing = requestedHost ? await getSite(this.env, requestedHost) : null;
      if (requestedHost && !existing) {
        // A provider that missed the rotation broadcast reconnects with the
        // host it still knows about; follow the tombstone instead of
        // allocating a second replacement domain.
        const previous = await getRetiredHost(this.env, requestedHost);
        const mappedSite = previous ? await getSite(this.env, previous.successor_host) : null;
        if (
          previous?.relay_id === relayId &&
          mappedSite &&
          mappedSite.relay_id === relayId &&
          (!mappedSite.owner_id || mappedSite.owner_id === (user?.id || null))
        ) {
          requestedHost = mappedSite.host;
          existing = mappedSite;
        }
      }
      let ownsExisting = Boolean(
        existing &&
          existing.relay_id === relayId &&
          (!existing.owner_id || existing.owner_id === (user?.id || null)),
      );
      let reclaimingTemporary = Boolean(
        existing &&
          existing.temporary === 1 &&
          existing.relay_id === relayId &&
          (!existing.owner_id || existing.owner_id === (user?.id || null)),
      );

      if (
        reclaimingTemporary &&
        existing?.expires_at &&
        existing.expires_at <= now
      ) {
        // The relay normally rotates before expiry. If an alarm was missed
        // while this provider was offline, rotate now instead of rejecting it.
        const rotated = await this.rotateTemporarySiteLocked(existing, ws);
        if (!rotated) {
          reply(false, "This temporary hostname has expired and could not be rotated");
          return;
        }
        await this.rememberWebGrace(settings);
        reply(true, undefined, {
          host: rotated.host,
          url: rotated.url,
          temporary: true,
          expiresAt: rotated.expiresAt,
          rotateSeconds: rotated.rotateSeconds,
          backendCount: backends.length,
          owner: user?.username,
        });
        return;
      }

      if (requestedHost) {
        if (!settings.allowCustomHost && !ownsExisting && !reservation) {
          reply(false, "Custom hostnames are disabled for this account");
          return;
        }
        if (!zone || !isSiteHostForZone(requestedHost, zone)) {
          reply(false, `Hostname must be a subdomain of ${zone || "WEB_ZONE"}`);
          return;
        }
        if (existing && existing.relay_id !== relayId && !reservedClaim) {
          reply(false, "This hostname is already bound to another relay");
          return;
        }
        if (existing?.owner_id && existing.owner_id !== (user?.id || null)) {
          reply(false, "This hostname is owned by another user");
          return;
        }
      }

      let host = requestedHost;
      if (!host) {
        if (!zone) {
          reply(false, "WEB_ZONE is not configured, so automatic hostnames are unavailable");
          return;
        }
        host = await this.allocateTemporaryHost(zone) || "";
        if (!host) {
          reply(false, "Could not allocate a hostname, please retry");
          return;
        }
      }

      if (!existing) {
        existing = await getSite(this.env, host);
      }
      if (existing && existing.relay_id !== relayId && !reservedClaim) {
        reply(false, "This hostname is already bound to another relay");
        return;
      }
      if (existing?.owner_id && existing.owner_id !== (user?.id || null)) {
        reply(false, "This hostname is owned by another user");
        return;
      }

      if (!existing && settings.maxSites > 0) {
        const ownerSites = user
          ? await listSites(this.env, user.id)
          : (await listSites(this.env)).filter(
              (site) => site.anonymous === 1 && site.token_hash === (tokenHash || null),
            );
        const activeSites = ownerSites.filter(
          (site) => !site.expires_at || site.expires_at > now,
        );
        if (activeSites.length >= settings.maxSites) {
          reply(false, `Site limit reached (${settings.maxSites})`);
          return;
        }
      }

      const temporary = !requestedHost || reclaimingTemporary;
      // The permanent-host grant is the merged custom-hostname permission: the
      // account published its own hostname, so the site never rotates and the
      // account-wide duration cap does not apply to it either. Reconnecting to
      // an existing temporary host stays temporary, grant or not.
      const permanentHost = Boolean(requestedHost) && settings.allowCustomHost && !temporary;
      let expiresAt = reservedClaim ? null : (existing?.expires_at ?? null);
      if (temporary && (!expiresAt || expiresAt <= now || !existing)) {
        expiresAt = settings.autoRotateSeconds > 0
          ? now + settings.autoRotateSeconds * 1000
          : null;
      }
      if (permanentHost) {
        expiresAt = null;
      } else if (settings.quotaDurationSeconds > 0) {
        const durationExpiry = now + settings.quotaDurationSeconds * 1000;
        expiresAt = expiresAt ? Math.min(expiresAt, durationExpiry) : durationExpiry;
      }

      const site: SiteRow = {
        host,
        relay_id: relayId,
        owner_id: user?.id || null,
        token_hash: tokenHash || null,
        temporary: temporary ? 1 : 0,
        expires_at: expiresAt,
        rotate_seconds: temporary ? settings.autoRotateSeconds : 0,
        backends_json: JSON.stringify(backends.map((backend) => backend.url)),
        backend_count: backends.length,
        insecure: msg.insecure ? 1 : 0,
        anonymous: user ? 0 : 1,
        quota_bytes: settings.quotaBytes,
        quota_requests: settings.quotaRequests,
        created_at: existing?.created_at ?? now,
        updated_at: now,
        last_seen_at: existing?.last_seen_at ?? null,
      };
      await upsertSite(this.env, site);
      this.siteCache.delete(host);
      await this.rememberWebGrace(settings);

      if (settings.forceTemporaryHost && originalRequestedHost && originalRequestedHost !== host) {
        const notice: LogMessage = {
          level: "info",
          msg: `Forced temporary hostnames are enabled; ${originalRequestedHost} is no longer used. New URL: ${siteUrl(host)}`,
          getType: () => MessageType.Log,
        };
        try {
          this.sendMessage(ws, notice);
        } catch {
          // The response below still carries the new host.
        }
      }

      const previousHosts = Array.isArray(meta.webHosts) ? meta.webHosts : [];
      for (const oldHost of previousHosts) {
        if (!oldHost || oldHost === host) continue;
        const oldSite = await getSite(this.env, oldHost);
        if (oldSite && oldSite.relay_id === relayId) {
          await deleteSite(this.env, oldHost);
          this.siteCache.delete(oldHost);
        }
      }

      meta.webHosts = [host];
      meta.webBackendCount = backends.length;
      meta.webOwnerId = user?.id || null;
      meta.webAnonymous = !user;
      ws.serializeAttachment(meta);
      this.syncFromState(true);
      await this.scheduleWebRotationAlarm();

      reply(true, undefined, {
        host,
        url: siteUrl(host),
        temporary,
        expiresAt: expiresAt ?? undefined,
        rotateSeconds: temporary ? settings.autoRotateSeconds : 0,
        backendCount: backends.length,
        owner: user?.username,
      });
    } catch (err) {
      reply(false, err instanceof Error ? err.message : String(err));
    } finally {
      release();
    }
  }

  private async handleWebResponse(msg: WebResponseMessage): Promise<void> {
    const pending = this.webPending.get(msg.channelId);
    if (!pending || pending.responseReceived) return;
    if (pending.method !== "HEAD" && msg.status !== 204 && msg.status !== 304 &&
      typeof msg.bodyLength === "number" && msg.bodyLength > 0 &&
      !await quotaCanFit(this.env, pending.quotaPrincipal, msg.bodyLength, pending.quotaBytes)) {
      this.finishWebPending(msg.channelId, new WebQuotaExceededError("Web transfer quota exceeded"));
      return;
    }
    if (this.webPending.get(msg.channelId) !== pending) return;
    pending.responseReceived = true;
    if (pending.timeout) {
      clearTimeout(pending.timeout);
      pending.timeout = null;
    }
    // Give the body a generous idle window after headers arrive.
    pending.timeout = setTimeout(() => {
      this.finishWebPending(msg.channelId, new Error("Web response body timed out"));
    }, 120_000);
    pending.resolve(msg);
  }

  private async queueWebAction(channelId: string, action: () => Promise<void>): Promise<void> {
    const previous = this.webDataQueues.get(channelId) || Promise.resolve();
    const current = previous.then(action);
    this.webDataQueues.set(channelId, current);
    try {
      await current;
    } finally {
      if (this.webDataQueues.get(channelId) === current) this.webDataQueues.delete(channelId);
    }
  }

  private async handleWebData(msg: WebDataMessage): Promise<void> {
    const pending = this.webPending.get(msg.channelId);
    if (!pending) return;
    if (pending.timeout) {
      clearTimeout(pending.timeout);
      pending.timeout = setTimeout(() => {
        this.finishWebPending(msg.channelId, new Error("Web response body timed out"));
      }, 120_000);
    }
    if (msg.data.byteLength > 0) {
      const allowed = await consumeQuota(this.env, pending.quotaPrincipal, msg.data.byteLength, 0,
        pending.quotaBytes, pending.quotaRequests);
      if (!allowed) {
        this.finishWebPending(msg.channelId, new Error("Web transfer quota exceeded"));
        return;
      }
      if (this.webPending.get(msg.channelId) !== pending) return;
      pending.bytes += msg.data.byteLength;
      this.reportWebBytes(pending.host, msg.data.byteLength);
      if (pending.controller) {
        try {
          pending.controller.enqueue(msg.data);
        } catch {
          // The client may have gone away.
        }
      }
    }
    if (msg.end) {
      this.finishWebPending(msg.channelId);
    }
  }

  private handleWebCancel(msg: WebCancelMessage): void {
    this.finishWebPending(msg.channelId, new Error(msg.reason || "cancelled"));
  }

  private finishWebPending(channelId: string, error?: Error): void {
    const pending = this.webPending.get(channelId);
    if (!pending) return;
    if (pending.timeout) {
      clearTimeout(pending.timeout);
      pending.timeout = null;
    }
    this.webPending.delete(channelId);
    if (!pending.responseReceived) {
      error ??= new Error("Web response ended before headers");
      pending.reject(error);
    }
    try {
      if (error) pending.controller?.error(error);
      else pending.controller?.close();
    } catch {
      // Stream may already be closed/cancelled.
    }
    if (error) {
      try {
        this.sendMessage(pending.ws, {
          channelId,
          reason: error.message,
          getType: () => MessageType.WebCancel,
        } as WebCancelMessage);
      } catch {
        // Ignore provider disconnect.
      }
    }
  }

  private cancelWebPending(channelId: string, reason: string): void {
    this.finishWebPending(channelId, new Error(reason));
  }

  private getWebProviders(host: string): WebSocket[] {
    this.syncFromState(true);
    const list = this.webProviders.get(host) || [];
    return list.filter((ws) => {
      try {
        const meta = ws.deserializeAttachment() as WebsocketMeta;
        return meta.web === true && Array.isArray(meta.webHosts) && meta.webHosts.includes(host);
      } catch {
        return false;
      }
    });
  }

  private webRotationLeadMs(rotateSeconds: number): number {
    const configured = Number(this.env.WEB_ROTATION_LEAD_MS);
    if (Number.isFinite(configured) && configured > 0) {
      return Math.min(configured, Math.max(1_000, rotateSeconds * 1_000 - 1_000));
    }
    const halfLife = rotateSeconds * 500;
    const beforeExpiry = rotateSeconds * 1_000 - 1_000;
    return Math.min(Relay.WEB_ROTATE_LEAD_MS, Math.max(1_000, Math.min(halfLife, beforeExpiry)));
  }

  private async lockSiteMutation(): Promise<() => void> {
    const previous = this.siteMutationChain;
    let release = () => {};
    this.siteMutationChain = previous.then(
      () => new Promise<void>((resolve) => {
        release = resolve;
      }),
    );
    await previous;
    return release;
  }

  private async withSiteMutation<T>(action: () => Promise<T>): Promise<T> {
    const release = await this.lockSiteMutation();
    try {
      return await action();
    } finally {
      release();
    }
  }

  private async allocateTemporaryHost(zone: string): Promise<string | null> {
    const reservedHosts = new Set(
      (await listReservations(this.env)).map((row) => row.host),
    );
    for (let attempt = 0; attempt < 8; attempt++) {
      const candidate = `${randomHostLabel()}.${zone}`;
      if (reservedHosts.has(candidate)) continue;
      if (await getRetiredHost(this.env, candidate)) continue;
      if (await getSite(this.env, candidate)) continue;
      return candidate;
    }
    return null;
  }

  /**
   * Schedule a relay alarm for the earliest temporary hostname that still has
   * an online provider. The alarm is the only rotation trigger; clients just
   * apply the WebRotate message they receive.
   */
  private async scheduleWebRotationAlarm(): Promise<void> {
    if (!this.env.DB) return;
    try {
      const relayId = this.state.id.toString();
      const sites = await listSitesByRelay(this.env, relayId);
      let nextAt: number | null = null;
      for (const site of sites) {
        if (site.temporary !== 1 || site.rotate_seconds <= 0 || !site.expires_at) continue;
        if (this.getWebProviders(site.host).length === 0) continue;
        const dueAt = site.expires_at - this.webRotationLeadMs(site.rotate_seconds);
        if (nextAt === null || dueAt < nextAt) nextAt = dueAt;
      }
      if (nextAt !== null) {
        // A failed rotation leaves the deadline in the past; retry later instead
        // of re-arming the same instant and spinning the alarm.
        const when = nextAt > Date.now() ? nextAt : Date.now() + 30_000;
        await this.scheduleAlarmAt(when);
      }
    } catch (error) {
      console.error("web rotation: failed to schedule alarm", error);
    }
  }

  private async rotateDueSites(): Promise<void> {
    if (!this.env.DB) return;
    const relayId = this.state.id.toString();
    const sites = await listSitesByRelay(this.env, relayId);
    const now = Date.now();
    for (const site of sites) {
      if (site.temporary !== 1 || site.rotate_seconds <= 0 || !site.expires_at) continue;
      if (site.expires_at - now > this.webRotationLeadMs(site.rotate_seconds)) continue;
      await this.rotateTemporarySite(site);
    }
  }

  /**
   * Rotate a temporary site once for every provider serving it. All providers
   * sharing the hostname must move together, otherwise the machines end up on
   * different domains.
   */
  private async rotateTemporarySite(site: SiteRow, initiator?: WebSocket): Promise<WebRotationResult | null> {
    return this.withSiteMutation(() => this.rotateTemporarySiteLocked(site, initiator));
  }

  // Caller must hold the site mutation lock.
  private async rotateTemporarySiteLocked(site: SiteRow, initiator?: WebSocket): Promise<WebRotationResult | null> {
    const relayId = this.state.id.toString();
    const latest = await getSite(this.env, site.host);
    if (!latest) {
      // Another provider already rotated this host while we queued. Follow the
      // tombstone to the current site instead of allocating a second domain.
      const retired = await getRetiredHost(this.env, site.host);
      if (!retired || retired.relay_id !== relayId) return null;
      const mappedSite = await getSite(this.env, retired.successor_host);
      if (!mappedSite || mappedSite.relay_id !== relayId) return null;
      return {
        host: mappedSite.host,
        url: siteUrl(mappedSite.host),
        expiresAt: mappedSite.expires_at || Date.now(),
        rotateSeconds: mappedSite.rotate_seconds,
      };
    }

    if (latest.rotate_seconds <= 0) return null;
    if (latest.expires_at && latest.expires_at - Date.now() > this.webRotationLeadMs(latest.rotate_seconds)) {
      return null;
    }

    const providers = this.getWebProviders(latest.host);
    if (initiator && !providers.includes(initiator)) providers.push(initiator);
    if (providers.length === 0) return null;

    // Local dev and some deployments leave WEB_ZONE empty and let the public
    // host double as the zone; providers carry that host in their attachment.
    let zone = normalizeHost(this.env.WEB_ZONE || "");
    if (!zone) {
      for (const provider of providers) {
        const { baseHost } = this.safeDeserializeAttachment(provider);
        zone = normalizeHost(baseHost || "");
        if (zone) break;
      }
    }
    if (!zone) return null;

    const host = await this.allocateTemporaryHost(zone);
    if (!host) return null;

    const now = Date.now();
    const expiresAt = now + latest.rotate_seconds * 1000;
    const next: SiteRow = {
      ...latest,
      host,
      expires_at: expiresAt,
      updated_at: now,
      last_seen_at: now,
    };
    await rotateSiteRecord(this.env, next, {
      host: latest.host,
      relay_id: relayId,
      owner_id: latest.owner_id,
      successor_host: host,
      retired_at: now,
    });
    this.siteCache.delete(latest.host);
    this.siteCache.delete(host);

    const rotateMessage: WebRotateMessage = {
      host,
      url: siteUrl(host),
      expiresAt,
      rotateSeconds: latest.rotate_seconds,
      getType: () => MessageType.WebRotate,
    };
    for (const provider of providers) {
      const meta = this.safeDeserializeAttachment(provider);
      meta.webHosts = [host];
      provider.serializeAttachment(meta);
      // The initiator gets the same data in its registration response.
      if (provider === initiator) continue;
      try {
        this.sendMessage(provider, rotateMessage);
      } catch {
        // The provider will re-register and follow the tombstone.
      }
    }
    this.syncFromState(true);
    await this.scheduleWebRotationAlarm();

    return {
      host,
      url: siteUrl(host),
      expiresAt,
      rotateSeconds: latest.rotate_seconds,
    };
  }

  private ensureWebUsageDay(): void {
    const today = utcDayKey();
    if (this.webUsageDay === today) return;
    this.webUsageDay = today;
    this.webUsage = {};
  }

  private getWebUsage(host: string): WebUsage {
    this.ensureWebUsageDay();
    let usage = this.webUsage[host];
    if (!usage) {
      usage = { requests: 0, bytes: 0, lastSeenAt: 0 };
      this.webUsage[host] = usage;
    }
    return usage;
  }

  /**
   * How long a relay group keeps its hostnames and connector tokens after the
   * last provider leaves.
   *
   * Web sites get a 24 hour hold so a provider restart, laptop sleep or brief
   * outage does not hand the hostname to somebody else. Forced temporary mode
   * deliberately gives that up: the host rotates and is released quickly.
   * Relays without any web site keep the historical 60 second grace.
   */
  private async getProviderDisconnectGraceMs(): Promise<number> {
    const stored = (await this.storage.get("webGraceMs")) as number | undefined;
    if (typeof stored === "number" && stored > 0) return stored;
    if (!this.env.DB) return Relay.PROVIDER_DISCONNECT_GRACE_MS;
    try {
      const sites = await listSitesByRelay(this.env, this.state.id.toString());
      return sites.length > 0 ? Relay.WEB_RELAY_HOLD_MS : Relay.PROVIDER_DISCONNECT_GRACE_MS;
    } catch {
      return Relay.PROVIDER_DISCONNECT_GRACE_MS;
    }
  }

  private async rememberWebGrace(settings: Settings): Promise<void> {
    const holdMs = settings.forceTemporaryHost
      ? Relay.WEB_FORCED_TEMP_HOLD_MS
      : Relay.WEB_RELAY_HOLD_MS;
    await this.storage.put("webGraceMs", holdMs);
  }

  private async markProviderOnline(): Promise<void> {
    const scheduled = await this.storage.get("providerDisconnectTime");
    if (scheduled) await this.storage.delete("providerDisconnectTime");
  }

  private incrementWebRequest(host: string): void {
    const usage = this.getWebUsage(host);
    usage.requests += 1;
    usage.lastSeenAt = Date.now();
    this.scheduleWebUsageFlush();
  }

  private reportWebBytes(host: string, bytes: number): void {
    if (bytes <= 0) return;
    const usage = this.getWebUsage(host);
    usage.bytes += bytes;
    usage.lastSeenAt = Date.now();
    this.scheduleWebUsageFlush();
  }

  private scheduleWebUsageFlush(): void {
    if (this.webUsageTimer) return;
    this.webUsageTimer = setTimeout(() => {
      this.webUsageTimer = null;
      this.flushWebUsage().catch(console.error);
    }, WEB_USAGE_FLUSH_DELAY_MS);
    this.scheduleAlarmAt(Date.now() + 60_000).catch(console.error);
  }

  private async flushWebUsage(): Promise<void> {
    this.ensureWebUsageDay();
    if (Object.keys(this.webUsage).length === 0) return;
    await this.storage.put("webUsageDay", this.webUsageDay);
    await this.storage.put("webUsage", this.webUsage);
  }

  private async getSiteCached(host: string): Promise<SiteRow | null> {
    const cached = this.siteCache.get(host);
    const now = Date.now();
    if (cached && now - cached.at < SITE_CACHE_TTL_MS) return cached.site;
    if (!this.env.DB) return null;
    let site: SiteRow | null = null;
    try {
      await ensureSchema(this.env);
      site = await getSite(this.env, host);
    } catch {
      site = null;
    }
    this.siteCache.set(host, { site, at: now });
    return site;
  }

  private webQuotaResponse(site: SiteRow, kind: "requests" | "bytes"): Response {
    const message =
      kind === "requests"
        ? `This site reached its request quota (${site.quota_requests.toLocaleString()}).`
        : `This site reached its transfer quota.`;
    return new Response(renderErrorPage("Quota exceeded", message, "429"), {
      status: 429,
      headers: { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" },
    });
  }

  private async handleWebHttp(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const host = normalizeHost(
      request.headers.get("X-LS-Site-Host") || url.searchParams.get("host") || "",
    );
    if (!host) {
      return new Response("Missing site host", { status: 400 });
    }

    const site = await this.getSiteCached(host);
    if (!site) {
      return new Response(renderErrorPage("Unknown site", `No site is configured for ${host}.`, "404"), {
        status: 404,
        headers: { "Content-Type": "text/html; charset=utf-8" },
      });
    }

    const now = Date.now();
    if (site.expires_at && site.expires_at <= now) {
      return new Response(renderExpiredPage(host, site.expires_at), {
        status: 410,
        headers: { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" },
      });
    }

    const providers = this.getWebProviders(host);
    if (providers.length === 0) {
      return new Response(
        renderErrorPage("No origin online", `No provider is currently serving ${host}.`, "503"),
        { status: 503, headers: { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" } },
      );
    }

    const declaredLength = Number(request.headers.get("Content-Length") || "0");
    if (Number.isFinite(declaredLength) && declaredLength > MAX_WEB_BODY_BYTES) {
      return new Response("Request body too large", { status: 413 });
    }
    let body = new Uint8Array(0);
    if (request.body) {
      const buffer = await request.arrayBuffer();
      if (buffer.byteLength > MAX_WEB_BODY_BYTES) {
        return new Response("Request body too large", { status: 413 });
      }
      body = new Uint8Array(buffer);
    }

    const quotaPrincipal = site.owner_id ? `user:${site.owner_id}` : `anonymous:${site.token_hash || site.relay_id}`;
    const allowed = await consumeQuota(this.env, quotaPrincipal, body.byteLength, 1, site.quota_bytes, site.quota_requests);
    if (!allowed) {
      return this.webQuotaResponse(site, site.quota_requests > 0 ? "requests" : "bytes");
    }

    const index = (this.webRoundRobin.get(host) || 0) % providers.length;
    this.webRoundRobin.set(host, index + 1);
    const ws = providers[index];
    const channelId = crypto.randomUUID();

    let controller: ReadableStreamDefaultController<Uint8Array> | null = null;
    const stream = new ReadableStream<Uint8Array>({
      start(c) {
        controller = c;
      },
      cancel: () => {
        this.cancelWebPending(channelId, "client disconnected");
      },
    });

    let resolveResponse!: (msg: WebResponseMessage) => void;
    let rejectResponse!: (err: Error) => void;
    const responsePromise = new Promise<WebResponseMessage>((resolve, reject) => {
      resolveResponse = resolve;
      rejectResponse = reject;
    });
    void responsePromise.catch(() => undefined);

    const pending: WebPending = {
      host,
      ws,
      method: request.method,
      quotaPrincipal,
      quotaBytes: site.quota_bytes,
      quotaRequests: site.quota_requests,
      controller,
      responseReceived: false,
      resolve: resolveResponse,
      reject: rejectResponse,
      timeout: null,
      bytes: 0,
    };
    this.webPending.set(channelId, pending);
    this.incrementWebRequest(host);
    this.reportWebBytes(host, body.byteLength + 512);

    const headers: Array<[string, string]> = [];
    for (const [name, value] of request.headers) {
      const lower = name.toLowerCase();
      if (isHopByHopHeader(lower) || isInternalHeader(lower)) continue;
      if (lower === "host" || lower === "content-length") continue;
      headers.push([name, value]);
    }
    const clientIp =
      request.headers.get("CF-Connecting-IP") ||
      request.headers.get("X-Forwarded-For")?.split(",")[0].trim() ||
      "unknown";
    const forwardedFor = request.headers.get("X-Forwarded-For");
    headers.push(["X-Forwarded-For", forwardedFor ? `${forwardedFor}, ${clientIp}` : clientIp]);
    headers.push(["X-Forwarded-Host", host]);
    headers.push([
      "X-Forwarded-Proto",
      request.headers.get("X-LS-Original-Proto") || "https",
    ]);

    const webRequest: WebRequestMessage = {
      channelId,
      method: request.method,
      path: request.headers.get("X-LS-Original-Path") || `${url.pathname}${url.search}`,
      headers,
      hasBody: body.byteLength > 0,
      bodyLength: body.byteLength,
      clientIp,
      scheme: request.headers.get("X-LS-Original-Proto") || "https",
      host,
      getType: () => MessageType.WebRequest,
    };

    try {
      this.sendMessage(ws, webRequest);
    } catch {
      this.finishWebPending(channelId, new Error("Provider disconnected"));
      return new Response("Origin disconnected", { status: 502 });
    }

    if (body.byteLength > 0) {
      for (let offset = 0; offset < body.byteLength; offset += WEB_DATA_CHUNK_BYTES) {
        const chunk = body.slice(offset, offset + WEB_DATA_CHUNK_BYTES);
        const end = offset + chunk.byteLength >= body.byteLength;
        try {
          this.sendMessage(ws, {
            channelId,
            data: chunk,
            end,
            getType: () => MessageType.WebData,
          } as WebDataMessage);
        } catch {
          this.finishWebPending(channelId, new Error("Provider disconnected"));
          return new Response("Origin disconnected", { status: 502 });
        }
      }
    }

    pending.timeout = setTimeout(() => {
      this.cancelWebPending(channelId, "Web provider response timeout");
    }, WEB_RESPONSE_TIMEOUT_MS);

    const onAbort = () => this.cancelWebPending(channelId, "client aborted");
    request.signal?.addEventListener?.("abort", onAbort);

    let responseMsg: WebResponseMessage;
    try {
      responseMsg = await responsePromise;
    } catch (err) {
      this.cancelWebPending(channelId, err instanceof Error ? err.message : String(err));
      if (err instanceof WebQuotaExceededError) return this.webQuotaResponse(site, "bytes");
      return new Response(
        renderErrorPage("Origin timeout", "The origin did not respond in time.", "504"),
        { status: 504, headers: { "Content-Type": "text/html; charset=utf-8" } },
      );
    } finally {
      request.signal?.removeEventListener?.("abort", onAbort);
    }

    const status = responseMsg.status >= 200 && responseMsg.status <= 599 ? responseMsg.status : 502;
    const responseHeaders = new Headers();
    for (const [name, value] of responseMsg.headers || []) {
      const lower = String(name).toLowerCase();
      if (isHopByHopHeader(lower)) continue;
      try {
        responseHeaders.append(String(name), String(value));
      } catch {
        // Ignore invalid header names/values from a provider.
      }
    }

    if (request.method === "HEAD" || status === 204 || status === 304) {
      this.finishWebPending(channelId);
      return new Response(null, { status, headers: responseHeaders });
    }

    return new Response(stream, { status, headers: responseHeaders });
  }

  private async handleWebSocketHttp(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const host = normalizeHost(request.headers.get("X-LS-Site-Host") || "");
    const site = host ? await this.getSiteCached(host) : null;
    if (!site) return new Response("Unknown site", { status: 404 });
    if (site.expires_at && site.expires_at <= Date.now()) {
      return new Response(renderExpiredPage(host, site.expires_at), { status: 410, headers: { "Content-Type": "text/html; charset=utf-8" } });
    }
    const providers = this.getWebProviders(host).filter((provider) =>
      supportsFeatureVersion(this.safeDeserializeAttachment(provider).peerProtocolVersion || 0, MIN_VERSION_WEBSOCKET));
    if (providers.length === 0) return new Response("No WebSocket-capable provider is online", { status: 503 });
    const principal = site.owner_id ? `user:${site.owner_id}` : `anonymous:${site.token_hash || site.relay_id}`;
    if (!await consumeQuota(this.env, principal, 0, 1, site.quota_bytes, site.quota_requests)) {
      return this.webQuotaResponse(site, site.quota_requests > 0 ? "requests" : "bytes");
    }
    const index = (this.webRoundRobin.get(host) || 0) % providers.length;
    this.webRoundRobin.set(host, index + 1);
    const provider = providers[index];
    const channelId = crypto.randomUUID();
    let resolveResponse!: (msg: WebResponseMessage) => void;
    let rejectResponse!: (err: Error) => void;
    const responsePromise = new Promise<WebResponseMessage>((resolve, reject) => {
      resolveResponse = resolve;
      rejectResponse = reject;
    });
    void responsePromise.catch(() => undefined);
    const pending: WebPending = {
      host, ws: provider, method: "GET", quotaPrincipal: principal,
      quotaBytes: site.quota_bytes, quotaRequests: site.quota_requests,
      controller: null, responseReceived: false, resolve: resolveResponse, reject: rejectResponse,
      timeout: null, bytes: 0,
    };
    this.webPending.set(channelId, pending);
    this.incrementWebRequest(host);
    const headers: Array<[string, string]> = [];
    for (const [name, value] of request.headers) {
      const lower = name.toLowerCase();
      if (isHopByHopHeader(lower) || isInternalHeader(lower) || lower === "host" || lower === "content-length") continue;
      headers.push([name, value]);
    }
    headers.push(["X-Forwarded-Host", host]);
    headers.push(["X-Forwarded-Proto", request.headers.get("X-LS-Original-Proto") || "https"]);
    const webRequest: WebRequestMessage = {
      channelId, method: "GET", path: request.headers.get("X-LS-Original-Path") || `${url.pathname}${url.search}`,
      headers, hasBody: false, host, webSocket: true, getType: () => MessageType.WebRequest,
    };
    try {
      this.sendMessage(provider, webRequest);
    } catch {
      this.finishWebPending(channelId, new Error("Provider disconnected"));
      return new Response("Origin disconnected", { status: 502 });
    }
    pending.timeout = setTimeout(() => this.cancelWebPending(channelId, "WebSocket provider response timeout"), WEB_RESPONSE_TIMEOUT_MS);
    let response: WebResponseMessage;
    try {
      response = await responsePromise;
    } catch {
      return new Response("Origin did not accept WebSocket", { status: 502 });
    }
    if (pending.timeout) clearTimeout(pending.timeout);
    this.webPending.delete(channelId);
    if (response.status !== 101) {
      return new Response("Origin rejected WebSocket", { status: response.status >= 400 && response.status <= 599 ? response.status : 502 });
    }
    const responseHeaders = new Headers();
    const offered = (request.headers.get("Sec-WebSocket-Protocol") || "").split(",").map((value) => value.trim()).filter(Boolean);
    for (const [name, value] of response.headers || []) {
      const lower = name.toLowerCase();
      if (lower === "sec-websocket-protocol" && !offered.includes(value)) {
        this.closeWebSocketChannel(channelId, 1002, "Invalid WebSocket subprotocol", true);
        return new Response("Invalid WebSocket subprotocol", { status: 502 });
      }
      if (isHopByHopHeader(lower) || lower === "sec-websocket-extensions" || lower === "content-length") continue;
      responseHeaders.append(name, value);
    }
    const pair = new WebSocketPair();
    const [client, server] = [pair[0], pair[1]];
    this.state.acceptWebSocket(server);
    server.serializeAttachment({
      webClient: true, webChannelId: channelId, webHost: host,
      quotaPrincipal: principal, quotaBytes: site.quota_bytes, quotaRequests: site.quota_requests,
    } satisfies WebsocketMeta);
    const providerMeta = this.safeDeserializeAttachment(provider);
    providerMeta.webSocketChannels = [...(providerMeta.webSocketChannels || []), channelId];
    provider.serializeAttachment(providerMeta);
    this.webSocketClients.set(channelId, server);
    this.webSocketProviders.set(channelId, provider);
    return new Response(null, { status: 101, webSocket: client, headers: responseHeaders });
  }

  private async forwardWebSocketClient(ws: WebSocket, message: ArrayBuffer | string, meta: WebsocketMeta): Promise<void> {
    const channelId = meta.webChannelId || "";
    const provider = this.webSocketProviders.get(channelId);
    if (!provider || this.webSocketClients.get(channelId) !== ws || !meta.quotaPrincipal) {
      this.closeWebSocketChannel(channelId, 1011, "Provider unavailable", true);
      return;
    }
    const data = typeof message === "string" ? new TextEncoder().encode(message) : new Uint8Array(message);
    if (!await consumeQuota(this.env, meta.quotaPrincipal, data.byteLength, 0, meta.quotaBytes || 0, meta.quotaRequests || 0)) {
      this.closeWebSocketChannel(channelId, 1008, "Transfer quota exceeded", true);
      return;
    }
    if (meta.webHost) this.reportWebBytes(meta.webHost, data.byteLength);
    this.sendMessage(provider, { channelId, opcode: typeof message === "string" ? 1 : 2, data, getType: () => MessageType.WebSocketFrame } as WebSocketFrameMessage);
  }

  private async forwardWebSocketProvider(ws: WebSocket, frame: WebSocketFrameMessage): Promise<void> {
    const client = this.webSocketClients.get(frame.channelId);
    if (!client || this.webSocketProviders.get(frame.channelId) !== ws) return;
    const meta = this.safeDeserializeAttachment(client);
    if (!meta.quotaPrincipal || !await consumeQuota(this.env, meta.quotaPrincipal, frame.data.byteLength, 0, meta.quotaBytes || 0, meta.quotaRequests || 0)) {
      this.closeWebSocketChannel(frame.channelId, 1008, "Transfer quota exceeded", true);
      return;
    }
    if (meta.webHost) this.reportWebBytes(meta.webHost, frame.data.byteLength);
    client.send(frame.opcode === 1 ? new TextDecoder("utf-8", { fatal: true, ignoreBOM: false }).decode(frame.data) : frame.data);
  }

  private closeWebSocketChannel(channelId: string, code: number, reason: string, notifyProvider: boolean): void {
    const client = this.webSocketClients.get(channelId);
    const provider = this.webSocketProviders.get(channelId);
    this.webSocketClients.delete(channelId);
    this.webSocketProviders.delete(channelId);
    if (provider) {
      const meta = this.safeDeserializeAttachment(provider);
      meta.webSocketChannels = (meta.webSocketChannels || []).filter((item) => item !== channelId);
      try { provider.serializeAttachment(meta); } catch {}
      if (notifyProvider) {
        try { this.sendMessage(provider, { channelId, code, reason, getType: () => MessageType.WebCancel } as WebCancelMessage); } catch {}
      }
    }
    if (client) {
      const safeCode = code === 1000 || (code >= 1001 && code <= 1014 && ![1004, 1005, 1006].includes(code)) || (code >= 3000 && code <= 4999) ? code : 1011;
      let safeReason = reason;
      while (new TextEncoder().encode(safeReason).length > 123) safeReason = safeReason.slice(0, -1);
      try { client.close(safeCode, safeReason); } catch {}
    }
  }

  async adminGetWebInfo(): Promise<{
    sites: Record<string, { providers: number; backends: number; requests: number; bytes: number; lastSeenAt: number }>;
  }> {
    this.syncFromState(true);
    this.ensureWebUsageDay();
    const sites: Record<
      string,
      { providers: number; backends: number; requests: number; bytes: number; lastSeenAt: number }
    > = {};
    for (const ws of this.state.getWebSockets()) {
      const meta = this.safeDeserializeAttachment(ws);
      if (meta.web !== true) continue;
      const hosts = Array.isArray(meta.webHosts) ? meta.webHosts : [];
      for (const host of hosts) {
        if (typeof host !== "string" || !host) continue;
        const entry = sites[host] || { providers: 0, backends: 0, requests: 0, bytes: 0, lastSeenAt: 0 };
        entry.providers += 1;
        entry.backends += Number(meta.webBackendCount) || 0;
        sites[host] = entry;
      }
    }
    for (const [host, usage] of Object.entries(this.webUsage)) {
      const entry = sites[host] || { providers: 0, backends: 0, requests: 0, bytes: 0, lastSeenAt: 0 };
      entry.requests = usage.requests;
      entry.bytes = usage.bytes;
      entry.lastSeenAt = usage.lastSeenAt;
      sites[host] = entry;
    }
    return { sites };
  }

  async adminDisconnectWebHost(host: string): Promise<void> {
    const normalized = normalizeHost(host);
    if (!normalized) return;
    for (const ws of this.state.getWebSockets()) {
      const meta = this.safeDeserializeAttachment(ws);
      if (meta.web !== true) continue;
      const hosts = Array.isArray(meta.webHosts) ? meta.webHosts : [];
      if (!hosts.includes(normalized)) continue;
      const remaining = hosts.filter((item) => item !== normalized);
      if (remaining.length === 0) {
        try {
          ws.close(1012, "Site deleted by administrator");
        } catch {
          // Ignore close errors.
        }
      } else {
        meta.webHosts = remaining;
        ws.serializeAttachment(meta);
      }
    }
    this.siteCache.delete(normalized);
    this.syncFromState(true);
  }

  private disconnectChannel(channelId: string) {
    const provider = this.providerChannels.get(channelId);
    const connector = this.connectorChannels.get(channelId);

    // Update provider metadata
    if (provider) {
      const meta = provider.deserializeAttachment() as WebsocketMeta;
      if (meta.channels) {
        meta.channels = meta.channels.filter((id) => id !== channelId);
        provider.serializeAttachment(meta);
      }
    }

    // Update connector metadata
    if (connector) {
      const meta = connector.deserializeAttachment() as WebsocketMeta;
      if (meta.channels) {
        meta.channels = meta.channels.filter((id) => id !== channelId);
        connector.serializeAttachment(meta);
      }
    }

    // Remove the channel from the maps
    this.providerChannels.delete(channelId);
    this.connectorChannels.delete(channelId);
  }

  async webSocketClose(ws: WebSocket, code: number = 1000, reason: string = "") {
    this.syncFromState(true);

    const closingMeta = this.safeDeserializeAttachment(ws);
    if (closingMeta.webClient === true && closingMeta.webChannelId) {
      this.closeWebSocketChannel(closingMeta.webChannelId, code, reason, true);
      return;
    }
    for (const [channelId, provider] of this.webSocketProviders) {
      if (provider === ws) this.closeWebSocketChannel(channelId, 1011, "Provider disconnected", false);
    }
    for (const [channelId, pending] of this.webPending) {
      if (pending.ws === ws) this.finishWebPending(channelId, new Error("Provider disconnected"));
    }

    // Flush pending bytes so short-lived traffic is not undercounted.
    await this.flushTraffic(true).catch(console.error);

    const meta = this.safeDeserializeAttachment(ws);

    // Remove from providers/connectors set
    if (meta.isProvider === true) {
      this.providers.delete(ws);
      // Broadcast updated providers count to connectors.
      // Pass the closing socket so it is not counted while still present in state.getWebSockets().
      this.broadcastPartnersCountToConnectors(ws);
      
      // If all providers have disconnected, keep the relay (and its published
      // web hostnames) around for the configured hold period before cleanup.
      // Note: during the close event, the closing socket can still appear in state.getWebSockets().
      // Exclude it to avoid missing the transition to 0 providers and never scheduling the alarm.
      if (this.getActualProviderCountExcluding(ws) === 0) {
        await this.storage.put('providerDisconnectTime', Date.now());
        await this.scheduleAlarmAt(Date.now() + await this.getProviderDisconnectGraceMs());
      }
    } else {
      this.connectors.delete(ws);
      // Broadcast updated connectors count to providers (exclude the closing connector).
      this.broadcastPartnersCountToProviders(ws);
    }

    // Update metadata after connection close
    await this.updateMetadata();

    // Find and disconnect all channels associated with this WebSocket
    for (const [channelId, provider] of this.providerChannels.entries()) {
      if (provider === ws || this.connectorChannels.get(channelId) === ws) {
        this.disconnectChannel(channelId);
      }
    }
  }

  private async invalidateConnectorTokens() {
    const tokens = await this.storage.get('tokens') as string[] || [];
    
    for (const tokenName of tokens) {
      const tokenKey = await this.toTokenKey(tokenName);
      if (!tokenKey) continue;
      const metadata = await this.token.getRelayMetadata(tokenKey);
      
      if (metadata && metadata.connectorTokens) {
        for (const connectorToken of metadata.connectorTokens) {
          const connectorTokenKey = await this.toTokenKey(connectorToken);
          if (connectorTokenKey) {
            await this.token.deleteToken(connectorTokenKey);
          }
        }
        await this.token.updateRelayMetadata(tokenKey, { connectorTokens: [] });
      }
    }

    // Close all connector connections (use DO-managed list to survive hibernation)
    for (const ws of this.state.getWebSockets()) {
      const meta = this.safeDeserializeAttachment(ws);
      if (meta.isProvider === true) continue;
      try {
        ws.close(1001, 'Connector tokens revoked.');
      } catch {
        // Ignore
      }
    }
    this.connectors.clear();
  }

  private async deleteRelayRecords() {
    const relayId = this.state.id.toString();
    const tokens = (await this.storage.get('tokens')) as string[] || [];

    for (const tokenName of tokens) {
      const tokenKey = await this.toTokenKey(tokenName);
      if (!tokenKey) continue;

      await this.token.deleteToken(tokenKey);

      if (this.isSha256Hex(tokenKey)) {
        const doubleHashKey = await this.sha256(tokenKey);
        const meta = await this.token.getRelayMetadata(doubleHashKey);
        if (meta?.relayId === relayId) {
          await this.token.deleteToken(doubleHashKey);
        }
      }
    }

    await this.token.deleteRelay(relayId);
  }

  private notifyConnectorsRelayTermination(graceMs: number = Relay.PROVIDER_DISCONNECT_GRACE_MS): number {
    const msg: LogMessage = {
      level: "warn",
      msg: `All providers in this relay group have been offline for more than ${humanizeDuration(graceMs)}. This relay is terminating and all connector tokens in this group will be revoked. Please wait for a provider to reconnect and use a new connector token.`,
      getType: () => MessageType.Log,
    };

    let notified = 0;

    for (const ws of this.state.getWebSockets()) {
      const meta = this.safeDeserializeAttachment(ws);
      if (meta.isProvider === true) continue;
      try {
        this.sendMessage(ws, msg);
        notified++;
      } catch {
        // Ignore send errors
      }
    }

    return notified;
  }

  async webSocketError(ws: WebSocket, error: Error) {
    console.error("WebSocket error:", error);
    await this.webSocketClose(ws);
  }

  async alarm() {
    this.syncFromState(true);

    await this.flushTraffic(true).catch(console.error);
    await this.flushWebUsage().catch(console.error);

    // The alarm is the relay's rotation clock. A failure here must not stop
    // disconnect cleanup, so log it and keep going.
    try {
      await this.rotateDueSites();
    } catch (e) {
      console.error("alarm: web rotation failed", e);
    }

    const providerCount = this.getActualProviderCount();
    if (providerCount > 0) {
      await this.storage.delete('providerDisconnectTime');
      try {
        await this.scheduleWebRotationAlarm();
      } catch (e) {
        console.error("alarm: failed to schedule next web rotation", e);
      }
      return;
    }

    const providerDisconnectTime = (await this.storage.get('providerDisconnectTime')) as number | undefined;
    if (!providerDisconnectTime) return;

    // Web relays keep their hostnames for 24 hours; plain relays still use the
    // short 60 second grace.
    const elapsed = Date.now() - providerDisconnectTime;
    const graceMs = await this.getProviderDisconnectGraceMs();
    if (elapsed < graceMs) {
      // Ensure we run again once the hold period has elapsed.
      await this.scheduleAlarmAt(Date.now() + (graceMs - elapsed));
      return;
    }

    try {
      // performStaleRelayCleanup notifies connectors itself.
      await this.performStaleRelayCleanup();
    } catch (e) {
      console.error("alarm: cleanup failed, rescheduling", e);
      // Re-schedule so another attempt can clean up later.
      await this.scheduleAlarmAt(Date.now() + 60_000);
    }
  }
}
