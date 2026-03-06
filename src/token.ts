import { DurableObject } from "cloudflare:workers";
import { Env } from "./types";

interface RelayMetadata {
  relayId: string;
  providerCount: number;
  connectorCount: number;
  connectorTokens: string[];
  createdAt: number;
}

interface ConnectionEvent {
  timestamp: number;
  bytes: number;
  type?: "traffic" | "channel";
}

interface DailyStats {
  connections: number;
  transferBytes: number;
}

interface GlobalStats {
  currentConnections: number;
  dailyStats: DailyStats;
}

type RevokedRelayMap = Record<string, number>;

export class Token extends DurableObject {
  private state: DurableObjectState;
  protected declare env: Env;
  private storage: DurableObjectStorage;
  private memoryEvents: ConnectionEvent[] = [];
  private alarmScheduled: boolean = false;

  private static readonly RELAY_TOMBSTONE_TTL_MS = 24 * 60 * 60 * 1000;

  constructor(state: DurableObjectState, env: Env) {
    super(state, env);
    this.state = state;
    this.env = env;
    this.storage = this.state.storage;

    this.state.blockConcurrencyWhile(async () => {
      const pending = (await this.storage.get("pendingEvents")) as ConnectionEvent[] | null;
      if (Array.isArray(pending) && pending.length > 0) {
        this.memoryEvents = pending;
      }
    });
  }

  async setRelay(token: string, relayId: string) {
    // Initialize relay metadata
    const metadata: RelayMetadata = {
      relayId,
      providerCount: 0,
      connectorCount: 0,
      connectorTokens: [],
      createdAt: Date.now()
    };
    
    await this.storage.put(`relay:${token}`, metadata);
    
    // Keep track of all relay tokens
    const relayTokens = await this.getAllRelayTokens();
    if (!relayTokens.includes(token)) {
      relayTokens.push(token);
      await this.storage.put('relayTokens', relayTokens);
    }
  }

  async getRelay(token: string): Promise<string | null> {
    const metadata = await this.storage.get(`relay:${token}`) as RelayMetadata | null;
    return metadata?.relayId || null;
  }

  async deleteToken(token: string) {
    await this.storage.delete(`relay:${token}`);
    const relayTokens = await this.getAllRelayTokens();
    await this.storage.put('relayTokens', relayTokens.filter(t => t !== token));
  }

  async getAllRelayTokens(): Promise<string[]> {
    const stored = ((await this.storage.get('relayTokens')) as string[]) || [];
    const cleaned = stored.filter((t) => typeof t === "string" && t.trim());

    const repaired = (await this.storage.get("relayTokensRepaired")) as boolean | undefined;
    if (repaired) return cleaned;

    // Best-effort index repair (once): older deployments might have relay metadata without
    // being present in relayTokens. Rebuild from storage keys.
    const listed = await this.storage.list({ prefix: "relay:" });
    const keySet = new Set<string>();
    for (const t of cleaned) keySet.add(t);
    for (const k of listed.keys()) {
      if (typeof k !== "string") continue;
      const tokenKey = k.slice("relay:".length);
      if (tokenKey) keySet.add(tokenKey);
    }

    const next = Array.from(keySet);
    if (next.length !== cleaned.length) {
      await this.storage.put('relayTokens', next);
    } else if (cleaned.length !== stored.length) {
      await this.storage.put('relayTokens', cleaned);
    }

    await this.storage.put("relayTokensRepaired", true);
    return next;
  }

  async updateRelayMetadata(token: string, updates: Partial<RelayMetadata>) {
    const metadata = await this.storage.get(`relay:${token}`) as RelayMetadata | null;
    if (metadata) {
      Object.assign(metadata, updates);
      await this.storage.put(`relay:${token}`, metadata);
    }
  }

  async getRelayMetadata(token: string): Promise<RelayMetadata | null> {
    return await this.storage.get(`relay:${token}`) as RelayMetadata | null;
  }

  async addConnectorToken(relayToken: string, connectorToken: string) {
    // Ensure the relay token is indexed so admin UI can enumerate it even if it existed
    // before relayTokens tracking was introduced.
    const relayTokens = await this.getAllRelayTokens();
    if (!relayTokens.includes(relayToken)) {
      relayTokens.push(relayToken);
      await this.storage.put('relayTokens', relayTokens);
    }

    const metadata = await this.getRelayMetadata(relayToken);
    if (metadata && !metadata.connectorTokens.includes(connectorToken)) {
      metadata.connectorTokens.push(connectorToken);
      await this.storage.put(`relay:${relayToken}`, metadata);
    }
  }

  async removeConnectorToken(relayToken: string, connectorToken: string) {
    const metadata = await this.getRelayMetadata(relayToken);
    if (metadata) {
      metadata.connectorTokens = metadata.connectorTokens.filter(t => t !== connectorToken);
      await this.storage.put(`relay:${relayToken}`, metadata);
    }
  }

  async markRelayDeleted(relayId: string) {
    const id = (relayId || "").trim();
    if (!id) return;
    const now = Date.now();
    const raw = (await this.storage.get("revokedRelays")) as RevokedRelayMap | undefined;
    const map: RevokedRelayMap = raw && typeof raw === "object" ? { ...raw } : {};
    map[id] = now;
    await this.storage.put("revokedRelays", map);
  }

  async isRelayDeleted(relayId: string): Promise<boolean> {
    const id = (relayId || "").trim();
    if (!id) return false;
    const raw = (await this.storage.get("revokedRelays")) as RevokedRelayMap | undefined;
    if (!raw || typeof raw !== "object") return false;

    const ts = raw[id];
    if (!ts) return false;

    const now = Date.now();
    if (now - ts > Token.RELAY_TOMBSTONE_TTL_MS) {
      const next: RevokedRelayMap = { ...raw };
      delete next[id];
      await this.storage.put("revokedRelays", next);
      return false;
    }
    return true;
  }

  async getStats(): Promise<GlobalStats> {
    const now = Date.now();
    const oneDayAgo = now - 24 * 60 * 60 * 1000;
    
    const storedEvents = (await this.storage.get('connectionEvents') as ConnectionEvent[]) || [];
    const allEvents = [...storedEvents, ...this.memoryEvents];
    const recentEvents = allEvents.filter(e => e.timestamp > oneDayAgo);

    const isChannelEvent = (e: ConnectionEvent) => {
      if (e.type) return e.type === "channel";
      return e.bytes === 0;
    };

    const isTrafficEvent = (e: ConnectionEvent) => {
      if (e.type) return e.type === "traffic";
      return e.bytes > 0;
    };
    
    const dailyStats = {
      connections: recentEvents.filter(isChannelEvent).length,
      transferBytes: recentEvents.filter(isTrafficEvent).reduce((sum, e) => sum + e.bytes, 0)
    };
    
    // Calculate current connections by deduping on relayId.
    const relayTokens = await this.getAllRelayTokens();

    const relayCounts = new Map<string, { providers: number; connectors: number }>();
    for (const token of relayTokens) {
      const metadata = await this.getRelayMetadata(token);
      if (!metadata?.relayId) continue;
      const existing = relayCounts.get(metadata.relayId) || { providers: 0, connectors: 0 };
      existing.providers = Math.max(existing.providers, metadata.providerCount || 0);
      existing.connectors = Math.max(existing.connectors, metadata.connectorCount || 0);
      relayCounts.set(metadata.relayId, existing);
    }

    let currentConnections = 0;
    for (const v of relayCounts.values()) {
      currentConnections += v.providers + v.connectors;
    }
    
    return { currentConnections, dailyStats };
  }

  async reportTraffic(bytes: number) {
    const now = Date.now();
    
    const existingEvent = this.memoryEvents[this.memoryEvents.length - 1];
    if (existingEvent && now - existingEvent.timestamp < 60000 && (existingEvent.type ? existingEvent.type === "traffic" : existingEvent.bytes > 0)) {
      existingEvent.bytes += bytes;
    } else {
      this.memoryEvents.push({ timestamp: now, bytes, type: "traffic" });
    }

    await this.storage.put("pendingEvents", this.memoryEvents);
    await this.scheduleAlarm();
  }

  async reportChannelCreated() {
    const now = Date.now();
    this.memoryEvents.push({ timestamp: now, bytes: 0, type: "channel" });
    await this.storage.put("pendingEvents", this.memoryEvents);
    await this.scheduleAlarm();
  }

  private async scheduleAlarm() {
    if (!this.alarmScheduled) {
      const alarmTime = Date.now() + 5 * 60 * 1000; // 5 minutes
      await this.storage.setAlarm(alarmTime);
      this.alarmScheduled = true;
    }
  }

  async alarm() {
    await this.flushToStorage();
  }

  private async flushToStorage() {
    if (this.memoryEvents.length > 0) {
      const now = Date.now();
      const oneDayAgo = now - 24 * 60 * 60 * 1000;
      
      const storedEvents = (await this.storage.get('connectionEvents') as ConnectionEvent[]) || [];
      const allEvents = [...storedEvents, ...this.memoryEvents];
      const recentEvents = allEvents.filter(e => e.timestamp > oneDayAgo);
      
      await this.storage.put('connectionEvents', recentEvents);
      this.memoryEvents = [];
    }

    await this.storage.delete("pendingEvents");
    
    this.alarmScheduled = false;
  }

  async calibrate() {
    await this.flushToStorage();
  }
}
