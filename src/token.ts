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
}

interface DailyStats {
  connections: number;
  transferBytes: number;
}

interface GlobalStats {
  currentConnections: number;
  dailyStats: DailyStats;
}

export class Token extends DurableObject {
  private state: DurableObjectState;
  protected declare env: Env;
  private storage: DurableObjectStorage;
  private memoryEvents: ConnectionEvent[] = [];
  private alarmScheduled: boolean = false;

  constructor(state: DurableObjectState, env: Env) {
    super(state, env);
    this.state = state;
    this.env = env;
    this.storage = this.state.storage;
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
    return (await this.storage.get('relayTokens') as string[]) || [];
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

  async getStats(): Promise<GlobalStats> {
    const now = Date.now();
    const oneDayAgo = now - 24 * 60 * 60 * 1000;
    
    const storedEvents = (await this.storage.get('connectionEvents') as ConnectionEvent[]) || [];
    const allEvents = [...storedEvents, ...this.memoryEvents];
    const recentEvents = allEvents.filter(e => e.timestamp > oneDayAgo);
    
    const dailyStats = {
      connections: recentEvents.length,
      transferBytes: recentEvents.reduce((sum, e) => sum + e.bytes, 0)
    };
    
    // Calculate current connections from all relay metadata
    const relayTokens = await this.getAllRelayTokens();
    let currentConnections = 0;
    for (const token of relayTokens) {
      const metadata = await this.getRelayMetadata(token);
      if (metadata) {
        currentConnections += (metadata.providerCount || 0) + (metadata.connectorCount || 0);
      }
    }
    
    return { currentConnections, dailyStats };
  }

  async reportTraffic(bytes: number) {
    const now = Date.now();
    
    const existingEvent = this.memoryEvents[this.memoryEvents.length - 1];
    if (existingEvent && now - existingEvent.timestamp < 60000) {
      existingEvent.bytes += bytes;
    } else {
      this.memoryEvents.push({ timestamp: now, bytes });
    }
    
    await this.scheduleAlarm();
  }

  async reportChannelCreated() {
    const now = Date.now();
    this.memoryEvents.push({ timestamp: now, bytes: 0 });
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
    
    this.alarmScheduled = false;
  }

  async calibrate() {
    await this.flushToStorage();
  }
}
