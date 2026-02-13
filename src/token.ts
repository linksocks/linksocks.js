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
}
