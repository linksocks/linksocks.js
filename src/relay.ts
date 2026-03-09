import { DurableObject } from "cloudflare:workers";
import type { WebSocket } from "@cloudflare/workers-types";
import zxcvbn from "zxcvbn";

import type { Env, WebsocketMeta } from "./types";
import {
  type ConnectMessage,
  type ConnectorMessage,
  type ConnectorResponseMessage,
  type ConnectResponseMessage,
  type DataMessage,
  type DisconnectMessage,
  type AuthMessage,
  type PartnersMessage,
  parseMessage,
  packMessage,
  MessageType,
  AuthResponseMessage,
  LogMessage,
} from "./message";
import { handleErrors } from "./common";
import { describeCloudflareColo } from "./colo";
import { type Token } from "./token";

export class Relay extends DurableObject {
  private static readonly PROVIDER_DISCONNECT_GRACE_MS = 60_000;
  private static readonly TRAFFIC_FLUSH_IDLE_MS = 60_000;
  private static readonly TRAFFIC_PERSIST_INTERVAL_MS = 500;
  private static readonly TRAFFIC_PERSIST_BYTES_THRESHOLD = 256 * 1024;
  private static readonly DEFAULT_CONNECTOR_WAIT_PROVIDER_MS = 5_000;

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

  private trafficPersistPromise: Promise<void> | null = null;
  private lastTrafficPersistAt: number = 0;
  private lastPersistedAccumulator: number = 0;

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

  private persistTrafficState() {
    const now = Date.now();
    const deltaBytes = Math.abs(this.trafficAccumulator - this.lastPersistedAccumulator);
    const dueByTime = now - this.lastTrafficPersistAt >= Relay.TRAFFIC_PERSIST_INTERVAL_MS;
    const dueByBytes = deltaBytes >= Relay.TRAFFIC_PERSIST_BYTES_THRESHOLD;
    if (!dueByTime && !dueByBytes) return;
    if (this.trafficPersistPromise) return;

    this.lastTrafficPersistAt = now;
    this.lastPersistedAccumulator = this.trafficAccumulator;

    this.trafficPersistPromise = this.storage
      .put("trafficState", {
        accumulator: this.trafficAccumulator,
        lastTrafficReport: this.lastTrafficReport,
      })
      .finally(() => {
        this.trafficPersistPromise = null;
      });
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
        | { accumulator?: number; lastTrafficReport?: number }
        | undefined;

      if (trafficState && typeof trafficState === "object") {
        if (typeof trafficState.accumulator === "number") this.trafficAccumulator = trafficState.accumulator;
        if (typeof trafficState.lastTrafficReport === "number") this.lastTrafficReport = trafficState.lastTrafficReport;
      }

      if (this.trafficAccumulator > 0) {
        await this.scheduleAlarmAt(Date.now() + Relay.TRAFFIC_FLUSH_IDLE_MS);
      }
    });

    // Setup traffic reporting alarm if not exists
    // We will use a simple interval check in webSocketMessage or similar, 
    // but for DO, we can just report periodically if there is activity.
    // Or just report on every N bytes to avoid too many RPC calls.
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

  private syncFromState(force: boolean = false) {
    const now = Date.now();
    if (!force && now - this.lastSyncAt < 2000) return;

    this.providers.clear();
    this.connectors.clear();
    this.providerChannels.clear();
    this.connectorChannels.clear();

    const providerByChannel = new Map<string, WebSocket>();
    const connectorByChannel = new Map<string, WebSocket>();

    for (const ws of this.state.getWebSockets()) {
      const meta = this.safeDeserializeAttachment(ws);
      const isProvider = meta.isProvider === true;
      if (isProvider) {
        this.providers.add(ws);
      } else {
        this.connectors.add(ws);
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
      if (meta.isProvider !== true) count++;
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

  async adminGetRuntimeInfo(): Promise<{ providerCount: number; connectorCount: number; channelCount: number }> {
    this.syncFromState(true);
    // Get actual WebSocket count from DO state (survives hibernation)
    const allWebSockets = this.state.getWebSockets();
    let actualProviderCount = 0;
    let actualConnectorCount = 0;
    
    for (const ws of allWebSockets) {
      const meta = this.safeDeserializeAttachment(ws);
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

  private async ensureCleanupScheduled() {
    const providerDisconnectTime = (await this.storage.get('providerDisconnectTime')) as number | undefined;
    const now = Date.now();
    if (!providerDisconnectTime) {
      await this.storage.put('providerDisconnectTime', now);
      await this.scheduleAlarmAt(now + Relay.PROVIDER_DISCONNECT_GRACE_MS);
      return;
    }

    const elapsed = now - providerDisconnectTime;
    if (elapsed < Relay.PROVIDER_DISCONNECT_GRACE_MS) {
      await this.scheduleAlarmAt(now + (Relay.PROVIDER_DISCONNECT_GRACE_MS - elapsed));
      return;
    }

    // Grace already elapsed, schedule immediate execution.
    await this.scheduleAlarmAt(now);
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

    // Canonicalize stored entries to keys (migrate older versions that stored raw tokens).
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
    const tokens = await this.storage.get('tokens') as string[] || [];
    
    // Get actual WebSocket count from DO state (survives hibernation)
    const allWebSockets = this.state.getWebSockets();
    let actualProviderCount = 0;
    let actualConnectorCount = 0;
    
    for (const ws of allWebSockets) {
      try {
        const meta = ws.deserializeAttachment() as WebsocketMeta;
        if (meta.isProvider) {
          actualProviderCount++;
        } else {
          actualConnectorCount++;
        }
      } catch (e) {
        // Ignore deserialization errors
      }
    }
    
    for (const tokenName of tokens) {
      const tokenKey = await this.toTokenKey(tokenName);
      if (!tokenKey) continue;
      await this.token.updateRelayMetadata(tokenKey, {
        providerCount: actualProviderCount,
        connectorCount: actualConnectorCount
      });
    }
  }

  private flushTraffic(force: boolean = false): Promise<void> {
    if (this.trafficFlushPromise) return this.trafficFlushPromise;

    this.trafficFlushPromise = (async () => {
      try {
        while (true) {
          const now = Date.now();
          const bytesToReport = this.trafficAccumulator;

          if (!force) {
            const overSize = bytesToReport >= 1024 * 1024;
            const overTime = now - this.lastTrafficReport >= Relay.TRAFFIC_FLUSH_IDLE_MS;
            if (!overSize && !(overTime && bytesToReport > 0)) break;
          }

          if (bytesToReport <= 0) {
            this.lastTrafficReport = now;
            break;
          }

          // Snapshot and reset BEFORE awaiting to avoid losing bytes due to interleaving.
          this.trafficAccumulator = 0;
          this.lastTrafficReport = now;

          try {
            await this.token.reportTraffic(bytesToReport);
          } catch (err) {
            // Restore bytes so we can retry later.
            this.trafficAccumulator += bytesToReport;
            throw err;
          }

          if (!force) break;
          if (this.trafficAccumulator <= 0) break;
        }
      } finally {
        // Persist to avoid losing pending bytes on eviction.
        await this.storage.put("trafficState", {
          accumulator: this.trafficAccumulator,
          lastTrafficReport: this.lastTrafficReport,
        });
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

    this.persistTrafficState();
  }

  async fetch(request: Request) {
    return await handleErrors(request, async () => {
      const url = new URL(request.url);

      if (request.headers.get("Upgrade") === "websocket") {
        const pair = new WebSocketPair();
        const [client, server] = [pair[0], pair[1]];

        // Accept the WebSocket
        this.state.acceptWebSocket(server);

        // Check is provider according to request URL
        const isProvider = url.pathname === "/provider";

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

        // Save token and update metadata when connection is established
        if (actualToken) {
          await this.saveToken(actualToken);
          await this.updateMetadata();

          if (isProvider) {
            await this.setOwnerTokenKeyFromActualToken(actualToken);
          }
        }

        // Save session data for hibernation
        server.serializeAttachment({ isProvider, actualToken } satisfies WebsocketMeta);

        const currentProviders = this.getActualProviderCount();
        const currentConnectors = this.getActualConnectorCount();

        const response: AuthResponseMessage = {
          success: true,
          token: actualToken,
          getType: () => MessageType.AuthResponse,
        };
        server.send(packMessage(response));

        const relayColo = request.cf && request.cf.colo ? String(request.cf.colo) : 'unknown';
        const relayColoDescription = describeCloudflareColo(relayColo);
        const clientCountry = request.headers.get('CF-IPCountry') || (request.cf && request.cf.country ? String(request.cf.country) : 'unknown');
        const clientIp = request.headers.get('CF-Connecting-IP') || request.headers.get('X-Forwarded-For')?.split(',')[0].trim() || 'unknown';
        const providerLabel = currentProviders === 1 ? 'provider' : 'providers';
        const connectorLabel = currentConnectors === 1 ? 'connector' : 'connectors';
        const log: LogMessage = {
          level: "info",
          msg: `Welcome to LinkSocks.js relay server. This server is running in datacenter: ${relayColoDescription}. Your connection comes from ${clientCountry} (${clientIp}). After you connected, this relay group has ${currentProviders} ${providerLabel} and ${currentConnectors} ${connectorLabel}.`,
          getType: () => MessageType.Log,
        };
        server.send(packMessage(log));

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

  // Helper method to broadcast partners count to all connectors
  private broadcastPartnersCountToConnectors() {
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
    const encodedMsg = packMessage(partnersMsg);
    for (const connector of this.connectors) {
      try {
        connector.send(encodedMsg);
      } catch (e) {
        // Ignore send errors
      }
    }
  }

  // Helper method to broadcast partners count to all providers
  private broadcastPartnersCountToProviders() {
    // Get actual connector count from DO state (survives hibernation)
    const allWebSockets = this.state.getWebSockets();
    let actualConnectorCount = 0;
    
    for (const ws of allWebSockets) {
      try {
        const meta = ws.deserializeAttachment() as WebsocketMeta;
        if (!meta.isProvider) {
          actualConnectorCount++;
        }
      } catch (e) {
        // Ignore
      }
    }
    
    const partnersMsg: PartnersMessage = {
      count: actualConnectorCount,
      getType: () => MessageType.Partners,
    };
    const encodedMsg = packMessage(partnersMsg);
    for (const provider of this.providers) {
      try {
        provider.send(encodedMsg);
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
      connector.send(packMessage(partnersMsg));
    } catch (e) {
      // Ignore send errors
    }
  }

  private getNextProvider(forceSync: boolean = false): WebSocket | null {
    this.syncFromState(forceSync);
    const providers = Array.from(this.providers);
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
      const messageData =
        typeof message === "string"
          ? new TextEncoder().encode(message)
          : new Uint8Array(message);
      
      // Report traffic (approximate size)
      this.reportTraffic(messageData.byteLength);

      const msg = parseMessage(messageData);

      const meta = this.safeDeserializeAttachment(ws);
      const isProvider = meta.isProvider === true;

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
            ws.send(packMessage(response));
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
              ws.send(packMessage(response));
              break;
            }
            const response: AuthResponseMessage = {
              success: true,
              token: actualToken,
              getType: () => MessageType.AuthResponse,
            };
            ws.send(packMessage(response));
            break;
          }

          const actualTokenKey = await this.toTokenKey(actualToken);
          if (authMsg.token !== actualToken && (!actualTokenKey || authMsg.token !== actualTokenKey)) {
            const response: AuthResponseMessage = {
              success: false,
              error: "Invalid token",
              getType: () => MessageType.AuthResponse,
            };
            ws.send(packMessage(response));
            break;
          }

          // For non-anonymous auth messages, we currently don't perform additional auth here.
          // The connection is already authenticated by the entry worker.
          const response: AuthResponseMessage = {
            success: true,
            token: actualToken,
            getType: () => MessageType.AuthResponse,
          };
          ws.send(packMessage(response));
          break;
        }

        case MessageType.Connect: {
          if (!isProvider) {
            // Only connectors can send Connect messages
            const connectMsg = msg as ConnectMessage;
            
            // Get round-robin provider
            const provider = await this.waitForNextProvider(Relay.DEFAULT_CONNECTOR_WAIT_PROVIDER_MS);
            if (!provider) {
              const response: ConnectResponseMessage = {
                success: false,
                error: 'No available providers',
                channelId: connectMsg.channelId,
                getType: () => MessageType.ConnectResponse,
              };
              ws.send(packMessage(response));
              break;
            }

            // Create a new channel
            const channelId = connectMsg.channelId;
            this.providerChannels.set(channelId, provider);
            this.connectorChannels.set(channelId, ws);

            // Report channel creation
            this.token.reportChannelCreated().catch(console.error);

            // Update metadata for both WebSockets
            const providerMeta = provider.deserializeAttachment() as WebsocketMeta;
            providerMeta.channels = providerMeta.channels || [];
            providerMeta.channels.push(channelId);
            provider.serializeAttachment(providerMeta);

            const connectorMeta = ws.deserializeAttachment() as WebsocketMeta;
            connectorMeta.channels = connectorMeta.channels || [];
            connectorMeta.channels.push(channelId);
            ws.serializeAttachment(connectorMeta);

            // Forward the connect message to provider
            provider.send(message);
          } else {
            ws.close(1008, "Invalid connector token.");
          }
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
              ws.send(packMessage(response));
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
                ws.send(packMessage(response));
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
              ws.send(packMessage(response));
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

            ws.send(packMessage(response));
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
          const channelId = dataMsg.channelId;

          if (isProvider) {
            const connector = this.connectorChannels.get(channelId);
            if (connector) {
              connector.send(message);
            }
          } else {
            const provider = this.providerChannels.get(channelId);
            if (provider) {
              provider.send(message);
            }
          }
          break;
        }

        case MessageType.Disconnect: {
          const disconnectMsg = msg as DisconnectMessage;
          const channelId = disconnectMsg.channelId;

          if (isProvider) {
            const connector = this.connectorChannels.get(channelId);
            if (connector) {
              connector.send(message);
            }
          } else {
            const provider = this.providerChannels.get(channelId);
            if (provider) {
              provider.send(message);
            }
          }
          // Clean up the channel
          this.disconnectChannel(channelId);
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

  async webSocketClose(ws: WebSocket) {
    this.syncFromState(true);

    // Flush pending bytes so short-lived traffic is not undercounted.
    await this.flushTraffic(true).catch(console.error);

    const meta = this.safeDeserializeAttachment(ws);

    // Remove from providers/connectors set
    if (meta.isProvider === true) {
      this.providers.delete(ws);
      // Broadcast updated providers count to connectors
      this.broadcastPartnersCountToConnectors();
      
      // If all providers have disconnected, schedule connector token invalidation after 60s
      // Note: during the close event, the closing socket can still appear in state.getWebSockets().
      // Exclude it to avoid missing the transition to 0 providers and never scheduling the alarm.
      if (this.getActualProviderCountExcluding(ws) === 0) {
        await this.storage.put('providerDisconnectTime', Date.now());
        await this.scheduleAlarmAt(Date.now() + Relay.PROVIDER_DISCONNECT_GRACE_MS);
      }
    } else {
      this.connectors.delete(ws);
      // Broadcast updated connectors count to providers
      this.broadcastPartnersCountToProviders();
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

  async webSocketError(ws: WebSocket, error: Error) {
    console.error("WebSocket error:", error);
    await this.webSocketClose(ws);
  }

  async alarm() {
    this.syncFromState(true);

    await this.flushTraffic(true).catch(console.error);

    const providerCount = this.getActualProviderCount();
    if (providerCount > 0) {
      await this.storage.delete('providerDisconnectTime');
      return;
    }

    const providerDisconnectTime = (await this.storage.get('providerDisconnectTime')) as number | undefined;
    if (!providerDisconnectTime) return;

    // Only proceed if providers have been disconnected for at least 60 seconds.
    const elapsed = Date.now() - providerDisconnectTime;
    const minWaitMs = Relay.PROVIDER_DISCONNECT_GRACE_MS;
    if (elapsed < minWaitMs) {
      // Ensure we run again once the minimum wait has elapsed.
      await this.scheduleAlarmAt(Date.now() + (minWaitMs - elapsed));
      return;
    }

    // Delete all tokens (both provider and connector tokens)
    const tokens = (await this.storage.get('tokens')) as string[] || [];
    for (const tokenName of tokens) {
      const tokenKey = await this.toTokenKey(tokenName);
      if (!tokenKey) continue;

      await this.token.deleteToken(tokenKey);

      // Clean up legacy double-hash entries (older versions hashed keys again).
      if (this.isSha256Hex(tokenKey)) {
        const doubleHashKey = await this.sha256(tokenKey);
        const meta = await this.token.getRelayMetadata(doubleHashKey);
        if (meta?.relayId === this.state.id.toString()) {
          await this.token.deleteToken(doubleHashKey);
        }
      }
    }

    // Clear local storage
    await this.storage.deleteAll();

    // Close all remaining connections
    this.adminDisconnectAll('All providers disconnected, relay terminated');
  }
}
