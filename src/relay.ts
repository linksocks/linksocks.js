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
  type PartnersMessage,
  parseMessage,
  packMessage,
  MessageType,
  AuthResponseMessage,
  LogMessage,
} from "./message";
import { handleErrors } from "./common";
import { type Token } from "./token";

export class Relay extends DurableObject {
  private providerChannels: Map<string, WebSocket>;
  private connectorChannels: Map<string, WebSocket>;
  private providers: Set<WebSocket>;
  private connectors: Set<WebSocket>;
  private currentProviderIndex: number;

  private state: DurableObjectState;
  protected declare env: Env;
  private storage: DurableObjectStorage;
  private token: DurableObjectStub<Token>;

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

    // Restore existing WebSockets and their attachments
    this.state.getWebSockets().forEach((ws) => {
      const meta = ws.deserializeAttachment() as WebsocketMeta;
      if (meta.isProvider) {
        this.providers.add(ws);

        // Restore provider channels
        if (meta.channels) {
          for (const channelId of meta.channels) {
            this.providerChannels.set(channelId, ws);
          }
        }
      } else {
        this.connectors.add(ws);
        // Restore connector channels
        if (meta.channels) {
          for (const channelId of meta.channels) {
            this.connectorChannels.set(channelId, ws);
          }
        }
      }
    });
  }

  
  private async sha256(tokenName: string): Promise<string> {
    const msgBuffer = new TextEncoder().encode(tokenName);
    const hashBuffer = await crypto.subtle.digest('SHA-256', msgBuffer);
    return Array.from(new Uint8Array(hashBuffer))
      .map(b => b.toString(16).padStart(2, '0'))
      .join('');
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

  private async saveToken(token: string) {
    const tokens = await this.storage.get('tokens') as string[] || [];
    if (!tokens.includes(token)) {
      tokens.push(token);
      await this.storage.put('tokens', tokens);
    }
  }

  private async updateMetadata() {
    const tokens = await this.storage.get('tokens') as string[] || [];
    for (const tokenName of tokens) {
      const tokenHash = await this.sha256(tokenName);
      await this.token.updateRelayMetadata(tokenHash, {
        providerCount: this.providers.size,
        connectorCount: this.connectors.size
      });
    }
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
        } else {
          this.connectors.add(server);
        }

        // Save session data for hibernation
        server.serializeAttachment({ isProvider });

        // Return the actual token for reconnection (especially for anonymous providers)
        const actualToken = url.searchParams.get("actualToken") || "";
        const response: AuthResponseMessage = {
          success: true,
          token: actualToken,
          getType: () => MessageType.AuthResponse,
        };
        server.send(packMessage(response));

        // Fetch IP address from ipv4.ip.sb
        fetch('https://ipinfo.io/ip')
          .then(res => {
            if (!res.ok) {
              throw new Error(`Failed to retrieve IP: status ${res.status}`);
            }
            return res.text();
          })
          .then(ip => {
            const colo = request.cf && request.cf.colo ? String(request.cf.colo) : 'unknown';
            const country = request.cf && request.cf.country ? String(request.cf.country) : 'unknown';
            const log: LogMessage = {
              level: "info",
              msg: `Welcome to LinkSocks.js server (colo = ${colo}, country = ${country}, ip = ${ip.trim()})`,
              getType: () => MessageType.Log,
            };
            server.send(packMessage(log));
          })
          .catch(err => {
            const colo = request.cf && request.cf.colo ? String(request.cf.colo) : 'unknown';
            const country = request.cf && request.cf.country ? String(request.cf.country) : 'unknown';
            const log: LogMessage = {
              level: "info",
              msg: `Welcome to LinkSocks.js server (colo = ${colo}, country = ${country}, ip = failed to retrieve: ${err})`,
              getType: () => MessageType.Log,
            };
            server.send(packMessage(log));
          });

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
    const partnersMsg: PartnersMessage = {
      count: this.providers.size,
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
    const partnersMsg: PartnersMessage = {
      count: this.connectors.size,
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
    const partnersMsg: PartnersMessage = {
      count: this.providers.size,
      getType: () => MessageType.Partners,
    };
    try {
      connector.send(packMessage(partnersMsg));
    } catch (e) {
      // Ignore send errors
    }
  }

  private getNextProvider(): WebSocket | null {
    const providers = Array.from(this.providers);
    if (providers.length === 0) return null;

    this.currentProviderIndex =
      (this.currentProviderIndex + 1) % providers.length;
    return providers[this.currentProviderIndex];
  }

  // WebSocket event handlers
  async webSocketMessage(ws: WebSocket, message: ArrayBuffer | string) {
    try {
      const messageData =
        typeof message === "string"
          ? new TextEncoder().encode(message)
          : new Uint8Array(message);
      const msg = parseMessage(messageData);

      const isProvider = this.providers.has(ws);

      switch (msg.getType()) {
        case MessageType.Connect: {
          if (!isProvider) {
            // Only connectors can send Connect messages
            const connectMsg = msg as ConnectMessage;
            
            // Get round-robin provider
            const provider = this.getNextProvider();
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

            const tokenHash = await this.sha256(connectorMsg.connectorToken)
            
            // Get the relay token that created this relay
            const tokens = await this.storage.get('tokens') as string[] || [];
            if (tokens.length > 0) {
              const relayTokenHash = await this.sha256(tokens[0]);
              await this.token.addConnectorToken(relayTokenHash, connectorMsg.connectorToken);
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
    const meta = ws.deserializeAttachment() as WebsocketMeta;

    // Remove from providers/connectors set
    if (meta.isProvider) {
      this.providers.delete(ws);
      // Broadcast updated providers count to connectors
      this.broadcastPartnersCountToConnectors();
      
      // If all providers have disconnected, schedule connector token invalidation after 60s
      if (this.providers.size === 0) {
        await this.storage.setAlarm(Date.now() + 60 * 1000);
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
      const tokenHash = await this.sha256(tokenName);
      const metadata = await this.token.getRelayMetadata(tokenHash);
      
      if (metadata && metadata.connectorTokens) {
        for (const connectorToken of metadata.connectorTokens) {
          const connectorTokenHash = await this.sha256(connectorToken);
          await this.token.deleteToken(connectorTokenHash);
        }
        await this.token.updateRelayMetadata(tokenHash, { connectorTokens: [] });
      }
    }

    for (const connector of this.connectors) {
      try {
        connector.close(1001, 'All providers disconnected, connector token invalidated.');
      } catch (e) {
        // Ignore close errors
      }
    }
  }

  async webSocketError(ws: WebSocket, error: Error) {
    console.error("WebSocket error:", error);
    this.webSocketClose(ws);
  }

  async alarm() {
    // Check if we still have no providers after 60s delay
    if (this.providers.size === 0) {
      await this.invalidateConnectorTokens();
    }
  }
}
