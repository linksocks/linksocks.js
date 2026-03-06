import type { Relay } from "./relay";
import type { Token } from "./token";

export interface Env {
  RELAY: DurableObjectNamespace<Relay>;
  TOKEN: DurableObjectNamespace<Token>;
  API_KEY: string;
  ADMIN_PASSWORD?: string;
}

export interface WebsocketMeta {
  isProvider?: boolean;
  channels?: string[];
  actualToken?: string;
}
