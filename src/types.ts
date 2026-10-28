import type { Relay } from "./relay";
import type { Token } from "./token";

/**
 * Environment bindings.
 *
 * The generated `worker-configuration.d.ts` (run `wrangler types`) provides
 * the authoritative `Cloudflare.Env` shape for bindings declared in
 * `wrangler.jsonc` (RELAY, TOKEN, DB, WEB_ZONE, BASE_HOST, ADMIN_USERNAME).
 * Secrets and optional settings are layered on top here.
 *
 * `DB` stays opt-in at runtime: legacy SOCKS/relay deployments keep working
 * without a D1 database, while the web proxy features return 503.
 */
export interface Env extends Cloudflare.Env {
  API_KEY?: string;
  ADMIN_PASSWORD?: string;
  SESSION_SECRET?: string;
  CF_API_TOKEN?: string;
  CF_ACCOUNT_ID?: string;
  RELAY: DurableObjectNamespace<Relay>;
  TOKEN: DurableObjectNamespace<Token>;
}

export interface DurableObjectUsage {
  requests: number;
  durationGbS: number;
  storedBytes: number;
  rowsRead: number;
  rowsWritten: number;
}

export interface HealthProbeChecks {
  durableObjectAccess: boolean;
  storageRoundTrip: boolean;
  workerRequest: boolean;
  storedDataWithinLimit: boolean;
}

export interface HealthStatusCache {
  active: boolean;
  checkedAt: number;
  cacheExpiresAt: number;
  refreshAfter: number;
  origin: string;
  checks: HealthProbeChecks;
  usage: DurableObjectUsage | null;
  reason: string | null;
}

export interface HealthStatusResponse extends HealthStatusCache {
  cacheAgeMs: number;
  cached: boolean;
  refreshed: boolean;
}

export interface WebsocketMeta {
  isProvider?: boolean;
  channels?: string[];
  actualToken?: string;
  clientIp?: string;
  peerProtocolVersion?: number;
  web?: boolean;
  webHosts?: string[];
  webBackendCount?: number;
  webOwnerId?: string | null;
  webAnonymous?: boolean;
  baseHost?: string;
}
