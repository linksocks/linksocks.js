import type { Relay } from "./relay";
import type { Token } from "./token";

export interface Env {
  RELAY: DurableObjectNamespace<Relay>;
  TOKEN: DurableObjectNamespace<Token>;
  API_KEY: string;
  ADMIN_PASSWORD?: string;
  CF_API_TOKEN?: string;
  CF_ACCOUNT_ID?: string;
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
}
