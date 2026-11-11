/**
 * Entry load balancer for multi-account linksocks backends.
 *
 * Routes:
 *   /api/health/poll  - Refresh health cache when needed (or force), return backend states
 *   /api/health       - Return cached backend states without probing
 *   other paths       - 302 redirect to a healthy backend
 *
 * Selection policy: error fallback (not round-robin).
 *
 *   BACKENDS is an ordered candidate list of peer backends (no primary/standby roles).
 *   Traffic always goes to the first currently healthy entry. Later entries are only
 *   used when earlier ones are unhealthy or have no fresh healthy cache.
 *
 *   Example with [A, B]:
 *     A healthy, B healthy  -> always redirect to A
 *     A unhealthy, B healthy -> redirect to B
 *     both unhealthy        -> 503
 *
 *   While one backend stays healthy, all clients share that same endpoint, so
 *   reconnects tend to stay on one Cloudflare account / Durable Object namespace.
 *   Rare races during a health flip between two clients are accepted.
 *
 * Health cache:
 *   Results live in caches.default. Healthy results expire after HEALTH_TTL_MS;
 *   failed probes may be retried after FAILED_RETRY_MS. Near-expiry backends are
 *   refreshed in the background via waitUntil.
 */

const BACKENDS = [
  "https://cdn.team1.cc.cd/",
  "https://cdn.team2.cc.cd/",
];

// Health status is considered fresh for this long.
const HEALTH_TTL_MS = 10 * 60 * 1000;

// Start a background refresh this far before health expiry.
const REFRESH_AHEAD_MS = 3 * 60 * 1000;

// After a failed health probe, wait this long before probing again.
const FAILED_RETRY_MS = 30 * 1000;

// Abort backend health checks after this timeout.
const HEALTH_CHECK_TIMEOUT_MS = 10 * 1000;

// Physical Cache API retention for health entries.
const HEALTH_PHYSICAL_CACHE_TTL_SECONDS = 24 * 60 * 60;

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const pathname = normalizePath(url.pathname);

    try {
      // -------------------- /api/health/poll --------------------
      if (pathname === "/api/health/poll") {
        const force = isTruthy(url.searchParams.get("force"));
        const states = await refreshHealthStates(env, { force });
        const healthyCount = states.filter((entry) => entry.state?.healthy).length;

        return jsonResponse(
          {
            success: healthyCount > 0,
            status: healthyCount > 0 ? "available" : "unavailable",
            polled: true,
            forced: force,
            mode: "error-fallback",
            checkedAt: new Date().toISOString(),
            healthyBackends: healthyCount,
            totalBackends: states.length,
            backends: states.map(toPublic),
          },
          healthyCount > 0 ? 200 : 503,
        );
      }

      // -------------------- /api/ech --------------------
      // The ECH config for this entry hostname, resolved through Cloudflare's
      // DNS-over-HTTPS resolver. Served here instead of redirecting, because a
      // client that needs it is often on a network that resets the backend
      // hostname by SNI, and the backend would be the redirect target.
      if (pathname === "/api/ech") {
        const host = url.hostname.toLowerCase();
        const ech = await lookupECHConfigList(host);
        return jsonResponse({ host, ech });
      }

      // -------------------- /api/health --------------------
      if (pathname === "/api/health") {
        const states = await getAllHealthStates(env);
        const healthyCount = states.filter((entry) => entry.state?.healthy).length;

        return jsonResponse(
          {
            success: healthyCount > 0,
            status: healthyCount > 0 ? "available" : "unavailable",
            polled: false,
            forced: false,
            mode: "error-fallback",
            checkedAt: new Date().toISOString(),
            healthyBackends: healthyCount,
            totalBackends: states.length,
            backends: states.map(toPublic),
          },
          healthyCount > 0 ? 200 : 503,
        );
      }

      // -------------------- traffic: first healthy backend redirect --------------------
      let states = await getAllHealthStates(env);

      // Cold start: no cache yet — probe synchronously once.
      if (states.every((entry) => !entry.state)) {
        states = await refreshHealthStates(env, { force: false });
      } else {
        const needsRefresh = states.some(({ state }) => shouldRefresh(state));
        if (needsRefresh) {
          ctx.waitUntil(refreshHealthStates(env, { force: false }));
        }
      }

      let selected = selectHealthyBackend(states);

      // Last chance: refresh if no healthy backend is cached.
      if (!selected) {
        states = await refreshHealthStates(env, { force: false });
        selected = selectHealthyBackend(states);
      }

      if (!selected) {
        return jsonResponse(
          {
            success: false,
            error: "No healthy backend available",
            mode: "error-fallback",
            backends: states.map(toPublic),
          },
          503,
        );
      }

      const redirectUrl = buildRedirectUrl(selected.backend.url, url);
      return buildRedirectResponse(redirectUrl, selected);
    } catch (err) {
      return jsonResponse(
        {
          success: false,
          error: err instanceof Error ? err.message : String(err),
        },
        500,
      );
    }
  },
};

// ==================== Error-fallback selection ====================

/**
 * Pick the first healthy backend in BACKENDS order.
 * Backends are peers; list order is only the try order when some are down.
 */
function selectHealthyBackend(states) {
  const now = Date.now();
  for (let index = 0; index < states.length; index++) {
    const entry = states[index];
    const state = entry.state;
    if (!state || state.healthy !== true) {
      continue;
    }
    if (!(Number(state.expiresAt) > now)) {
      continue;
    }
    return {
      backend: entry.backend,
      priority: index,
    };
  }
  return null;
}

function buildRedirectResponse(redirectUrl, selection) {
  return new Response(null, {
    status: 302,
    statusText: "Found",
    headers: {
      Location: redirectUrl.toString(),
      "Cache-Control": "no-store",
      "X-LB-Backend": selection.backend.url,
      "X-LB-Mode": "error-fallback",
      "X-LB-Priority": String(selection.priority),
    },
  });
}

// ==================== Health probing ====================

async function refreshHealthStates(env, { force }) {
  const current = await getAllHealthStates(env);
  const toPoll = current.filter(({ state }) => force || shouldRefresh(state));
  await Promise.all(toPoll.map(({ backend }) => pollBackend(backend, env)));
  return getAllHealthStates(env);
}

async function pollBackend(backend, env) {
  const startedAt = Date.now();
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), HEALTH_CHECK_TIMEOUT_MS);
  const healthUrl = new URL("/api/health/poll?force=1", backend.url).toString();

  try {
    const response = await fetch(healthUrl, {
      method: "GET",
      signal: controller.signal,
      redirect: "manual",
      headers: {
        Accept: "application/json",
        "Cache-Control": "no-cache, no-store",
        Pragma: "no-cache",
      },
      cf: { cacheTtl: 0, cacheEverything: false },
    });

    const latencyMs = Date.now() - startedAt;
    const body = await readJsonSafely(response);

    // Backend health payload shape: { active: true/false, ... }
    const healthy = body?.active === true;

    const state = {
      backend: backend.url,
      healthy,
      httpStatus: response.status,
      checkedAt: startedAt,
      expiresAt: startedAt + (healthy ? HEALTH_TTL_MS : FAILED_RETRY_MS),
      latencyMs,
      reason: healthy ? null : body?.reason || `HTTP ${response.status}`,
      raw: body,
    };

    await writeHealthState(backend, state, env);
    return state;
  } catch (err) {
    const latencyMs = Date.now() - startedAt;
    const state = {
      backend: backend.url,
      healthy: false,
      httpStatus: null,
      checkedAt: startedAt,
      expiresAt: startedAt + FAILED_RETRY_MS,
      latencyMs,
      reason: err instanceof Error ? err.message : String(err),
      raw: null,
    };

    await writeHealthState(backend, state, env);
    return state;
  } finally {
    clearTimeout(timeout);
  }
}

// ==================== Cache helpers (caches.default only) ====================

async function getAllHealthStates(env) {
  return Promise.all(
    BACKENDS.map(async (url, index) => {
      const backend = { id: index, url: normalizeBackendUrl(url) };
      const state = await readHealthState(backend);
      return { backend, state };
    }),
  );
}

async function readHealthState(backend) {
  return readCacheJson(healthCacheKey(backend));
}

async function writeHealthState(backend, state, env) {
  await writeCacheJson(healthCacheKey(backend), state, HEALTH_PHYSICAL_CACHE_TTL_SECONDS);
}

function healthCacheKey(backend) {
  return `health:${backend.id}:${backend.url}`;
}

async function readCacheJson(key) {
  const cache = caches.default;
  const match = await cache.match(cacheUrlForKey(key));
  if (!match) {
    return null;
  }
  try {
    return await match.json();
  } catch {
    return null;
  }
}

async function writeCacheJson(key, value, physicalTtlSeconds = HEALTH_PHYSICAL_CACHE_TTL_SECONDS) {
  const cache = caches.default;
  const response = new Response(JSON.stringify(value), {
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": `public, max-age=${physicalTtlSeconds}`,
    },
  });
  await cache.put(cacheUrlForKey(key), response);
}

function cacheUrlForKey(key) {
  return `https://health-cache.local/${encodeURIComponent(key)}`;
}

// ==================== Helpers ====================

function shouldRefresh(state) {
  if (!state) {
    return true;
  }
  const expiresAt = Number(state.expiresAt);
  if (!Number.isFinite(expiresAt)) {
    return true;
  }
  return expiresAt - Date.now() <= REFRESH_AHEAD_MS;
}

function buildRedirectUrl(backendUrl, incomingUrl) {
  const destination = new URL(backendUrl);
  const basePath = destination.pathname === "/" ? "" : destination.pathname.replace(/\/+$/, "");
  const incomingPath = incomingUrl.pathname.startsWith("/")
    ? incomingUrl.pathname
    : `/${incomingUrl.pathname}`;
  destination.pathname = `${basePath}${incomingPath}`;
  destination.search = incomingUrl.search;
  destination.hash = "";
  return destination;
}

function normalizeBackendUrl(value) {
  const url = new URL(value);
  url.search = "";
  url.hash = "";
  if (!url.pathname.endsWith("/")) {
    url.pathname += "/";
  }
  return url.toString();
}

function normalizePath(pathname) {
  return pathname === "/" ? "/" : pathname.replace(/\/+$/, "");
}

function isTruthy(value) {
  return value && ["1", "true", "yes", "on"].includes(value.toLowerCase());
}

async function readJsonSafely(response) {
  const contentType = (response.headers.get("Content-Type") || "").toLowerCase();
  if (!contentType.includes("json")) {
    return null;
  }
  try {
    return await response.clone().json();
  } catch {
    return null;
  }
}

function toPublic({ backend, state }) {
  if (!state) {
    return {
      backend: backend.url,
      priority: backend.id,
      healthy: false,
      status: "unknown",
      httpStatus: null,
      latencyMs: null,
      checkedAt: null,
      expiresAt: null,
      remainingMs: 0,
      refreshRequired: true,
      reason: "No cached health status",
    };
  }

  const now = Date.now();
  const expiresAt = Number(state.expiresAt);
  const expired = !Number.isFinite(expiresAt) || expiresAt <= now;
  const healthy = state.healthy === true && !expired;

  return {
    backend: backend.url,
    priority: backend.id,
    healthy,
    status: expired ? "expired" : healthy ? "healthy" : "unhealthy",
    httpStatus: state.httpStatus ?? null,
    latencyMs: state.latencyMs ?? null,
    checkedAt: state.checkedAt ?? null,
    checkedAtIso: state.checkedAt ? new Date(state.checkedAt).toISOString() : null,
    expiresAt: state.expiresAt ?? null,
    expiresAtIso: state.expiresAt ? new Date(state.expiresAt).toISOString() : null,
    remainingMs: Math.max(0, expiresAt - now),
    refreshRequired: shouldRefresh(state),
    reason: state.reason ?? null,
  };
}

function jsonResponse(data, status = 200, extraHeaders = {}) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store",
      ...extraHeaders,
    },
  });
}

// lookupECHConfigList resolves the HTTPS record of host through Cloudflare's
// DNS-over-HTTPS resolver and returns the base64 ech parameter, following
// CNAME chains because some names only carry the record on the terminal name.
async function lookupECHConfigList(host) {
  let name = host;
  for (let hop = 0; hop < 8 && name; hop++) {
    let response;
    try {
      response = await fetch(
        `https://cloudflare-dns.com/dns-query?name=${encodeURIComponent(name)}&type=HTTPS`,
        { headers: { accept: "application/dns-json" } },
      );
    } catch {
      return "";
    }
    if (!response.ok) return "";
    let payload;
    try {
      payload = await response.json();
    } catch {
      return "";
    }
    const answers = Array.isArray(payload.Answer) ? payload.Answer : [];
    const https = answers.find((answer) => answer.type === 65 && typeof answer.data === "string");
    if (https && https.data) {
      const match = /ech="?([A-Za-z0-9+/=_-]+)/.exec(https.data);
      return match ? match[1] : "";
    }
    const cname = [...answers].reverse().find((answer) => answer.type === 5 && typeof answer.data === "string");
    name = cname && cname.data ? cname.data.replace(/\.$/, "") : "";
  }
  return "";
}
