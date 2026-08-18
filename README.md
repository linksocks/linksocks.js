# LinkSocks.js

A SOCKS proxy over WebSocket service built on Cloudflare Workers, designed for simple intranet penetration with no server setup required.

If you own certain intranet server, please use [FRP](https://github.com/fatedier/frp) or [Cloudflare Tunnel](https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/).

This project is mainly used to quickly connect to uncertain user intranet environment.

[中文文档 / Chinese README](README.cn.md)

## Installation

### Client Setup

1. Download the linksocks client from https://github.com/linksocks/linksocks/releases.
2. Select the appropriate version for your OS (Windows, Linux, macOS).
3. Extract and add to your PATH or run directly.

### Server Setup (Optional)

This repository contains the Cloudflare Worker server-side code. You can:

1. Use our public server: `https://l.zetx.tech`
2. Deploy your own:

   [![Deploy to Cloudflare](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/linksocks/linksocks.js)

## Usage

```bash
# Step 1: On machine A (inside the network you want to access)
linksocks provider -u l.zetx.tech -c your_token

# Step 2: On machine B (where you want to access the network)
linksocks connector -t your_token -u l.zetx.tech -p 1180
```

After running both commands, you can access the internal network through the SOCKS5 proxy at `127.0.0.1:1180` on machine B.

Configure your browser or applications to use this SOCKS5 proxy to access internal network resources.

## Key Benefits

- **Zero Infrastructure**: No need to maintain servers, infinite bandwidth.
- **Plug and Play**: Just two commands to establish connection, no config file, no online setup.
- **Proxy at Cloudflare Edge**: Low latency for global application.
- **Load Balancing**: Multiple provider using same token will share the load.

## Note

This service uses Cloudflare Durable Objects which has free tier limitations (13,000 GB-s / day).
For more information on limits and pricing, see: https://developers.cloudflare.com/durable-objects/platform/pricing/

## Health Check API

The worker exposes a health status API for external monitoring. Health results are cached for 1 hour in the Durable Object and refreshed when the cache is close to expiring.

### `GET /api/health`

Returns the current cached health status. If no cache exists or it has expired, a fresh probe is run automatically.

**Status codes:**

- `200` — healthy (all checks passed)
- `503` — unhealthy (one or more checks failed)

**Response fields:**

| Field | Type | Description |
|---|---|---|
| `active` | `boolean` | `true` when all checks pass |
| `checkedAt` | `number` | Timestamp (ms) when the check ran |
| `cacheExpiresAt` | `number` | Timestamp (ms) when the cache expires |
| `refreshAfter` | `number` | Timestamp (ms) after which a poll request will trigger a refresh |
| `origin` | `string` | Worker origin used for the self-request probe |
| `checks` | `object` | Individual check results (see below) |
| `usage` | `object\|null` | Current-day Durable Object usage from GraphQL, or `null` if unavailable |
| `reason` | `string\|null` | Semicolon-separated failure reasons, or `null` if healthy |
| `cached` | `boolean` | Whether this response was served from cache |
| `refreshed` | `boolean` | Whether a fresh probe was run for this response |
| `cacheAgeMs` | `number` | Milliseconds since the cached check ran (0 if freshly refreshed) |

**`checks` object:**

| Field | Type | Description |
|---|---|---|
| `durableObjectAccess` | `boolean` | Successfully called a method on the Token Durable Object |
| `storageRoundTrip` | `boolean` | Wrote a value to DO storage and read it back |
| `workerRequest` | `boolean` | Worker fetched its own `/api/health/ping` endpoint |
| `storedDataWithinLimit` | `boolean` | `storedBytes` is below 5 GB |

**Example:**

```bash
curl https://your-worker.example.com/api/health
```

### `GET /api/health/poll`

Polling endpoint designed for external uptime monitors. Behaves the same as `/api/health`, but additionally checks whether the cache is close to expiring (within 10 minutes). If so, a fresh probe is run before returning.

Typical usage: configure your monitoring tool to poll every 5 minutes. The cache will auto-refresh once per hour, and near-expiry polls will keep it fresh.

```bash
curl https://your-worker.example.com/api/health/poll
```

#### Force Refresh

Append `?force=1` to skip the cache entirely and run a fresh probe immediately:

```bash
curl https://your-worker.example.com/api/health/poll?force=1
```
