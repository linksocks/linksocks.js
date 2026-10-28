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

## Web Reverse Proxy (Web Mode)

Besides SOCKS5 relaying, linksocks.js can publish a local website directly from
the Cloudflare edge:

```bash
# Run locally: publishes 127.0.0.1:8080 as https://<random>.example.com
linksocks web -u wss://your-worker.example.com -l http://127.0.0.1:8080
```

Cloudflare never dials your intranet. Incoming HTTP requests are forwarded over
the existing WebSocket tunnel to the `linksocks web` process, which calls the
local backend and streams the response back to the edge. No public IP, no inbound
port, and no origin address stored in the Worker.

### Server Setup

1. Create a D1 database and wire it into `wrangler.jsonc`:

   ```bash
   wrangler d1 create linksocks
   # put the printed database_id into d1_databases[0].database_id
   ```

   Tables are created lazily on first use. You can also run
   `wrangler d1 migrations apply linksocks`.

2. Configure secrets and variables:

   ```bash
   wrangler secret put ADMIN_PASSWORD    # auto-creates the admin account
   wrangler secret put SESSION_SECRET    # optional session signing secret
   ```

   Variables in `wrangler.jsonc`:

   | Variable | Description |
   |---|---|
   | `WEB_ZONE` | Base domain for generated hosts, e.g. `example.com` |
   | `BASE_HOST` | Hostname of the control plane, e.g. `linksocks.example.com` (falls back to `WEB_ZONE`) |
   | `ADMIN_USERNAME` | Initial administrator username, defaults to `admin` |

3. DNS: point both `example.com` and `*.example.com` at this Worker (custom
   domain or routes) so generated temporary hosts resolve.

4. Deploy:

   ```bash
   wrangler deploy
   ```

### CLI

```bash
linksocks web -u <server> -l <local-url> [options]
```

| Flag | Description |
|---|---|
| `-t, --token` | Provider token from `/dashboard`; anonymous when omitted |
| `-u, --url` | LinkSocks.js server (`ws://`, `wss://`, `http://`, `https://`) |
| `-l, --local` | Local backend URL, repeatable for in-provider load balancing |
| `--domain` | Custom remote hostname (must be a `WEB_ZONE` subdomain and allowed) |
| `--insecure` | Skip TLS verification for `https://` backends |
| `-R, --no-reconnect` | Stop when the server disconnects |
| `-d, --debug` | Debug logs (`-dd` for trace) |

Examples:

```bash
# Anonymous temporary host
linksocks web -u wss://linksocks.example.com -l http://127.0.0.1:3000

# Account-bound permanent host with two origins
linksocks web -u wss://linksocks.example.com -t ls_xxx \
  --domain blog.example.com \
  -l http://127.0.0.1:3000 -l http://127.0.0.1:3001
```

### User System (D1)

- `/admin` — administrator console. The admin account is auto-created from
  `ADMIN_USERNAME` / `ADMIN_PASSWORD` on first boot. Manage users, tokens, sites
  and the global anonymous policy.
- `/dashboard` — user console limited to the signed-in user's sites, tokens,
  traffic and origins.
- `/register` — self-registration, gated by the global `allowRegistration`
  setting.
- Tokens created in `/dashboard` bind any provider that connects with
  `-t <token>` to that account. The UI shows ownership, temporary badges,
  expiry countdowns, origin counts and live provider counts.
- Administrators can edit anonymous access, custom hostname allowance,
  registration, and whether users may edit their own policy; per-user overrides
  live on the user detail page.

### Quotas and Temporary Hosts

Each site freezes a policy at creation time:

| Setting | Description |
|---|---|
| `autoRotateSeconds` | Lifetime of generated hosts; providers re-register before expiry |
| `quotaBytes` | Transfer quota (request + response, 0 = unlimited) |
| `quotaRequests` | Request quota (0 = unlimited) |
| `quotaDurationSeconds` | Maximum site lifetime (0 = unlimited) |
| `maxSites` | Sites per account / per anonymous token |
| `maxBackends` | Origins allowed per registration |

Auto-generated hosts are always `temporary` with a countdown in the UI; custom
hosts are permanent by default. Quota overruns return `429` at the edge and
expired temporary hosts return `410`.

The byte and request quotas are hard limits shared by every site of the same
principal (one account, or one anonymous provider token). SOCKS5 relay traffic
and web proxy traffic draw from the same counter, so a transfer that already
exhausted the budget blocks new pages and closes in-flight relay channels.

### Load Balancing

- One provider with several `-l` backends round-robins locally.
- Several providers registering the same hostname round-robin at the edge; the
  dashboard shows origin count and online provider count separately.

### Protocol Additions

Binary message types 0x12–0x19 (shared with the Go client):

| Type | Direction | Purpose |
|---|---|---|
| `web_register` (0x12) | Provider → Relay | Host, origins, TLS verification and rotation intent |
| `web_register_response` (0x13) | Relay → Provider | Final host, temporary flag, expiry, origin count |
| `web_request` (0x14) | Relay → Provider | Method, path, headers, body metadata |
| `web_response` (0x15) | Provider → Relay | Status code and response headers |
| `web_data` (0x16) | Both | Request/response body chunks; last chunk has `end` |
| `web_cancel` (0x17) | Both | Cancel a channel |
| `web_rotate` (0x18) | Relay → Provider | New hostname after rotation |
| `websocket_frame` (0x19) | Both | WebSocket text/binary frame payload |

### WebSocket Traffic

WebSocket upgrades on a proxied hostname are tunneled through the same Web
provider: the origin's own upgrade response (including the negotiated
subprotocol), text frames and binary frames are forwarded in both directions,
and a clean close propagates to both sides. WebSocket payload bytes count
toward the same transfer quota as HTTP traffic.

### Known Limitations

- WebSocket permessage-deflate extensions are not negotiated through the proxy.
- Request bodies are capped at 16 MB; responses are streamed.
- Temporary hosts must be re-registered after expiry.

