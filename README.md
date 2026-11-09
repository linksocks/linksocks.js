# LinkSocks.js

A SOCKS proxy over WebSocket service built on Cloudflare Workers, designed for simple intranet penetration with no server setup required.

Proxy traffic is end-to-end encrypted: the provider and connector negotiate their own session per channel, and the relay only forwards ciphertext.

If you own certain intranet server, please use [FRP](https://github.com/fatedier/frp) or [Cloudflare Tunnel](https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/).

This project is mainly used to quickly connect to uncertain user intranet environment.

[中文文档 / Chinese README](README.cn.md)

**Website**: [l.zetx.tech](https://l.zetx.tech) ·
[Public zones](https://l.zetx.tech/zones)

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

## Zone Networks

A zone connects machines into a virtual IPv4 network: every member gets an
address, members reach each other's TCP and UDP services, and traffic between
them is end-to-end encrypted. Create a zone on `/zones`, then join it from each
machine. No account is needed to join:

```bash
# Machine A
linksocks zone join <zone-id> -u wss://l.zetx.tech

# Machine B
linksocks zone join <zone-id> -u wss://l.zetx.tech
```

Each machine prints the address it received and starts a local SOCKS5 proxy on
`127.0.0.1:1080`; members are then reachable by address or by name:

```bash
curl --socks5-hostname 127.0.0.1:1080 http://machine-b:8080
```

On Linux, `--vlan` (root) joins with an L2 TAP interface instead, adding ARP,
ICMP, broadcast, multicast and Wake-on-LAN. `linksocks zone leave` releases the
address at once and `linksocks zone list` shows public zones.

New zones are private: only machines given the zone ID can join; administrators
can create public ones. Owners and administrators manage members, firewall
rules, the welcome message and kicking from the same `/zones` page. The
[zone guide](https://linksocks.github.io/guide/zones) covers identity files,
platform requirements and a verified Linux run. Zones have no Python API.

### Enable Zones on Your Deployment

Zones need the `ZONE` Durable Object binding, which the checked-in
`wrangler.jsonc` already declares, plus the D1 migrations `0005_zones.sql` and
`0006_zone_visibility.sql`:

```bash
npx wrangler d1 migrations apply linksocks --remote
npx wrangler deploy
```

Zones do not need wildcard DNS or `WEB_ZONE`; those are for publishing
websites.

## Key Benefits

- **End-to-end encrypted**: proxy channels negotiate a Noise XX session between provider and connector, so destinations and payloads stay inside the ciphertext — the relay only routes frames by channel ID. Zone packets are encrypted between members. Site publishing is the exception, because the relay reads HTTP to serve the hostname.
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
curl https://l.zetx.tech/api/health
```

### `GET /api/health/poll`

Polling endpoint designed for external uptime monitors. Behaves the same as `/api/health`, but additionally checks whether the cache is close to expiring (within 10 minutes). If so, a fresh probe is run before returning.

Typical usage: configure your monitoring tool to poll every 5 minutes. The cache will auto-refresh once per hour, and near-expiry polls will keep it fresh.

```bash
curl https://l.zetx.tech/api/health/poll
```

#### Force Refresh

Append `?force=1` to skip the cache entirely and run a fresh probe immediately:

```bash
curl https://l.zetx.tech/api/health/poll?force=1
```

## Web Reverse Proxy (Web Mode)

Besides SOCKS5 relaying, linksocks.js can publish a local website directly from
the Cloudflare edge:

```bash
# Publishes 127.0.0.1:8080 under a generated host on the public relay
linksocks web -u wss://l.zetx.tech -l http://127.0.0.1:8080
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
   | `BASE_HOST` | Hostname of the control plane, e.g. `cdn.example.com` (falls back to `WEB_ZONE`) |
   | `ADMIN_USERNAME` | Initial administrator username, defaults to `admin` |

3. DNS: point both `example.com` and `*.example.com` at this Worker, and add
   the Worker route `*.example.com/*` for `WEB_ZONE`. The wildcard DNS record
   alone is not enough: without the route Cloudflare answers the generated
   host with `DNS points to prohibited IP` (403). The route must belong to the
   same zone that hosts `WEB_ZONE`.

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
| `-p, --domain-prefix` | Prefix for a host of your own under `WEB_ZONE`, e.g. `blog` for `blog.<WEB_ZONE>` |
| `--insecure` | Skip TLS verification for `https://` backends |
| `-R, --no-reconnect` | Stop when the server disconnects |
| `-d, --debug` | Debug logs (`-dd` for trace) |

Examples:

```bash
# Anonymous temporary host
linksocks web -u wss://l.zetx.tech -l http://127.0.0.1:3000

# Account-bound permanent host with two origins
linksocks web -u wss://l.zetx.tech -t ls_xxx \
  --domain-prefix blog \
  -l http://127.0.0.1:3000 -l http://127.0.0.1:3001
```

### User System (D1)

- `/admin` — administrator console. The admin account is auto-created from
  `ADMIN_USERNAME` / `ADMIN_PASSWORD` on first boot. Manage users, tokens, sites,
  hostname reservations and the global anonymous policy.
- `/dashboard` — user console limited to the signed-in user's sites, tokens,
  traffic and origins.
- `/register` — self-registration, gated by the global `allowRegistration`
  setting.
- Tokens created in `/dashboard` bind any provider that connects with
  `-t <token>` to that account. The UI shows ownership, temporary badges,
  expiry countdowns, origin counts and live provider counts.
- Administrators can edit anonymous site publishing, custom hostname
  allowance, registration, and whether users may edit their own policy;
  per-user overrides live on the user detail page.
- `allowCustomHost` on a user is the **permanent hostname grant**: it lets the
  account publish under a domain prefix of its own (`--domain-prefix`). It is
  denied by default, so operators grant it account by account.

### Hostname Holds and Reservations

When the last provider of a web relay disconnects, its published hostnames stay
bound to that relay for **24 hours**. A provider restart or a short outage
therefore keeps the same URL, and other accounts cannot take it over during
the hold. With `forceTemporaryHost` enabled the hold is intentionally short
(60 seconds) and the provider receives a log message with the new URL when the
host rotates. Plain SOCKS relays keep the historical 60 second grace.

Administrators can additionally **reserve** a hostname for an account in
`/admin?tab=reservations`. A reservation does not create a site and is invisible
to users: the hostname simply remains unavailable to everyone else after the
relay is released, until its owner publishes it again. Reservations survive
relay cleanup and are deleted only when the reservation or the account is
removed.

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

Auto-generated hosts are always `temporary` with a countdown in the UI. A
prefix of your own (`--domain-prefix`) needs the account's permanent-host grant;
see the [web publishing guide](https://linksocks.github.io/guide/quick-start)
for how the grant, rotation and forced temporary mode interact at the provider.
Quota overruns return `429` at the edge and expired temporary hosts return
`410`.

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
| `web_register` (0x12) | Provider → Relay | Domain prefix (a bare label, empty for a generated host), origins, TLS verification and rotation intent |
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
