# LinkSocks.js

A SOCKS proxy over WebSocket service built on Cloudflare Workers, designed for simple intranet penetration with no server setup required.

If you own certain intranet server, please use [FRP](https://github.com/fatedier/frp) or [Cloudflare Tunnel](https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/).

This project is mainly used to quickly connect to uncertain user intranet environment.

[中文文档 / Chinese README](README.cn.md)

## Installation

### Client Setup

1. Download the linksocks client from https://github.com/zetxtech/linksocks/releases.
2. Select the appropriate version for your OS (Windows, Linux, macOS).
3. Extract and add to your PATH or run directly.

### Server Setup (Optional)

This repository contains the Cloudflare Worker server-side code. You can:

1. Use our public server: `https://linksocks.zetx.tech`
2. Deploy your own:

   [![Deploy to Cloudflare](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/zetxtech/linksocks.js)

## Usage

```bash
# Step 1: On machine A (inside the network you want to access)
linksocks provider -u linksocks.zetx.tech -c your_token

# Step 2: On machine B (where you want to access the network)
linksocks connector -t your_token -u linksocks.zetx.tech -p 1180
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

