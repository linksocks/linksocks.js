# LinkSocks.js

这是一个基于 Cloudflare Workers 构建的 WebSocket SOCKS 代理服务，用于简单内网穿透，无需任何服务器部署。

如果你已有特定的内网服务器，建议使用 [FRP](https://github.com/fatedier/frp) 或 [Cloudflare Tunnel](https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/)。

本项目主要用于快速连接到不确定用户的内网环境。

## 安装

### 客户端设置

1. 从 https://github.com/zetxtech/linksocks/releases 下载 linksocks 客户端
2. 选择适合你系统的版本（Windows、Linux、macOS）
3. 解压后添加到 PATH 或直接在命令行运行

### 服务端设置（可选）

本仓库包含 Cloudflare Worker 服务端代码。你可以：

1. 使用我们的公共服务器：`https://linksocks.zetx.tech`
2. 部署自己的服务：

   [![部署到 Cloudflare](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/zetxtech/linksocks.js)

## 使用方法

```bash
# 步骤 1：在机器 A 上（位于你想访问的网络内）
linksocks provider -t any_token -u linksocks.zetx.tech -c your_token

# 步骤 2：在机器 B 上（你想从这里访问网络）
linksocks connector -t your_token -u linksocks.zetx.tech -p 1180
```

运行这两个命令后，你可以通过机器 B 上的 SOCKS5 代理（`127.0.0.1:1180`）访问内部网络。

将你的浏览器或应用程序配置为使用此 SOCKS5 代理来访问内部网络资源。

## 核心优势

- **零基础设施**：无需维护服务器，带宽不受限制
- **即插即用**：仅需两条命令建立连接，无需配置文件，无需在线预设置
- **Cloudflare 边缘代理**：全球覆盖，低延迟访问
- **负载均衡**：使用相同令牌的多个 "provider" 将自动分担负载

## 注意事项

本服务使用 Cloudflare Durable Objects，有免费层限制（每天 13,000 GB-s）。
更多关于限制和定价的信息，请参见：https://developers.cloudflare.com/durable-objects/platform/pricing/
