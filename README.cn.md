# LinkSocks.js

这是一个基于 Cloudflare Workers 构建的 WebSocket SOCKS 代理服务，用于简单内网穿透，无需任何服务器部署。

如果你已有特定的内网服务器，建议使用 [FRP](https://github.com/fatedier/frp) 或 [Cloudflare Tunnel](https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/)。

本项目主要用于快速连接到不确定用户的内网环境。

## 安装

### 客户端设置

1. 从 https://github.com/linksocks/linksocks/releases 下载 linksocks 客户端
2. 选择适合你系统的版本（Windows、Linux、macOS）
3. 解压后添加到 PATH 或直接在命令行运行

### 服务端设置（可选）

本仓库包含 Cloudflare Worker 服务端代码。你可以：

1. 使用我们的公共服务器：`https://l.zetx.tech`
2. 部署自己的服务：

   [![部署到 Cloudflare](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/zetxtech/linksocks.js)

## 使用方法

```bash
# 步骤 1：在机器 A 上（位于你想访问的网络内）
linksocks provider -t any_token -u l.zetx.tech -c your_token

# 步骤 2：在机器 B 上（你想从这里访问网络）
linksocks connector -t your_token -u l.zetx.tech -p 1180
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

## 健康检查 API

Worker 提供健康状态接口，用于外部监控。健康检查结果缓存在 Durable Object 中，有效期 1 小时，接近过期时自动刷新。

### `GET /api/health`

返回当前缓存的健康状态。如果缓存不存在或已过期，会自动运行一次探测。

**状态码：**

- `200` — 健康（所有检查项通过）
- `503` — 异常（一项或多项检查失败）

**返回字段：**

| 字段 | 类型 | 说明 |
|---|---|---|
| `active` | `boolean` | 所有检查通过时为 `true` |
| `checkedAt` | `number` | 检查执行的时间戳（毫秒） |
| `cacheExpiresAt` | `number` | 缓存过期时间戳（毫秒） |
| `refreshAfter` | `number` | 此时间戳之后的轮询请求会触发刷新 |
| `origin` | `string` | 探测使用的 Worker 域名 |
| `checks` | `object` | 各项检查结果（见下表） |
| `usage` | `object\|null` | 当日 Durable Object 用量（GraphQL），不可用时为 `null` |
| `reason` | `string\|null` | 失败原因（分号分隔），健康时为 `null` |
| `cached` | `boolean` | 是否从缓存返回 |
| `refreshed` | `boolean` | 本次是否运行了新的探测 |
| `cacheAgeMs` | `number` | 缓存已存在的时间（毫秒），新刷新时为 0 |

**`checks` 对象：**

| 字段 | 类型 | 说明 |
|---|---|---|
| `durableObjectAccess` | `boolean` | 成功调用了 Token Durable Object 的方法 |
| `storageRoundTrip` | `boolean` | 向 DO 存储写入一个值并读回，验证一致 |
| `workerRequest` | `boolean` | Worker 请求了自身的 `/api/health/ping` 端点 |
| `storedDataWithinLimit` | `boolean` | `storedBytes` 低于 5 GB |

**示例：**

```bash
curl https://your-worker.example.com/api/health
```

### `GET /api/health/poll`

为外部监控工具设计的轮询端点。行为与 `/api/health` 相同，但额外检查缓存是否接近过期（10 分钟内）。如果是，会先运行一次探测再返回。

典型用法：配置监控工具每 5 分钟轮询一次。缓存每小时自动刷新一次，接近过期时的轮询会保持缓存新鲜。

```bash
curl https://your-worker.example.com/api/health/poll
```

#### 强制刷新

在 URL 后附加 `?force=1` 可跳过缓存，立即运行一次全新探测：

```bash
curl https://your-worker.example.com/api/health/poll?force=1
```
