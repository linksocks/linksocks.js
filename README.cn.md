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

## 网站反向代理（Web 模式）

除 SOCKS5 中转外，linksocks.js 现在还可以把本地网站直接发布到 Cloudflare 边缘：

```bash
# 在本机运行，将 127.0.0.1:8080 发布为 https://<随机>.example.com
linksocks web -u wss://your-worker.example.com -l http://127.0.0.1:8080
```

服务端不再直接访问你的内网：Cloudflare 收到 HTTP 请求后，通过现有的 WebSocket 隧道
把请求转发给 `linksocks web` 进程，由它访问本地后端并把响应流回边缘。因此不需要公网
IP、不需要开放入站端口，也不需要在 Worker 中配置任何源站地址。

### 服务端准备

1. 创建 D1 数据库并写入 `wrangler.jsonc`：

   ```bash
   wrangler d1 create linksocks
   # 把输出的 database_id 填入 wrangler.jsonc 的 d1_databases[0].database_id
   ```

   首次访问时 Worker 会自动建表；也可以执行 `wrangler d1 migrations apply linksocks`。

2. 设置密钥与变量：

   ```bash
   wrangler secret put ADMIN_PASSWORD    # 管理员密码，首次启动自动创建管理员
   wrangler secret put SESSION_SECRET    # 可选，登录会话签名
   ```

   `wrangler.jsonc` 中的变量：

   | 变量 | 说明 |
   |---|---|
   | `WEB_ZONE` | 生成域名使用的基础域，例如 `example.com` |
   | `BASE_HOST` | Control Plane 所在主机名，例如 `linksocks.example.com`；为空时回退到 `WEB_ZONE` |
   | `ADMIN_USERNAME` | 初始管理员用户名，默认 `admin` |

3. DNS：把 `example.com` 与 `*.example.com` 都指向这个 Worker（自定义域或路由），
   这样自动生成的临时域名才能被访问。

4. 部署：

   ```bash
   wrangler deploy
   ```

### 命令行

```bash
linksocks web -u <server> -l <local-url> [选项]
```

| 参数 | 说明 |
|---|---|
| `-t, --token` | 在 `/dashboard` 创建的 Provider Token；省略时为匿名 |
| `-u, --url` | linksocks.js 服务地址，支持 `ws://`、`wss://`、`http://`、`https://` |
| `-l, --local` | 本地后端地址，可重复指定以在同一 Provider 内做负载均衡 |
| `--domain` | 自定义远程域名（必须是 `WEB_ZONE` 的子域，且服务端允许自定义域名） |
| `--insecure` | 本地后端为 `https://` 时跳过证书校验 |
| `-R, --no-reconnect` | 服务端断开后不再重连 |
| `-d, --debug` | 调试日志（`-dd` 为 trace） |

示例：

```bash
# 匿名发布一个临时域名
linksocks web -u wss://linksocks.example.com -l http://127.0.0.1:3000

# 使用账号令牌发布固定域名，并配置两个源站
linksocks web -u wss://linksocks.example.com -t ls_xxx \
  --domain blog.example.com \
  -l http://127.0.0.1:3000 -l http://127.0.0.1:3001
```

### 用户系统（基于 D1）

- `/admin`：管理员控制台。首次启动时根据 `ADMIN_USERNAME` / `ADMIN_PASSWORD`
  自动创建管理员账号，登录后可以管理用户、令牌、站点与全局匿名策略。
- `/dashboard`：普通用户面板，只能看到自己的站点、令牌、流量与源站状态。
- `/register`：用户自助注册，受全局设置 `allowRegistration` 控制。
- 用户在 `/dashboard` 生成 Token 后，用 `-t <token>` 运行的 Provider 会自动绑定到该账号；
  面板中会显示站点归属、临时域名徽章、到期倒计时、源站数量与在线 Provider 数量。
- 管理员可以在“Settings”中配置匿名访问开关、是否允许自定义域名、是否允许用户注册、
  是否允许用户编辑自己的策略；在用户详情页可以查看/覆盖该用户的策略。

### 配额与临时域名

每个站点在创建时冻结一份策略：

| 设置 | 说明 |
|---|---|
| `autoRotateSeconds` | 自动生成域名的有效期；Provider 会在到期前自动请求新域名 |
| `quotaBytes` | 转发字节上限（请求 + 响应，0 表示不限） |
| `quotaRequests` | 请求数上限（0 表示不限） |
| `quotaDurationSeconds` | 站点最长存活时间（0 表示不限） |
| `maxSites` | 每个账号 / 每个匿名令牌可创建的站点数 |
| `maxBackends` | 单次注册允许的源站数量 |

自动生成的域名永远标记为 `temporary`，界面会显示剩余有效期；自定义域名默认长期有效。
超过配额时边缘会直接返回 `429`，临时域名过期后返回 `410`。

字节与请求数配额是硬上限，由同一身份的所有站点共享（一个账号，或一个匿名 Provider 令牌）：
SOCKS5 中继流量与网站代理流量消耗同一个计数器，额度用尽后会阻止新页面并断开进行中的中继通道。

### 负载均衡

- 同一个 Provider 用多个 `-l` 指定多个本地后端，`linksocks web` 内部轮询。
- 同一个域名用多个 `linksocks web` 进程（相同 Token）注册时，边缘在不同 Provider
  之间轮询；面板中的“源站数量/在线 Provider”会分别统计。

### 新增协议消息

二进制消息类型 0x12–0x19（与 Go 客户端共用）：

| 类型 | 方向 | 说明 |
|---|---|---|
| `web_register` (0x12) | Provider → Relay | 上报域名（可空）、源站列表、TLS 校验与轮换意图 |
| `web_register_response` (0x13) | Relay → Provider | 返回最终域名、是否临时、到期时间、源站数量 |
| `web_request` (0x14) | Relay → Provider | 请求方法、路径、头部、请求体长度等元数据 |
| `web_response` (0x15) | Provider → Relay | 响应状态码与响应头 |
| `web_data` (0x16) | 双向 | 请求体 / 响应体分片，末片带 `end` 标志 |
| `web_cancel` (0x17) | 双向 | 取消某个通道 |
| `web_rotate` (0x18) | Relay → Provider | 通知域名已轮换 |
| `websocket_frame` (0x19) | 双向 | WebSocket 文本/二进制帧载荷 |

### WebSocket 流量

被代理域名上的 WebSocket 握手会通过同一个 Web Provider 隧道转发：源站自己的升级响应
（包含协商出的子协议）、文本帧与二进制帧都会双向透传，正常关闭也会同步到两端。WebSocket
载荷字节同样计入该身份的网络代理限额。

### 已知限制

- 代理不会协商 WebSocket 的 permessage-deflate 扩展。
- 请求体上限 16 MB；响应体为流式转发。
- 每个临时域名有失效时间，过期后必须重新注册才能获得新域名。

