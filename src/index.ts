import type { Env } from "./types";
import zxcvbn from "zxcvbn";

import { handleErrors } from "./common";
import { AuthResponseMessage, packMessage, MessageType } from "./message";
export { Relay } from "./relay";
export { Token } from "./token";

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext) {
    return await handleErrors(request, async () => {
      const url = new URL(request.url);
      const path = url.pathname.split("/");
      
      if (path[1] === "") {
        const tokenDO = env.TOKEN.get(env.TOKEN.idFromName("main"));
        const stats = await tokenDO.getStats();

        const html = `
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>LinkSocks Public Relay Server</title>
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet" />
  <style>
    :root {
      --bg0: #050816;
      --bg1: #0b1226;
      --card: rgba(255,255,255,0.06);
      --card2: rgba(255,255,255,0.08);
      --stroke: rgba(255,255,255,0.12);
      --text: rgba(255,255,255,0.92);
      --muted: rgba(255,255,255,0.68);
      --muted2: rgba(255,255,255,0.52);
      --brand: #7c3aed;
      --brand2: #22d3ee;
      --danger: #fb7185;
      --ok: #22c55e;
      --shadow: 0 25px 60px rgba(0,0,0,0.45);
      --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
    }

    * { box-sizing: border-box; }
    html, body { height: 100%; }
    body {
      margin: 0;
      font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, "Apple Color Emoji", "Segoe UI Emoji";
      color: var(--text);
      background:
        radial-gradient(800px 400px at 15% 10%, rgba(124,58,237,0.35), transparent 60%),
        radial-gradient(800px 400px at 85% 15%, rgba(34,211,238,0.28), transparent 60%),
        radial-gradient(1200px 700px at 50% 90%, rgba(99,102,241,0.22), transparent 60%),
        linear-gradient(180deg, var(--bg0), var(--bg1));
      display: grid;
      place-items: center;
      padding: 32px 18px;
    }

    .shell {
      width: min(1080px, 94%);
    }

    .hero {
      border: 1px solid rgba(255,255,255,0.06);
      background: linear-gradient(180deg, rgba(255,255,255,0.06), rgba(255,255,255,0.02));
      backdrop-filter: blur(14px);
      border-radius: 24px;
      box-shadow: 0 0 0 1px rgba(255,255,255,0.03), 0 20px 40px -10px rgba(0,0,0,0.4);
      overflow: hidden;
    }

    .heroTop {
      padding: 32px 32px 24px 32px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
    }

    .brand {
      display: flex;
      align-items: center;
      gap: 16px;
      min-width: 0;
      flex: 1;
      padding-right: 16px;
    }

    .logo {
      width: 52px;
      height: 52px;
      border-radius: 16px;
      background: linear-gradient(135deg, var(--brand), var(--brand2));
      box-shadow: 0 14px 30px rgba(124,58,237,0.28);
      position: relative;
      flex-shrink: 0;
    }

    .logo::after {
      content: "";
      position: absolute;
      inset: 2px;
      border-radius: 14px;
      background: radial-gradient(circle at 30% 30%, rgba(255,255,255,0.45), transparent 55%);
      opacity: 0.9;
      mix-blend-mode: overlay;
    }

    .titleWrap { 
      min-width: 0; 
      flex: 1;
      display: flex;
      flex-direction: column;
      justify-content: center;
    }
    .title {
      font-size: 22px;
      font-weight: 800;
      letter-spacing: -0.03em;
      line-height: 1.2;
      margin: 0;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      text-shadow: 0 2px 10px rgba(0,0,0,0.2);
    }
    .subtitle {
      margin: 4px 0 0 0;
      color: var(--muted);
      font-size: 13.5px;
      font-weight: 500;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      opacity: 0.8;
    }

    .pill {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 7px 14px;
      border-radius: 999px;
      border: 1px solid inset rgba(255,255,255,0.08);
      background: rgba(255,255,255,0.04);
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.05);
      color: var(--muted);
      font-size: 12.5px;
      font-weight: 600;
      white-space: nowrap;
      flex-shrink: 0;
    }
    .dot {
      width: 7px;
      height: 7px;
      border-radius: 50%;
      background: #22c55e;
      box-shadow: 0 0 0 4px rgba(34,197,94,0.16);
    }

    .heroBody {
      padding: 0 32px 32px 32px;
    }

    .stats {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 16px;
      margin-top: 10px;
    }

    .stat {
      border: 1px solid rgba(255,255,255,0.08);
      background: rgba(255,255,255,0.02);
      border-radius: 18px;
      padding: 20px;
      position: relative;
      overflow: hidden;
      display: flex;
      flex-direction: column;
      justify-content: center;
    }
    .stat::before {
      content: "";
      position: absolute;
      inset: -60px -60px auto auto;
      width: 140px;
      height: 140px;
      background: radial-gradient(circle at 30% 30%, rgba(124,58,237,0.15), transparent 60%);
      transform: rotate(10deg);
    }

    .statValue {
      font-size: 32px;
      font-weight: 800;
      letter-spacing: -0.04em;
      line-height: 1;
      margin: 0;
    }
    .statLabel {
      margin-top: 10px;
      color: var(--muted2);
      font-size: 12px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.1em;
    }

    .grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 24px;
      margin-top: 24px;
    }

    .panel {
      border: 1px solid rgba(255,255,255,0.06);
      background: rgba(0,0,0,0.18);
      border-radius: 20px;
      padding: 24px;
      display: flex;
      flex-direction: column;
      min-width: 0;
    }

    .panelTitle {
      margin: 0 0 16px 0;
      font-size: 14px;
      font-weight: 700;
      color: rgba(255,255,255,0.9);
      letter-spacing: -0.01em;
      display: flex;
      align-items: center;
      gap: 8px;
    }

    .cmd {
      background: rgba(0,0,0,0.25);
      border: 1px solid rgba(255,255,255,0.06);
      border-radius: 12px;
      padding: 14px 16px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      min-width: 0;
    }
    .cmd pre {
      margin: 0;
      font-family: var(--mono);
      color: rgba(255,255,255,0.92);
      font-size: 13px;
      white-space: pre;
      overflow-x: auto;
      scrollbar-width: none;
      flex: 1;
      min-width: 0;
    }
    .cmd pre::-webkit-scrollbar { display: none; }

    .cmdLabel {
      margin: 0 0 8px 0;
      color: var(--muted2);
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 0.06em;
      text-transform: uppercase;
    }

    .copyBtn {
      border: 1px solid rgba(255,255,255,0.12);
      background: rgba(255,255,255,0.06);
      color: rgba(255,255,255,0.9);
      padding: 6px 12px;
      border-radius: 8px;
      font-size: 11.5px;
      font-weight: 600;
      cursor: pointer;
      user-select: none;
      flex-shrink: 0;
      transition: all 0.2s;
      white-space: nowrap;
    }
    .copyBtn:hover {
      background: rgba(255,255,255,0.15);
      border-color: rgba(255,255,255,0.25);
      color: #fff;
    }
    .copyBtn:active { transform: translateY(1px); }

    .qsSteps {
      margin: 0 0 24px 0;
      padding-left: 18px;
      color: var(--muted);
      font-size: 13.5px;
      line-height: 1.6;
    }
    .qsSteps li { margin-bottom: 6px; }

    .meta {
      display: flex;
      flex-direction: column;
      gap: 12px;
    }
    .metaRow {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      border: 1px solid rgba(255,255,255,0.06);
      background: rgba(0,0,0,0.25);
      border-radius: 14px;
      padding: 12px 16px;
      min-height: 48px;
      min-width: 0;
    }
    .metaKey {
      color: var(--muted2);
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 0.06em;
      text-transform: uppercase;
      flex-shrink: 0;
    }
    .metaVal {
      color: rgba(255,255,255,0.9);
      font-size: 13px;
      font-weight: 600;
      font-family: var(--mono);
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      text-align: right;
      flex: 1;
      min-width: 0;
    }

    .footer {
      padding: 2px 32px 24px 32px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      color: var(--muted2);
      font-size: 12.5px;
      background: transparent;
      opacity: 0.6;
    }
      background: rgba(255,255,255,0.03);
    }
    a { color: rgba(255,255,255,0.9); text-decoration: none; }
    a:hover { text-decoration: underline; }

    @media (max-width: 860px) {
      .stats { grid-template-columns: 1fr; }
      .grid { grid-template-columns: 1fr; }
      .heroTop { flex-direction: column; align-items: flex-start; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <div class="hero">
      <div class="heroTop">
        <div class="brand">
          <div class="logo" aria-hidden="true"></div>
          <div class="titleWrap">
            <h1 class="title">LinkSocks Public Relay Server</h1>
            <p class="subtitle">Public relay endpoint · Cloudflare Workers</p>
          </div>
        </div>
        <div class="pill"><span class="dot" aria-hidden="true"></span> Online</div>
      </div>

      <div class="heroBody">
        <div class="stats">
          <div class="stat">
            <p class="statValue">${stats.currentConnections}</p>
            <div class="statLabel">Current connections</div>
          </div>
          <div class="stat">
            <p class="statValue">${stats.dailyStats.connections}</p>
            <div class="statLabel">Connections today</div>
          </div>
          <div class="stat">
            <p class="statValue">${(stats.dailyStats.transferBytes / 1024 / 1024).toFixed(2)}</p>
            <div class="statLabel">MB transferred today</div>
          </div>
          <div class="stat">
            <p class="statValue">${(stats.dailyStats.transferBytes / 1024 / 1024 / 1024).toFixed(3)}</p>
            <div class="statLabel">GB transferred</div>
          </div>
        </div>

        <div class="grid">
          <div class="panel">
            <p class="panelTitle">Quick start</p>
            <ol class="qsSteps">
              <li>Download the <b>linksocks</b> client: <a href="https://github.com/linksocks/linksocks/releases" target="_blank" rel="noreferrer">GitHub Releases</a></li>
              <li>Run <b>Provider</b> on the machine that can reach your service (can be in a private network); run <b>Connector</b> on the client machine.</li>
            </ol>

            <p class="cmdLabel">Provider</p>
            <div class="cmd" data-copy="linksocks provider -u ${url.origin} -c your_connector_token">
              <pre>linksocks provider -u ${url.origin} -c your_connector_token</pre>
              <button class="copyBtn" type="button" aria-label="Copy provider command" data-copy-btn>Copy</button>
            </div>

            <p class="cmdLabel" style="margin-top: 24px">Connector</p>
            <div class="cmd" data-copy="linksocks connector -u ${url.origin} -t your_connector_token">
              <pre>linksocks connector -u ${url.origin} -t your_connector_token</pre>
              <button class="copyBtn" type="button" aria-label="Copy connector command" data-copy-btn>Copy</button>
            </div>
          </div>
          <div class="panel">
            <p class="panelTitle">INFO</p>
            <div class="meta">
              <div class="metaRow">
                <div class="metaKey">Endpoint Region</div>
                <div class="metaVal">${String(request.cf?.colo || "-")}</div>
              </div>
              <div class="metaRow">
                <div class="metaKey">Visitor Region</div>
                <div class="metaVal">${String(request.cf?.country || "-")}</div>
              </div>
              <div class="metaRow" data-copy="${url.origin}">
                <div class="metaKey">URL</div>
                <div class="metaVal" title="${url.origin}">${url.origin}</div>
                <button class="copyBtn" type="button" aria-label="Copy endpoint URL" data-copy-btn>Copy</button>
              </div>
            </div>
          </div>
        </div>
      </div>

      <div class="footer">
        <span>Client: <a href="https://github.com/linksocks/linksocks" target="_blank" rel="noreferrer">github.com/linksocks/linksocks</a></span>
        <span>LinkSocks.js</span>
      </div>
    </div>
  </div>

  <div id="toast" class="toast" role="status" aria-live="polite">Copied</div>

  <script>
    (function () {
      const toast = document.getElementById('toast');
      let toastTimer = 0;

      function showToast(text) {
        if (!toast) return;
        toast.textContent = text;
        toast.classList.add('show');
        if (toastTimer) clearTimeout(toastTimer);
        toastTimer = setTimeout(() => toast.classList.remove('show'), 1200);
      }

      async function copyText(text) {
        try {
          if (navigator.clipboard && navigator.clipboard.writeText) {
            await navigator.clipboard.writeText(text);
            return true;
          }
        } catch (e) {
          // ignore
        }

        try {
          const ta = document.createElement('textarea');
          ta.value = text;
          ta.style.position = 'fixed';
          ta.style.left = '-9999px';
          ta.style.top = '-9999px';
          document.body.appendChild(ta);
          ta.focus();
          ta.select();
          const ok = document.execCommand('copy');
          document.body.removeChild(ta);
          return ok;
        } catch (e) {
          return false;
        }
      }

      document.addEventListener('click', async (ev) => {
        const target = ev.target;
        if (!(target instanceof Element)) return;
        const btn = target.closest('[data-copy-btn]');
        if (!btn) return;
        const container = btn.closest('[data-copy]');
        if (!container) return;
        const text = container.getAttribute('data-copy') || '';
        if (!text) return;
        const ok = await copyText(text);
        showToast(ok ? 'Copied' : 'Copy failed');
      });
    })();
  </script>
</body>
</html>`;

        return new Response(html, {
          status: 200,
          headers: { "Content-Type": "text/html" }
        });
      }

      if (path[1] === "admin") {
        if (!env.ADMIN_PASSWORD) {
          return new Response("Missing ADMIN_PASSWORD", {
            status: 500,
            headers: { "Content-Type": "text/plain" },
          });
        }

        const sha256Hex = async (input: string): Promise<string> => {
          const data = new TextEncoder().encode(input);
          const hash = await crypto.subtle.digest("SHA-256", data);
          return Array.from(new Uint8Array(hash))
            .map((b) => b.toString(16).padStart(2, "0"))
            .join("");
        };

        const expectedAuth = await sha256Hex(env.ADMIN_PASSWORD);
        const cookieHeader = request.headers.get("Cookie") || "";
        const getCookie = (name: string) => {
          const match = cookieHeader.match(new RegExp('(^| )' + name + '=([^;]+)'));
          return match ? match[2] : null;
        };
        const authCookie = getCookie('ls_auth');

        if (request.method === "POST" && url.searchParams.has("login")) {
          const form = await request.formData();
          const password = String(form.get("password") || "");
          if (password === env.ADMIN_PASSWORD) {
            return new Response(null, {
              status: 303,
              headers: {
                "Location": "/admin",
                "Set-Cookie": `ls_auth=${expectedAuth}; Path=/; HttpOnly; Secure; SameSite=Strict; Max-Age=604800`
              }
            });
          }
          return new Response("Invalid password. <a href='/admin'>Try again</a>", { status: 401, headers: { "Content-Type": "text/html" } });
        }

        if (url.searchParams.has("logout")) {
          return new Response(null, {
            status: 303,
            headers: {
              "Location": "/admin",
              "Set-Cookie": `ls_auth=; Path=/; HttpOnly; Secure; SameSite=Strict; Max-Age=0`
            }
          });
        }

        if (authCookie !== expectedAuth) {
          const loginHtml = `
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>LinkSocks Admin Login</title>
  <style>
    :root { --bg0: #050816; --bg1: #0b1226; --text: rgba(255,255,255,0.92); --brand: #7c3aed; --brand2: #22d3ee; }
    body { margin: 0; font-family: system-ui, -apple-system, sans-serif; color: var(--text); background: linear-gradient(180deg, var(--bg0), var(--bg1)); height: 100vh; display: grid; place-items: center; }
    .card { background: rgba(255,255,255,0.05); padding: 2rem; border-radius: 1rem; border: 1px solid rgba(255,255,255,0.1); width: 100%; max-width: 320px; backdrop-filter: blur(10px); box-shadow: 0 25px 60px rgba(0,0,0,0.45); }
    h1 { margin: 0 0 1.5rem 0; font-size: 1.5rem; text-align: center; font-weight: 700; }
    input { width: 100%; padding: 0.75rem; border-radius: 0.5rem; border: 1px solid rgba(255,255,255,0.2); background: rgba(0,0,0,0.3); color: white; margin-bottom: 1rem; box-sizing: border-box; outline: none; }
    input:focus { border-color: var(--brand2); }
    button { width: 100%; padding: 0.75rem; border-radius: 0.5rem; border: none; background: linear-gradient(135deg, var(--brand), var(--brand2)); color: white; font-weight: bold; cursor: pointer; font-size: 1rem; }
    button:hover { opacity: 0.9; }
  </style>
</head>
<body>
  <div class="card">
    <h1>Admin Login</h1>
    <form method="POST" action="/admin?login">
      <input type="password" name="password" placeholder="Password" required autofocus />
      <button type="submit">Login</button>
    </form>
  </div>
</body>
</html>`;
          return new Response(loginHtml, { headers: { "Content-Type": "text/html" } });
        }

        const tokenDO = env.TOKEN.get(env.TOKEN.idFromName("main"));

        if (request.method === "POST") {
          const form = await request.formData();
          const action = String(form.get("action") || "");
          const token = String(form.get("token") || "");
          const relayId = String(form.get("relayId") || "");

          if (action === "deleteToken" && token) {
            await tokenDO.deleteToken(token);
            // Get relay ID and disconnect
            const meta = await tokenDO.getRelayMetadata(token);
            if (meta?.relayId) {
              const relay = env.RELAY.get(env.RELAY.idFromString(meta.relayId));
              await relay.adminDisconnectAll("Token deleted");
            }
          }

          if (action === "deleteRelayTokens" && relayId) {
            const all = await tokenDO.getAllRelayTokens();
            for (const t of all) {
              const meta = await tokenDO.getRelayMetadata(t);
              if (meta?.relayId === relayId) {
                await tokenDO.deleteToken(t);
              }
            }
            // Disconnect all connections for this relay
            const relay = env.RELAY.get(env.RELAY.idFromString(relayId));
            await relay.adminDisconnectAll("Token deleted");
          }

          if (action === "disconnectRelay" && relayId) {
            const relay = env.RELAY.get(env.RELAY.idFromString(relayId));
            await relay.adminDisconnectAll("Admin disconnect");
          }

          if (action === "revokeConnectors" && relayId) {
            const relay = env.RELAY.get(env.RELAY.idFromString(relayId));
            await relay.adminRevokeConnectorTokens();
          }

          if (action === "revokeConnectorToken" && token) {
            const meta = await tokenDO.getRelayMetadata(token);
            if (meta?.connectorTokens?.length) {
              for (const raw of meta.connectorTokens) {
                const h = await sha256Hex(raw);
                await tokenDO.deleteToken(h);
              }
              await tokenDO.updateRelayMetadata(token, { connectorTokens: [] });
            }
          }

          if (action === "clearAllRelays") {
            const all = await tokenDO.getAllRelayTokens();
            const relayIds = new Set<string>();
            
            for (const t of all) {
              const meta = await tokenDO.getRelayMetadata(t);
              if (meta?.relayId) {
                relayIds.add(meta.relayId);
              }
              await tokenDO.deleteToken(t);
            }
            
            for (const relayId of relayIds) {
              const relay = env.RELAY.get(env.RELAY.idFromString(relayId));
              await relay.adminDisconnectAll("Token deleted");
            }
          }

          if (action === "calibrateStats") {
            const allTokens = await tokenDO.getAllRelayTokens();
            const tokenEntries = await Promise.all(
              allTokens.map(async (t) => {
                const meta = await tokenDO.getRelayMetadata(t);
                return { token: t, meta };
              }),
            );

            const relayIds = new Set<string>();
            for (const e of tokenEntries) {
              if (e.meta?.relayId) {
                relayIds.add(e.meta.relayId);
              }
            }

            const runtimeInfos = await Promise.all(
              Array.from(relayIds).map(async (relayId) => {
                try {
                  const relay = env.RELAY.get(env.RELAY.idFromString(relayId));
                  const info = await relay.adminGetRuntimeInfo();
                  return info;
                } catch (e) {
                  return null;
                }
              })
            );

            const totalConnections = runtimeInfos
              .filter(info => info !== null)
              .reduce((sum, info) => sum + (info.providerCount || 0) + (info.connectorCount || 0), 0);

            await tokenDO.calibrate(totalConnections);
          }

          return Response.redirect(`${url.origin}/admin`, 303);
        }

        const relayTokens = await tokenDO.getAllRelayTokens();
        const tokenEntries = await Promise.all(
          relayTokens.map(async (t) => {
            const meta = await tokenDO.getRelayMetadata(t);
            return { token: t, meta };
          }),
        );

        const groups = new Map<
          string,
          {
            relayId: string;
            providerCount: number;
            connectorCount: number;
            providerToken: string;
            connectorTokens: string[];
            channelCount?: number;
            createdAt?: number;
          }
        >();

        for (const e of tokenEntries) {
          if (!e.meta) continue;
          const rid = e.meta.relayId;
          const g = groups.get(rid);
          if (!g) {
            groups.set(rid, {
              relayId: rid,
              providerCount: e.meta.providerCount || 0,
              connectorCount: e.meta.connectorCount || 0,
              providerToken: e.token,
              connectorTokens: Array.isArray(e.meta.connectorTokens) ? [...e.meta.connectorTokens] : [],
              createdAt: e.meta.createdAt,
            });
          } else {
            g.providerCount = Math.max(g.providerCount, e.meta.providerCount || 0);
            g.connectorCount = Math.max(g.connectorCount, e.meta.connectorCount || 0);
            if (e.meta.createdAt && (!g.createdAt || e.meta.createdAt < g.createdAt)) {
              g.createdAt = e.meta.createdAt;
            }
            if (Array.isArray(e.meta.connectorTokens)) {
              for (const ct of e.meta.connectorTokens) {
                if (!g.connectorTokens.includes(ct)) g.connectorTokens.push(ct);
              }
            }
          }
        }

        const relayGroups = Array.from(groups.values());
        relayGroups.sort((a, b) => b.connectorTokens.length - a.connectorTokens.length);

        const page = parseInt(url.searchParams.get("page") || "1", 10);
        const pageSize = 10;
        const totalPages = Math.ceil(relayGroups.length / pageSize);
        const startIdx = (page - 1) * pageSize;
        const endIdx = startIdx + pageSize;
        const paginatedGroups = relayGroups.slice(startIdx, endIdx);

        await Promise.all(
          paginatedGroups.map(async (g) => {
            try {
              const relay = env.RELAY.get(env.RELAY.idFromString(g.relayId));
              const runtime = await relay.adminGetRuntimeInfo();
              g.channelCount = runtime.channelCount;
              g.providerCount = runtime.providerCount;
              g.connectorCount = runtime.connectorCount;
            } catch (e) {
              // Ignore
            }
          })
        );

        const globalStats = await tokenDO.getStats();
        const totals = relayGroups.reduce(
          (acc, g) => {
            acc.providers += g.providerCount || 0;
            acc.connectors += g.connectorCount || 0;
            acc.tokens += (g.connectorTokens?.length || 0) + 1; // +1 for provider token
            acc.channels += g.channelCount || 0;
            return acc;
          },
          { providers: 0, connectors: 0, tokens: 0, channels: 0 },
        );

        const short = (s: string, head = 10, tail = 6) => {
          if (!s) return "";
          if (s.length <= head + tail + 3) return s;
          return `${s.slice(0, head)}…${s.slice(-tail)}`;
        };

        const formatUptime = (ms: number) => {
          const seconds = Math.floor(ms / 1000);
          const minutes = Math.floor(seconds / 60);
          const hours = Math.floor(minutes / 60);
          const days = Math.floor(hours / 24);
          
          if (days > 0) return `${days}d ${hours % 24}h`;
          if (hours > 0) return `${hours}h ${minutes % 60}m`;
          if (minutes > 0) return `${minutes}m ${seconds % 60}s`;
          return `${seconds}s`;
        };

        const html = `
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>LinkSocks Admin</title>
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet" />
  <style>
    :root {
      --bg0: #050816;
      --bg1: #0b1226;
      --card: rgba(255,255,255,0.06);
      --card2: rgba(255,255,255,0.08);
      --stroke: rgba(255,255,255,0.12);
      --text: rgba(255,255,255,0.92);
      --muted: rgba(255,255,255,0.68);
      --muted2: rgba(255,255,255,0.52);
      --brand: #7c3aed;
      --brand2: #22d3ee;
      --danger: #fb7185;
      --ok: #22c55e;
      --shadow: 0 25px 60px rgba(0,0,0,0.45);
      --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
    }

    * { box-sizing: border-box; }
    html, body { height: 100%; }
    body {
      margin: 0;
      font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, "Apple Color Emoji", "Segoe UI Emoji";
      color: var(--text);
      background: var(--bg0);
      min-height: 100vh;
      padding: 32px 18px;
      position: relative;
    }
    body::before {
      content: "";
      position: fixed;
      inset: 0;
      background:
        radial-gradient(900px 450px at 18% 8%, rgba(124,58,237,0.35), transparent 60%),
        radial-gradient(900px 450px at 88% 10%, rgba(34,211,238,0.28), transparent 60%),
        radial-gradient(1200px 700px at 50% 92%, rgba(99,102,241,0.22), transparent 60%),
        linear-gradient(180deg, var(--bg0), var(--bg1));
      z-index: -1;
    }

    .wrap { width: min(1200px, 94%); margin: 0 auto; }

    .top {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 14px;
      margin-bottom: 24px;
    }

    .brand { display: flex; align-items: center; gap: 14px; min-width: 0; }
    .logo {
      width: 44px; height: 44px; border-radius: 14px;
      background: linear-gradient(135deg, var(--brand), var(--brand2));
      box-shadow: 0 14px 30px rgba(124,58,237,0.28);
      position: relative;
      flex: 0 0 auto;
    }
    .logo::after {
      content: "";
      position: absolute;
      inset: 2px;
      border-radius: 12px;
      background: radial-gradient(circle at 30% 30%, rgba(255,255,255,0.45), transparent 55%);
      opacity: 0.9;
      mix-blend-mode: overlay;
    }
    h1 { margin: 0; font-size: 20px; font-weight: 800; letter-spacing: -0.03em; }
    .sub { margin: 4px 0 0 0; color: var(--muted); font-size: 13px; font-weight: 600; }

    .pill {
      display: inline-flex; align-items: center; gap: 8px;
      padding: 8px 12px; border-radius: 999px;
      border: 1px solid var(--stroke);
      background: rgba(255,255,255,0.06);
      color: var(--muted);
      font-size: 12px; font-weight: 700;
      white-space: nowrap;
    }
    .dot { width: 8px; height: 8px; border-radius: 50%; background: var(--ok); box-shadow: 0 0 0 4px rgba(34,197,94,0.16); }

    .grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 16px;
      margin-bottom: 24px;
    }

    .card {
      border: 1px solid rgba(255,255,255,0.06);
      background: linear-gradient(180deg, rgba(255,255,255,0.06), rgba(255,255,255,0.02));
      backdrop-filter: blur(14px);
      border-radius: 20px;
      box-shadow: 0 20px 40px -10px rgba(0,0,0,0.4);
      overflow: hidden;
    }

    .metric {
      border: 1px solid rgba(255,255,255,0.08);
      background: rgba(255,255,255,0.02);
      border-radius: 18px;
      padding: 20px;
      position: relative;
      overflow: hidden;
    }
    .metric::before {
      content: "";
      position: absolute;
      inset: -60px -60px auto auto;
      width: 160px;
      height: 160px;
      background: radial-gradient(circle at 30% 30%, rgba(34,211,238,0.22), transparent 60%);
      transform: rotate(12deg);
    }
    .mVal { font-size: 28px; font-weight: 800; letter-spacing: -0.04em; line-height: 1; margin: 0; }
    .mLab { margin-top: 10px; color: var(--muted); font-size: 12px; font-weight: 800; text-transform: uppercase; letter-spacing: 0.12em; }

    .toolbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      padding: 14px 16px;
      border-bottom: 1px solid rgba(255,255,255,0.08);
      background: rgba(255,255,255,0.03);
    }
    .toolbarTitle { font-weight: 800; letter-spacing: -0.02em; }
    .toolbarRight { display: flex; align-items: center; gap: 10px; }
    .search {
      width: 320px;
      max-width: 52vw;
      border-radius: 999px;
      border: 1px solid rgba(255,255,255,0.14);
      background: rgba(0,0,0,0.22);
      padding: 10px 12px;
      color: var(--text);
      outline: none;
      font-size: 13px;
    }
    .search::placeholder { color: rgba(255,255,255,0.45); }
    .logoutBtn { text-decoration: none; color: var(--muted); font-size: 12px; font-weight: 700; padding: 6px 12px; border: 1px solid var(--stroke); border-radius: 999px; background: rgba(255,255,255,0.05); transition: all 0.2s; }
    .logoutBtn:hover { background: rgba(255,255,255,0.1); color: white; }

    table { width: 100%; border-collapse: collapse; }
    thead th {
      text-align: left;
      padding: 12px 16px;
      font-size: 11px;
      color: rgba(255,255,255,0.55);
      text-transform: uppercase;
      letter-spacing: 0.14em;
      border-bottom: 1px solid rgba(255,255,255,0.08);
      background: rgba(255,255,255,0.02);
      font-weight: 800;
    }
    tbody td {
      padding: 16px;
      border-bottom: 1px solid rgba(255,255,255,0.04);
      vertical-align: top;
      font-size: 13.5px;
      color: rgba(255,255,255,0.9);
    }
    tbody tr:hover td { background: rgba(255,255,255,0.03); }

    code {
      font-family: var(--mono);
      font-size: 13px;
      color: rgba(255,255,255,0.95);
      background: transparent;
      border: none;
      padding: 0;
      display: inline-flex;
      align-items: center;
      gap: 10px;
    }

    .badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 4px 10px;
      border-radius: 6px;
      border: 1px solid rgba(255,255,255,0.08); /* More subtle */
      background: rgba(255,255,255,0.02);
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 0.02em;
      color: var(--muted);
      white-space: nowrap;
    }
    
    .badge b { color: rgba(255,255,255,0.9); font-weight: 800; }

    .btn {
      appearance: none;
      border: 1px solid rgba(255,255,255,0.12);
      background: rgba(255,255,255,0.04);
      color: rgba(255,255,255,0.88);
      padding: 8px 10px;
      border-radius: 12px;
      font-size: 12px;
      font-weight: 800;
      cursor: pointer;
    }
    .btn:hover { background: rgba(255,255,255,0.09); }
    .btnDanger { border-color: rgba(251,113,133,0.35); background: rgba(251,113,133,0.08); }
    .btnDanger:hover { background: rgba(251,113,133,0.14); }
    .btnPrimary { border-color: rgba(124,58,237,0.35); background: rgba(124,58,237,0.12); }
    .btnPrimary:hover { background: rgba(124,58,237,0.18); }

    .actions { display: flex; gap: 8px; flex-wrap: wrap; }
    form { margin: 0; }

    .iconBtn {
      appearance: none;
      border: 1px solid rgba(255,255,255,0.12);
      background: rgba(255,255,255,0.04);
      color: rgba(255,255,255,0.88);
      padding: 10px;
      border-radius: 10px;
      cursor: pointer;
      transition: all 0.2s;
      line-height: 1;
      display: inline-flex;
      align-items: center;
      justify-content: center;
    }
    .iconBtn svg { display: block; }
    .iconBtn:hover { background: rgba(255,255,255,0.12); transform: scale(1.05); }
    .iconBtn.btnDanger { border-color: rgba(251,113,133,0.35); background: rgba(251,113,133,0.08); }
    .iconBtn.btnDanger:hover { background: rgba(251,113,133,0.14); }

    details { margin-top: 8px; border: none; background: transparent; padding: 0; }
    summary { 
      cursor: pointer; 
      color: rgba(255,255,255,0.6); 
      font-weight: 700; 
      font-size: 11.5px; 
      letter-spacing: 0.02em; 
      display: inline-flex;
      align-items: center;
      gap: 6px;
      transition: color 0.2s;
    }
    summary:hover { color: #fff; }
    
    .tokenList { display: flex; flex-direction: column; gap: 6px; }
    
    .tokenRow { 
      display: flex; 
      align-items: center; 
      justify-content: space-between;
      gap: 12px; 
      background: rgba(255,255,255,0.02); 
      border: 1px solid rgba(255,255,255,0.06); 
      border-radius: 8px; 
      padding: 6px 8px 6px 12px; 
      transition: background 0.2s;
    }
    .tokenRow:hover { background: rgba(255,255,255,0.05); }
    
    .tokenRow code { 
      font-family: var(--mono); 
      font-size: 12px; 
      color: rgba(255,255,255,0.9);
      background: transparent;
      border: none;
      padding: 0;
      flex: 1;
      min-width: 0;
      display: flex;
      align-items: center;
      gap: 10px;
    }
    
    .tokenRow form { display: flex; flex-shrink: 0; }

    /* Scrollable container for hidden tokens */
    .scrollBox {
      max-height: 220px;
      overflow-y: auto;
      display: flex;
      flex-direction: column;
      gap: 6px;
      margin-top: 8px;
      padding-right: 4px;
      border-left: 2px solid rgba(255,255,255,0.1); /* Indent guide */
      padding-left: 10px;
    }
    .scrollBox::-webkit-scrollbar { width: 4px; }
    .scrollBox::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.15); border-radius: 4px; }
    .scrollBox::-webkit-scrollbar-track { background: transparent; }

    .copy {
      border: 1px solid rgba(255,255,255,0.1);
      background: rgba(255,255,255,0.04);
      color: rgba(255,255,255,0.84);
      padding: 6px 8px;
      border-radius: 8px;
      font-size: 12px;
      font-weight: 900;
      cursor: pointer;
      margin: 0;
      line-height: 1;
    }
    .copy:hover { background: rgba(255,255,255,0.07); color: #fff; }
    
    .copyIcon {
      appearance: none;
      border: none;
      background: transparent;
      color: rgba(255,255,255,0.4);
      padding: 4px;
      border-radius: 4px;
      cursor: pointer;
      line-height: 1;
      display: flex;
      align-items: center;
      justify-content: center;
      transition: all 0.15s;
    }
    .copyIcon:hover { 
      color: rgba(255,255,255,0.9); 
      background: rgba(255,255,255,0.08); 
    }
    
    .deleteItemBtn {
      appearance: none;
      border: none;
      background: transparent;
      color: rgba(255,255,255,0.3);
      padding: 4px;
      border-radius: 4px;
      cursor: pointer;
      line-height: 1;
      display: flex;
      align-items: center;
      justify-content: center;
      transition: all 0.15s;
    }
    .deleteItemBtn:hover { color: #fb7185; background: rgba(251,113,133,0.1); }

    .muted { color: rgba(255,255,255,0.55); font-size: 12px; font-weight: 700; }

    /* Toast styles from Public Page */
    .toast {
      position: fixed;
      bottom: 24px;
      left: 50%;
      transform: translateX(-50%) translateY(20px);
      background: #fff;
      color: #000;
      padding: 8px 16px;
      border-radius: 999px;
      font-size: 13px;
      font-weight: 600;
      box-shadow: 0 10px 30px rgba(0,0,0,0.3);
      opacity: 0;
      pointer-events: none;
      transition: all 0.2s cubic-bezier(0.16, 1, 0.3, 1);
      z-index: 100;
    }
    .toast.show {
      transform: translateX(-50%) translateY(0);
      opacity: 1;
    }

    @media (max-width: 980px) {
      .grid { grid-template-columns: 1fr 1fr; }
    }
    @media (max-width: 640px) {
      .top { flex-direction: column; align-items: flex-start; }
      .grid { grid-template-columns: 1fr; }
      .search { width: 100%; max-width: 100%; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="top">
      <div class="brand">
        <div class="logo" aria-hidden="true"></div>
        <div>
          <h1>LinkSocks Admin</h1>
          <p class="sub">Management console</p>
        </div>
      </div>
      <a href="/admin?logout" class="logoutBtn">Logout</a>
    </div>

    <div class="grid">
      <div class="metric"><p class="mVal">${globalStats.currentConnections}</p><div class="mLab">Current connections</div></div>
      <div class="metric"><p class="mVal">${globalStats.dailyStats.connections}</p><div class="mLab">Connections today</div></div>
      <div class="metric"><p class="mVal">${(globalStats.dailyStats.transferBytes / 1024 / 1024).toFixed(2)}</p><div class="mLab">MB transferred today</div></div>
      <div class="metric"><p class="mVal">${relayGroups.length}</p><div class="mLab">Active relays</div></div>
    </div>

    <div class="card">
      <div class="toolbar">
        <div>
          <div class="toolbarTitle">Relays</div>
          <div class="muted">Providers: <b>${totals.providers}</b> · Connectors: <b>${totals.connectors}</b> · Tokens: <b>${totals.tokens}</b> · Channels: <b>${totals.channels}</b></div>
        </div>
        <input id="q" class="search" placeholder="Filter by relay id or token…" />
      </div>

      <div style="overflow:auto;">
        <table>
          <thead>
            <tr>
              <th style="min-width: 220px;">Token & Stats</th>
              <th style="min-width: 100px;">Runtime</th>
              <th style="min-width: 300px;">Connector Tokens</th>
              <th style="min-width: 120px;">Actions</th>
            </tr>
          </thead>
          <tbody id="tbody">
            ${paginatedGroups
              .map((g) => {
                const connectorTokens = g.connectorTokens || [];
                
                const renderRow = (t: string) => `
    <div class="tokenRow" data-token="${t}">
      <span title="${t}" style="font-family:var(--mono); font-size:11.5px; opacity:0.9; flex:1; min-width:0;">${short(t, 6, 4)}</span>
      <div style="display:flex; gap:4px; align-items:center;">
        <button class="copyIcon" type="button" data-copy="${t}" title="Copy Token">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect width="8" height="4" x="8" y="2" rx="1" ry="1"/><path d="M16 4h2a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h2"/></svg>
        </button>
        <form method="POST" action="/admin" onsubmit="return confirm('Delete this token?');">
          <input type="hidden" name="action" value="deleteToken" />
          <input type="hidden" name="token" value="${t}" />
          <button class="deleteItemBtn" type="submit" title="Delete token">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"></line><line x1="6" y1="6" x2="18" y2="18"></line></svg>
          </button>
        </form>
      </div>
    </div>`;

                const MAX_SHOW = 6;
                const hasMore = connectorTokens.length > MAX_SHOW;
                const displayTokens = hasMore ? connectorTokens.slice(0, MAX_SHOW) : connectorTokens;
                const hiddenTokens = hasMore ? connectorTokens.slice(MAX_SHOW) : [];
                const remaining = connectorTokens.length - MAX_SHOW;
                
                const tokensHtml = connectorTokens.length === 0 
                  ? '<div class="muted" style="padding:4px 0;">No connector tokens</div>'
                  : `
<div class="tokenList">
  ${displayTokens.map(renderRow).join("")}
  ${hasMore ? `
    <details>
      <summary class="tokenRow" style="background:rgba(255,255,255,0.06); color:var(--muted); font-size:11px; font-weight:700; padding:4px 8px; justify-content:center; cursor:pointer; list-style:none;">
        <span style="opacity:0.8;">+ ${remaining} more tokens...</span>
      </summary>
      <div class="scrollBox">
        ${hiddenTokens.map(renderRow).join("")}
      </div>
    </details>` : ''}
</div>`;

                return `
            <tr data-relay="${g.relayId}">
              <td>
                <div style="display:flex; flex-direction:column; gap:4px;">
                  <!-- Token Display -->
                  <div style="display:flex; align-items:center; ">
                    <code><span class="tok" style="font-weight:600; letter-spacing:0.02em;">${short(g.providerToken, 14, 8)}</span><button class="copy" type="button" data-copy="${g.providerToken}" title="Copy"><svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="display:block;"><rect width="8" height="4" x="8" y="2" rx="1" ry="1"/><path d="M16 4h2a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h2"/></svg></button></code>
                  </div>
                  <!-- Inline Badges -->
                  <div style="display:flex; gap:6px; flex-wrap:wrap; opacity:0.8;">
                    <span class="badge"><b>P</b> ${g.providerCount}</span>
                    <span class="badge"><b>C</b> ${g.connectorCount}</span>
                    <span class="badge"><b>CH</b> ${typeof g.channelCount === "number" ? g.channelCount : "-"}</span>
                  </div>
                </div>
              </td>
              <td style="vertical-align:middle;">
                <div class="muted" style="white-space:nowrap;">${g.createdAt ? formatUptime(Date.now() - g.createdAt) : 'Unknown'}</div>
              </td>
              <td style="vertical-align:middle;">
                ${tokensHtml}
              </td>
              <td style="vertical-align:middle;">
                <div class="actions">
                  <form method="POST" action="/admin" onsubmit="return confirm('Delete all connector tokens for this relay? This will disconnect all connectors.');">
                    <input type="hidden" name="action" value="revokeConnectors" />
                    <input type="hidden" name="relayId" value="${g.relayId}" />
                    <button class="iconBtn" type="submit" title="Delete all connector tokens">
                      <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <circle cx="8" cy="8" r="6"/>
                        <line x1="4" y1="4" x2="12" y2="12"/>
                      </svg>
                    </button>
                  </form>
                  <form method="POST" action="/admin" onsubmit="return confirm('Delete ALL tokens and disconnect all connections for this relay?');">
                    <input type="hidden" name="action" value="deleteRelayTokens" />
                    <input type="hidden" name="relayId" value="${g.relayId}" />
                    <button class="iconBtn btnDanger" type="submit" title="Delete all tokens">
                      <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <path d="M2 4h12M5 4V3a1 1 0 0 1 1-1h4a1 1 0 0 1 1 1v1m2 0v9a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V4h10z"/>
                      </svg>
                    </button>
                  </form>
                </div>
              </td>
            </tr>`;
              })
              .join("")}
            ${paginatedGroups.length === 0 ? `<tr><td colspan="4" style="padding: 22px 16px; color: rgba(255,255,255,0.6);">No tokens found.</td></tr>` : ""}
          </tbody>
        </table>
      </div>

      <div style="padding: 16px; border-top: 1px solid rgba(255,255,255,0.08); display: flex; align-items: center; justify-content: space-between; gap: 12px;">
        <div class="muted">Page ${page} of ${totalPages} · Total: ${relayGroups.length} relays</div>
        <div style="display: flex; gap: 8px;">
          ${page > 1 ? `<a href="/admin?page=${page - 1}" class="btn">← Previous</a>` : `<span class="btn" style="opacity: 0.3; cursor: not-allowed;">← Previous</span>`}
          ${page < totalPages ? `<a href="/admin?page=${page + 1}" class="btn">Next →</a>` : `<span class="btn" style="opacity: 0.3; cursor: not-allowed;">Next →</span>`}
        </div>
      </div>
    </div>

    <div style="margin-top: 16px; display: flex; gap: 12px;">
      <form method="POST" action="/admin" onsubmit="return confirm('Calibrate statistics by collecting real-time data from all relays?');" style="flex: 1;">
        <input type="hidden" name="action" value="calibrateStats" />
        <button class="btn btnPrimary" type="submit" style="width: 100%;">Calibrate Statistics</button>
      </form>
      <form method="POST" action="/admin" onsubmit="return confirm('Delete ALL relay tokens? This cannot be undone!');" style="flex: 1;">
        <input type="hidden" name="action" value="clearAllRelays" />
        <button class="btn btnDanger" type="submit" style="width: 100%;">Clear All Relays</button>
      </form>
    </div>
  </div>

  <div id="toast" class="toast" role="status" aria-live="polite">Copied</div>

  <script>
    const q = document.getElementById('q');
    const tbody = document.getElementById('tbody');

    function norm(s) { return (s || '').toLowerCase().trim(); }

    q?.addEventListener('input', () => {
      const v = norm(q.value);
      for (const tr of tbody.querySelectorAll('tr[data-relay]')) {
        const relay = norm(tr.getAttribute('data-relay'));
        const tokens = Array.from(tr.querySelectorAll('[data-token]')).map(x => norm(x.getAttribute('data-token'))).join(' ');
        tr.style.display = (!v || relay.includes(v) || tokens.includes(v)) ? '' : 'none';
      }
    });

    const toast = document.getElementById('toast');
    let toastTimer = 0;
    function showToast(text) {
      if (!toast) return;
      toast.textContent = text;
      toast.classList.add('show');
      if (toastTimer) clearTimeout(toastTimer);
      toastTimer = setTimeout(() => toast.classList.remove('show'), 1200);
    }

    document.addEventListener('click', async (e) => {
      const t = e.target;
      if (!(t instanceof HTMLElement)) return;
      
      // Handle both .copy (Provider) and .copyIcon (Connector) buttons
      const btn = t.closest('.copy, .copyIcon');
      if (!btn) return;
      
      const v = btn.getAttribute('data-copy');
      if (!v) return;
      
      try {
        await navigator.clipboard.writeText(v);
        showToast('Copied');
      } catch (err) {
        // Fallback for older browsers or non-secure contexts
        const textArea = document.createElement("textarea");
        textArea.value = v;
        document.body.appendChild(textArea);
        textArea.select();
        try {
          document.execCommand('copy');
          showToast('Copied');
        } catch (e) {
          showToast('Failed');
        }
        document.body.removeChild(textArea);
      }
    });
  </script>
</body>
</html>`;

        return new Response(html, { status: 200, headers: { "Content-Type": "text/html" } });
      }
      
      if (path[1] === "socket" && request.headers.get("Upgrade") === "websocket") {
        const token = url.searchParams.get("token");
        const reverse = url.searchParams.get("reverse");
        
        if (!token) {
          throw Error('Missing token parameter.');
        }
        
        return await handleWebsocket(request, env, token, reverse === "1" || reverse === "true");
      }

      return new Response("Not found", { status: 404 });
    });
  },
};

function isTokenComplexEnough(token: string): { valid: boolean; reason?: string } {
  if (token.length < 8) {
    return { valid: false, reason: "Token must be at least 8 characters" };
  }
  
  const result = zxcvbn(token);
  // score: 0 = too guessable, 1 = very guessable, 2 = somewhat guessable, 3 = safely unguessable, 4 = very unguessable
  // Require at least score 2 (somewhat guessable)
  if (result.score < 2) {
    const feedback = result.feedback.warning || result.feedback.suggestions[0] || "Token is too weak";
    return { valid: false, reason: feedback };
  }
  
  return { valid: true };
}

function rejectWithMessage(message: string): Response {
  const pair = new WebSocketPair();
  const [client, server] = [pair[0], pair[1]];
  server.accept();
  const response: AuthResponseMessage = {
    success: false,
    error: message,
    getType: () => MessageType.AuthResponse,
  };
  server.send(packMessage(response));
  return new Response(null, { status: 101, webSocket: client });
}

async function handleWebsocket(request: Request, env: Env, token: string, isProvider: boolean): Promise<Response> {
  // Validate request
  if (request.headers.get("Upgrade") !== "websocket") {
    return new Response("Expected WebSocket", { status: 426 });
  }

  let relayId: DurableObjectId;
  let actualToken = token;

  // sha256("anonymous") in lowercase hex
  const ANONYMOUS_TOKEN_HASH = "2f183a4e64493af3f377f745eda502363cd3e7ef6e4d266d444758de0a85fcc8";

  if (!isProvider) {
    const tokenDO = env.TOKEN.get(env.TOKEN.idFromName("main"));
    const relayStr = await tokenDO.getRelay(token);
    if (!relayStr) {
      return rejectWithMessage(`invalid token (${request.url})`);
    }
    relayId = env.RELAY.idFromString(relayStr);
  } else {
    // For provider: validate token complexity (except "anonymous")
    if (token !== ANONYMOUS_TOKEN_HASH) {
      const validation = isTokenComplexEnough(token);
      if (!validation.valid) {
        return rejectWithMessage(validation.reason!);
      }
    }
    
    if (token === ANONYMOUS_TOKEN_HASH) {
      actualToken = crypto.randomUUID();
    }
    relayId = env.RELAY.idFromName(actualToken);
  }

  // Check if the request is from APAC region and set locationHint accordingly
  const apacCountries = ["CN", "HK", "JP", "SG", "MO", "TW", "KR"];
  const isFromApac = request.cf && request.cf.country && apacCountries.includes(request.cf.country as string);
  const relay = isFromApac 
    ? env.RELAY.get(relayId, { locationHint: "apac" })
    : env.RELAY.get(relayId);

  // Add provider/connector information to the URL for the relay
  const newUrl = new URL(request.url);
  newUrl.pathname = isProvider ? "/provider" : "/connector";
  newUrl.searchParams.set("actualToken", actualToken);

  // Forward to relay
  return await relay.fetch(new Request(newUrl, request));
}
