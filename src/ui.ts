import type { Settings } from "./db";
import { formatBytes } from "./db";

export interface SiteView {
  host: string;
  url: string;
  owner: string;
  ownerId: string | null;
  temporary: boolean;
  expiresAt: number | null;
  rotateSeconds: number;
  backendCount: number;
  providerCount: number;
  requests: number;
  bytes: number;
  anonymous: boolean;
  insecure: boolean;
  createdAt: number;
  relayId: string;
}

export interface TokenView {
  id: string;
  prefix: string;
  label: string;
  createdAt: number;
  lastUsedAt: number | null;
  revoked: boolean;
  username?: string;
}

export interface UserView {
  id: string;
  username: string;
  isAdmin: boolean;
  disabled: boolean;
  createdAt: number;
  lastLoginAt: number | null;
  siteCount: number;
  tokenCount: number;
}

export interface Overview {
  users: number;
  sites: number;
  providers: number;
  requests: number;
  bytes: number;
  temporarySites: number;
  tokens: number;
}

export interface Flash {
  kind: "ok" | "error";
  message: string;
  token?: string;
}

export interface AdminPageData {
  user: { username: string };
  csrf: string;
  tab: "overview" | "users" | "sites" | "tokens" | "settings";
  users: UserView[];
  sites: SiteView[];
  tokens: TokenView[];
  overview: Overview;
  globalSettings: Settings;
  selectedUser?: UserView;
  selectedUserSettings?: Settings;
  flash?: Flash | null;
}

export interface DashboardPageData {
  user: { username: string; isAdmin: boolean };
  csrf: string;
  tab: "overview" | "sites" | "tokens" | "settings";
  sites: SiteView[];
  tokens: TokenView[];
  overview: Overview;
  settings: Settings;
  allowUserSettings: boolean;
  flash?: Flash | null;
}

export function escapeHtml(value: unknown): string {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

export function formatDate(timestamp: number | null | undefined): string {
  if (!timestamp) return "—";
  const date = new Date(timestamp);
  return date.toISOString().replace("T", " ").slice(0, 19) + " UTC";
}

export function formatDuration(ms: number): string {
  if (!Number.isFinite(ms) || ms <= 0) return "expired";
  const seconds = Math.floor(ms / 1000);
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  if (days > 0) return `${days}d ${hours}h`;
  if (hours > 0) return `${hours}h ${minutes}m`;
  if (minutes > 0) return `${minutes}m ${seconds % 60}s`;
  return `${seconds}s`;
}

function expiryCell(site: SiteView): string {
  if (!site.expiresAt) return `<span class="muted">permanent</span>`;
  const remaining = site.expiresAt - Date.now();
  const cls = remaining <= 0 ? "badge danger" : remaining < 60 * 60 * 1000 ? "badge warn" : "badge";
  return `<span class="${cls}" data-countdown="${site.expiresAt}">${remaining <= 0 ? "expired" : formatDuration(remaining)}</span>
    <div class="sub">${escapeHtml(formatDate(site.expiresAt))}</div>`;
}

function flashHtml(flash: Flash | null | undefined): string {
  if (!flash) return "";
  const tokenBlock = flash.token
    ? `<div class="token-reveal"><div class="sub">Copy this token now. It is shown only once.</div><pre id="new-token">${escapeHtml(flash.token)}</pre><button type="button" class="btn small" data-copy="new-token">Copy</button></div>`
    : "";
  return `<div class="flash ${flash.kind === "error" ? "error" : "ok"}">${escapeHtml(flash.message)}${tokenBlock}</div>`;
}

const STYLE = `
:root{
  --bg:#070b16;--bg2:#0b1226;--card:rgba(255,255,255,.055);--card2:rgba(255,255,255,.08);
  --stroke:rgba(255,255,255,.12);--text:rgba(255,255,255,.93);--muted:rgba(255,255,255,.62);
  --muted2:rgba(255,255,255,.42);--brand:#7c3aed;--brand2:#22d3ee;--ok:#34d399;--warn:#fbbf24;
  --danger:#fb7185;--shadow:0 24px 60px rgba(0,0,0,.45);
  --mono:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,"Liberation Mono",monospace;
}
*{box-sizing:border-box}
html,body{min-height:100%}
body{
  margin:0;color:var(--text);font-family:Inter,system-ui,-apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
  background:
    radial-gradient(900px 500px at 12% 0%,rgba(124,58,237,.28),transparent 62%),
    radial-gradient(900px 500px at 88% 8%,rgba(34,211,238,.20),transparent 60%),
    radial-gradient(1200px 800px at 50% 100%,rgba(99,102,241,.16),transparent 62%),
    linear-gradient(180deg,var(--bg),var(--bg2));
  background-attachment:fixed;
}
a{color:#a5b4fc;text-decoration:none}
a:hover{text-decoration:underline}
.wrap{max-width:1180px;margin:0 auto;padding:28px 20px 64px}
.topbar{display:flex;align-items:center;justify-content:space-between;gap:16px;flex-wrap:wrap;margin-bottom:26px}
.brand{display:flex;align-items:center;gap:12px}
.logo{width:38px;height:38px;border-radius:12px;background:linear-gradient(135deg,var(--brand),var(--brand2));box-shadow:0 8px 24px rgba(124,58,237,.35)}
h1{font-size:1.35rem;margin:0;letter-spacing:-.02em}
h2{font-size:1.05rem;margin:0 0 14px;letter-spacing:-.01em}
h3{font-size:.95rem;margin:0 0 10px}
.sub{font-size:.78rem;color:var(--muted2);margin-top:3px}
.muted{color:var(--muted)}
.nav{display:flex;gap:6px;flex-wrap:wrap}
.nav a{padding:8px 12px;border-radius:10px;color:var(--muted);border:1px solid transparent;font-size:.9rem}
.nav a:hover{background:var(--card);text-decoration:none;color:var(--text)}
.nav a.active{background:var(--card2);border-color:var(--stroke);color:var(--text)}
.grid{display:grid;gap:16px}
.cards{grid-template-columns:repeat(auto-fit,minmax(170px,1fr))}
.card{background:var(--card);border:1px solid var(--stroke);border-radius:16px;padding:18px;box-shadow:var(--shadow);backdrop-filter:blur(12px)}
.card .value{font-size:1.65rem;font-weight:700;letter-spacing:-.03em;margin-top:6px}
.card .label{font-size:.78rem;text-transform:uppercase;letter-spacing:.08em;color:var(--muted2)}
.section{margin-top:22px}
.table-wrap{overflow-x:auto;border:1px solid var(--stroke);border-radius:16px;background:rgba(255,255,255,.035)}
table{width:100%;border-collapse:collapse;font-size:.88rem;min-width:760px}
th,td{padding:12px 14px;text-align:left;border-bottom:1px solid rgba(255,255,255,.07);vertical-align:top}
th{font-size:.72rem;text-transform:uppercase;letter-spacing:.08em;color:var(--muted2);font-weight:600;background:rgba(255,255,255,.025)}
tr:last-child td{border-bottom:none}
tr:hover td{background:rgba(255,255,255,.025)}
.mono{font-family:var(--mono);font-size:.82rem}
.badge{display:inline-flex;align-items:center;gap:5px;padding:3px 8px;border-radius:999px;background:rgba(124,58,237,.18);border:1px solid rgba(124,58,237,.35);font-size:.72rem;font-weight:600}
.badge.ok{background:rgba(52,211,153,.14);border-color:rgba(52,211,153,.35);color:#a7f3d0}
.badge.warn{background:rgba(251,191,36,.14);border-color:rgba(251,191,36,.35);color:#fde68a}
.badge.danger{background:rgba(251,113,133,.14);border-color:rgba(251,113,133,.35);color:#fecdd3}
.badge.gray{background:rgba(255,255,255,.07);border-color:rgba(255,255,255,.14);color:var(--muted)}
.actions{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
.btn{appearance:none;border:1px solid var(--stroke);background:var(--card2);color:var(--text);padding:9px 14px;border-radius:10px;font-size:.85rem;font-weight:600;cursor:pointer;font-family:inherit}
.btn:hover{background:rgba(255,255,255,.12)}
.btn.primary{background:linear-gradient(135deg,var(--brand),#4f46e5);border-color:transparent;box-shadow:0 8px 24px rgba(124,58,237,.3)}
.btn.primary:hover{filter:brightness(1.08)}
.btn.danger{color:#fecdd3;border-color:rgba(251,113,133,.35);background:rgba(251,113,133,.12)}
.btn.small{padding:6px 10px;font-size:.78rem;border-radius:8px}
input,select,textarea{width:100%;padding:10px 12px;border-radius:10px;border:1px solid var(--stroke);background:rgba(0,0,0,.28);color:var(--text);font-family:inherit;font-size:.9rem;outline:none}
input:focus,select:focus,textarea:focus{border-color:rgba(34,211,238,.6);box-shadow:0 0 0 3px rgba(34,211,238,.12)}
label{display:block;font-size:.8rem;color:var(--muted);margin-bottom:6px;font-weight:600}
.field{margin-bottom:14px}
.field .hint{font-size:.74rem;color:var(--muted2);margin-top:5px}
.form-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:0 16px}
.check{display:flex;align-items:center;gap:9px;margin:10px 0;font-size:.86rem;color:var(--muted)}
.check input{width:auto;accent-color:#7c3aed}
.inline{display:inline-flex;gap:8px;align-items:center}
.flash{padding:12px 14px;border-radius:12px;margin-bottom:18px;font-size:.86rem;border:1px solid}
.flash.ok{background:rgba(52,211,153,.1);border-color:rgba(52,211,153,.3);color:#a7f3d0}
.flash.error{background:rgba(251,113,133,.1);border-color:rgba(251,113,133,.3);color:#fecdd3}
.token-reveal{margin-top:10px}
.token-reveal pre{background:rgba(0,0,0,.4);border:1px solid var(--stroke);padding:10px 12px;border-radius:10px;overflow-x:auto;font-family:var(--mono);font-size:.82rem}
.two-col{display:grid;grid-template-columns:minmax(0,1.4fr) minmax(280px,1fr);gap:16px}
@media(max-width:860px){.two-col{grid-template-columns:1fr}}
.login{min-height:100vh;display:grid;place-items:center;padding:24px}
.login .card{width:100%;max-width:380px;padding:26px}
.login h1{text-align:center;margin-bottom:4px}
.login .sub{text-align:center;margin-bottom:22px}
.error-page{min-height:100vh;display:grid;place-items:center;padding:24px;text-align:center}
.error-page .code{font-size:4rem;font-weight:800;letter-spacing:-.06em;background:linear-gradient(135deg,var(--brand),var(--brand2));-webkit-background-clip:text;background-clip:text;color:transparent}
footer{margin-top:36px;color:var(--muted2);font-size:.78rem;text-align:center}
.copy-note{font-size:.76rem;color:var(--muted2)}
`;

function layout(opts: {
  title: string;
  subtitle?: string;
  nav?: Array<{ href: string; label: string; active?: boolean }>;
  userLabel?: string;
  logoutAction?: string;
  content: string;
  scripts?: string;
}): string {
  const nav = (opts.nav || [])
    .map(
      (item) =>
        `<a href="${escapeHtml(item.href)}" class="${item.active ? "active" : ""}">${escapeHtml(item.label)}</a>`,
    )
    .join("");
  const logout = opts.logoutAction
    ? `<form method="POST" action="${escapeHtml(opts.logoutAction)}" class="inline"><button class="btn small" type="submit">Sign out</button></form>`
    : "";
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>${escapeHtml(opts.title)} · LinkSocks</title>
<style>${STYLE}</style>
</head>
<body>
<div class="wrap">
  <div class="topbar">
    <div class="brand">
      <div class="logo"></div>
      <div>
        <h1>${escapeHtml(opts.title)}</h1>
        <div class="sub">${escapeHtml(opts.subtitle || "LinkSocks web proxy control plane")}</div>
      </div>
    </div>
    <div class="actions">
      ${opts.userLabel ? `<span class="badge gray">${escapeHtml(opts.userLabel)}</span>` : ""}
      ${logout}
    </div>
  </div>
  <div class="nav">${nav}</div>
  <div class="section">${opts.content}</div>
  <footer>LinkSocks.js · Web reverse proxy · D1 control plane</footer>
</div>
<script>
(function(){
  function fmt(ms){
    if(!Number.isFinite(ms)||ms<=0)return "expired";
    var s=Math.floor(ms/1000),d=Math.floor(s/86400),h=Math.floor((s%86400)/3600),m=Math.floor((s%3600)/60);
    if(d>0)return d+"d "+h+"h";
    if(h>0)return h+"h "+m+"m";
    if(m>0)return m+"m "+(s%60)+"s";
    return s+"s";
  }
  function tick(){
    document.querySelectorAll("[data-countdown]").forEach(function(el){
      var target=Number(el.getAttribute("data-countdown"));
      if(!Number.isFinite(target))return;
      el.textContent=fmt(target-Date.now());
    });
  }
  tick();setInterval(tick,1000);
  document.querySelectorAll("[data-copy]").forEach(function(btn){
    btn.addEventListener("click",function(){
      var el=document.getElementById(btn.getAttribute("data-copy"));
      if(!el)return;
      var text=el.textContent||"";
      if(navigator.clipboard&&navigator.clipboard.writeText){navigator.clipboard.writeText(text).then(function(){btn.textContent="Copied";setTimeout(function(){btn.textContent="Copy"},1200);});}
    });
  });
  document.querySelectorAll("form[data-confirm]").forEach(function(form){
    form.addEventListener("submit",function(e){
      if(!confirm(form.getAttribute("data-confirm")))e.preventDefault();
    });
  });
})();
</script>
${opts.scripts || ""}
</body>
</html>`;
}

export function renderLoginPage(opts: {
  title: string;
  action: string;
  error?: string;
  username?: string;
  admin?: boolean;
  mode?: "login" | "register";
  registerHref?: string;
  signInHref?: string;
}): string {
  const isRegister = opts.mode === "register";
  const submitLabel = isRegister ? "Create account" : "Sign in";
  const footerParts: string[] = [];
  if (isRegister && opts.signInHref) {
    footerParts.push(`<a href="${escapeHtml(opts.signInHref)}">Already have an account? Sign in</a>`);
  }
  if (!isRegister && opts.registerHref) {
    footerParts.push(`<a href="${escapeHtml(opts.registerHref)}">Create an account</a>`);
  }
  if (opts.admin) {
    footerParts.push(
      `Admin account is created automatically from <span class="mono">ADMIN_USERNAME</span> / <span class="mono">ADMIN_PASSWORD</span>.`,
    );
  }
  return layout({
    title: opts.title,
    subtitle: isRegister
      ? "Create a user account"
      : opts.admin
        ? "Administrator sign in"
        : "User sign in",
    content: `<div class="login">
      <div class="card">
        <h1>${escapeHtml(opts.title)}</h1>
        <div class="sub">${
          isRegister
            ? "Register to create tokens, publish sites and track your own traffic."
            : opts.admin
              ? "Manage users, sites and anonymous quotas."
              : "View your sites, tokens and traffic."
        }</div>
        ${opts.error ? `<div class="flash error">${escapeHtml(opts.error)}</div>` : ""}
        <form method="POST" action="${escapeHtml(opts.action)}">
          <div class="field"><label for="username">Username</label><input id="username" name="username" autocomplete="username" value="${escapeHtml(opts.username || "")}" required autofocus/></div>
          <div class="field"><label for="password">Password</label><input id="password" name="password" type="password" autocomplete="${isRegister ? "new-password" : "current-password"}" ${isRegister ? 'minlength="8"' : ""} required/></div>
          ${isRegister ? `<div class="field"><label for="confirm">Confirm password</label><input id="confirm" name="confirm" type="password" autocomplete="new-password" minlength="8" required/></div>` : ""}
          <button class="btn primary" type="submit" style="width:100%">${submitLabel}</button>
        </form>
        ${footerParts.length > 0 ? `<div class="sub" style="margin-top:16px">${footerParts.join(" · ")}</div>` : ""}
      </div>
    </div>`,
  });
}

function settingsForm(opts: {
  action: string;
  csrf: string;
  settings: Settings;
  global: boolean;
  title: string;
  description: string;
}): string {
  const s = opts.settings;
  const check = (name: string, label: string, checked: boolean, hint = "") =>
    `<label class="check"><input type="checkbox" name="${name}" value="1" ${checked ? "checked" : ""}/> ${escapeHtml(label)}${hint ? `<span class="hint">${escapeHtml(hint)}</span>` : ""}</label>`;
  const num = (name: string, label: string, value: number, hint: string) =>
    `<div class="field"><label for="${name}">${escapeHtml(label)}</label><input id="${name}" name="${name}" type="number" min="0" step="1" value="${value}"/><div class="hint">${escapeHtml(hint)}</div></div>`;
  return `<div class="card">
    <h2>${escapeHtml(opts.title)}</h2>
    <div class="sub" style="margin-bottom:16px">${escapeHtml(opts.description)}</div>
    <form method="POST" action="${escapeHtml(opts.action)}">
      <input type="hidden" name="csrf" value="${escapeHtml(opts.csrf)}"/>
      <input type="hidden" name="action" value="saveSettings"/>
      ${opts.global ? `<input type="hidden" name="scope" value="global"/>` : `<input type="hidden" name="scope" value="user"/>`}
      ${opts.global ? check("allowAnonymous", "Allow anonymous providers", s.allowAnonymous) : ""}
      ${opts.global ? check("allowRegistration", "Allow user self-registration", s.allowRegistration) : ""}
      ${opts.global ? check("allowUserSettings", "Allow users to edit their own policy", s.allowUserSettings) : ""}
      ${check("allowCustomHost", "Allow custom hostnames", s.allowCustomHost, "Requires a hostname under WEB_ZONE.")}
      <div class="form-grid">
        ${num("autoRotateSeconds", "Temporary URL lifetime (seconds)", s.autoRotateSeconds, "0 disables automatic expiry. Auto-generated hosts are temporary.")}
        ${num("quotaBytes", "Transfer quota (bytes)", s.quotaBytes, "0 = unlimited. Request and response bytes are counted.")}
        ${num("quotaRequests", "Request quota", s.quotaRequests, "0 = unlimited.")}
        ${num("quotaDurationSeconds", "Site duration (seconds)", s.quotaDurationSeconds, "0 = no duration limit.")}
        ${num("maxSites", "Max sites", s.maxSites, "Per owner / per anonymous token.")}
        ${num("maxBackends", "Max backends", s.maxBackends, "Per provider registration.")}
      </div>
      <button class="btn primary" type="submit">Save settings</button>
    </form>
  </div>`;
}

function sitesTable(sites: SiteView[], opts: { csrf: string; action: string; admin: boolean }): string {
  if (sites.length === 0) {
    return `<div class="card"><div class="muted">No proxied sites yet. Run <span class="mono">linksocks web -u &lt;server&gt; -l http://127.0.0.1:8080</span> to create one.</div></div>`;
  }
  const rows = sites
    .map((site) => {
      const type = site.temporary
        ? `<span class="badge warn">temporary</span>`
        : `<span class="badge ok">permanent</span>`;
      const status = site.providerCount > 0
        ? `<span class="badge ok">${site.providerCount} online</span>`
        : `<span class="badge danger">offline</span>`;
      return `<tr>
        <td><a class="mono" href="${escapeHtml(site.url)}" target="_blank" rel="noreferrer">${escapeHtml(site.host)}</a>
          <div class="sub">${site.anonymous ? "anonymous" : "user"} · ${site.insecure ? "TLS verify off" : "TLS verify on"}</div></td>
        <td>${opts.admin ? escapeHtml(site.owner) : ""}${opts.admin ? `<div class="sub">${escapeHtml(site.relayId.slice(0, 12))}…</div>` : ""}</td>
        <td>${type}<div class="sub">${site.rotateSeconds > 0 ? `rotate ${formatDuration(site.rotateSeconds * 1000)}` : "no rotation"}</div></td>
        <td>${expiryCell(site)}</td>
        <td><span class="badge gray">${site.backendCount} origin${site.backendCount === 1 ? "" : "s"}</span><div class="sub">${status}</div></td>
        <td class="mono">${site.requests.toLocaleString()}</td>
        <td class="mono">${escapeHtml(formatBytes(site.bytes))}</td>
        <td>
          <form method="POST" action="${escapeHtml(opts.action)}" class="actions" data-confirm="Delete this site mapping? The provider will be disconnected.">
            <input type="hidden" name="csrf" value="${escapeHtml(opts.csrf)}"/>
            <input type="hidden" name="action" value="deleteSite"/>
            <input type="hidden" name="host" value="${escapeHtml(site.host)}"/>
            <button class="btn small danger" type="submit">Delete</button>
          </form>
        </td>
      </tr>`;
    })
    .join("");
  return `<div class="table-wrap"><table>
    <thead><tr><th>Host</th>${opts.admin ? "<th>Owner</th>" : ""}<th>Type</th><th>Expires</th><th>Origins</th><th>Requests</th><th>Bytes</th><th></th></tr></thead>
    <tbody>${rows}</tbody>
  </table></div>`;
}

function tokensTable(tokens: TokenView[], opts: { csrf: string; action: string; admin: boolean }): string {
  if (tokens.length === 0) {
    return `<div class="card"><div class="muted">No tokens yet. Create one to bind providers to your account.</div></div>`;
  }
  const rows = tokens
    .map(
      (token) => `<tr>
      <td class="mono">${escapeHtml(token.prefix)}…${token.revoked ? ` <span class="badge danger">revoked</span>` : ""}</td>
      ${opts.admin ? `<td>${escapeHtml(token.username || "")}</td>` : ""}
      <td>${escapeHtml(token.label || "—")}</td>
      <td>${escapeHtml(formatDate(token.createdAt))}</td>
      <td>${token.lastUsedAt ? escapeHtml(formatDate(token.lastUsedAt)) : `<span class="muted">never</span>`}</td>
      <td>${token.revoked ? "" : `<form method="POST" action="${escapeHtml(opts.action)}" data-confirm="Revoke this token?">
        <input type="hidden" name="csrf" value="${escapeHtml(opts.csrf)}"/>
        <input type="hidden" name="action" value="revokeToken"/>
        <input type="hidden" name="tokenId" value="${escapeHtml(token.id)}"/>
        <button class="btn small danger" type="submit">Revoke</button>
      </form>`}</td>
    </tr>`,
    )
    .join("");
  return `<div class="table-wrap"><table>
    <thead><tr><th>Token</th>${opts.admin ? "<th>Owner</th>" : ""}<th>Label</th><th>Created</th><th>Last used</th><th></th></tr></thead>
    <tbody>${rows}</tbody>
  </table></div>`;
}

export function renderAdminPage(data: AdminPageData): string {
  const nav = [
    { href: "/admin?tab=overview", label: "Overview", active: data.tab === "overview" },
    { href: "/admin?tab=users", label: "Users", active: data.tab === "users" },
    { href: "/admin?tab=sites", label: "Sites", active: data.tab === "sites" },
    { href: "/admin?tab=tokens", label: "Tokens", active: data.tab === "tokens" },
    { href: "/admin?tab=settings", label: "Settings", active: data.tab === "settings" },
  ];

  let content = flashHtml(data.flash);
  if (data.tab === "overview") {
    content += `<div class="grid cards">
      ${card("Users", data.overview.users)}
      ${card("Sites", data.overview.sites)}
      ${card("Live providers", data.overview.providers)}
      ${card("Temporary sites", data.overview.temporarySites)}
      ${card("Requests", data.overview.requests.toLocaleString())}
      ${card("Transfer", formatBytes(data.overview.bytes))}
    </div>
    <div class="section two-col">
      <div class="card"><h2>Recent sites</h2>${sitesTable(data.sites.slice(0, 8), { csrf: data.csrf, action: "/admin/action", admin: true })}</div>
      <div class="card"><h2>Anonymous policy</h2>
        <div class="sub">These limits apply to providers without a registered token.</div>
        <ul class="muted" style="line-height:1.9;padding-left:18px;margin:14px 0 0">
          <li>Anonymous access: <b>${data.globalSettings.allowAnonymous ? "enabled" : "disabled"}</b></li>
          <li>Custom hostnames: <b>${data.globalSettings.allowCustomHost ? "allowed" : "blocked"}</b></li>
          <li>Temporary URL lifetime: <b>${formatDuration(data.globalSettings.autoRotateSeconds * 1000)}</b></li>
          <li>Transfer quota: <b>${data.globalSettings.quotaBytes ? formatBytes(data.globalSettings.quotaBytes) : "unlimited"}</b></li>
          <li>Request quota: <b>${data.globalSettings.quotaRequests || "unlimited"}</b></li>
        </ul>
        <div style="margin-top:14px"><a class="btn small" href="/admin?tab=settings">Edit policy</a></div>
      </div>
    </div>`;
  } else if (data.tab === "users") {
    const selected = data.selectedUser;
    content += `<div class="two-col">
      <div class="card"><h2>Users</h2>
        <div class="table-wrap"><table>
          <thead><tr><th>User</th><th>Role</th><th>Sites</th><th>Tokens</th><th>Created</th><th></th></tr></thead>
          <tbody>${data.users
            .map(
              (user) => `<tr>
              <td><b>${escapeHtml(user.username)}</b>${user.disabled ? ` <span class="badge danger">disabled</span>` : ""}<div class="sub">${escapeHtml(user.id.slice(0, 8))}…</div></td>
              <td>${user.isAdmin ? `<span class="badge">admin</span>` : `<span class="badge gray">user</span>`}</td>
              <td class="mono">${user.siteCount}</td>
              <td class="mono">${user.tokenCount}</td>
              <td>${escapeHtml(formatDate(user.createdAt))}</td>
              <td><a class="btn small" href="/admin?tab=users&user=${encodeURIComponent(user.id)}">Manage</a></td>
            </tr>`,
            )
            .join("")}</tbody>
        </table></div>
        <h3 style="margin-top:22px">Create user</h3>
        <form method="POST" action="/admin/action">
          <input type="hidden" name="csrf" value="${escapeHtml(data.csrf)}"/>
          <input type="hidden" name="action" value="createUser"/>
          <div class="form-grid">
            <div class="field"><label for="new-username">Username</label><input id="new-username" name="username" required/></div>
            <div class="field"><label for="new-password">Password</label><input id="new-password" name="password" type="password" minlength="8" required/></div>
          </div>
          <label class="check"><input type="checkbox" name="isAdmin" value="1"/> Grant administrator role</label>
          <button class="btn primary" type="submit">Create user</button>
        </form>
      </div>
      <div class="card"><h2>${selected ? `Manage ${escapeHtml(selected.username)}` : "Select a user"}</h2>
        ${selected ? userManageForm(selected, data.csrf) : `<div class="muted">Choose “Manage” next to a user to edit their policy and tokens.</div>`}
      </div>
    </div>
    ${selected && data.selectedUserSettings ? settingsForm({
      action: "/admin/action",
      csrf: data.csrf,
      settings: data.selectedUserSettings,
      global: false,
      title: `Policy for ${selected.username}`,
      description: "Per-user limits applied to sites created with this user's tokens.",
    }) : ""}`;
  } else if (data.tab === "sites") {
    content += `<div class="card"><h2>Sites</h2><div class="sub" style="margin-bottom:14px">All active hostname mappings. “Origins” is the number of local backends declared by connected providers.</div>${sitesTable(data.sites, { csrf: data.csrf, action: "/admin/action", admin: true })}</div>`;
  } else if (data.tab === "tokens") {
    content += `<div class="card"><h2>Tokens</h2><div class="sub" style="margin-bottom:14px">Provider tokens are bound to the user who created them. Raw tokens are shown only once at creation.</div>${tokensTable(data.tokens, { csrf: data.csrf, action: "/admin/action", admin: true })}</div>`;
  } else {
    content += settingsForm({
      action: "/admin/action",
      csrf: data.csrf,
      settings: data.globalSettings,
      global: true,
      title: "Global anonymous policy",
      description: "Defaults for providers that connect without a registered token. Registered users have their own policy.",
    });
  }

  return layout({
    title: "Admin console",
    subtitle: "Users, tokens, sites and anonymous quotas",
    nav,
    userLabel: `${data.user.username} · admin`,
    logoutAction: "/admin?logout",
    content,
  });
}

function userManageForm(user: UserView, csrf: string): string {
  return `<form method="POST" action="/admin/action">
      <input type="hidden" name="csrf" value="${escapeHtml(csrf)}"/>
      <input type="hidden" name="action" value="updateUser"/>
      <input type="hidden" name="userId" value="${escapeHtml(user.id)}"/>
      <div class="field"><label for="reset-password">New password</label><input id="reset-password" name="password" type="password" minlength="8" placeholder="Leave blank to keep current"/></div>
      <label class="check"><input type="checkbox" name="isAdmin" value="1" ${user.isAdmin ? "checked" : ""}/> Administrator</label>
      <label class="check"><input type="checkbox" name="disabled" value="1" ${user.disabled ? "checked" : ""}/> Disabled</label>
      <div class="actions">
        <button class="btn primary" type="submit">Save user</button>
        <button class="btn" type="submit" form="create-token-form">Create token</button>
      </div>
    </form>
    <form method="POST" action="/admin/action" id="create-token-form" style="margin-top:12px">
      <input type="hidden" name="csrf" value="${escapeHtml(csrf)}"/>
      <input type="hidden" name="action" value="createToken"/>
      <input type="hidden" name="userId" value="${escapeHtml(user.id)}"/>
      <div class="field"><label for="token-label">Token label</label><input id="token-label" name="label" placeholder="laptop / ci"/></div>
      <button class="btn" type="submit">Create token for ${escapeHtml(user.username)}</button>
    </form>
    <form method="POST" action="/admin/action" style="margin-top:18px" data-confirm="Delete this user, their tokens and sessions?">
      <input type="hidden" name="csrf" value="${escapeHtml(csrf)}"/>
      <input type="hidden" name="action" value="deleteUser"/>
      <input type="hidden" name="userId" value="${escapeHtml(user.id)}"/>
      <button class="btn danger" type="submit">Delete user</button>
    </form>`;
}

function card(label: string, value: string | number): string {
  return `<div class="card"><div class="label">${escapeHtml(label)}</div><div class="value">${escapeHtml(value)}</div></div>`;
}

export function renderDashboardPage(data: DashboardPageData): string {
  const nav = [
    { href: "/dashboard?tab=overview", label: "Overview", active: data.tab === "overview" },
    { href: "/dashboard?tab=sites", label: "My sites", active: data.tab === "sites" },
    { href: "/dashboard?tab=tokens", label: "Tokens", active: data.tab === "tokens" },
    { href: "/dashboard?tab=settings", label: "Settings", active: data.tab === "settings" },
  ];
  let content = flashHtml(data.flash);
  if (data.tab === "overview") {
    content += `<div class="grid cards">
      ${card("My sites", data.overview.sites)}
      ${card("Live providers", data.overview.providers)}
      ${card("Temporary sites", data.overview.temporarySites)}
      ${card("Requests", data.overview.requests.toLocaleString())}
      ${card("Transfer", formatBytes(data.overview.bytes))}
      ${card("Tokens", data.overview.tokens)}
    </div>
    <div class="section card"><h2>My sites</h2>${sitesTable(data.sites.slice(0, 8), { csrf: data.csrf, action: "/dashboard/action", admin: false })}</div>`;
  } else if (data.tab === "sites") {
    content += `<div class="card"><h2>My sites</h2><div class="sub" style="margin-bottom:14px">Each site can have multiple origins behind one hostname. Temporary hosts expire automatically.</div>${sitesTable(data.sites, { csrf: data.csrf, action: "/dashboard/action", admin: false })}</div>`;
  } else if (data.tab === "tokens") {
    content += `<div class="two-col">
      <div class="card"><h2>My tokens</h2>${tokensTable(data.tokens, { csrf: data.csrf, action: "/dashboard/action", admin: false })}</div>
      <div class="card"><h2>Create token</h2>
        <div class="sub" style="margin-bottom:14px">Use a token with <span class="mono">linksocks web -t &lt;token&gt;</span> to bind the site to your account.</div>
        <form method="POST" action="/dashboard/action">
          <input type="hidden" name="csrf" value="${escapeHtml(data.csrf)}"/>
          <input type="hidden" name="action" value="createToken"/>
          <div class="field"><label for="token-label">Label</label><input id="token-label" name="label" placeholder="laptop / ci"/></div>
          <button class="btn primary" type="submit">Create token</button>
        </form>
      </div>
    </div>`;
  } else {
    content += data.allowUserSettings
      ? settingsForm({
          action: "/dashboard/action",
          csrf: data.csrf,
          settings: data.settings,
          global: false,
          title: "My provider policy",
          description: "These limits apply to sites created with your tokens. Set a value to 0 for unlimited.",
        })
      : `<div class="card"><h2>Settings</h2><div class="muted">Your administrator has disabled per-user policy editing.</div></div>`;
  }
  return layout({
    title: "My dashboard",
    subtitle: "Your web proxy sites, origins and traffic",
    nav,
    userLabel: data.user.isAdmin ? `${data.user.username} · admin` : data.user.username,
    logoutAction: "/dashboard?logout",
    content,
  });
}

export function renderErrorPage(title: string, message: string, code = "Error"): string {
  return layout({
    title,
    content: `<div class="error-page"><div>
      <div class="code">${escapeHtml(code)}</div>
      <h1>${escapeHtml(title)}</h1>
      <div class="muted" style="margin-top:10px">${escapeHtml(message)}</div>
      <div style="margin-top:20px"><a class="btn" href="/">Back to home</a></div>
    </div></div>`,
  });
}

export function renderExpiredPage(host: string, expiresAt: number | null): string {
  return renderErrorPage(
    "This temporary URL has expired",
    `${host} expired${expiresAt ? ` at ${formatDate(expiresAt)}` : ""}. Restart or re-register the provider to receive a new hostname.`,
    "410",
  );
}
