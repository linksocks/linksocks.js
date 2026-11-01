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
  reserved: boolean;
}

export interface ReservationView {
  host: string;
  userId: string;
  username: string;
  note: string;
  createdAt: number;
  active: boolean;
  siteOwner: string | null;
  temporary: boolean;
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
  onlineSites: number;
  providers: number;
  requests: number;
  bytes: number;
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
  tab: "overview" | "users" | "sites" | "tokens" | "reservations" | "settings";
  users: UserView[];
  sites: SiteView[];
  tokens: TokenView[];
  reservations: ReservationView[];
  overview: Overview;
  globalSettings: Settings;
  serverUrl: string;
  zone: string;
  selectedUser?: UserView;
  selectedUserSettings?: Settings;
  userModal?: "password" | "access" | "policy" | "delete";
  flash?: Flash | null;
}

export interface DashboardPageData {
  user: { username: string; isAdmin: boolean };
  csrf: string;
  tab: "overview" | "sites" | "tokens";
  sites: SiteView[];
  tokens: TokenView[];
  overview: Overview;
  serverUrl: string;
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

function formatDateCompact(timestamp: number | null | undefined): string {
  if (!timestamp) return "—";
  const date = new Date(timestamp);
  return date.toISOString().slice(5, 16).replace("T", " ") + " UTC";
}

export function formatDuration(ms: number): string {
  if (!Number.isFinite(ms) || ms <= 0) return "expired";
  const seconds = Math.floor(ms / 1000);
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  if (days > 0) return hours > 0 ? `${days}d ${hours}h` : `${days}d`;
  if (hours > 0) return minutes > 0 ? `${hours}h ${minutes}m` : `${hours}h`;
  if (minutes > 0) return seconds % 60 > 0 ? `${minutes}m ${seconds % 60}s` : `${minutes}m`;
  return `${seconds}s`;
}

function expiryCell(site: SiteView): string {
  if (!site.expiresAt) return `<span class="muted">permanent</span>`;
  const remaining = site.expiresAt - Date.now();
  const cls = remaining <= 0 ? "badge danger" : remaining < 60 * 60 * 1000 ? "badge warn" : "badge";
  return `<span class="${cls}" data-countdown="${site.expiresAt}">${remaining <= 0 ? "expired" : formatDuration(remaining)}</span>
    <div class="sub">${escapeHtml(formatDateCompact(site.expiresAt))}</div>`;
}

function flashHtml(flash: Flash | null | undefined): string {
  if (!flash) return "";
  return `<div class="flash ${flash.kind === "error" ? "error" : "ok"}">${escapeHtml(flash.message)}</div>`;
}

const STYLE = `
:root{
  --bg:#f3f4f1;
  --surface:#ffffff;
  --surface-2:#f7f8f5;
  --surface-3:#eef0ec;
  --ink:#15181c;
  --text:#24282d;
  --muted:#5f6872;
  --faint:#6f7880;
  --line:#dde1da;
  --line-strong:#c7cdc5;
  --accent:#2454d6;
  --accent-hover:#1c44b4;
  --accent-soft:#edf1ff;
  --ok:#117347;
  --ok-soft:#e8f6ef;
  --warn:#8a5600;
  --warn-soft:#fff5e2;
  --danger:#b42318;
  --danger-soft:#fff0ee;
  --radius:8px;
  --radius-sm:6px;
  --mono:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,"Liberation Mono","Courier New",monospace;
  --sans:Inter,ui-sans-serif,system-ui,-apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
  color-scheme:light;
}
*{box-sizing:border-box}
*{scrollbar-width:thin;scrollbar-color:#c6cbc4 transparent}
*::-webkit-scrollbar{width:9px;height:9px}
*::-webkit-scrollbar-track{background:transparent}
*::-webkit-scrollbar-thumb{background:#c6cbc4;border-radius:999px;border:2px solid transparent;background-clip:content-box}
*::-webkit-scrollbar-thumb:hover{background:#aab1aa;background-clip:content-box}
html{background:var(--bg)}
html,body{min-height:100%}
body{
  min-height:100vh;margin:0;color:var(--text);
  font-family:var(--sans);font-size:14.5px;line-height:1.55;
  background:var(--bg);
  -webkit-font-smoothing:antialiased;
}
a{color:var(--accent);text-decoration:none}
a:hover{color:var(--accent-hover);text-decoration:underline;text-underline-offset:2px}
.wrap{max-width:1120px;margin:0 auto;padding:0 28px 72px}
.topbar{display:flex;align-items:center;justify-content:space-between;gap:24px;flex-wrap:wrap;padding:24px 0 22px;border-bottom:1px solid var(--line)}
.brand{display:flex;align-items:center;gap:13px;min-width:0}
.logo{width:34px;height:34px;border-radius:6px;background:var(--ink);color:#fff;display:grid;place-items:center;font-family:var(--mono);font-size:10px;font-weight:700;letter-spacing:.04em;flex:0 0 auto}
.eyebrow{font-family:var(--mono);font-size:10px;letter-spacing:.15em;text-transform:uppercase;color:var(--faint);font-weight:600;margin-bottom:2px}
h1{font-size:17px;margin:0;letter-spacing:-.015em;line-height:1.3;font-weight:600;color:var(--ink)}
h2{font-size:15px;margin:0 0 10px;letter-spacing:-.01em;font-weight:600;color:var(--ink)}
h3{font-size:13.5px;margin:0 0 8px;font-weight:600;color:var(--ink)}
.sub{font-size:12.5px;color:var(--muted);margin-top:2px;line-height:1.55}
.muted{color:var(--muted)}
.nav{display:flex;gap:26px;border-bottom:1px solid var(--line);margin:0 0 32px;overflow-x:auto;scrollbar-width:none}
.nav::-webkit-scrollbar{display:none}
.nav a{padding:12px 0 11px;border-bottom:2px solid transparent;margin-bottom:-1px;color:var(--muted);font-size:13.5px;font-weight:500;white-space:nowrap}
.nav a:hover{color:var(--ink);text-decoration:none}
.nav a.active{color:var(--ink);border-bottom-color:var(--ink);font-weight:600}
.wrap > .section{margin-top:0}
.section{margin-top:24px}
.page-head{display:flex;align-items:flex-end;justify-content:space-between;gap:18px;flex-wrap:wrap;margin:4px 0 24px}
.page-head h2{font-size:26px;margin:0;letter-spacing:-.025em;font-weight:600}
.page-head .sub{margin-top:5px;max-width:760px;color:var(--muted)}
.grid{display:grid;gap:18px}
.stats{display:flex;flex-wrap:wrap;gap:1px;background:var(--line);border:1px solid var(--line);border-radius:var(--radius);overflow:hidden;margin-bottom:26px}
.stat{flex:1 1 150px;background:var(--surface);padding:18px 20px 17px;min-width:0}
.stat .label{font-family:var(--mono);font-size:10px;text-transform:uppercase;letter-spacing:.11em;color:var(--faint);font-weight:600;white-space:nowrap}
.stat .value{font-size:27px;font-weight:600;letter-spacing:-.035em;margin-top:7px;line-height:1.15;color:var(--ink);font-variant-numeric:tabular-nums}
.stat .hint{font-size:11.5px;color:var(--faint);margin-top:3px}
.card{background:var(--surface);border:1px solid var(--line);border-radius:var(--radius);padding:22px}
.card .value{font-size:26px;font-weight:600;letter-spacing:-.03em;margin-top:6px;color:var(--ink)}
.card .label{font-family:var(--mono);font-size:10px;text-transform:uppercase;letter-spacing:.11em;color:var(--faint)}
.panel{background:var(--surface);border:1px solid var(--line);border-radius:var(--radius);overflow:hidden}
.panel.combo-panel{overflow:visible}
.combo-panel .panel-head{border-radius:var(--radius) var(--radius) 0 0}
.panel-head{display:flex;align-items:center;justify-content:space-between;gap:14px;flex-wrap:wrap;padding:17px 22px;border-bottom:1px solid var(--line);background:var(--surface)}
.panel-head h2{font-size:14px;margin:0;font-weight:600}
.panel-body{padding:22px}
.panel-body.flush{padding:0 0 16px}
.panel-body.flush .table-wrap{border:0;border-radius:0}
.panel-body.flush .empty{padding:56px 24px}
.panel-note{padding:12px 22px;border-bottom:1px solid var(--line);font-size:12.5px;color:var(--muted);background:var(--surface-2)}
.stack{display:grid;gap:18px}
.table-wrap{overflow:auto;border:1px solid var(--line);border-radius:var(--radius-sm);background:var(--surface);scrollbar-width:thin;scrollbar-color:#c6cbc4 transparent}
.table-wrap::-webkit-scrollbar{height:9px;width:9px}
.table-wrap::-webkit-scrollbar-track{background:transparent}
.table-wrap::-webkit-scrollbar-thumb{background:#c6cbc4;border-radius:999px;border:2px solid transparent;background-clip:padding-box}
.table-wrap::-webkit-scrollbar-thumb:hover{background:#aab1aa;background-clip:padding-box}
table{width:100%;border-collapse:collapse;font-size:13.5px;min-width:780px}
thead th{background:var(--surface-2);font-family:var(--mono);font-size:10px;text-transform:uppercase;letter-spacing:.1em;color:var(--faint);font-weight:600;text-align:left;padding:10px 18px;border-bottom:1px solid var(--line);white-space:nowrap}
td{padding:14px 18px;text-align:left;border-bottom:1px solid #edf0ea;vertical-align:top;white-space:nowrap;color:var(--text)}
td .sub{white-space:normal}
th:first-child,td:first-child{padding-left:22px}
th:last-child,td:last-child{padding-right:22px}
tbody tr:last-child td{border-bottom:none}
tbody tr:hover td{background:#fafbf8}
td .sub{font-size:11.5px;margin-top:3px;color:var(--faint)}
table th .muted{text-transform:none;letter-spacing:0;font-weight:500;font-size:10px;color:var(--faint)}
.table-wrap.compact table{min-width:0}
.user-table table{min-width:620px}
.table-wrap.compact thead th{padding:9px 16px}
.table-wrap.compact td{padding:11px 16px;font-size:12.8px}
.table-wrap.compact th:first-child,.table-wrap.compact td:first-child{padding-left:22px}
.table-wrap.compact th:last-child,.table-wrap.compact td:last-child{padding-right:22px}
.mono{font-family:var(--mono);font-size:12.5px;font-variant-numeric:tabular-nums}
.badge{display:inline-flex;align-items:center;gap:5px;padding:2px 7px;border-radius:4px;background:var(--accent-soft);border:1px solid #d9e2ff;color:#23449c;font-family:var(--mono);font-size:10.5px;font-weight:600;line-height:1.5;white-space:nowrap;vertical-align:middle}
.badge.ok{background:var(--ok-soft);border-color:#cbe9da;color:var(--ok)}
.badge.warn{background:var(--warn-soft);border-color:#f1dfb6;color:var(--warn)}
.badge.danger{background:var(--danger-soft);border-color:#f0cbc6;color:var(--danger)}
.badge.gray{background:var(--surface-3);border-color:var(--line);color:var(--muted)}
.badge.accent{background:var(--accent-soft);border-color:#d9e2ff;color:var(--accent)}
.badge + .badge{margin-left:4px}
.dot{width:5px;height:5px;border-radius:999px;background:currentColor;display:inline-block;flex:0 0 auto}
.actions{display:flex;gap:10px;flex-wrap:wrap;align-items:center}
.identity{display:inline-flex;align-items:center;gap:8px;min-width:0;color:var(--muted);font-size:12.5px}
.avatar{width:27px;height:27px;border-radius:50%;display:grid;place-items:center;flex:0 0 auto;background:var(--surface-3);border:1px solid var(--line);color:var(--text);font-family:var(--mono);font-size:11px;font-weight:600}
.identity-name{max-width:170px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-weight:600;color:var(--text)}
.panel-tools{display:flex;align-items:center;gap:10px;flex-wrap:wrap;justify-content:flex-end}
.search{width:210px;min-height:32px;padding:6px 10px;font-size:12.5px;background:var(--surface)}
.row-actions{display:flex;align-items:center;gap:4px;white-space:nowrap}
.row-actions .btn{padding:4px 8px}
.user-table{max-height:min(64vh,640px);overflow:auto}
.user-table thead th{position:sticky;top:0;z-index:1}
.user-table .user-name{max-width:220px;overflow:hidden;text-overflow:ellipsis}
.table-note{padding:11px 22px;border-top:1px solid var(--line);color:var(--faint);font-size:12px;background:var(--surface-2)}
dialog.modal{display:none;width:min(560px,calc(100vw - 36px));max-height:calc(100vh - 48px);padding:0;border:1px solid var(--line);border-radius:10px;background:var(--surface);color:var(--text);overflow:hidden}
dialog.modal[open]{display:flex;flex-direction:column}
dialog.modal::backdrop{background:rgba(21,24,28,.42)}
dialog.modal.wide{width:min(720px,calc(100vw - 36px))}
.modal-head{display:flex;align-items:flex-start;justify-content:space-between;gap:18px;padding:18px 22px;border-bottom:1px solid var(--line);flex:0 0 auto}
.modal-head h2{margin:0;font-size:16px;letter-spacing:-.01em}
.modal-head .sub{margin-top:5px;font-size:12.5px;color:var(--muted);line-height:1.5}
.modal-close{appearance:none;border:0;background:transparent;color:var(--faint);width:30px;height:30px;border-radius:5px;font-size:20px;line-height:1;cursor:pointer;flex:0 0 auto}
.modal-close:hover{background:var(--surface-3);color:var(--ink)}
.modal-body{padding:22px;overflow:auto}
.modal-body .form-title:first-child{margin-top:0}
.modal-body .form-title:not(:first-of-type){margin-top:20px;padding-top:16px}
.modal-body .field{margin-bottom:14px}
.modal-body .toggle-grid{margin-bottom:2px}
.modal-actions{display:flex;justify-content:flex-end;align-items:center;gap:10px;margin-top:22px;padding-top:16px;border-top:1px solid var(--line);flex-wrap:wrap}
.modal-actions .danger-left{margin-right:auto}
.token-value{background:var(--surface-2);border:1px solid var(--line);padding:12px;border-radius:var(--radius-sm);overflow-x:auto;font-family:var(--mono);font-size:12.5px;margin:10px 0 14px;white-space:pre-wrap;word-break:break-all}
.combo{position:relative}
.combo input{background:var(--surface)}
.combo-menu{position:absolute;left:0;right:0;top:calc(100% + 4px);z-index:20;display:none;max-height:220px;overflow:auto;background:var(--surface);border:1px solid var(--line-strong);border-radius:6px;box-shadow:0 10px 26px rgba(21,24,28,.14);padding:4px}
.combo.open .combo-menu{display:block}
.combo-option{appearance:none;display:flex;align-items:center;justify-content:space-between;gap:12px;width:100%;border:0;background:transparent;color:var(--text);padding:8px 10px;border-radius:4px;font-family:inherit;font-size:13px;text-align:left;cursor:pointer}
.combo-option[hidden]{display:none}
.combo-option:hover,.combo-option.is-active{background:var(--accent-soft);color:var(--accent-hover)}
.combo-option .who{font-weight:600}
.combo-option .role{font-family:var(--mono);font-size:10px;color:var(--faint);text-transform:uppercase;letter-spacing:.08em}
.combo-empty{padding:10px;color:var(--faint);font-size:12.5px}
.form-grid .span-2{grid-column:1/-1}
.btn{appearance:none;display:inline-flex;align-items:center;justify-content:center;gap:6px;border:1px solid var(--line-strong);background:var(--surface);color:var(--ink);padding:7px 12px;min-height:34px;border-radius:var(--radius-sm);font-size:13px;font-weight:550;cursor:pointer;font-family:inherit;transition:background .15s ease,border-color .15s ease,color .15s ease}
.btn:hover{background:var(--surface-2);border-color:#adb5ad;text-decoration:none}
.btn.primary{background:var(--accent);border-color:var(--accent);color:#fff}
.btn.primary:hover{background:var(--accent-hover);border-color:var(--accent-hover)}
.btn.danger{color:var(--danger);border-color:#ecc9c4;background:var(--surface)}
.btn.danger:hover{background:var(--danger-soft);border-color:#dfa9a2}
.btn.ghost{background:transparent;border-color:transparent;color:var(--accent);padding-left:6px;padding-right:6px}
.btn.ghost:hover{background:var(--surface-2);border-color:transparent;color:var(--accent-hover)}
.btn.small{min-height:28px;padding:4px 9px;font-size:12px;border-radius:5px}
input,select,textarea{width:100%;padding:9px 11px;border-radius:var(--radius-sm);border:1px solid var(--line-strong);background:var(--surface);color:var(--ink);font-family:inherit;font-size:14px;line-height:1.4;outline:none;min-height:38px;transition:border-color .15s ease,box-shadow .15s ease}
input::placeholder,textarea::placeholder{color:#a2a9b1}
input:focus,select:focus,textarea:focus{border-color:var(--accent);box-shadow:0 0 0 3px rgba(36,84,214,.12)}
label{display:block;font-size:12.5px;color:var(--ink);margin-bottom:6px;font-weight:550}
.field{margin-bottom:16px}
.field .hint{font-size:11.5px;color:var(--faint);margin-top:5px;line-height:1.5}
.form-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:0 18px}
.form-section{margin-top:22px;padding-top:18px;border-top:1px solid var(--line)}
.form-danger{margin-top:18px;padding-top:18px;border-top:1px solid var(--line)}
.check{display:flex;align-items:flex-start;gap:9px;margin:10px 0;font-size:13px;color:var(--text);cursor:pointer;font-weight:400}
.check input{width:15px;height:15px;min-height:0;margin:2px 0 0;flex:0 0 auto;accent-color:var(--accent)}
.check .hint{display:block;font-size:11.5px;color:var(--faint);margin-top:2px;line-height:1.45;font-weight:400}
.check-body{display:block;min-width:0}
.check-label{display:block;line-height:1.4}
.form-title{font-family:var(--mono);font-size:10px;text-transform:uppercase;letter-spacing:.12em;color:var(--faint);font-weight:600;margin:4px 0 12px}
.form-title:not(:first-of-type){margin-top:28px;padding-top:22px;border-top:1px solid var(--line)}
.form-actions{margin-top:18px}
.toggle-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:10px;margin-bottom:4px}
.toggle-grid .check{margin:0;padding:13px 15px;border:1px solid var(--line);border-radius:var(--radius-sm);background:var(--surface-2);line-height:1.4;display:grid;grid-template-columns:15px minmax(0,1fr);gap:10px;align-items:start}
.toggle-grid .check:hover{border-color:var(--line-strong);background:var(--surface)}
.inline{display:inline-flex;gap:8px;align-items:center;flex-wrap:wrap;margin:0}
.flash{padding:12px 14px;border-radius:var(--radius-sm);margin-bottom:22px;font-size:13px;border:1px solid}
.flash.ok{background:var(--ok-soft);border-color:#cbe9da;color:var(--ok)}
.flash.error{background:var(--danger-soft);border-color:#f0cbc6;color:var(--danger)}
.token-reveal{margin-top:12px}
.token-reveal pre{background:var(--surface-2);border:1px solid var(--line);padding:12px;border-radius:var(--radius-sm);overflow-x:auto;font-family:var(--mono);font-size:12.5px;margin:8px 0}
.two-col{display:grid;grid-template-columns:minmax(0,1.5fr) minmax(300px,1fr);gap:18px;align-items:start}
.two-col.user-manage{grid-template-columns:minmax(0,1fr) minmax(360px,1.35fr)}
.user-manage .form-grid{grid-template-columns:repeat(auto-fit,minmax(170px,1fr))}
.empty{display:flex;flex-direction:column;align-items:center;justify-content:center;gap:7px;padding:52px 24px;text-align:center;color:var(--muted)}
.empty .empty-icon{display:none}
.mobile-only,.only-phone{display:none}
.empty strong{color:var(--ink);font-weight:600;font-size:13.5px}
.empty .sub{max-width:460px;margin-top:0;font-size:12.5px}
.empty-command{display:inline-block;max-width:100%;overflow-x:auto;white-space:nowrap;margin:7px 0 2px;padding:7px 10px;border:1px solid var(--line);border-radius:5px;background:var(--surface-2);font-family:var(--mono);font-size:11.5px;color:var(--text)}
.empty code,.code{font-family:var(--mono);font-size:12px;background:var(--surface-2);border:1px solid var(--line);padding:3px 7px;border-radius:4px;color:var(--text)}
.empty code{display:inline-block;max-width:100%;white-space:normal;overflow-wrap:anywhere;line-height:1.6}
.note{display:flex;gap:10px;align-items:flex-start;padding:13px 15px;border-radius:var(--radius-sm);border:1px solid var(--line);background:var(--surface-2);color:var(--muted);font-size:12.5px;line-height:1.55}
.note strong{color:var(--ink);font-weight:600}
.nowrap{white-space:nowrap!important}
.policy{display:grid;gap:0}
.policy-row{display:flex;justify-content:space-between;gap:16px;align-items:baseline;padding:12px 0;border-bottom:1px solid #edf0ea;font-size:13px}
.policy-row:last-child{border-bottom:none}
.policy-row span{color:var(--muted)}
.policy-row b{font-weight:600;color:var(--ink);text-align:right;font-variant-numeric:tabular-nums}
.login{min-height:100vh;min-height:100dvh;display:grid;place-items:center;padding:32px 20px}
.login-box{width:100%;max-width:380px}
.login-brand{display:flex;align-items:center;justify-content:center;gap:10px;margin-bottom:22px;font-weight:600;letter-spacing:-.01em;color:var(--ink);font-size:15px}
.login-brand .logo{width:30px;height:30px;border-radius:5px;font-size:9px}
.login .card{width:100%;max-width:380px;padding:32px;border:1px solid var(--line);border-radius:var(--radius);background:var(--surface)}
.login h1{text-align:center;margin-bottom:5px;font-size:22px;letter-spacing:-.025em}
.login .sub{text-align:center;margin-bottom:24px}
.login .btn.primary{width:100%;margin-top:4px}
.login .field:last-of-type{margin-bottom:18px}
.error-page{min-height:100vh;min-height:100dvh;display:grid;place-items:center;padding:32px 20px;text-align:center}
.error-page .code{font-size:64px;font-weight:600;letter-spacing:-.06em;color:var(--ink);border:0;background:transparent;padding:0;line-height:1}
.error-page h1{margin-top:8px;font-size:20px}
footer{margin-top:56px;padding-top:20px;border-top:1px solid var(--line);color:var(--faint);font-size:12px;display:flex;justify-content:space-between;gap:14px;flex-wrap:wrap}
footer a{color:var(--muted)}
footer a:hover{color:var(--ink)}
.copy-note{font-size:11.5px;color:var(--faint)}
@media(max-width:960px){
  .two-col,.two-col.user-manage{grid-template-columns:1fr}
}
@media(max-width:760px){
  .wrap{padding:0 18px 56px}
  .topbar{padding:18px 0 16px;gap:14px}
  h1{font-size:16px}
  .nav{gap:20px;margin-bottom:26px}
  .page-head{align-items:flex-start;margin-bottom:20px}
  .page-head h2{font-size:22px}
  .stats{margin-bottom:22px}
  .stat{padding:15px 16px}
  .stat .value{font-size:23px}
  .card{padding:18px}
  .panel-head{padding:15px 16px}
  .panel-body{padding:16px}
  .panel-body.flush .empty{padding:44px 18px}
  table{min-width:680px}
  thead th{padding-left:14px;padding-right:14px}
  td{padding-left:14px;padding-right:14px}
  th:first-child,td:first-child{padding-left:16px}
  th:last-child,td:last-child{padding-right:16px}
  .login{padding:24px 16px}
  .login .card{padding:26px 22px}
  footer{margin-top:44px}
}
@media(max-width:700px){
  .user-table table{min-width:0}
  .user-table .col-sites,.user-table .col-tokens,.user-table .col-created{display:none}
  .row-actions{gap:3px}
  .row-actions .btn{padding:4px 7px}
  .user-table table{min-width:0}
  .token-table .col-created,.token-table .col-last-used{display:none}
  .token-table table{min-width:0}
  .res-table .owner-col,.res-table .col-state{display:none}
  .res-table table{min-width:0}
  .only-phone{display:block}
}
@media(max-width:760px){
  .site-table .col-owner,.site-table .col-type,.site-table .col-expires,.site-table .col-requests,.site-table .col-transfer{display:none}
  .site-table table{min-width:0}
  .mobile-only{display:block}
}
@media(max-width:760px){
  dialog.modal,dialog.modal.wide{width:calc(100vw - 24px);max-height:calc(100vh - 24px)}
  .modal-head{padding:16px 18px}
  .modal-body{padding:18px}
  .modal-actions{margin-top:18px}
  .panel-tools{width:100%;justify-content:space-between}
  .search{width:100%;flex:1 1 180px}
  .identity-name{max-width:110px}
}
@media(max-width:560px){
  .owner-col,.user-table .col-role{display:none}
  .user-table th:first-child,.user-table td:first-child{padding-left:16px}
  .user-table th:last-child,.user-table td:last-child{padding-right:16px}
  .topbar .actions{width:100%;justify-content:space-between}
  .nav{flex-wrap:wrap;gap:2px 20px;overflow:visible}
  .nav a{padding:9px 0 8px}
  .stat{flex-basis:130px;padding:14px 15px}
  .stat .value{font-size:22px}
  .recent-table .col-requests,.recent-table .col-transfer{display:none}
  .recent-table table{min-width:0}
}
`;

function layout(opts: {
  title: string;
  subtitle?: string;
  nav?: Array<{ href: string; label: string; active?: boolean }>;
  userLabel?: string;
  logoutAction?: string;
  content: string;
  scripts?: string;
  bare?: boolean;
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
${opts.bare ? opts.content : `<div class="wrap">
  <div class="topbar">
    <div class="brand">
      <div class="logo" aria-hidden="true">LS</div>
      <div>
        <div class="eyebrow">LinkSocks</div>
        <h1>${escapeHtml(opts.title)}</h1>
        <div class="sub">${escapeHtml(opts.subtitle || "Web proxy control plane")}</div>
      </div>
    </div>
    <div class="actions">
      ${opts.userLabel ? `<span class="identity" title="Signed in as ${escapeHtml(opts.userLabel)}"><span class="avatar" aria-hidden="true">${escapeHtml(opts.userLabel.slice(0, 1).toUpperCase())}</span><span class="identity-name">${escapeHtml(opts.userLabel)}</span></span>` : ""}
      ${logout}
    </div>
  </div>
  ${nav ? `<div class="nav">${nav}</div>` : ""}
  <div class="section">${opts.content}</div>
  <footer><span>LinkSocks.js · Web reverse proxy · D1 control plane</span><span><a href="/">Home</a> · <a href="/dashboard">Dashboard</a></span></footer>
</div>`}
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
  document.querySelectorAll("dialog.modal").forEach(function(dialog){
    dialog.querySelectorAll("[data-close]").forEach(function(button){
      button.addEventListener("click",function(){dialog.close();});
    });
    dialog.addEventListener("click",function(e){if(e.target===dialog)dialog.close();});
    if(dialog.hasAttribute("data-auto-open")){
      try{dialog.showModal();}catch(err){}
    }
  });
  document.querySelectorAll("[data-open]").forEach(function(button){
    button.addEventListener("click",function(){
      var dialog=document.getElementById(button.getAttribute("data-open"));
      if(dialog&&!dialog.open)dialog.showModal();
    });
  });
  var userSearch=document.getElementById("user-search");
  if(userSearch){
    var userRows=Array.prototype.slice.call(document.querySelectorAll("[data-user-row]"));
    var userCount=document.getElementById("user-count");
    var userEmpty=document.getElementById("user-empty");
    var userTotal=userRows.length;
    var filterUsers=function(){
      var q=userSearch.value.trim().toLowerCase(), shown=0;
      userRows.forEach(function(row){
        var hit=!q||(row.getAttribute("data-search")||"").indexOf(q)!==-1;
        row.hidden=!hit;
        if(hit)shown++;
      });
      if(userEmpty)userEmpty.hidden=shown!==0;
      if(userCount)userCount.textContent=shown===userTotal?userTotal+" account"+(userTotal===1?"":"s"):shown+" of "+userTotal+" accounts";
    };
    userSearch.addEventListener("input",filterUsers);
  }
  document.querySelectorAll("[data-combo]").forEach(function(root){
    var comboInput=root.querySelector(".combo-input");
    var comboHidden=root.querySelector("input[type=hidden]");
    var comboMenu=root.querySelector(".combo-menu");
    var comboEmpty=root.querySelector(".combo-empty");
    var comboOptions=Array.prototype.slice.call(root.querySelectorAll(".combo-option"));
    var activeIndex=-1;
    if(!comboInput||!comboHidden||!comboMenu)return;
    function visibleOptions(){return comboOptions.filter(function(option){return !option.hidden;});}
    function setComboOpen(open){
      root.classList.toggle("open",open);
      comboInput.setAttribute("aria-expanded",open?"true":"false");
      if(!open)activeIndex=-1;
    }
    function filterCombo(){
      var q=comboInput.value.trim().toLowerCase(), shown=0;
      comboOptions.forEach(function(option){
        var hit=!q||(option.getAttribute("data-label")||"").toLowerCase().indexOf(q)!==-1;
        option.hidden=!hit;
        if(hit)shown++;
      });
      if(comboEmpty)comboEmpty.hidden=shown!==0;
    }
    function chooseCombo(option){
      if(!option)return;
      comboInput.value=option.getAttribute("data-label")||"";
      comboHidden.value=option.getAttribute("data-value")||"";
      comboInput.setCustomValidity("");
      comboOptions.forEach(function(item){item.setAttribute("aria-selected","false");item.classList.remove("is-active");});
      option.setAttribute("aria-selected","true");
      setComboOpen(false);
    }
    function exactOption(){
      var value=comboInput.value.trim().toLowerCase();
      return comboOptions.filter(function(option){return (option.getAttribute("data-label")||"").toLowerCase()===value;})[0];
    }
    comboInput.addEventListener("focus",function(){filterCombo();setComboOpen(true);});
    comboInput.addEventListener("input",function(){comboHidden.value="";filterCombo();setComboOpen(true);});
    comboInput.addEventListener("blur",function(){
      if(!comboHidden.value){var exact=exactOption();if(exact)chooseCombo(exact);}
      setTimeout(function(){setComboOpen(false);},120);
    });
    comboInput.addEventListener("keydown",function(e){
      var visible=visibleOptions();
      if(e.key==="ArrowDown"||e.key==="ArrowUp"){
        if(!visible.length)return;
        e.preventDefault();
        if(e.key==="ArrowDown")activeIndex=activeIndex<0?0:Math.min(activeIndex+1,visible.length-1);
        else activeIndex=activeIndex<0?visible.length-1:Math.max(activeIndex-1,0);
        comboOptions.forEach(function(option){option.classList.remove("is-active");});
        visible[activeIndex].classList.add("is-active");
        visible[activeIndex].scrollIntoView({block:"nearest"});
      }else if(e.key==="Enter"){
        var pick=visible[activeIndex>=0?activeIndex:0];
        if(pick){e.preventDefault();chooseCombo(pick);}
      }else if(e.key==="Escape"){
        setComboOpen(false);
      }
    });
    comboOptions.forEach(function(option){
      option.addEventListener("mousedown",function(e){e.preventDefault();chooseCombo(option);});
    });
    var comboForm=root.closest("form");
    if(comboForm){
      comboForm.addEventListener("submit",function(e){
        if(!comboHidden.value){var exact=exactOption();if(exact)chooseCombo(exact);}
        if(!comboHidden.value){
          e.preventDefault();
          comboInput.setCustomValidity("Select an account from the list.");
          comboInput.reportValidity();
        }
      });
    }
    document.addEventListener("click",function(e){if(!root.contains(e.target))setComboOpen(false);});
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
  const footerParts: string[] = [`<a href="/">Back to home</a>`];
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
    bare: true,
    content: `<div class="login">
      <div class="login-box">
        <div class="login-brand"><div class="logo" aria-hidden="true">LS</div><span>LinkSocks.js</span></div>
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
      </div>
    </div>`,
  });
}

function settingsForm(opts: {
  action: string;
  csrf: string;
  settings: Settings;
  global: boolean;
  userId?: string;
  title: string;
  description: string;
  bare?: boolean;
  modal?: boolean;
}): string {
  const s = opts.settings;
  const check = (name: string, label: string, checked: boolean, hint = "") =>
    `<label class="check"><input type="checkbox" name="${name}" value="1" ${checked ? "checked" : ""}/><span class="check-body"><span class="check-label">${escapeHtml(label)}</span>${hint ? `<span class="hint">${escapeHtml(hint)}</span>` : ""}</span></label>`;
  const num = (name: string, label: string, value: number, hint: string) =>
    `<div class="field"><label for="${name}">${escapeHtml(label)}</label><input id="${name}" name="${name}" type="number" min="0" step="1" value="${value}"/><div class="hint">${escapeHtml(hint)}</div></div>`;
  const toggles = [
    opts.global ? check("allowAnonymous", "Allow anonymous providers", s.allowAnonymous, "Providers without a token can publish sites.") : "",
    opts.global ? check("allowRegistration", "Allow self-registration", s.allowRegistration, "Anyone can create an account from /register.") : "",
    check("allowCustomHost", "Allow custom hostnames", s.allowCustomHost, "Requires a hostname under WEB_ZONE."),
    opts.global ? check("forceTemporaryHost", "Force temporary hostnames", s.forceTemporaryHost, "Ignores requested hostnames and applies to registered accounts too. Every provider gets a fresh URL on connect.") : "",
  ].filter(Boolean).join("");
  const connection = opts.global
    ? `<h3 class="form-title">Connection</h3>
        <div class="form-grid">
          <div class="field span-2"><label for="publicUrl">Primary connection URL</label>
            <input id="publicUrl" name="publicUrl" type="text" inputmode="url" value="${escapeHtml(s.publicUrl)}" placeholder="relay.example.com or https://relay.example.com"/>
            <div class="hint">Used in the homepage quick start, CLI examples and the endpoint panel. Leave blank to use the host this request arrived on.</div>
          </div>
        </div>`
    : "";
  const form = `<form method="POST" action="${escapeHtml(opts.action)}">
        <input type="hidden" name="csrf" value="${escapeHtml(opts.csrf)}"/>
        <input type="hidden" name="action" value="saveSettings"/>
        ${opts.global ? `<input type="hidden" name="scope" value="global"/>` : `<input type="hidden" name="scope" value="user"/>`}
        ${opts.userId ? `<input type="hidden" name="userId" value="${escapeHtml(opts.userId)}"/>` : ""}
        ${connection}
        <h3 class="form-title">Access</h3>
        <div class="toggle-grid">${toggles}</div>
        <h3 class="form-title">Limits</h3>
        <div class="form-grid">
          ${num("maxSites", "Max sites", s.maxSites, "Per owner / per anonymous token.")}
          ${num("maxBackends", "Max backends", s.maxBackends, "Per provider registration.")}
          ${num("autoRotateSeconds", "Temporary URL lifetime (seconds)", s.autoRotateSeconds, "Seconds. 0 disables automatic expiry.")}
        </div>
        <h3 class="form-title">Quotas</h3>
        <div class="form-grid">
          ${num("quotaBytes", "Total transfer quota (bytes)", s.quotaBytes, `Lifetime cap per account or anonymous token; request and response bytes count. ${s.quotaBytes ? `Currently ${formatBytes(s.quotaBytes)}.` : "0 = unlimited."}`)}
          ${num("quotaRequests", "Total request quota", s.quotaRequests, "Lifetime cap. 0 = unlimited.")}
          ${num("quotaDurationSeconds", "Site duration (seconds)", s.quotaDurationSeconds, "Seconds. 0 = no duration limit.")}
        </div>
        ${!opts.global && s.forceTemporaryHost ? `<div class="note" style="margin-bottom:16px"><div><strong>Forced temporary hostnames are enabled</strong> by the administrator. Custom hostnames are ignored while this policy is active.</div></div>` : ""}
        <div class="${opts.modal ? "modal-actions" : "form-actions"}"><button class="btn primary" type="submit">${opts.modal ? "Save policy" : "Save settings"}</button></div>
      </form>`;
  if (opts.bare) return form;
  return `<div class="panel">
    <div class="panel-head"><div><h2>${escapeHtml(opts.title)}</h2><div class="sub">${escapeHtml(opts.description)}</div></div></div>
    <div class="panel-body">${form}</div>
  </div>`;
}

function sitesTable(sites: SiteView[], opts: { csrf: string; action: string; admin: boolean; serverUrl?: string }): string {
  if (sites.length === 0) {
    const server = opts.serverUrl || "<server>";
    return `<div class="empty">
      <div class="empty-icon">◌</div>
      <strong>No sites yet</strong>
      <div class="sub">Publish one with the CLI. It will appear here after the provider connects.</div>
      <div class="empty-command">linksocks web -u ${escapeHtml(server)} -l http://127.0.0.1:8080</div>
    </div>`;
  }
  const rows = sites
    .map((site) => {
      const type = site.temporary
        ? `<span class="badge warn">temporary</span>`
        : `<span class="badge ok">permanent</span>`;
      const status = site.providerCount > 0
        ? `<span class="badge ok"><span class="dot"></span>${site.providerCount} online</span>`
        : `<span class="badge danger"><span class="dot"></span>offline</span>`;
      const reserveButton = opts.admin && site.ownerId && !site.reserved
        ? `<form method="POST" action="${escapeHtml(opts.action)}" class="actions" data-confirm="Reserve ${escapeHtml(site.host)} for ${escapeHtml(site.owner)}?">
            <input type="hidden" name="csrf" value="${escapeHtml(opts.csrf)}"/>
            <input type="hidden" name="action" value="createReservation"/>
            <input type="hidden" name="host" value="${escapeHtml(site.host)}"/>
            <input type="hidden" name="userId" value="${escapeHtml(site.ownerId)}"/>
            <button class="btn small" type="submit">Reserve</button>
          </form>`
        : "";
      return `<tr>
        <td>
          <a class="mono" href="${escapeHtml(site.url)}" target="_blank" rel="noreferrer">${escapeHtml(site.host)}</a>
          ${opts.admin && site.reserved ? `<span class="badge accent" title="Reserved hostnames stay bound to their owner while offline">reserved</span>` : ""}
          <div class="sub">${site.anonymous ? "anonymous" : "account"} · ${site.insecure ? "TLS verify off" : "TLS verify on"}</div>
          <div class="sub mobile-only">${site.temporary ? "temporary" : "permanent"}${site.expiresAt ? ` · ${formatDuration(site.expiresAt - Date.now())} left` : ""}</div>
        </td>
        ${opts.admin ? `<td class="col-owner">${escapeHtml(site.owner)}<div class="sub mono">${escapeHtml(site.relayId.slice(0, 12))}…</div></td>` : ""}
        <td class="col-type">${type}<div class="sub">${site.rotateSeconds > 0 ? `${formatDuration(site.rotateSeconds * 1000)} rotation` : "no rotation"}</div></td>
        <td class="col-expires">${expiryCell(site)}</td>
        <td>${status}<div class="sub">${site.backendCount} origin${site.backendCount === 1 ? "" : "s"}</div></td>
        <td class="mono col-requests">${site.requests.toLocaleString()}</td>
        <td class="mono col-transfer">${escapeHtml(formatBytes(site.bytes))}</td>
        <td>
          <div class="actions">
            ${reserveButton}
            <form method="POST" action="${escapeHtml(opts.action)}" class="actions" data-confirm="Delete this site mapping? The provider will be disconnected.">
              <input type="hidden" name="csrf" value="${escapeHtml(opts.csrf)}"/>
              <input type="hidden" name="action" value="deleteSite"/>
              <input type="hidden" name="host" value="${escapeHtml(site.host)}"/>
              <button class="btn small danger" type="submit">Delete</button>
            </form>
          </div>
        </td>
      </tr>`;
    })
    .join("");
  return `<div class="table-wrap site-table"><table>
    <thead><tr><th>Host</th>${opts.admin ? `<th class="col-owner">Owner</th>` : ""}<th class="col-type">Type</th><th class="col-expires">Expires</th><th>Status</th><th class="col-requests">Requests <span class="muted">today</span></th><th class="col-transfer">Transfer <span class="muted">today</span></th><th></th></tr></thead>
    <tbody>${rows}</tbody>
  </table></div>`;
}

function recentSitesTable(sites: SiteView[], admin: boolean): string {
  if (sites.length === 0) {
    return `<div class="empty"><div class="empty-icon">◌</div><strong>No sites yet</strong><div class="sub">Published hostnames appear here.</div></div>`;
  }
  return `<div class="table-wrap compact recent-table"><table>
    <thead><tr><th>Host</th>${admin ? `<th class="owner-col">Owner</th>` : ""}<th>Status</th><th class="col-requests">Requests <span class="muted">today</span></th><th class="col-transfer">Transfer <span class="muted">today</span></th></tr></thead>
    <tbody>${sites
      .map((site) => {
        const status = site.providerCount > 0
          ? `<span class="badge ok"><span class="dot"></span>online</span>`
          : `<span class="badge danger"><span class="dot"></span>offline</span>`;
        return `<tr>
          <td><span class="mono">${escapeHtml(site.host)}</span>${admin && site.reserved ? ` <span class="badge accent">reserved</span>` : ""}<div class="sub">${site.temporary ? "temporary" : "permanent"}</div></td>
          ${admin ? `<td class="owner-col">${escapeHtml(site.owner)}</td>` : ""}
          <td>${status}<div class="sub">${site.backendCount} origin${site.backendCount === 1 ? "" : "s"}</div></td>
          <td class="mono col-requests">${site.requests.toLocaleString()}</td>
          <td class="mono col-transfer">${escapeHtml(formatBytes(site.bytes))}</td>
        </tr>`;
      })
      .join("")}</tbody>
  </table></div>`;
}

function tokensTable(tokens: TokenView[], opts: { csrf: string; action: string; admin: boolean }): string {
  if (tokens.length === 0) {
    return `<div class="empty">
      <div class="empty-icon">⌁</div>
      <strong>No tokens yet</strong>
      <div class="sub">${opts.admin ? "Tokens created by users appear here. Create one from a user's page." : "Create a token to bind providers to your account and keep their sites after reconnects."}</div>
    </div>`;
  }
  const rows = tokens
    .map(
      (token) => `<tr>
      <td class="mono">${escapeHtml(token.prefix)}…${token.revoked ? ` <span class="badge danger">revoked</span>` : ""}</td>
      ${opts.admin ? `<td class="owner-col">${escapeHtml(token.username || "")}</td>` : ""}
      <td>${escapeHtml(token.label || "—")}</td>
      <td class="col-created">${escapeHtml(formatDate(token.createdAt))}</td>
      <td class="col-last-used">${token.lastUsedAt ? escapeHtml(formatDate(token.lastUsedAt)) : `<span class="muted">never</span>`}</td>
      <td>${token.revoked ? "" : `<form method="POST" action="${escapeHtml(opts.action)}" data-confirm="Revoke this token?">
        <input type="hidden" name="csrf" value="${escapeHtml(opts.csrf)}"/>
        <input type="hidden" name="action" value="revokeToken"/>
        <input type="hidden" name="tokenId" value="${escapeHtml(token.id)}"/>
        <button class="btn small danger" type="submit">Revoke</button>
      </form>`}</td>
    </tr>`,
    )
    .join("");
  return `<div class="table-wrap token-table"><table>
    <thead><tr><th>Token</th>${opts.admin ? `<th class="owner-col">Owner</th>` : ""}<th>Label</th><th class="col-created">Created</th><th class="col-last-used">Last used</th><th></th></tr></thead>
    <tbody>${rows}</tbody>
  </table></div>`;
}

function reservationsTable(reservations: ReservationView[], opts: { csrf: string; action: string }): string {
  if (reservations.length === 0) {
    return `<div class="empty">
      <div class="empty-icon">◈</div>
      <strong>No reservations</strong>
      <div class="sub">Reserve a hostname to keep it bound to one account while its provider is offline. The reservation is invisible to users.</div>
    </div>`;
  }
  const rows = reservations
    .map((reservation) => {
      const state = reservation.active
        ? `<span class="badge ok"><span class="dot"></span>published</span>
           <div class="sub">${reservation.temporary ? "temporary" : "permanent"}${reservation.siteOwner ? ` · by ${escapeHtml(reservation.siteOwner)}` : ""}</div>`
        : `<span class="badge gray"><span class="dot"></span>held</span><div class="sub">not published</div>`;
      const meta = [
        reservation.note ? escapeHtml(reservation.note) : "",
        `<span class="nowrap">reserved ${escapeHtml(formatDateCompact(reservation.createdAt))}</span>`,
      ].filter(Boolean).join(" · ");
      return `<tr>
        <td><div class="mono">${escapeHtml(reservation.host)}</div><div class="sub">${meta}</div><div class="sub only-phone">owned by ${escapeHtml(reservation.username)}</div><div class="only-phone" style="margin-top:6px">${state}</div></td>
        <td class="owner-col">${escapeHtml(reservation.username)}<div class="sub mono">${escapeHtml(reservation.userId.slice(0, 8))}…</div></td>
        <td class="col-state">${state}</td>
        <td class="actions">
          <form method="POST" action="${escapeHtml(opts.action)}" data-confirm="Remove the reservation for ${escapeHtml(reservation.host)}?">
            <input type="hidden" name="csrf" value="${escapeHtml(opts.csrf)}"/>
            <input type="hidden" name="action" value="deleteReservation"/>
            <input type="hidden" name="host" value="${escapeHtml(reservation.host)}"/>
            <button class="btn small danger" type="submit">Release</button>
          </form>
        </td>
      </tr>`;
    })
    .join("");
  return `<div class="table-wrap compact res-table"><table>
    <thead><tr><th>Hostname</th><th class="owner-col">Owner</th><th class="col-state">State</th><th></th></tr></thead>
    <tbody>${rows}</tbody>
  </table></div>`;
}

function pageHead(title: string, description: string): string {
  return `<div class="page-head"><div><h2>${escapeHtml(title)}</h2><div class="sub">${escapeHtml(description)}</div></div></div>`;
}

function statCard(label: string, value: string | number, hint = ""): string {
  return `<div class="stat"><div class="label">${escapeHtml(label)}</div><div class="value">${escapeHtml(value)}</div>${hint ? `<div class="hint">${escapeHtml(hint)}</div>` : ""}</div>`;
}

function modalDialog(opts: {
  id: string;
  title: string;
  subtitle?: string;
  body: string;
  auto?: boolean;
  wide?: boolean;
}): string {
  return `<dialog class="modal${opts.wide ? " wide" : ""}" id="${escapeHtml(opts.id)}"${opts.auto ? " data-auto-open" : ""}>
    <div class="modal-head">
      <div><h2>${escapeHtml(opts.title)}</h2>${opts.subtitle ? `<div class="sub">${escapeHtml(opts.subtitle)}</div>` : ""}</div>
      <button type="button" class="modal-close" data-close aria-label="Close">&times;</button>
    </div>
    <div class="modal-body">${opts.body}</div>
  </dialog>`;
}

function ownerCombobox(users: UserView[]): string {
  const options = users
    .map(
      (user) => `<button type="button" class="combo-option" role="option" aria-selected="false" data-value="${escapeHtml(user.id)}" data-label="${escapeHtml(user.username)}">
        <span class="who">${escapeHtml(user.username)}</span>${user.isAdmin ? `<span class="role">admin</span>` : ""}
      </button>`,
    )
    .join("");
  return `<div class="combo" data-combo>
    <input id="reserve-owner" class="combo-input" type="text" name="userName" placeholder="Search accounts" autocomplete="off" autocapitalize="off" spellcheck="false" role="combobox" aria-expanded="false" aria-controls="reserve-owner-menu" required/>
    <input type="hidden" name="userId"/>
    <div class="combo-menu" id="reserve-owner-menu" role="listbox">${options}<div class="combo-empty" hidden>No matching account.</div></div>
  </div>`;
}

export function renderAdminPage(data: AdminPageData): string {
  const nav = [
    { href: "/admin?tab=overview", label: "Overview", active: data.tab === "overview" },
    { href: "/admin?tab=users", label: "Users", active: data.tab === "users" },
    { href: "/admin?tab=sites", label: "Sites", active: data.tab === "sites" },
    { href: "/admin?tab=tokens", label: "Tokens", active: data.tab === "tokens" },
    { href: "/admin?tab=reservations", label: "Reservations", active: data.tab === "reservations" },
    { href: "/admin?tab=settings", label: "Settings", active: data.tab === "settings" },
    { href: "/dashboard", label: "My dashboard", active: false },
  ];

  let content = data.flash?.token ? "" : flashHtml(data.flash);
  if (data.tab === "overview") {
    content += pageHead("Overview", "A live snapshot of accounts, published sites and traffic. Request and transfer figures cover the current UTC day.");
    content += `<div class="stats">
      ${statCard("Users", data.overview.users, "registered accounts")}
      ${statCard("Sites", data.overview.sites, `${data.overview.onlineSites} online now`)}
      ${statCard("Live providers", data.overview.providers, "connected providers")}
      ${statCard("Requests", data.overview.requests.toLocaleString(), "today UTC")}
      ${statCard("Transfer", formatBytes(data.overview.bytes), "today UTC")}
      ${statCard("Tokens", data.overview.tokens, "active provider tokens")}
    </div>
    <div class="section two-col">
      <div class="panel">
        <div class="panel-head"><h2>Recent sites</h2><a class="btn small ghost" href="/admin?tab=sites">View all</a></div>
        <div class="panel-body flush">${recentSitesTable(data.sites.slice(0, 8), true)}</div>
      </div>
      <div class="panel">
        <div class="panel-head"><h2>Anonymous policy</h2><a class="btn small ghost" href="/admin?tab=settings">Edit</a></div>
        <div class="panel-body">
          <div class="policy">
            <div class="policy-row"><span>Anonymous access</span><b>${data.globalSettings.allowAnonymous ? "enabled" : "disabled"}</b></div>
            <div class="policy-row"><span>Custom hostnames</span><b>${data.globalSettings.allowCustomHost ? "allowed" : "blocked"}</b></div>
            <div class="policy-row"><span>Forced temporary hosts</span><b>${data.globalSettings.forceTemporaryHost ? "enabled" : "off"}</b></div>
            <div class="policy-row"><span>Temporary URL lifetime</span><b>${formatDuration(data.globalSettings.autoRotateSeconds * 1000)}</b></div>
            <div class="policy-row"><span>Total transfer quota</span><b>${data.globalSettings.quotaBytes ? `${formatBytes(data.globalSettings.quotaBytes)} total` : "unlimited"}</b></div>
            <div class="policy-row"><span>Total request quota</span><b>${data.globalSettings.quotaRequests ? `${data.globalSettings.quotaRequests.toLocaleString()} total` : "unlimited"}</b></div>
          </div>
        </div>
      </div>
    </div>`;
  } else if (data.tab === "users") {
    const selected = data.selectedUser;
    const userRows = data.users
      .map(
        (user) => `<tr data-user-row data-search="${escapeHtml((user.username + " " + user.id).toLowerCase())}">
            <td class="user-name"><b>${escapeHtml(user.username)}</b>${user.disabled ? ` <span class="badge danger">disabled</span>` : ""}<div class="sub mono">${escapeHtml(user.id.slice(0, 8))}…</div></td>
            <td class="col-role">${user.isAdmin ? `<span class="badge">admin</span>` : `<span class="badge gray">user</span>`}</td>
            <td class="mono col-sites">${user.siteCount}</td>
            <td class="mono col-tokens">${user.tokenCount}</td>
            <td class="col-created">${escapeHtml(formatDate(user.createdAt))}</td>
            <td><div class="row-actions">
              <a class="btn small" href="/admin?tab=users&user=${encodeURIComponent(user.id)}&modal=policy" aria-haspopup="dialog">Policy</a>
              <a class="btn small" href="/admin?tab=users&user=${encodeURIComponent(user.id)}&modal=password" aria-haspopup="dialog">Password</a>
              <a class="btn small" href="/admin?tab=users&user=${encodeURIComponent(user.id)}&modal=access" aria-haspopup="dialog">Access</a>
            </div></td>
          </tr>`,
      )
      .join("");
    content += pageHead("Users", "Accounts, roles and per-user policy. Every action opens in a focused dialog.");
    content += `<div class="panel">
      <div class="panel-head">
        <div><h2 id="user-count">${data.users.length} account${data.users.length === 1 ? "" : "s"}</h2><div class="sub">Search by username or account ID.</div></div>
        <div class="panel-tools">
          <input id="user-search" class="search" type="search" placeholder="Search users…" aria-label="Search users" autocomplete="off"/>
          <button class="btn primary" type="button" data-open="create-user">New user</button>
        </div>
      </div>
      <div class="panel-body flush"><div class="table-wrap user-table"><table>
        <thead><tr><th>User</th><th class="col-role">Role</th><th class="col-sites">Sites</th><th class="col-tokens">Tokens</th><th class="col-created">Created</th><th>Actions</th></tr></thead>
        <tbody>${userRows}<tr id="user-empty" hidden><td colspan="6"><div class="empty" style="padding:32px 18px"><strong>No accounts match</strong><div class="sub">Try a different username or ID.</div></div></td></tr></tbody>
      </table></div></div>
    </div>`;

    content += modalDialog({
      id: "create-user",
      title: "Create user",
      subtitle: "The account can publish sites with its own tokens.",
      body: `<form method="POST" action="/admin/action">
        <input type="hidden" name="csrf" value="${escapeHtml(data.csrf)}"/>
        <input type="hidden" name="action" value="createUser"/>
        <div class="form-grid">
          <div class="field"><label for="new-username">Username</label><input id="new-username" name="username" autocomplete="off" required/></div>
          <div class="field"><label for="new-password">Password</label><input id="new-password" name="password" type="password" autocomplete="new-password" minlength="8" required/><div class="hint">At least 8 characters.</div></div>
        </div>
        <label class="check"><input type="checkbox" name="isAdmin" value="1"/> Grant administrator role</label>
        <div class="modal-actions"><button type="button" class="btn" data-close>Cancel</button><button class="btn primary" type="submit">Create user</button></div>
      </form>`,
    });

    if (selected) {
      const preserve = `${selected.isAdmin ? `<input type="hidden" name="isAdmin" value="1"/>` : ""}${selected.disabled ? `<input type="hidden" name="disabled" value="1"/>` : ""}`;
      content += modalDialog({
        id: "user-password",
        title: "Change password",
        subtitle: selected.username,
        auto: data.userModal === "password",
        body: `<form method="POST" action="/admin/action">
          <input type="hidden" name="csrf" value="${escapeHtml(data.csrf)}"/>
          <input type="hidden" name="action" value="updateUser"/>
          <input type="hidden" name="userId" value="${escapeHtml(selected.id)}"/>
          ${preserve}
          <div class="field"><label for="reset-password">New password</label><input id="reset-password" name="password" type="password" autocomplete="new-password" minlength="8" required autofocus/><div class="hint">At least 8 characters. The account keeps its current role and access state.</div></div>
          <div class="modal-actions"><button type="button" class="btn" data-close>Cancel</button><button class="btn primary" type="submit">Update password</button></div>
        </form>`,
      });
      content += modalDialog({
        id: "user-access",
        title: "Account access",
        subtitle: selected.username,
        auto: data.userModal === "access",
        body: `<form method="POST" action="/admin/action">
          <input type="hidden" name="csrf" value="${escapeHtml(data.csrf)}"/>
          <input type="hidden" name="action" value="updateUser"/>
          <input type="hidden" name="userId" value="${escapeHtml(selected.id)}"/>
          <label class="check"><input type="checkbox" name="isAdmin" value="1" ${selected.isAdmin ? "checked" : ""}/><span class="check-body"><span class="check-label">Administrator</span><span class="hint">Can manage users, sites, reservations and global policy.</span></span></label>
          <label class="check"><input type="checkbox" name="disabled" value="1" ${selected.disabled ? "checked" : ""}/><span class="check-body"><span class="check-label">Disabled</span><span class="hint">Blocks sign-in and rejects this account's provider tokens.</span></span></label>
          <div class="modal-actions">
            <button type="button" class="btn danger danger-left" data-open="user-delete">Delete account</button>
            <button type="button" class="btn" data-close>Cancel</button>
            <button class="btn primary" type="submit">Save access</button>
          </div>
        </form>`,
      });
      if (data.selectedUserSettings) {
        content += modalDialog({
          id: "user-policy",
          title: `Policy for ${selected.username}`,
          subtitle: "Limits applied to sites created with this account's tokens.",
          auto: data.userModal === "policy",
          wide: true,
          body: settingsForm({
            action: "/admin/action",
            csrf: data.csrf,
            settings: {
              ...data.selectedUserSettings,
              forceTemporaryHost: data.selectedUserSettings.forceTemporaryHost || data.globalSettings.forceTemporaryHost,
            },
            global: false,
            userId: selected.id,
            title: `Policy for ${selected.username}`,
            description: "Per-user limits applied to sites created with this user's tokens.",
            bare: true,
            modal: true,
          }),
        });
      }
      content += modalDialog({
        id: "user-delete",
        title: "Delete account",
        subtitle: selected.username,
        auto: data.userModal === "delete",
        body: `<form method="POST" action="/admin/action">
          <input type="hidden" name="csrf" value="${escapeHtml(data.csrf)}"/>
          <input type="hidden" name="action" value="deleteUser"/>
          <input type="hidden" name="userId" value="${escapeHtml(selected.id)}"/>
          <p class="sub" style="margin:0 0 18px">This removes the account, its sessions, tokens and sites. The action cannot be undone.</p>
          <div class="modal-actions"><button type="button" class="btn" data-close>Cancel</button><button class="btn danger" type="submit">Delete account</button></div>
        </form>`,
      });
    }
  } else if (data.tab === "sites") {
    content += pageHead("Sites", "Every published hostname, its owner, origins and today's traffic. Reserved hosts stay bound to their owner while offline.");
    content += `<div class="panel"><div class="panel-head"><h2>${data.sites.length} site${data.sites.length === 1 ? "" : "s"}</h2></div><div class="panel-body flush">${sitesTable(data.sites, { csrf: data.csrf, action: "/admin/action", admin: true, serverUrl: data.serverUrl })}</div></div>`;
  } else if (data.tab === "tokens") {
    content += pageHead("Tokens", "Provider tokens are bound to the account that created them. Raw tokens are shown only once.");
    content += `<div class="panel"><div class="panel-head"><h2>${data.tokens.length} token${data.tokens.length === 1 ? "" : "s"}</h2></div><div class="panel-body flush">${tokensTable(data.tokens, { csrf: data.csrf, action: "/admin/action", admin: true })}</div></div>`;
  } else if (data.tab === "reservations") {
    content += pageHead("Reservations", "Admin-only hostname holds. A reservation keeps a name bound to one account while its provider is offline, and is invisible on the user side.");
    content += `<div class="two-col">
      <div class="panel">
        <div class="panel-head"><h2>${data.reservations.length} reservation${data.reservations.length === 1 ? "" : "s"}</h2></div>
        <div class="panel-body flush">${reservationsTable(data.reservations, { csrf: data.csrf, action: "/admin/action" })}</div>
      </div>
      <div class="panel combo-panel">
        <div class="panel-head"><h2>Reserve a hostname</h2></div>
        <div class="panel-body">
          <form method="POST" action="/admin/action">
            <input type="hidden" name="csrf" value="${escapeHtml(data.csrf)}"/>
            <input type="hidden" name="action" value="createReservation"/>
            <div class="field"><label for="reserve-host">Hostname</label><input id="reserve-host" name="host" placeholder="blog${data.zone ? `.${escapeHtml(data.zone)}` : ".example.com"}" required/><div class="hint">${data.zone ? `Must be a subdomain of ${escapeHtml(data.zone)}.` : "WEB_ZONE is not configured; custom hosts cannot be validated."}</div></div>
            <div class="field"><label for="reserve-owner">Owner</label>${ownerCombobox(data.users)}<div class="hint">Type to search accounts. Only the selected account can claim this hostname.</div></div>
            <div class="field"><label for="reserve-note">Note</label><input id="reserve-note" name="note" placeholder="optional, admin only"/></div>
            <button class="btn primary" type="submit">Create reservation</button>
          </form>
          <div class="note" style="margin-top:14px"><div>Reservations do not proxy anything. The hostname keeps returning <strong>Unknown site</strong> until its owner publishes it again. Other accounts cannot claim it.</div></div>
        </div>
      </div>
    </div>`;
  } else {
    content += pageHead("Settings", "Global defaults for anonymous providers. Registered accounts keep their own limits; force-temporary mode is global.");
    content += settingsForm({
      action: "/admin/action",
      csrf: data.csrf,
      settings: data.globalSettings,
      global: true,
      title: "Global anonymous policy",
      description: "Defaults for providers that connect without a registered token.",
    });
  }

  if (data.flash?.token) content += tokenCreatedDialog(data.flash.token);

  return layout({
    title: "Admin console",
    subtitle: "Users, tokens, sites, reservations and quotas",
    nav,
    userLabel: data.user.username,
    logoutAction: "/admin?logout",
    content,
  });
}

function createTokenDialog(csrf: string, serverUrl: string): string {
  return modalDialog({
    id: "create-token",
    title: "Create token",
    subtitle: `Used by the provider CLI with ${serverUrl}.`,
    body: `<form method="POST" action="/dashboard/action">
      <input type="hidden" name="csrf" value="${escapeHtml(csrf)}"/>
      <input type="hidden" name="action" value="createToken"/>
      <div class="field"><label for="token-label">Label</label><input id="token-label" name="label" placeholder="laptop / ci" autocomplete="off"/><div class="hint">Only for your own reference. The raw token is shown once after creation.</div></div>
      <div class="modal-actions"><button type="button" class="btn" data-close>Cancel</button><button class="btn primary" type="submit">Create token</button></div>
    </form>`,
  });
}

function tokenCreatedDialog(token: string): string {
  return modalDialog({
    id: "token-created",
    title: "Token created",
    subtitle: "Copy it now. It will not be shown again.",
    auto: true,
    body: `<pre class="token-value" id="new-token">${escapeHtml(token)}</pre>
      <div class="modal-actions"><button type="button" class="btn" data-copy="new-token">Copy</button><button type="button" class="btn primary" data-close>Done</button></div>`,
  });
}

export function renderDashboardPage(data: DashboardPageData): string {
  const nav = [
    { href: "/dashboard?tab=overview", label: "Overview", active: data.tab === "overview" },
    { href: "/dashboard?tab=sites", label: "My sites", active: data.tab === "sites" },
    { href: "/dashboard?tab=tokens", label: "Tokens", active: data.tab === "tokens" },
  ];
  if (data.user.isAdmin) {
    nav.push({ href: "/admin", label: "Admin console", active: false });
  }
  let content = data.flash?.token ? "" : flashHtml(data.flash);
  if (data.tab === "overview") {
    content += pageHead("Overview", "Your sites, origins and today's traffic.");
    content += `<div class="stats">
      ${statCard("My sites", data.overview.sites, `${data.overview.onlineSites} online now`)}
      ${statCard("Live providers", data.overview.providers, "connected providers")}
      ${statCard("Requests", data.overview.requests.toLocaleString(), "today UTC")}
      ${statCard("Transfer", formatBytes(data.overview.bytes), "today UTC")}
      ${statCard("Tokens", data.overview.tokens, "provider tokens")}
    </div>
    <div class="section panel">
      <div class="panel-head"><h2>My sites</h2><a class="btn small ghost" href="/dashboard?tab=sites">View all</a></div>
      <div class="panel-body flush">${sitesTable(data.sites.slice(0, 8), { csrf: data.csrf, action: "/dashboard/action", admin: false, serverUrl: data.serverUrl })}</div>
    </div>`;
  } else if (data.tab === "sites") {
    content += pageHead("My sites", "Each site can have multiple origins behind one hostname. Temporary hosts expire automatically.");
    content += `<div class="panel"><div class="panel-head"><h2>${data.sites.length} site${data.sites.length === 1 ? "" : "s"}</h2></div><div class="panel-body flush">${sitesTable(data.sites, { csrf: data.csrf, action: "/dashboard/action", admin: false, serverUrl: data.serverUrl })}</div></div>`;
  } else if (data.tab === "tokens") {
    content += pageHead("Tokens", "Tokens bind providers to your account. Keep them private.");
    content += `<div class="panel">
      <div class="panel-head">
        <div><h2>${data.tokens.length} token${data.tokens.length === 1 ? "" : "s"}</h2><div class="sub">Use one with <span class="mono">linksocks web -u ${escapeHtml(data.serverUrl)} -t &lt;token&gt;</span>.</div></div>
        <div class="panel-tools"><button class="btn primary" type="button" data-open="create-token">Create token</button></div>
      </div>
      <div class="panel-body flush">${tokensTable(data.tokens, { csrf: data.csrf, action: "/dashboard/action", admin: false })}</div>
    </div>`;
    content += createTokenDialog(data.csrf, data.serverUrl);
    if (data.flash?.token) content += tokenCreatedDialog(data.flash.token);
  }
  return layout({
    title: "My dashboard",
    subtitle: "Your web proxy sites, origins and traffic",
    nav,
    userLabel: data.user.username,
    logoutAction: "/dashboard?logout",
    content,
  });
}

export function renderErrorPage(title: string, message: string, code = "Error"): string {
  return layout({
    title,
    bare: true,
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
