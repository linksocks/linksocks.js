import type { Env } from "./types";

/**
 * D1-backed user, token, site and session store.
 *
 * The schema is created lazily on first use so a fresh deployment works
 * without running a separate migration step. `migrations/0001_web.sql`
 * mirrors the same statements for teams that prefer migrations.
 */

export interface Settings {
  allowAnonymous: boolean;
  allowCustomHost: boolean;
  allowRegistration: boolean;
  allowUserSettings: boolean;
  autoRotateSeconds: number;
  quotaBytes: number;
  quotaRequests: number;
  quotaDurationSeconds: number;
  maxSites: number;
  maxBackends: number;
}

export const DEFAULT_ANONYMOUS_SETTINGS: Settings = {
  allowAnonymous: true,
  allowCustomHost: false,
  allowRegistration: false,
  allowUserSettings: true,
  autoRotateSeconds: 6 * 60 * 60,
  quotaBytes: 256 * 1024 * 1024,
  quotaRequests: 5000,
  quotaDurationSeconds: 24 * 60 * 60,
  maxSites: 3,
  maxBackends: 8,
};

export const DEFAULT_USER_SETTINGS: Settings = {
  allowAnonymous: true,
  allowCustomHost: true,
  allowRegistration: false,
  allowUserSettings: true,
  autoRotateSeconds: 24 * 60 * 60,
  quotaBytes: 0,
  quotaRequests: 0,
  quotaDurationSeconds: 0,
  maxSites: 20,
  maxBackends: 16,
};

export interface UserRow {
  id: string;
  username: string;
  password_hash: string;
  is_admin: number;
  disabled: number;
  created_at: number;
  last_login_at: number | null;
}

export interface TokenRow {
  id: string;
  user_id: string;
  token_hash: string;
  token_prefix: string;
  label: string | null;
  created_at: number;
  last_used_at: number | null;
  revoked: number;
}

export interface SiteRow {
  host: string;
  relay_id: string;
  owner_id: string | null;
  token_hash: string | null;
  temporary: number;
  expires_at: number | null;
  rotate_seconds: number;
  backends_json: string;
  backend_count: number;
  insecure: number;
  anonymous: number;
  quota_bytes: number;
  quota_requests: number;
  created_at: number;
  updated_at: number;
  last_seen_at: number | null;
}

export interface SessionRow {
  id: string;
  user_id: string;
  csrf: string;
  created_at: number;
  expires_at: number;
}

const SCHEMA: string[] = [
  `CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    is_admin INTEGER NOT NULL DEFAULT 0,
    disabled INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    last_login_at INTEGER
  )`,
  `CREATE TABLE IF NOT EXISTS tokens (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    token_hash TEXT NOT NULL UNIQUE,
    token_prefix TEXT NOT NULL,
    label TEXT,
    created_at INTEGER NOT NULL,
    last_used_at INTEGER,
    revoked INTEGER NOT NULL DEFAULT 0
  )`,
  `CREATE INDEX IF NOT EXISTS idx_tokens_user ON tokens(user_id)`,
  `CREATE INDEX IF NOT EXISTS idx_tokens_hash ON tokens(token_hash)`,
  `CREATE TABLE IF NOT EXISTS sites (
    host TEXT PRIMARY KEY,
    relay_id TEXT NOT NULL,
    owner_id TEXT,
    token_hash TEXT,
    temporary INTEGER NOT NULL DEFAULT 0,
    expires_at INTEGER,
    rotate_seconds INTEGER NOT NULL DEFAULT 0,
    backends_json TEXT NOT NULL DEFAULT '[]',
    backend_count INTEGER NOT NULL DEFAULT 0,
    insecure INTEGER NOT NULL DEFAULT 0,
    anonymous INTEGER NOT NULL DEFAULT 0,
    quota_bytes INTEGER NOT NULL DEFAULT 0,
    quota_requests INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    last_seen_at INTEGER
  )`,
  `CREATE INDEX IF NOT EXISTS idx_sites_owner ON sites(owner_id)`,
  `CREATE INDEX IF NOT EXISTS idx_sites_relay ON sites(relay_id)`,
  `CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    csrf TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL
  )`,
  `CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id)`,
  `CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at INTEGER NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS quota_usage (
    principal TEXT PRIMARY KEY,
    bytes INTEGER NOT NULL DEFAULT 0,
    requests INTEGER NOT NULL DEFAULT 0
  )`,
];

let schemaPromise: Promise<void> | null = null;

export async function ensureSchema(env: Env): Promise<void> {
  if (!env.DB) {
    throw new Error("D1 binding DB is not configured");
  }
  if (!schemaPromise) {
    schemaPromise = initSchema(env).catch((err) => {
      schemaPromise = null;
      throw err;
    });
  }
  return schemaPromise;
}

async function initSchema(env: Env): Promise<void> {
  const db = requireDb(env);
  await db.batch(SCHEMA.map((sql) => db.prepare(sql)));
  await ensureAdminUser(env);
}

function requireDb(env: Env): D1Database {
  if (!env.DB) throw new Error("D1 binding DB is not configured");
  return env.DB;
}

export async function consumeQuota(
  env: Env,
  principal: string,
  bytes: number,
  requests: number,
  maxBytes: number,
  maxRequests: number,
): Promise<boolean> {
  const result = await requireDb(env)
    .prepare(
      `INSERT INTO quota_usage (principal, bytes, requests)
       SELECT ?, ?, ? WHERE (? = 0 OR ? <= ?) AND (? = 0 OR ? <= ?)
       ON CONFLICT(principal) DO UPDATE SET
         bytes = quota_usage.bytes + excluded.bytes,
         requests = quota_usage.requests + excluded.requests
       WHERE (? = 0 OR quota_usage.bytes + excluded.bytes <= ?)
         AND (excluded.requests = 0 OR ? = 0 OR quota_usage.bytes < ?)
         AND (? = 0 OR quota_usage.requests + excluded.requests <= ?)`,
    )
    .bind(principal, bytes, requests, maxBytes, bytes, maxBytes, maxRequests, requests, maxRequests,
      maxBytes, maxBytes, maxBytes, maxBytes, maxRequests, maxRequests)
    .run();
  return result.meta.changes === 1;
}

export async function quotaCanFit(env: Env, principal: string, bytes: number, maxBytes: number): Promise<boolean> {
  if (maxBytes === 0) return true;
  const usage = await requireDb(env).withSession("first-primary")
    .prepare("SELECT bytes FROM quota_usage WHERE principal = ?")
    .bind(principal)
    .first<{ bytes: number }>();
  return Number(usage?.bytes || 0) + bytes <= maxBytes;
}

async function ensureAdminUser(env: Env): Promise<void> {
  if (!env.ADMIN_PASSWORD) return;
  const db = requireDb(env);
  const existingAdmin = await db
    .prepare("SELECT id FROM users WHERE is_admin = 1 LIMIT 1")
    .first<{ id: string }>();
  if (existingAdmin) return;

  const username = (env.ADMIN_USERNAME || "admin").trim().toLowerCase();
  const existing = await findUserByUsername(env, username);
  if (existing) {
    await db
      .prepare("UPDATE users SET is_admin = 1, disabled = 0, password_hash = ? WHERE id = ?")
      .bind(await hashPassword(env.ADMIN_PASSWORD), existing.id)
      .run();
    return;
  }

  await createUser(env, username, env.ADMIN_PASSWORD, true);
}

/* -------------------------------------------------------------------------- */
/* Passwords and sessions                                                     */
/* -------------------------------------------------------------------------- */

const PBKDF2_ITERATIONS = 120_000;

function toBase64(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
}

function fromBase64(value: string): Uint8Array {
  const binary = atob(value);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

function constantTimeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}

export async function hashPassword(password: string): Promise<string> {
  const salt = crypto.getRandomValues(new Uint8Array(16));
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(password),
    "PBKDF2",
    false,
    ["deriveBits"],
  );
  const bits = await crypto.subtle.deriveBits(
    { name: "PBKDF2", hash: "SHA-256", salt, iterations: PBKDF2_ITERATIONS },
    key,
    256,
  );
  return `pbkdf2$${PBKDF2_ITERATIONS}$${toBase64(salt)}$${toBase64(new Uint8Array(bits))}`;
}

export async function verifyPassword(password: string, stored: string): Promise<boolean> {
  const parts = (stored || "").split("$");
  if (parts.length !== 4 || parts[0] !== "pbkdf2") return false;
  const iterations = Number(parts[1]);
  if (!Number.isFinite(iterations) || iterations < 1000) return false;
  const salt = fromBase64(parts[2]);
  const expected = parts[3];
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(password),
    "PBKDF2",
    false,
    ["deriveBits"],
  );
  const bits = await crypto.subtle.deriveBits(
    { name: "PBKDF2", hash: "SHA-256", salt, iterations },
    key,
    256,
  );
  return constantTimeEqual(toBase64(new Uint8Array(bits)), expected);
}

export function parseCookies(request: Request): Record<string, string> {
  const header = request.headers.get("Cookie") || "";
  const cookies: Record<string, string> = {};
  for (const part of header.split(";")) {
    const index = part.indexOf("=");
    if (index <= 0) continue;
    const name = part.slice(0, index).trim();
    const value = part.slice(index + 1).trim();
    if (name) cookies[name] = decodeURIComponent(value);
  }
  return cookies;
}

export function sessionCookie(id: string, maxAgeSeconds: number): string {
  return `ls_session=${encodeURIComponent(id)}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=${maxAgeSeconds}`;
}

export function clearSessionCookie(): string {
  return "ls_session=; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=0";
}

export interface SessionContext {
  session: SessionRow;
  user: UserRow;
}

export async function createSession(env: Env, userId: string): Promise<SessionRow> {
  const db = requireDb(env);
  const now = Date.now();
  const session: SessionRow = {
    id: randomHex(32),
    user_id: userId,
    csrf: randomHex(24),
    created_at: now,
    expires_at: now + 7 * 24 * 60 * 60 * 1000,
  };
  await db
    .prepare(
      "INSERT INTO sessions (id, user_id, csrf, created_at, expires_at) VALUES (?, ?, ?, ?, ?)",
    )
    .bind(session.id, session.user_id, session.csrf, session.created_at, session.expires_at)
    .run();
  return session;
}

export async function getSessionContext(env: Env, request: Request): Promise<SessionContext | null> {
  const sessionId = parseCookies(request).ls_session;
  if (!sessionId) return null;
  const db = requireDb(env);
  const row = await db
    .prepare(
      `SELECT s.id, s.user_id, s.csrf, s.created_at, s.expires_at,
              u.username, u.password_hash, u.is_admin, u.disabled,
              u.created_at AS user_created_at, u.last_login_at
         FROM sessions s
         JOIN users u ON u.id = s.user_id
        WHERE s.id = ? LIMIT 1`,
    )
    .bind(sessionId)
    .first<SessionRow & UserRow & { user_created_at: number }>();
  if (!row) return null;
  if (row.expires_at <= Date.now() || row.disabled) {
    await db.prepare("DELETE FROM sessions WHERE id = ?").bind(sessionId).run();
    return null;
  }
  return {
    session: {
      id: row.id,
      user_id: row.user_id,
      csrf: row.csrf,
      created_at: row.created_at,
      expires_at: row.expires_at,
    },
    user: {
      id: row.user_id,
      username: row.username,
      password_hash: row.password_hash,
      is_admin: row.is_admin,
      disabled: row.disabled,
      created_at: row.user_created_at,
      last_login_at: row.last_login_at,
    },
  };
}

export async function deleteSession(env: Env, sessionId: string): Promise<void> {
  await requireDb(env).prepare("DELETE FROM sessions WHERE id = ?").bind(sessionId).run();
}

export async function deleteExpiredSessions(env: Env): Promise<void> {
  await requireDb(env).prepare("DELETE FROM sessions WHERE expires_at <= ?").bind(Date.now()).run();
}

/* -------------------------------------------------------------------------- */
/* Users                                                                      */
/* -------------------------------------------------------------------------- */

export async function findUserByUsername(env: Env, username: string): Promise<UserRow | null> {
  return (
    (await requireDb(env)
      .prepare("SELECT * FROM users WHERE username = ? LIMIT 1")
      .bind(username.trim().toLowerCase())
      .first<UserRow>()) || null
  );
}

export async function findUserById(env: Env, id: string): Promise<UserRow | null> {
  return (
    (await requireDb(env).prepare("SELECT * FROM users WHERE id = ? LIMIT 1").bind(id).first<UserRow>()) ||
    null
  );
}

export async function createUser(
  env: Env,
  username: string,
  password: string,
  isAdmin = false,
): Promise<UserRow> {
  const db = requireDb(env);
  const normalized = username.trim().toLowerCase();
  if (!/^[a-z0-9][a-z0-9_.-]{2,31}$/.test(normalized)) {
    throw new Error("Username must be 3-32 characters (a-z, 0-9, _, ., -)");
  }
  if (password.length < 8) {
    throw new Error("Password must be at least 8 characters");
  }
  if (await findUserByUsername(env, normalized)) {
    throw new Error("Username already exists");
  }
  const user: UserRow = {
    id: crypto.randomUUID(),
    username: normalized,
    password_hash: await hashPassword(password),
    is_admin: isAdmin ? 1 : 0,
    disabled: 0,
    created_at: Date.now(),
    last_login_at: null,
  };
  await db
    .prepare(
      "INSERT INTO users (id, username, password_hash, is_admin, disabled, created_at, last_login_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(
      user.id,
      user.username,
      user.password_hash,
      user.is_admin,
      user.disabled,
      user.created_at,
      user.last_login_at,
    )
    .run();
  return user;
}

export async function listUsers(env: Env): Promise<UserRow[]> {
  const result = await requireDb(env)
    .prepare("SELECT * FROM users ORDER BY created_at ASC")
    .all<UserRow>();
  return result.results || [];
}

export async function updateUser(
  env: Env,
  userId: string,
  updates: { password?: string; isAdmin?: boolean; disabled?: boolean },
): Promise<void> {
  const db = requireDb(env);
  const sets: string[] = [];
  const values: unknown[] = [];
  if (updates.password) {
    sets.push("password_hash = ?");
    values.push(await hashPassword(updates.password));
  }
  if (typeof updates.isAdmin === "boolean") {
    sets.push("is_admin = ?");
    values.push(updates.isAdmin ? 1 : 0);
  }
  if (typeof updates.disabled === "boolean") {
    sets.push("disabled = ?");
    values.push(updates.disabled ? 1 : 0);
  }
  if (sets.length === 0) return;
  values.push(userId);
  await db.prepare(`UPDATE users SET ${sets.join(", ")} WHERE id = ?`).bind(...values).run();
  if (updates.disabled) {
    await db.prepare("DELETE FROM sessions WHERE user_id = ?").bind(userId).run();
  }
}

export async function touchUserLogin(env: Env, userId: string): Promise<void> {
  await requireDb(env)
    .prepare("UPDATE users SET last_login_at = ? WHERE id = ?")
    .bind(Date.now(), userId)
    .run();
}

export async function deleteUser(env: Env, userId: string): Promise<void> {
  const db = requireDb(env);
  await db.prepare("DELETE FROM tokens WHERE user_id = ?").bind(userId).run();
  await db.prepare("DELETE FROM sessions WHERE user_id = ?").bind(userId).run();
  await db.prepare("DELETE FROM settings WHERE key = ?").bind(`user:${userId}`).run();
  await db.prepare("DELETE FROM users WHERE id = ?").bind(userId).run();
}

/* -------------------------------------------------------------------------- */
/* Tokens                                                                     */
/* -------------------------------------------------------------------------- */

export async function sha256Hex(input: string): Promise<string> {
  const data = new TextEncoder().encode(input);
  const hash = await crypto.subtle.digest("SHA-256", data);
  return Array.from(new Uint8Array(hash))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

export function randomHex(byteLength: number): string {
  const bytes = crypto.getRandomValues(new Uint8Array(byteLength));
  return Array.from(bytes)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

export interface CreatedToken {
  raw: string;
  row: TokenRow;
}

export async function createToken(
  env: Env,
  userId: string,
  label: string,
): Promise<CreatedToken> {
  const db = requireDb(env);
  const raw = `ls_${randomHex(24)}`;
  const row: TokenRow = {
    id: crypto.randomUUID(),
    user_id: userId,
    token_hash: await sha256Hex(raw),
    token_prefix: raw.slice(0, 11),
    label: label.trim() || null,
    created_at: Date.now(),
    last_used_at: null,
    revoked: 0,
  };
  await db
    .prepare(
      "INSERT INTO tokens (id, user_id, token_hash, token_prefix, label, created_at, last_used_at, revoked) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(
      row.id,
      row.user_id,
      row.token_hash,
      row.token_prefix,
      row.label,
      row.created_at,
      row.last_used_at,
      row.revoked,
    )
    .run();
  return { raw, row };
}

export async function listTokens(env: Env, userId?: string): Promise<TokenRow[]> {
  const db = requireDb(env);
  const query = userId
    ? db.prepare("SELECT * FROM tokens WHERE user_id = ? ORDER BY created_at DESC").bind(userId)
    : db.prepare("SELECT * FROM tokens ORDER BY created_at DESC");
  const result = await query.all<TokenRow>();
  return result.results || [];
}

export async function findTokenRow(env: Env, tokenId: string): Promise<TokenRow | null> {
  return (
    (await requireDb(env)
      .prepare("SELECT * FROM tokens WHERE id = ? LIMIT 1")
      .bind(tokenId)
      .first<TokenRow>()) || null
  );
}

export async function findUserByTokenHash(env: Env, tokenHash: string): Promise<UserRow | null> {
  if (!tokenHash) return null;
  const db = requireDb(env);
  const row = await db
    .prepare(
      `SELECT u.* FROM tokens t
         JOIN users u ON u.id = t.user_id
        WHERE t.token_hash = ? AND t.revoked = 0 AND u.disabled = 0
        LIMIT 1`,
    )
    .bind(tokenHash)
    .first<UserRow>();
  if (!row) return null;
  await db
    .prepare("UPDATE tokens SET last_used_at = ? WHERE token_hash = ?")
    .bind(Date.now(), tokenHash)
    .run();
  return row;
}

export async function revokeToken(env: Env, tokenId: string): Promise<void> {
  await requireDb(env)
    .prepare("UPDATE tokens SET revoked = 1 WHERE id = ?")
    .bind(tokenId)
    .run();
}

/* -------------------------------------------------------------------------- */
/* Sites                                                                      */
/* -------------------------------------------------------------------------- */

export async function getSite(env: Env, host: string): Promise<SiteRow | null> {
  return (
    (await requireDb(env)
      .prepare("SELECT * FROM sites WHERE host = ? LIMIT 1")
      .bind(host.trim().toLowerCase())
      .first<SiteRow>()) || null
  );
}

export async function listSites(env: Env, ownerId?: string): Promise<SiteRow[]> {
  const db = requireDb(env);
  const query = ownerId
    ? db.prepare("SELECT * FROM sites WHERE owner_id = ? ORDER BY created_at DESC").bind(ownerId)
    : db.prepare("SELECT * FROM sites ORDER BY created_at DESC");
  const result = await query.all<SiteRow>();
  return result.results || [];
}

export async function listSitesByRelay(env: Env, relayId: string): Promise<SiteRow[]> {
  const result = await requireDb(env)
    .prepare("SELECT * FROM sites WHERE relay_id = ? ORDER BY created_at DESC")
    .bind(relayId)
    .all<SiteRow>();
  return result.results || [];
}

export async function upsertSite(env: Env, site: SiteRow): Promise<void> {
  await requireDb(env)
    .prepare(
      `INSERT INTO sites (
         host, relay_id, owner_id, token_hash, temporary, expires_at, rotate_seconds,
         backends_json, backend_count, insecure, anonymous, quota_bytes, quota_requests,
         created_at, updated_at, last_seen_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(host) DO UPDATE SET
         relay_id = excluded.relay_id,
         owner_id = excluded.owner_id,
         token_hash = excluded.token_hash,
         temporary = excluded.temporary,
         expires_at = excluded.expires_at,
         rotate_seconds = excluded.rotate_seconds,
         backends_json = excluded.backends_json,
         backend_count = excluded.backend_count,
         insecure = excluded.insecure,
         anonymous = excluded.anonymous,
         quota_bytes = excluded.quota_bytes,
         quota_requests = excluded.quota_requests,
         updated_at = excluded.updated_at`,
    )
    .bind(
      site.host,
      site.relay_id,
      site.owner_id,
      site.token_hash,
      site.temporary,
      site.expires_at,
      site.rotate_seconds,
      site.backends_json,
      site.backend_count,
      site.insecure,
      site.anonymous,
      site.quota_bytes,
      site.quota_requests,
      site.created_at,
      site.updated_at,
      site.last_seen_at,
    )
    .run();
}

export async function touchSite(env: Env, host: string): Promise<void> {
  await requireDb(env)
    .prepare("UPDATE sites SET last_seen_at = ? WHERE host = ?")
    .bind(Date.now(), host)
    .run();
}

export async function deleteSite(env: Env, host: string): Promise<void> {
  await requireDb(env).prepare("DELETE FROM sites WHERE host = ?").bind(host).run();
}

export async function countSitesByOwner(env: Env, ownerId: string): Promise<number> {
  const row = await requireDb(env)
    .prepare("SELECT COUNT(*) AS count FROM sites WHERE owner_id = ?")
    .bind(ownerId)
    .first<{ count: number }>();
  return Number(row?.count || 0);
}

/* -------------------------------------------------------------------------- */
/* Settings                                                                   */
/* -------------------------------------------------------------------------- */

function coerceNumber(value: unknown, fallback: number): number {
  const num = Number(value);
  if (!Number.isFinite(num) || num < 0) return fallback;
  return Math.floor(num);
}

function coerceSettings(raw: Record<string, unknown> | null, defaults: Settings): Settings {
  const source = raw && typeof raw === "object" ? raw : {};
  const bool = (key: keyof Settings, fallback: boolean) =>
    typeof source[key] === "boolean" ? (source[key] as boolean) : fallback;
  const num = (key: keyof Settings, fallback: number) =>
    coerceNumber(source[key], fallback);
  return {
    allowAnonymous: bool("allowAnonymous", defaults.allowAnonymous),
    allowCustomHost: bool("allowCustomHost", defaults.allowCustomHost),
    allowRegistration: bool("allowRegistration", defaults.allowRegistration),
    allowUserSettings: bool("allowUserSettings", defaults.allowUserSettings),
    autoRotateSeconds: num("autoRotateSeconds", defaults.autoRotateSeconds),
    quotaBytes: num("quotaBytes", defaults.quotaBytes),
    quotaRequests: num("quotaRequests", defaults.quotaRequests),
    quotaDurationSeconds: num("quotaDurationSeconds", defaults.quotaDurationSeconds),
    maxSites: num("maxSites", defaults.maxSites),
    maxBackends: Math.max(1, num("maxBackends", defaults.maxBackends)),
  };
}

async function readSettings(env: Env, key: string): Promise<Record<string, unknown> | null> {
  const row = await requireDb(env)
    .prepare("SELECT value FROM settings WHERE key = ? LIMIT 1")
    .bind(key)
    .first<{ value: string }>();
  if (!row?.value) return null;
  try {
    const parsed = JSON.parse(row.value);
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch {
    return null;
  }
}

export async function getAnonymousSettings(env: Env): Promise<Settings> {
  return coerceSettings(await readSettings(env, "global"), DEFAULT_ANONYMOUS_SETTINGS);
}

export async function getUserSettings(env: Env, userId: string): Promise<Settings> {
  return coerceSettings(await readSettings(env, `user:${userId}`), DEFAULT_USER_SETTINGS);
}

export async function saveAnonymousSettings(env: Env, settings: Settings): Promise<void> {
  await saveSettings(env, "global", settings);
}

export async function saveUserSettings(env: Env, userId: string, settings: Settings): Promise<void> {
  await saveSettings(env, `user:${userId}`, settings);
}

async function saveSettings(env: Env, key: string, settings: Settings): Promise<void> {
  await requireDb(env)
    .prepare(
      `INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?)
       ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`,
    )
    .bind(key, JSON.stringify(settings), Date.now())
    .run();
}

export async function effectiveSettings(env: Env, user: UserRow | null): Promise<Settings> {
  return user ? getUserSettings(env, user.id) : getAnonymousSettings(env);
}

/* -------------------------------------------------------------------------- */
/* Host helpers                                                               */
/* -------------------------------------------------------------------------- */

const HOST_LABEL_ALPHABET = "abcdefghijkmnpqrstuvwxyz23456789";

export function randomHostLabel(length = 8): string {
  const bytes = crypto.getRandomValues(new Uint8Array(length));
  let out = "";
  for (const byte of bytes) out += HOST_LABEL_ALPHABET[byte % HOST_LABEL_ALPHABET.length];
  return out;
}

export function normalizeHost(input: string): string {
  let value = (input || "").trim().toLowerCase();
  if (!value) return "";
  value = value.replace(/^[a-z]+:\/\//, "");
  value = value.split("/")[0];
  value = value.split(":")[0];
  return value.replace(/\.+$/, "");
}

export function isValidHostname(host: string): boolean {
  if (!host || host.length > 253) return false;
  const labels = host.split(".");
  if (labels.length < 2) return false;
  return labels.every(
    (label) =>
      label.length > 0 &&
      label.length <= 63 &&
      /^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/.test(label),
  );
}

export function isSiteHostForZone(host: string, zone: string): boolean {
  const normalizedZone = normalizeHost(zone);
  if (!normalizedZone) return false;
  if (!isValidHostname(host)) return false;
  if (host === normalizedZone) return false;
  return host.endsWith(`.${normalizedZone}`);
}

export function siteUrl(host: string, scheme = "https"): string {
  return `${scheme}://${host}`;
}

export function formatBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes <= 0) return "0 B";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let value = bytes;
  let index = 0;
  while (value >= 1024 && index < units.length - 1) {
    value /= 1024;
    index++;
  }
  return `${value >= 10 || index === 0 ? value.toFixed(0) : value.toFixed(1)} ${units[index]}`;
}
