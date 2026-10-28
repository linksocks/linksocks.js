import type { Env } from "./types";
import {
  type Flash,
  type AdminPageData,
  type DashboardPageData,
  type Overview,
  type SiteView,
  type TokenView,
  type UserView,
  renderAdminPage,
  renderDashboardPage,
  renderErrorPage,
  renderLoginPage,
} from "./ui";
import {
  type Settings,
  type SiteRow,
  type TokenRow,
  type UserRow,
  clearSessionCookie,
  createSession,
  createToken,
  createUser,
  deleteSession,
  deleteSite,
  deleteUser,
  ensureSchema,
  findUserById,
  findUserByUsername,
  findTokenRow,
  getAnonymousSettings,
  getSessionContext,
  getUserSettings,
  listSites,
  listTokens,
  listUsers,
  parseCookies,
  revokeToken,
  saveAnonymousSettings,
  saveUserSettings,
  sessionCookie,
  touchUserLogin,
  updateUser,
  verifyPassword,
} from "./db";

const FLASH_COOKIE = "ls_flash";

function textResponse(message: string, status = 400): Response {
  return new Response(message, {
    status,
    headers: { "Content-Type": "text/plain; charset=utf-8", "Cache-Control": "no-store" },
  });
}

async function requireDb(env: Env): Promise<boolean> {
  if (!env.DB) return false;
  try {
    await ensureSchema(env);
    return true;
  } catch {
    return false;
  }
}

function flashCookie(flash: Flash): string {
  const value = encodeURIComponent(JSON.stringify(flash));
  return `${FLASH_COOKIE}=${value}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=60`;
}

function clearFlashCookie(): string {
  return `${FLASH_COOKIE}=; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=0`;
}

function readFlash(request: Request): Flash | null {
  const raw = parseCookies(request)[FLASH_COOKIE];
  if (!raw) return null;
  try {
    const parsed = JSON.parse(decodeURIComponent(raw));
    if (!parsed || typeof parsed !== "object") return null;
    return {
      kind: parsed.kind === "error" ? "error" : "ok",
      message: String(parsed.message || ""),
      token: parsed.token ? String(parsed.token) : undefined,
    };
  } catch {
    return null;
  }
}

function redirectWithFlash(url: string, flash: Flash): Response {
  return new Response(null, {
    status: 303,
    headers: {
      Location: url,
      "Set-Cookie": flashCookie(flash),
      "Cache-Control": "no-store",
    },
  });
}

function parseSettingsForm(form: FormData, current: Settings): Settings {
  const global = String(form.get("scope") || "global") === "global";
  const checked = (name: string) => form.get(name) === "1";
  const num = (name: string, fallback: number) => {
    if (!form.has(name)) return fallback;
    const value = Number(form.get(name));
    if (!Number.isFinite(value) || value < 0) return fallback;
    return Math.floor(value);
  };
  return {
    allowAnonymous: global ? checked("allowAnonymous") : current.allowAnonymous,
    allowCustomHost: checked("allowCustomHost"),
    allowRegistration: global ? checked("allowRegistration") : current.allowRegistration,
    allowUserSettings: global ? checked("allowUserSettings") : current.allowUserSettings,
    autoRotateSeconds: num("autoRotateSeconds", current.autoRotateSeconds),
    quotaBytes: num("quotaBytes", current.quotaBytes),
    quotaRequests: num("quotaRequests", current.quotaRequests),
    quotaDurationSeconds: num("quotaDurationSeconds", current.quotaDurationSeconds),
    maxSites: num("maxSites", current.maxSites),
    maxBackends: Math.max(1, num("maxBackends", current.maxBackends)),
  };
}

function toTokenView(token: TokenRow, users: Map<string, UserRow>): TokenView {
  return {
    id: token.id,
    prefix: token.token_prefix,
    label: token.label || "",
    createdAt: token.created_at,
    lastUsedAt: token.last_used_at,
    revoked: token.revoked === 1,
    username: users.get(token.user_id)?.username,
  };
}

function toUserView(user: UserRow, sites: SiteRow[], tokens: TokenRow[]): UserView {
  return {
    id: user.id,
    username: user.username,
    isAdmin: user.is_admin === 1,
    disabled: user.disabled === 1,
    createdAt: user.created_at,
    lastLoginAt: user.last_login_at,
    siteCount: sites.filter((site) => site.owner_id === user.id).length,
    tokenCount: tokens.filter((token) => token.user_id === user.id && token.revoked === 0).length,
  };
}

interface LiveSiteInfo {
  providers: number;
  backends: number;
  requests: number;
  bytes: number;
}

async function loadLiveInfo(
  env: Env,
  sites: SiteRow[],
): Promise<Map<string, LiveSiteInfo>> {
  const result = new Map<string, LiveSiteInfo>();
  const relayIds = Array.from(new Set(sites.map((site) => site.relay_id))).slice(0, 64);
  await Promise.all(
    relayIds.map(async (relayId) => {
      try {
        const relay = env.RELAY.get(env.RELAY.idFromString(relayId));
        const info = await relay.adminGetWebInfo();
        for (const [host, value] of Object.entries(info.sites || {})) {
          result.set(host, {
            providers: Number(value.providers) || 0,
            backends: Number(value.backends) || 0,
            requests: Number(value.requests) || 0,
            bytes: Number(value.bytes) || 0,
          });
        }
      } catch {
        // Ignore relays that have been removed or are unreachable.
      }
    }),
  );
  return result;
}

function toSiteViews(
  sites: SiteRow[],
  live: Map<string, LiveSiteInfo>,
  users: Map<string, UserRow>,
): SiteView[] {
  return sites.map((site) => {
    const info = live.get(site.host);
    return {
      host: site.host,
      url: `https://${site.host}`,
      owner: site.owner_id ? users.get(site.owner_id)?.username || "deleted user" : "anonymous",
      ownerId: site.owner_id,
      temporary: site.temporary === 1,
      expiresAt: site.expires_at,
      rotateSeconds: site.rotate_seconds,
      backendCount: info?.backends || site.backend_count,
      providerCount: info?.providers || 0,
      requests: info?.requests || 0,
      bytes: info?.bytes || 0,
      anonymous: site.anonymous === 1,
      insecure: site.insecure === 1,
      createdAt: site.created_at,
      relayId: site.relay_id,
    };
  });
}

function buildOverview(
  users: UserRow[],
  sites: SiteRow[],
  tokens: TokenRow[],
  siteViews: SiteView[],
): Overview {
  return {
    users: users.length,
    sites: sites.length,
    providers: siteViews.reduce((sum, site) => sum + site.providerCount, 0),
    requests: siteViews.reduce((sum, site) => sum + site.requests, 0),
    bytes: siteViews.reduce((sum, site) => sum + site.bytes, 0),
    temporarySites: sites.filter((site) => site.temporary === 1).length,
    tokens: tokens.filter((token) => token.revoked === 0).length,
  };
}

export async function handleAdmin(request: Request, env: Env, url: URL): Promise<Response> {
  if (!(await requireDb(env))) {
    return textResponse("Web control plane requires a D1 database binding named DB.", 503);
  }

  if (url.searchParams.has("logout")) {
    const context = await getSessionContext(env, request);
    if (context) await deleteSession(env, context.session.id);
    return new Response(null, {
      status: 303,
      headers: {
        Location: "/admin",
        "Set-Cookie": clearSessionCookie(),
        "Cache-Control": "no-store",
      },
    });
  }

  if (request.method === "POST" && url.searchParams.has("login")) {
    const form = await request.formData();
    const username = String(form.get("username") || "").trim().toLowerCase();
    const password = String(form.get("password") || "");
    const user = await findUserByUsername(env, username);
    if (!user || user.disabled || user.is_admin !== 1 || !(await verifyPassword(password, user.password_hash))) {
      return new Response(
        renderLoginPage({
          title: "Admin sign in",
          action: "/admin?login",
          admin: true,
          username,
          error: "Invalid administrator credentials.",
        }),
        { status: 401, headers: { "Content-Type": "text/html; charset=utf-8" } },
      );
    }
    const session = await createSession(env, user.id);
    await touchUserLogin(env, user.id);
    return new Response(null, {
      status: 303,
      headers: {
        Location: "/admin",
        "Set-Cookie": sessionCookie(session.id, 7 * 24 * 60 * 60),
        "Cache-Control": "no-store",
      },
    });
  }

  const context = await getSessionContext(env, request);
  if (!context || context.user.is_admin !== 1) {
    const allowRegistration = (await getAnonymousSettings(env)).allowRegistration;
    return new Response(
      renderLoginPage({
        title: "Admin sign in",
        action: "/admin?login",
        admin: true,
        error: context ? "This account is not an administrator." : undefined,
        registerHref: allowRegistration ? "/register" : undefined,
      }),
      { status: context ? 403 : 200, headers: { "Content-Type": "text/html; charset=utf-8" } },
    );
  }

  if (url.pathname === "/admin/action" && request.method === "POST") {
    return await handleAdminAction(request, env, url, context);
  }

  const tabParam = url.searchParams.get("tab") || "overview";
  const tab = (["overview", "users", "sites", "tokens", "settings"].includes(tabParam)
    ? tabParam
    : "overview") as AdminPageData["tab"];

  const [users, sites, tokens, globalSettings] = await Promise.all([
    listUsers(env),
    listSites(env),
    listTokens(env),
    getAnonymousSettings(env),
  ]);
  const usersById = new Map(users.map((user) => [user.id, user]));
  const live = await loadLiveInfo(env, sites);
  const siteViews = toSiteViews(sites, live, usersById);
  const tokenViews = tokens.map((token) => toTokenView(token, usersById));
  const userViews = users.map((user) => toUserView(user, sites, tokens));

  const selectedUserId = url.searchParams.get("user");
  const selectedUser = selectedUserId
    ? userViews.find((user) => user.id === selectedUserId)
    : undefined;
  const selectedUserSettings = selectedUser
    ? await getUserSettings(env, selectedUser.id)
    : undefined;

  const data: AdminPageData = {
    user: { username: context.user.username },
    csrf: context.session.csrf,
    tab,
    users: userViews,
    sites: siteViews,
    tokens: tokenViews,
    overview: buildOverview(users, sites, tokens, siteViews),
    globalSettings,
    selectedUser,
    selectedUserSettings,
    flash: readFlash(request),
  };

  return new Response(renderAdminPage(data), {
    headers: {
      "Content-Type": "text/html; charset=utf-8",
      "Cache-Control": "no-store",
      ...(readFlash(request) ? { "Set-Cookie": clearFlashCookie() } : {}),
    },
  });
}

async function handleAdminAction(
  request: Request,
  env: Env,
  url: URL,
  context: NonNullable<Awaited<ReturnType<typeof getSessionContext>>>,
): Promise<Response> {
  const form = await request.formData();
  if (String(form.get("csrf") || "") !== context.session.csrf) {
    return textResponse("Invalid CSRF token", 403);
  }
  const action = String(form.get("action") || "");
  const back = `${url.origin}/admin`;
  try {
    if (action === "createUser") {
      const username = String(form.get("username") || "");
      const password = String(form.get("password") || "");
      const user = await createUser(env, username, password, form.has("isAdmin"));
      return redirectWithFlash(`${back}?tab=users&user=${encodeURIComponent(user.id)}`, {
        kind: "ok",
        message: `User ${user.username} created.`,
      });
    }

    if (action === "updateUser") {
      const userId = String(form.get("userId") || "");
      const target = await findUserById(env, userId);
      if (!target) return redirectWithFlash(`${back}?tab=users`, { kind: "error", message: "User not found." });
      const nextAdmin = form.has("isAdmin");
      const nextDisabled = form.has("disabled");
      if (target.is_admin === 1 && (!nextAdmin || nextDisabled)) {
        const admins = (await listUsers(env)).filter((user) => user.is_admin === 1 && user.disabled === 0);
        if (admins.length <= 1) {
          return redirectWithFlash(`${back}?tab=users&user=${encodeURIComponent(userId)}`, {
            kind: "error",
            message: "The last active administrator cannot be demoted or disabled.",
          });
        }
      }
      await updateUser(env, userId, {
        password: String(form.get("password") || "") || undefined,
        isAdmin: nextAdmin,
        disabled: nextDisabled,
      });
      return redirectWithFlash(`${back}?tab=users&user=${encodeURIComponent(userId)}`, {
        kind: "ok",
        message: `User ${target.username} updated.`,
      });
    }

    if (action === "deleteUser") {
      const userId = String(form.get("userId") || "");
      if (userId === context.user.id) {
        return redirectWithFlash(`${back}?tab=users`, { kind: "error", message: "You cannot delete your own account." });
      }
      const target = await findUserById(env, userId);
      if (!target) return redirectWithFlash(`${back}?tab=users`, { kind: "error", message: "User not found." });
      if (target.is_admin === 1) {
        const admins = (await listUsers(env)).filter((user) => user.is_admin === 1 && user.disabled === 0);
        if (admins.length <= 1) {
          return redirectWithFlash(`${back}?tab=users`, { kind: "error", message: "The last administrator cannot be deleted." });
        }
      }
      const sites = (await listSites(env, userId));
      for (const site of sites) {
        await deleteSite(env, site.host);
        await disconnectHost(env, site.relay_id, site.host);
      }
      await deleteUser(env, userId);
      return redirectWithFlash(`${back}?tab=users`, { kind: "ok", message: `User ${target.username} deleted.` });
    }

    if (action === "createToken") {
      const userId = String(form.get("userId") || "");
      const user = await findUserById(env, userId);
      if (!user) return redirectWithFlash(`${back}?tab=users`, { kind: "error", message: "User not found." });
      const created = await createToken(env, userId, String(form.get("label") || ""));
      return redirectWithFlash(`${back}?tab=users&user=${encodeURIComponent(userId)}`, {
        kind: "ok",
        message: `Token created for ${user.username}.`,
        token: created.raw,
      });
    }

    if (action === "revokeToken") {
      const tokenId = String(form.get("tokenId") || "");
      const token = await findTokenRow(env, tokenId);
      if (!token) return redirectWithFlash(`${back}?tab=tokens`, { kind: "error", message: "Token not found." });
      await revokeToken(env, tokenId);
      return redirectWithFlash(`${back}?tab=tokens`, { kind: "ok", message: "Token revoked." });
    }

    if (action === "deleteSite") {
      const host = String(form.get("host") || "").toLowerCase();
      const site = (await listSites(env)).find((row) => row.host === host);
      if (!site) return redirectWithFlash(`${back}?tab=sites`, { kind: "error", message: "Site not found." });
      await deleteSite(env, host);
      await disconnectHost(env, site.relay_id, host);
      return redirectWithFlash(`${back}?tab=sites`, { kind: "ok", message: `Site ${host} deleted.` });
    }

    if (action === "disconnectSite") {
      const host = String(form.get("host") || "").toLowerCase();
      const site = (await listSites(env)).find((row) => row.host === host);
      if (!site) return redirectWithFlash(`${back}?tab=sites`, { kind: "error", message: "Site not found." });
      await disconnectHost(env, site.relay_id, host);
      return redirectWithFlash(`${back}?tab=sites`, { kind: "ok", message: `Providers for ${host} disconnected.` });
    }

    if (action === "saveSettings") {
      const scope = String(form.get("scope") || "global");
      if (scope === "global") {
        const current = await getAnonymousSettings(env);
        await saveAnonymousSettings(env, parseSettingsForm(form, current));
        return redirectWithFlash(`${back}?tab=settings`, { kind: "ok", message: "Global policy saved." });
      }
      const userId = String(form.get("userId") || "");
      const user = await findUserById(env, userId);
      if (!user) return redirectWithFlash(`${back}?tab=users`, { kind: "error", message: "User not found." });
      const current = await getUserSettings(env, userId);
      await saveUserSettings(env, userId, parseSettingsForm(form, current));
      return redirectWithFlash(`${back}?tab=users&user=${encodeURIComponent(userId)}`, {
        kind: "ok",
        message: `Policy for ${user.username} saved.`,
      });
    }

    return redirectWithFlash(back, { kind: "error", message: `Unknown action: ${action}` });
  } catch (err) {
    return redirectWithFlash(back, {
      kind: "error",
      message: err instanceof Error ? err.message : String(err),
    });
  }
}

async function disconnectHost(env: Env, relayId: string, host: string): Promise<void> {
  try {
    const relay = env.RELAY.get(env.RELAY.idFromString(relayId));
    await relay.adminDisconnectWebHost(host);
  } catch {
    // Ignore stale relay ids.
  }
}

export async function handleDashboard(request: Request, env: Env, url: URL): Promise<Response> {
  if (!(await requireDb(env))) {
    return textResponse("Web control plane requires a D1 database binding named DB.", 503);
  }

  if (url.searchParams.has("logout")) {
    const context = await getSessionContext(env, request);
    if (context) await deleteSession(env, context.session.id);
    return new Response(null, {
      status: 303,
      headers: {
        Location: "/dashboard",
        "Set-Cookie": clearSessionCookie(),
        "Cache-Control": "no-store",
      },
    });
  }

  if (request.method === "POST" && url.searchParams.has("login")) {
    const form = await request.formData();
    const username = String(form.get("username") || "").trim().toLowerCase();
    const password = String(form.get("password") || "");
    const user = await findUserByUsername(env, username);
    if (!user || user.disabled || !(await verifyPassword(password, user.password_hash))) {
      return new Response(
        renderLoginPage({
          title: "Sign in",
          action: "/dashboard?login",
          username,
          error: "Invalid username or password.",
        }),
        { status: 401, headers: { "Content-Type": "text/html; charset=utf-8" } },
      );
    }
    const session = await createSession(env, user.id);
    await touchUserLogin(env, user.id);
    return new Response(null, {
      status: 303,
      headers: {
        Location: "/dashboard",
        "Set-Cookie": sessionCookie(session.id, 7 * 24 * 60 * 60),
        "Cache-Control": "no-store",
      },
    });
  }

  const context = await getSessionContext(env, request);
  if (!context) {
    const global = await getAnonymousSettings(env);
    const page = renderLoginPage({
      title: "Sign in",
      action: "/dashboard?login",
      registerHref: global.allowRegistration ? "/register" : undefined,
    });
    return new Response(page, { headers: { "Content-Type": "text/html; charset=utf-8" } });
  }

  if (url.pathname === "/dashboard/action" && request.method === "POST") {
    return await handleDashboardAction(request, env, url, context);
  }

  const tabParam = url.searchParams.get("tab") || "overview";
  const tab = (["overview", "sites", "tokens", "settings"].includes(tabParam)
    ? tabParam
    : "overview") as DashboardPageData["tab"];

  const [allSites, allTokens, global, userSettings] = await Promise.all([
    listSites(env, context.user.id),
    listTokens(env, context.user.id),
    getAnonymousSettings(env),
    getUserSettings(env, context.user.id),
  ]);
  const usersById = new Map([[context.user.id, context.user]]);
  const live = await loadLiveInfo(env, allSites);
  const siteViews = toSiteViews(allSites, live, usersById);
  const tokenViews = allTokens.map((token) => toTokenView(token, usersById));

  const data: DashboardPageData = {
    user: { username: context.user.username, isAdmin: context.user.is_admin === 1 },
    csrf: context.session.csrf,
    tab,
    sites: siteViews,
    tokens: tokenViews,
    overview: buildOverview([context.user], allSites, allTokens, siteViews),
    settings: userSettings,
    allowUserSettings: global.allowUserSettings,
    flash: readFlash(request),
  };

  return new Response(renderDashboardPage(data), {
    headers: {
      "Content-Type": "text/html; charset=utf-8",
      "Cache-Control": "no-store",
      ...(readFlash(request) ? { "Set-Cookie": clearFlashCookie() } : {}),
    },
  });
}

async function handleDashboardAction(
  request: Request,
  env: Env,
  url: URL,
  context: NonNullable<Awaited<ReturnType<typeof getSessionContext>>>,
): Promise<Response> {
  const form = await request.formData();
  if (String(form.get("csrf") || "") !== context.session.csrf) {
    return textResponse("Invalid CSRF token", 403);
  }
  const action = String(form.get("action") || "");
  const back = `${url.origin}/dashboard`;
  try {
    if (action === "createToken") {
      const created = await createToken(env, context.user.id, String(form.get("label") || ""));
      return redirectWithFlash(`${back}?tab=tokens`, {
        kind: "ok",
        message: "Token created.",
        token: created.raw,
      });
    }

    if (action === "revokeToken") {
      const tokenId = String(form.get("tokenId") || "");
      const token = await findTokenRow(env, tokenId);
      if (!token || token.user_id !== context.user.id) {
        return redirectWithFlash(`${back}?tab=tokens`, { kind: "error", message: "Token not found." });
      }
      await revokeToken(env, tokenId);
      return redirectWithFlash(`${back}?tab=tokens`, { kind: "ok", message: "Token revoked." });
    }

    if (action === "deleteSite") {
      const host = String(form.get("host") || "").toLowerCase();
      const sites = await listSites(env, context.user.id);
      const site = sites.find((row) => row.host === host);
      if (!site) {
        return redirectWithFlash(`${back}?tab=sites`, { kind: "error", message: "Site not found." });
      }
      await deleteSite(env, host);
      await disconnectHost(env, site.relay_id, host);
      return redirectWithFlash(`${back}?tab=sites`, { kind: "ok", message: `Site ${host} deleted.` });
    }

    if (action === "saveSettings") {
      const global = await getAnonymousSettings(env);
      if (!global.allowUserSettings) {
        return redirectWithFlash(`${back}?tab=settings`, {
          kind: "error",
          message: "Per-user policy editing is disabled by the administrator.",
        });
      }
      const current = await getUserSettings(env, context.user.id);
      await saveUserSettings(env, context.user.id, parseSettingsForm(form, current));
      return redirectWithFlash(`${back}?tab=settings`, { kind: "ok", message: "Your policy has been saved." });
    }

    return redirectWithFlash(back, { kind: "error", message: `Unknown action: ${action}` });
  } catch (err) {
    return redirectWithFlash(back, {
      kind: "error",
      message: err instanceof Error ? err.message : String(err),
    });
  }
}

export async function handleRegister(request: Request, env: Env, url: URL): Promise<Response> {
  if (!(await requireDb(env))) {
    return textResponse("Web control plane requires a D1 database binding named DB.", 503);
  }
  const global = await getAnonymousSettings(env);
  if (!global.allowRegistration) {
    return new Response(
      renderErrorPage("Registration disabled", "User self-registration is disabled on this server.", "403"),
      { status: 403, headers: { "Content-Type": "text/html; charset=utf-8" } },
    );
  }

  if (request.method === "POST") {
    const form = await request.formData();
    const username = String(form.get("username") || "").trim();
    const password = String(form.get("password") || "");
    const confirm = String(form.get("confirm") || "");
    if (password !== confirm) {
      return new Response(
        renderLoginPage({
          title: "Create account",
          action: "/register",
          mode: "register",
          signInHref: "/dashboard",
          username,
          error: "Passwords do not match.",
        }),
        { status: 400, headers: { "Content-Type": "text/html; charset=utf-8" } },
      );
    }
    try {
      const user = await createUser(env, username, password, false);
      const session = await createSession(env, user.id);
      await touchUserLogin(env, user.id);
      return new Response(null, {
        status: 303,
        headers: {
          Location: "/dashboard",
          "Set-Cookie": sessionCookie(session.id, 7 * 24 * 60 * 60),
          "Cache-Control": "no-store",
        },
      });
    } catch (err) {
      return new Response(
        renderLoginPage({
          title: "Create account",
          action: "/register",
          mode: "register",
          signInHref: "/dashboard",
          username,
          error: err instanceof Error ? err.message : String(err),
        }),
        { status: 400, headers: { "Content-Type": "text/html; charset=utf-8" } },
      );
    }
  }

  const page = renderLoginPage({
    title: "Create account",
    action: "/register",
    mode: "register",
    signInHref: "/dashboard",
  });
  return new Response(page, { headers: { "Content-Type": "text/html; charset=utf-8" } });
}
