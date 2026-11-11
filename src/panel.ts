import type { Env } from "./types";
import {
  type Flash,
  type AdminPageData,
  type AdminZoneView,
  type DashboardPageData,
  type Overview,
  type ReservationView,
  type SiteView,
  type TokenView,
  type UserView,
  renderAdminPage,
  renderDashboardPage,
  renderErrorPage,
  renderLoginPage,
} from "./ui";
import {
  type QuantityUnit,
  type Settings,
  type SiteRow,
  type TokenRow,
  type UserRow,
  BYTE_UNITS,
  DURATION_UNITS,
  clearSessionCookie,
  createSession,
  createToken,
  createUser,
  deleteReservation,
  deleteSession,
  deleteSite,
  deleteUser,
  ensureSchema,
  findUserById,
  findUserByUsername,
  findTokenRow,
  getAnonymousSettings,
  getReservation,
  getSessionContext,
  getUserSettings,
  isValidHostname,
  isSiteHostForZone,
  listReservations,
  listSites,
  listTokens,
  listUsers,
  normalizeHost,
  normalizePublicUrl,
  parseCookies,
  revokeToken,
  saveAnonymousSettings,
  saveUserSettings,
  sessionCookie,
  touchUserLogin,
  updateUser,
  upsertReservation,
  verifyPassword,
} from "./db";
import { deleteZone, getZone, listZones } from "./zone";

const FLASH_COOKIE = "ls_flash";

function textResponse(message: string, status = 400): Response {
  return new Response(message, {
    status,
    headers: { "Content-Type": "text/plain; charset=utf-8", "Cache-Control": "no-store" },
  });
}

async function requireDb(env: Env): Promise<string | null> {
  if (!env.DB) return "D1 binding DB is not configured";
  try {
    await ensureSchema(env);
    return null;
  } catch (err) {
    const reason = err instanceof Error ? err.message : String(err);
    console.error("control plane initialization failed:", reason);
    return reason;
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
  const quantity = (name: string, unitName: string, fallback: number, units: readonly QuantityUnit[]) => {
    if (!form.has(name)) return fallback;
    const value = Number(form.get(name));
    const unit = Number(form.get(unitName) || 1);
    if (!Number.isFinite(value) || value < 0 || !units.some((item) => item.value === unit)) return fallback;
    return Math.floor(value * unit);
  };
  return {
    publicUrl: global ? normalizePublicUrl(form.get("publicUrl")) : current.publicUrl,
    allowAnonymous: global ? checked("allowAnonymous") : current.allowAnonymous,
    allowCustomHost: checked("allowCustomHost"),
    forceTemporaryHost: global ? checked("forceTemporaryHost") : current.forceTemporaryHost,
    allowRegistration: global ? checked("allowRegistration") : current.allowRegistration,
    autoRotateSeconds: quantity("autoRotateSeconds", "autoRotateSecondsUnit", current.autoRotateSeconds, DURATION_UNITS),
    quotaBytes: quantity("quotaBytes", "quotaBytesUnit", current.quotaBytes, BYTE_UNITS),
    quotaRequests: num("quotaRequests", current.quotaRequests),
    quotaDurationSeconds: quantity("quotaDurationSeconds", "quotaDurationSecondsUnit", current.quotaDurationSeconds, DURATION_UNITS),
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
  reservedHosts: Set<string> = new Set(),
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
      reserved: reservedHosts.has(site.host),
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
    onlineSites: siteViews.filter((site) => site.providerCount > 0).length,
    providers: siteViews.reduce((sum, site) => sum + site.providerCount, 0),
    requests: siteViews.reduce((sum, site) => sum + site.requests, 0),
    bytes: siteViews.reduce((sum, site) => sum + site.bytes, 0),
    tokens: tokens.filter((token) => token.revoked === 0).length,
  };
}

function toReservationViews(
  reservations: Awaited<ReturnType<typeof listReservations>>,
  sites: SiteRow[],
  users: Map<string, UserRow>,
): ReservationView[] {
  const sitesByHost = new Map(sites.map((site) => [site.host, site]));
  return reservations.map((reservation) => {
    const site = sitesByHost.get(reservation.host);
    return {
      host: reservation.host,
      userId: reservation.user_id,
      username: users.get(reservation.user_id)?.username || "deleted user",
      note: reservation.note || "",
      createdAt: reservation.created_at,
      active: Boolean(site),
      siteOwner: site
        ? site.owner_id
          ? users.get(site.owner_id)?.username || "deleted user"
          : "anonymous"
        : null,
      temporary: site?.temporary === 1,
    };
  });
}

// loadAdminZones lists every zone on the server with its owner and live member
// counts, so the admin console covers accounts the viewer does not own.
async function loadAdminZones(env: Env, usersById: Map<string, UserRow>): Promise<AdminZoneView[]> {
  const zones = await listZones(env);
  const rosters = await Promise.all(
    zones.slice(0, 64).map(async (zone) => {
      try {
        return await env.ZONE.get(env.ZONE.idFromName(zone.id)).getMembers();
      } catch {
        return null;
      }
    }),
  );
  return zones.map((zone, index) => {
    const members = rosters[index] ?? null;
    return {
      id: zone.id,
      name: zone.name,
      cidr: zone.cidr,
      visibility: zone.visibility,
      maxMembers: zone.max_members,
      owner: usersById.get(zone.owner_id)?.username ?? "deleted account",
      createdAt: zone.created_at,
      members: members?.length ?? 0,
      online: members?.filter((member) => member.online).length ?? 0,
    };
  });
}

export async function handleAdmin(request: Request, env: Env, url: URL): Promise<Response> {
  const dbError = await requireDb(env);
  if (dbError) {
    return textResponse(`Web control plane unavailable: ${dbError}`, 503);
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
  const tab = (["overview", "users", "sites", "tokens", "reservations", "zones", "settings"].includes(tabParam)
    ? tabParam
    : "overview") as AdminPageData["tab"];

  const [users, sites, tokens, reservations, globalSettings] = await Promise.all([
    listUsers(env),
    listSites(env),
    listTokens(env),
    listReservations(env),
    getAnonymousSettings(env),
  ]);
  const usersById = new Map(users.map((user) => [user.id, user]));
  const live = await loadLiveInfo(env, sites);
  const siteViews = toSiteViews(sites, live, usersById, new Set(reservations.map((row) => row.host)));
  const tokenViews = tokens.map((token) => toTokenView(token, usersById));
  const userViews = users.map((user) => toUserView(user, sites, tokens));
  const reservationViews = toReservationViews(reservations, sites, usersById);
  const adminZones = await loadAdminZones(env, usersById);

  const selectedUserId = url.searchParams.get("user");
  const selectedUser = selectedUserId
    ? userViews.find((user) => user.id === selectedUserId)
    : undefined;
  const selectedUserSettings = selectedUser
    ? await getUserSettings(env, selectedUser.id)
    : undefined;
  const modalParam = url.searchParams.get("modal") || "";
  const userModal = selectedUser && ["password", "access", "policy", "delete"].includes(modalParam)
    ? (modalParam as AdminPageData["userModal"])
    : undefined;

  const data: AdminPageData = {
    user: { username: context.user.username },
    csrf: context.session.csrf,
    tab,
    users: userViews,
    sites: siteViews,
    tokens: tokenViews,
    reservations: reservationViews,
    zones: adminZones,
    overview: buildOverview(users, sites, tokens, siteViews),
    globalSettings,
    serverUrl: globalSettings.publicUrl || url.origin,
    zone: normalizeHost(env.WEB_ZONE || env.BASE_HOST || ""),
    selectedUser,
    selectedUserSettings,
    userModal,
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
      return redirectWithFlash(`${back}?tab=users`, {
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

    if (action === "deleteZone") {
      const zoneId = String(form.get("zoneId") || "");
      const zone = await getZone(env, zoneId);
      if (!zone) return redirectWithFlash(`${back}?tab=zones`, { kind: "error", message: "Zone not found." });
      await deleteZone(env, zone.id);
      return redirectWithFlash(`${back}?tab=zones`, { kind: "ok", message: `Zone ${zone.name} deleted; its members were disconnected.` });
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

    if (action === "createReservation") {
      const host = normalizeHost(String(form.get("host") || ""));
      let userId = String(form.get("userId") || "");
      const requestedName = String(form.get("userName") || "").trim().toLowerCase();
      if (!userId && requestedName) {
        userId = (await listUsers(env)).find((row) => row.username === requestedName)?.id || "";
      }
      const note = String(form.get("note") || "").trim().slice(0, 200);
      const user = await findUserById(env, userId);
      if (!user) {
        return redirectWithFlash(`${back}?tab=reservations`, { kind: "error", message: "Select an owner from the account list." });
      }
      if (!host || !isValidHostname(host)) {
        return redirectWithFlash(`${back}?tab=reservations`, { kind: "error", message: "Enter a valid hostname." });
      }
      const zone = normalizeHost(env.WEB_ZONE || env.BASE_HOST || "");
      if (!zone || !isSiteHostForZone(host, zone)) {
        return redirectWithFlash(`${back}?tab=reservations`, {
          kind: "error",
          message: `Hostname must be a subdomain of ${zone || "WEB_ZONE"}.`,
        });
      }
      const site = (await listSites(env)).find((row) => row.host === host);
      if (site && site.owner_id !== userId) {
        return redirectWithFlash(`${back}?tab=reservations`, {
          kind: "error",
          message: `${host} is currently published by ${site.owner_id ? "another account" : "an anonymous provider"}. Delete that site first.`,
        });
      }
      const existing = await getReservation(env, host);
      if (existing && existing.user_id !== userId) {
        return redirectWithFlash(`${back}?tab=reservations`, {
          kind: "error",
          message: `${host} is already reserved by another account.`,
        });
      }
      await upsertReservation(env, {
        host,
        user_id: userId,
        note: note || null,
        created_at: existing?.created_at || Date.now(),
      });
      return redirectWithFlash(`${back}?tab=reservations`, {
        kind: "ok",
        message: `Reserved ${host} for ${user.username}.`,
      });
    }

    if (action === "deleteReservation") {
      const host = normalizeHost(String(form.get("host") || ""));
      if (!host) {
        return redirectWithFlash(`${back}?tab=reservations`, { kind: "error", message: "Hostname is required." });
      }
      await deleteReservation(env, host);
      return redirectWithFlash(`${back}?tab=reservations`, {
        kind: "ok",
        message: `Reservation for ${host} removed.`,
      });
    }

    if (action === "saveSettings") {
      const scope = String(form.get("scope") || "global");
      if (scope === "global") {
        const enteredUrl = String(form.get("publicUrl") || "").trim();
        if (enteredUrl && !normalizePublicUrl(enteredUrl)) {
          return redirectWithFlash(`${back}?tab=settings`, {
            kind: "error",
            message: "Primary connection URL must be an http(s) origin, for example https://relay.example.com.",
          });
        }
        const current = await getAnonymousSettings(env);
        await saveAnonymousSettings(env, parseSettingsForm(form, current));
        return redirectWithFlash(`${back}?tab=settings`, { kind: "ok", message: "Global policy saved." });
      }
      const userId = String(form.get("userId") || "");
      const user = await findUserById(env, userId);
      if (!user) return redirectWithFlash(`${back}?tab=users`, { kind: "error", message: "User not found." });
      const current = await getUserSettings(env, userId);
      await saveUserSettings(env, userId, parseSettingsForm(form, current));
      return redirectWithFlash(`${back}?tab=users`, {
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

// loadNetworkStats counts what the overview cards show beyond the sites
// themselves: the relays this user's tokens created (with the provider and
// connector links attached to them) and this user's zones (with the members
// currently online).
async function loadNetworkStats(
  env: Env,
  userId: string,
  tokens: TokenRow[],
  sites: SiteRow[],
): Promise<{
  relays: number;
  relayProviders: number;
  relayConnectors: number;
  zones: number;
  zoneMembersOnline: number;
}> {
  const relayIds = new Set<string>();
  for (const site of sites) {
    if (site.relay_id) relayIds.add(site.relay_id);
  }
  const activeHashes = tokens.filter((token) => token.revoked === 0).map((token) => token.token_hash);
  if (activeHashes.length > 0) {
    try {
      const tokenDO = env.TOKEN.get(env.TOKEN.idFromName("main"));
      const metadata = await tokenDO.getRelayMetadataBatch(activeHashes.slice(0, 64));
      for (const entry of metadata.values()) {
        if (entry.relayId) relayIds.add(entry.relayId);
      }
    } catch {
      // The token store is best effort here; the cards still render.
    }
  }

  const runtimeInfos = await Promise.all(
    Array.from(relayIds).slice(0, 64).map(async (relayId) => {
      try {
        return await env.RELAY.get(env.RELAY.idFromString(relayId)).adminGetRuntimeInfo();
      } catch {
        return null;
      }
    }),
  );
  const live = runtimeInfos.filter((info) => info !== null);
  const relays = live.filter((info) => info.providerCount + info.connectorCount > 0).length;
  const relayProviders = live.reduce((sum, info) => sum + (Number(info.providerCount) || 0), 0);
  const relayConnectors = live.reduce((sum, info) => sum + (Number(info.connectorCount) || 0), 0);

  const zones = await listZones(env, userId);
  const rosters = await Promise.all(
    zones.slice(0, 32).map(async (zone) => {
      try {
        return await env.ZONE.get(env.ZONE.idFromName(zone.id)).getMembers();
      } catch {
        return null;
      }
    }),
  );
  const zoneMembersOnline = rosters
    .filter((members) => members !== null)
    .reduce((sum, members) => sum + (members ?? []).filter((member) => member.online).length, 0);

  return { relays, relayProviders, relayConnectors, zones: zones.length, zoneMembersOnline };
}

export async function handleDashboard(request: Request, env: Env, url: URL): Promise<Response> {
  const dbError = await requireDb(env);
  if (dbError) {
    return textResponse(`Web control plane unavailable: ${dbError}`, 503);
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
  const tab = (["overview", "sites", "tokens"].includes(tabParam)
    ? tabParam
    : "overview") as DashboardPageData["tab"];

  const [allSites, allTokens, global] = await Promise.all([
    listSites(env, context.user.id),
    listTokens(env, context.user.id),
    getAnonymousSettings(env),
  ]);
  const usersById = new Map([[context.user.id, context.user]]);
  const [live, network] = await Promise.all([
    loadLiveInfo(env, allSites),
    loadNetworkStats(env, context.user.id, allTokens, allSites),
  ]);
  const siteViews = toSiteViews(allSites, live, usersById);
  const tokenViews = allTokens.map((token) => toTokenView(token, usersById));

  const data: DashboardPageData = {
    user: { username: context.user.username, isAdmin: context.user.is_admin === 1 },
    csrf: context.session.csrf,
    tab,
    sites: siteViews,
    tokens: tokenViews,
    overview: {
      sites: siteViews.length,
      onlineSites: siteViews.filter((site) => site.providerCount > 0).length,
      siteProviders: siteViews.reduce((sum, site) => sum + site.providerCount, 0),
      requests: siteViews.reduce((sum, site) => sum + site.requests, 0),
      bytes: siteViews.reduce((sum, site) => sum + site.bytes, 0),
      relays: network.relays,
      relayProviders: network.relayProviders,
      relayConnectors: network.relayConnectors,
      zones: network.zones,
      zoneMembersOnline: network.zoneMembersOnline,
    },
    serverUrl: global.publicUrl || url.origin,
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

    return redirectWithFlash(back, { kind: "error", message: `Unknown action: ${action}` });
  } catch (err) {
    return redirectWithFlash(back, {
      kind: "error",
      message: err instanceof Error ? err.message : String(err),
    });
  }
}

export async function handleRegister(request: Request, env: Env, url: URL): Promise<Response> {
  const dbError = await requireDb(env);
  if (dbError) {
    return textResponse(`Web control plane unavailable: ${dbError}`, 503);
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
