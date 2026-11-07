import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";
import { Miniflare } from "miniflare";

const root = fileURLToPath(new URL("..", import.meta.url));

test("administrator can enable and disable policy checkboxes", { timeout: 20000 }, async (context) => {
  const bundle = await build({
    absWorkingDir: root,
    entryPoints: ["src/index.ts"],
    bundle: true,
    format: "esm",
    platform: "browser",
    external: ["cloudflare:workers"],
    write: false,
  });
  const password = randomBytes(20).toString("hex");
  const worker = new Miniflare({
    modules: true,
    script: bundle.outputFiles[0].text,
    compatibilityDate: "2025-04-13",
    durableObjects: {
      RELAY: { className: "Relay", useSQLite: true },
      TOKEN: { className: "Token", useSQLite: true },
      ZONE: { className: "Zone", useSQLite: true },
    },
    d1Databases: { DB: "settings-test" },
    bindings: { ADMIN_USERNAME: "admin", ADMIN_PASSWORD: password },
    outboundService: () => new Response("colo=TEST\n"),
  });
  context.after(async () => worker.dispose());

  const login = await worker.dispatchFetch("http://localhost/admin?login", {
    method: "POST",
    redirect: "manual",
    body: new URLSearchParams({ username: "admin", password }),
  });
  assert.equal(login.status, 303);
  const session = login.headers.get("set-cookie")?.split(";", 1)[0];
  assert.ok(session?.startsWith("ls_session="));
  const headers = { Cookie: session };

  const settings = await worker.dispatchFetch("http://localhost/admin?tab=settings", { headers });
  const html = await settings.text();
  const csrf = html.match(/name="csrf" value="([^"]+)"/)?.[1];
  assert.ok(csrf);
  assert.ok(html.includes("Allow anonymous site providers"), "anonymous access is a site-provider policy, not a relay one");
  assert.ok(html.includes("Force temporary site hostnames"), "temporary-host policy must name site providers");

  async function save(fields) {
    const response = await worker.dispatchFetch("http://localhost/admin/action", {
      method: "POST",
      redirect: "manual",
      headers,
      body: new URLSearchParams({ csrf, action: "saveSettings", scope: "global", ...fields }),
    });
    assert.equal(response.status, 303);
  }

  await save({ allowAnonymous: "1", allowRegistration: "1", allowCustomHost: "1" });
  assert.equal((await worker.dispatchFetch("http://localhost/register")).status, 200);

  await save({});
  const disabled = await worker.dispatchFetch("http://localhost/admin?tab=settings", { headers });
  const updated = await disabled.text();
  for (const name of ["allowAnonymous", "allowRegistration", "allowCustomHost"]) {
    assert.equal(new RegExp(`name="${name}" value="1"\\s+checked`).test(updated), false, `${name} should be disabled`);
  }
  assert.equal((await worker.dispatchFetch("http://localhost/register")).status, 403);

  const created = await worker.dispatchFetch("http://localhost/admin/action", {
    method: "POST", redirect: "manual", headers,
    body: new URLSearchParams({ csrf, action: "createUser", username: "quota-user", password: randomBytes(20).toString("hex") }),
  });
  assert.equal(created.status, 303);
  const userId = new URL(created.headers.get("location")).searchParams.get("user");
  assert.ok(userId);

  // The Workers runtime rejects PBKDF2 iteration counts above 100000, so a
  // higher value would break every dashboard login in production while still
  // passing under Miniflare. Pin the parameters here.
  const db = await worker.getD1Database("DB");
  const stored = await db.prepare("SELECT password_hash FROM users WHERE id = ?").bind(userId).first();
  const [, iterations] = String(stored?.password_hash || "").split("$");
  assert.equal(Number(iterations), 100000, "password hashing must stay within the Workers PBKDF2 limit");
  const userPage = await worker.dispatchFetch(`http://localhost/admin?tab=users&user=${userId}`, { headers });
  const userHtml = await userPage.text();
  const policyForm = userHtml.split('name="action" value="saveSettings"')[1]?.split("</form>")[0] || "";
  assert.ok(policyForm.includes(`name="userId" value="${userId}"`), "user policy form must submit its user ID");
  const savedUser = await worker.dispatchFetch("http://localhost/admin/action", {
    method: "POST", redirect: "manual", headers,
    body: new URLSearchParams({ csrf, action: "saveSettings", scope: "user", userId, quotaBytes: "2048", allowCustomHost: "1" }),
  });
  assert.equal(savedUser.status, 303);
  const updatedUser = await worker.dispatchFetch(`http://localhost/admin?tab=users&user=${userId}`, { headers });
  const updatedUserHtml = await updatedUser.text();
  assert.ok(/name="quotaBytes"[^>]*value="2"/.test(updatedUserHtml), "2048 bytes should display as 2 KiB");
  assert.ok(/<option value="1024" selected>/.test(updatedUserHtml), "2048 bytes should select the KiB unit");

  await save({
    autoRotateSeconds: "2",
    autoRotateSecondsUnit: "86400",
    quotaBytes: "2",
    quotaBytesUnit: "1048576",
    quotaDurationSeconds: "3",
    quotaDurationSecondsUnit: "3600",
  });
  const quantities = await (await worker.dispatchFetch("http://localhost/admin?tab=settings", { headers })).text();
  assert.ok(/name="quotaBytes"[^>]*value="2"/.test(quantities), "2 MiB should display as 2 in the number input");
  assert.ok(/<option value="1048576" selected>/.test(quantities), "2 MiB should select the MiB unit");
  assert.ok(/name="autoRotateSeconds"[^>]*value="2"/.test(quantities), "2 days should display as 2");
  assert.ok(/<option value="86400" selected>/.test(quantities), "2 days should select the days unit");
  assert.ok(/name="quotaDurationSeconds"[^>]*value="3"/.test(quantities), "3 hours should display as 3");

  await save({ publicUrl: "relay.example.com" });
  const publicHome = await (await worker.dispatchFetch("http://localhost/")).text();
  assert.ok(
    publicHome.includes("linksocks provider -u https://relay.example.com"),
    "the configured primary URL must replace the request origin in quick start commands",
  );
  await save({});

  const dashboard = await worker.dispatchFetch("http://localhost/dashboard?tab=settings", { headers });
  const dashboardHtml = await dashboard.text();
  assert.equal(dashboardHtml.includes('id="settingsForm"'), false, "regular users must not edit quotas or limits");
  assert.ok(dashboardHtml.includes("<h2>Overview</h2>"));

  const users = await (await worker.dispatchFetch("http://localhost/admin?tab=users", { headers })).text();
  assert.ok(users.includes("modal=password"), "password changes must open in a dialog");
  assert.ok(users.includes("modal=policy"), "policy changes must open in a dialog");
  assert.equal(users.includes("user-create-token"), false, "user management no longer creates tokens inline");

  const reservations = await (await worker.dispatchFetch("http://localhost/admin?tab=reservations", { headers })).text();
  assert.ok(reservations.includes('role="combobox"'), "reservation owner must be a searchable combobox");
});
