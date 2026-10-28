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

  async function save(fields) {
    const response = await worker.dispatchFetch("http://localhost/admin/action", {
      method: "POST",
      redirect: "manual",
      headers,
      body: new URLSearchParams({ csrf, action: "saveSettings", scope: "global", ...fields }),
    });
    assert.equal(response.status, 303);
  }

  await save({ allowAnonymous: "1", allowRegistration: "1", allowUserSettings: "1", allowCustomHost: "1" });
  assert.equal((await worker.dispatchFetch("http://localhost/register")).status, 200);

  await save({});
  const disabled = await worker.dispatchFetch("http://localhost/admin?tab=settings", { headers });
  const updated = await disabled.text();
  for (const name of ["allowAnonymous", "allowRegistration", "allowUserSettings", "allowCustomHost"]) {
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
  assert.ok(/name="quotaBytes"[^>]*value="2048"/.test(updatedUserHtml), "user quota must be saved");
});