// The admin console lists every account's zones, not just the viewer's.
import assert from "node:assert/strict";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";
import { Miniflare } from "miniflare";

const root = fileURLToPath(new URL("..", import.meta.url));

test("admin console lists zones of every account and can delete one", { timeout: 30000 }, async (context) => {
  const bundle = await build({
    absWorkingDir: root,
    entryPoints: ["src/index.ts"],
    bundle: true,
    format: "esm",
    platform: "browser",
    external: ["cloudflare:workers"],
    write: false,
  });
  const worker = new Miniflare({
    name: "admin-zones-test",
    modules: true,
    script: bundle.outputFiles[0].text,
    compatibilityDate: "2025-04-13",
    durableObjects: {
      RELAY: { className: "Relay", useSQLite: true },
      TOKEN: { className: "Token", useSQLite: true },
      ZONE: { className: "Zone", useSQLite: true },
    },
    d1Databases: { DB: "admin-zones-test" },
    bindings: { WEB_ZONE: "example.com", BASE_HOST: "localhost" },
    outboundService: () => new Response("colo=TEST\n"),
  });
  context.after(() => worker.dispose());

  const origin = "http://localhost";
  const db = await worker.getD1Database("DB");
  // Touch the schema, then create two accounts with live sessions.
  await worker.dispatchFetch(`${origin}/api/zones/public`);
  for (const [id, admin] of [["admin", 1], ["alice", 0]]) {
    await db.prepare("INSERT INTO users (id, username, password_hash, is_admin, disabled, created_at) VALUES (?, ?, '', ?, 0, ?)")
      .bind(id, id, admin, Date.now()).run();
    await db.prepare("INSERT INTO sessions (id, user_id, csrf, created_at, expires_at) VALUES (?, ?, ?, ?, ?)")
      .bind(`session-${id}`, id, `csrf-${id}`, Date.now(), Date.now() + 600000).run();
  }

  const created = await worker.dispatchFetch(`${origin}/api/zones`, {
    method: "POST",
    headers: { Cookie: "ls_session=session-alice", "X-CSRF-Token": "csrf-alice", "Content-Type": "application/json" },
    body: JSON.stringify({ name: "Alice network", cidr: "10.91.0.0/24", max_members: 8 }),
  });
  assert.equal(created.status, 201, await created.clone().text());
  const zone = (await created.json()).zone;

  const adminHeaders = { Cookie: "ls_session=session-admin" };
  const page = await worker.dispatchFetch(`${origin}/admin?tab=zones`, { headers: adminHeaders });
  assert.equal(page.status, 200);
  const html = await page.text();
  assert.ok(html.includes('href="/admin?tab=zones"'), "the admin navigation needs a Zones entry");
  assert.ok(!html.includes(">My Zones<"), "the admin console manages every account's zones, not the viewer's own");
  assert.ok(html.includes("Alice network"), "another account's zone must appear");
  assert.ok(html.includes("alice"), "the zone owner must be shown");
  assert.ok(html.includes("10.91.0.0/24"), "the zone network must be shown");

  const csrf = html.match(/name="csrf" value="([^"]+)"/)?.[1];
  assert.ok(csrf);
  const deleted = await worker.dispatchFetch(`${origin}/admin/action`, {
    method: "POST",
    redirect: "manual",
    headers: adminHeaders,
    body: new URLSearchParams({ csrf, action: "deleteZone", zoneId: zone.id }),
  });
  assert.equal(deleted.status, 303);
  assert.equal(deleted.headers.get("location"), `${origin}/admin?tab=zones`);

  const aliceZones = await (await worker.dispatchFetch(`${origin}/api/zones`, { headers: { Cookie: "ls_session=session-alice" } })).json();
  assert.deepEqual(aliceZones.zones, [], "the deleted zone is gone for its owner too");

  // A regular account cannot reach the admin console.
  const refused = await worker.dispatchFetch(`${origin}/admin?tab=zones`, { headers: { Cookie: "ls_session=session-alice" }, redirect: "manual" });
  assert.ok(refused.status === 403 || refused.status === 303, `regular account must not read the admin console: ${refused.status}`);
});
