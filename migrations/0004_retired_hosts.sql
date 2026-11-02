-- Temporary hostnames that a rotation has retired stay unclaimable until the
-- relay that owned the rotation chain retires. `successor_host` always points
-- at the current hostname in the chain.
CREATE TABLE IF NOT EXISTS retired_hosts (
  host TEXT PRIMARY KEY,
  relay_id TEXT NOT NULL,
  owner_id TEXT,
  successor_host TEXT NOT NULL,
  retired_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_retired_hosts_relay ON retired_hosts(relay_id);
CREATE INDEX IF NOT EXISTS idx_retired_hosts_successor ON retired_hosts(successor_host);
