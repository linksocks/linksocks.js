CREATE TABLE IF NOT EXISTS zones (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  cidr TEXT NOT NULL,
  max_members INTEGER NOT NULL CHECK (max_members BETWEEN 2 AND 256),
  owner_id TEXT NOT NULL,
  created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_zones_owner ON zones(owner_id);
