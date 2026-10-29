-- Reservations keep a hostname bound to one account while its provider is
-- offline. They are intentionally separate from `sites`: a reserved hostname
-- does not proxy anything until the owner publishes it again.
CREATE TABLE IF NOT EXISTS reservations (
  host TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  note TEXT,
  created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_reservations_user ON reservations(user_id);
