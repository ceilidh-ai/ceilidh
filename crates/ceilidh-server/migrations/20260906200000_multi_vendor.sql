ALTER TABLE sessions ADD COLUMN parent_id TEXT;
ALTER TABLE turns ADD COLUMN cancel_requested_at TEXT;

CREATE INDEX IF NOT EXISTS idx_sessions_parent_id
    ON sessions(parent_id);
