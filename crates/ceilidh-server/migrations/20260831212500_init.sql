CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY NOT NULL,
    title TEXT NOT NULL,
    lane_json TEXT NOT NULL,
    profile_json TEXT NOT NULL,
    runner_affinity TEXT,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS turns (
    id TEXT PRIMARY KEY NOT NULL,
    session_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    input TEXT NOT NULL,
    lane_override_json TEXT,
    status TEXT NOT NULL,
    envelope_json TEXT,
    error TEXT,
    commit_sha TEXT,
    resume_token TEXT,
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE,
    UNIQUE (session_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_sessions_created_at
    ON sessions(created_at);

CREATE INDEX IF NOT EXISTS idx_turns_session_seq
    ON turns(session_id, seq);

CREATE INDEX IF NOT EXISTS idx_turns_status_created_at
    ON turns(status, created_at);

CREATE TABLE IF NOT EXISTS runners (
    runner TEXT PRIMARY KEY NOT NULL,
    last_seen TEXT NOT NULL,
    harnesses TEXT NOT NULL,
    active_turns TEXT NOT NULL
);
