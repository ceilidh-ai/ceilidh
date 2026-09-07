export type SessionId = string
export type TurnId = string
export type RunnerId = string

export type Harness = 'claude-code' | 'codex' | 'cursor' | 'mock'

export type Lane = {
  harness: Harness
  model: string
  effort?: string | null
}

export type SessionProfile = {
  repo_url?: string | null
  base_branch?: string | null
  allow_push: boolean
}

export type SessionStatus = 'active' | 'archived'

export type Session = {
  id: SessionId
  title: string
  lane: Lane
  profile: SessionProfile
  runner_affinity?: RunnerId | null
  status: SessionStatus
  /** Set when this session was spawned as a sub-agent of another session. */
  parent_id?: SessionId | null
  created_at: string
  updated_at: string
}

export type TurnStatus =
  | 'queued'
  | 'claimed'
  | 'working'
  | 'done'
  | 'error'
  | 'capped'
  | 'cancelled'

export type Question = {
  text: string
  recommendation?: string | null
}

export type Envelope = {
  headline: string
  work_complete: boolean
  cannot_proceed: boolean
  body_markdown: string
  questions: Question[]
}

export type Turn = {
  id: TurnId
  session_id: SessionId
  seq: number
  input: string
  lane_override?: Lane | null
  status: TurnStatus
  envelope?: Envelope | null
  error?: string | null
  commit?: string | null
  resume_token?: string | null
  /** True once a cancel was asked for and the runner has not reported yet. */
  cancel_requested: boolean
  created_at: string
  started_at?: string | null
  finished_at?: string | null
}

export type RunnerStatusInfo = {
  runner: RunnerId
  online: boolean
  last_seen: string
  active_turns: number
}

/** One row of the model menu the caller offers, grouped by vendor. */
export type ModelChoice = {
  vendor: string
  harness: Harness
  model: string
  label: string
}

export type CallerConfig = {
  default_repo_url?: string | null
  models: ModelChoice[]
}

export type CreateSessionRequest = {
  title: string
  lane?: Lane
  profile?: SessionProfile
  parent_id?: SessionId
}

export type PostTurnRequest = {
  input: string
  lane?: Lane
}

export type Event =
  | { type: 'turn_queued'; turn: Turn }
  | { type: 'turn_claimed'; turn_id: TurnId; runner: RunnerId }
  | { type: 'chunk'; turn_id: TurnId; text: string }
  | { type: 'turn_done'; turn: Turn }
  | { type: 'turn_error'; turn_id: TurnId; message: string }
  | { type: 'turn_cancelled'; turn: Turn }
  | { type: 'session_created'; session: Session }
  | { type: 'session_updated'; session: Session }
  | { type: 'session_deleted'; session_id: SessionId }
  | { type: 'runner_status'; runner: RunnerStatusInfo }

export type RepoChoice = {
  full_name: string
  owner: string
  name: string
  private: boolean
  url: string
  pushed_at?: string | null
}

export type RepoOwner = {
  login: string
  repos: RepoChoice[]
}

export type RepoList = {
  owners: RepoOwner[]
  available: boolean
  error?: string | null
}
