export type SessionId = string
export type TurnId = string
export type RunnerId = string

export type Harness = 'claude-code' | 'codex' | 'mock'

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

export type CreateSessionRequest = {
  title: string
  lane?: Lane
  profile?: SessionProfile
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
  | { type: 'runner_status'; runner: RunnerStatusInfo }
