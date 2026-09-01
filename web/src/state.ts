import type {
  Event,
  RunnerStatusInfo,
  Session,
  SessionId,
  Turn,
  TurnId,
} from './protocol'

export type ClientState = {
  sessions: Session[]
  turnsBySession: Record<SessionId, Turn[]>
  liveChunks: Record<TurnId, string>
  runners: RunnerStatusInfo[]
}

export const emptyClientState = (): ClientState => ({
  sessions: [],
  turnsBySession: {},
  liveChunks: {},
  runners: [],
})

export const setSessions = (
  state: ClientState,
  sessions: Session[],
): ClientState => ({
  ...state,
  sessions: sortSessions(sessions),
})

export const setTurns = (
  state: ClientState,
  sessionId: SessionId,
  turns: Turn[],
): ClientState => ({
  ...state,
  turnsBySession: {
    ...state.turnsBySession,
    [sessionId]: sortTurns(turns),
  },
})

export const setRunners = (
  state: ClientState,
  runners: RunnerStatusInfo[],
): ClientState => ({
  ...state,
  runners: sortRunners(runners),
})

export const foldEvent = (state: ClientState, event: Event): ClientState => {
  switch (event.type) {
    case 'turn_queued':
      return upsertTurn(state, event.turn)
    case 'turn_claimed':
      return updateTurn(state, event.turn_id, (turn) => ({
        ...turn,
        status: turn.status === 'queued' ? 'claimed' : turn.status,
      }))
    case 'chunk':
      return {
        ...updateTurn(state, event.turn_id, (turn) => ({
          ...turn,
          status:
            turn.status === 'queued' || turn.status === 'claimed'
              ? 'working'
              : turn.status,
        })),
        liveChunks: {
          ...state.liveChunks,
          [event.turn_id]: `${state.liveChunks[event.turn_id] ?? ''}${event.text}`,
        },
      }
    case 'turn_done':
      return removeLiveChunk(upsertTurn(state, event.turn), event.turn.id)
    case 'turn_error':
      return removeLiveChunk(
        updateTurn(state, event.turn_id, (turn) => ({
          ...turn,
          status: 'error',
          error: event.message,
        })),
        event.turn_id,
      )
    case 'runner_status':
      return upsertRunner(state, event.runner)
  }
}

const upsertTurn = (state: ClientState, turn: Turn): ClientState => {
  const turns = state.turnsBySession[turn.session_id] ?? []
  const nextTurns = turns.some((item) => item.id === turn.id)
    ? turns.map((item) => (item.id === turn.id ? turn : item))
    : [...turns, turn]

  return {
    ...state,
    turnsBySession: {
      ...state.turnsBySession,
      [turn.session_id]: sortTurns(nextTurns),
    },
  }
}

const updateTurn = (
  state: ClientState,
  turnId: TurnId,
  update: (turn: Turn) => Turn,
): ClientState => {
  let found = false
  const turnsBySession = Object.fromEntries(
    Object.entries(state.turnsBySession).map(([sessionId, turns]) => {
      let sessionChanged = false
      const nextTurns = turns.map((turn) => {
        if (turn.id !== turnId) {
          return turn
        }

        found = true
        sessionChanged = true
        return update(turn)
      })

      return [sessionId, sessionChanged ? sortTurns(nextTurns) : turns]
    }),
  )

  if (!found) {
    return state
  }

  return {
    ...state,
    turnsBySession,
  }
}

const upsertRunner = (
  state: ClientState,
  runner: RunnerStatusInfo,
): ClientState => ({
  ...state,
  runners: sortRunners(
    state.runners.some((item) => item.runner === runner.runner)
      ? state.runners.map((item) =>
          item.runner === runner.runner ? runner : item,
        )
      : [...state.runners, runner],
  ),
})

const removeLiveChunk = (state: ClientState, turnId: TurnId): ClientState => {
  const liveChunks = { ...state.liveChunks }
  delete liveChunks[turnId]

  return {
    ...state,
    liveChunks,
  }
}

const sortTurns = (turns: Turn[]): Turn[] =>
  [...turns].sort((left, right) => left.seq - right.seq)

const sortSessions = (sessions: Session[]): Session[] =>
  [...sessions].sort(
    (left, right) =>
      Date.parse(right.updated_at) - Date.parse(left.updated_at) ||
      left.title.localeCompare(right.title),
  )

const sortRunners = (runners: RunnerStatusInfo[]): RunnerStatusInfo[] =>
  [...runners].sort((left, right) => left.runner.localeCompare(right.runner))
