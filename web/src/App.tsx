import {
  useCallback,
  useEffect,
  useMemo,
  useState,
  type FormEvent,
  type KeyboardEvent,
} from 'react'
import ReactMarkdown from 'react-markdown'
import type {
  CreateSessionRequest,
  Event,
  Harness,
  Lane,
  RunnerStatusInfo,
  Session,
  SessionId,
  Turn,
  TurnId,
} from './protocol'
import {
  emptyClientState,
  foldEvent,
  setRunners,
  setSessions,
  setTurns,
  type ClientState,
} from './state'

const tokenStorageKey = 'ceilidh.token'
const inFlightStatuses = new Set(['queued', 'claimed', 'working'])
const eventNames = [
  'turn_queued',
  'turn_claimed',
  'chunk',
  'turn_done',
  'turn_error',
  'runner_status',
]

type ApiRequestInit = Omit<RequestInit, 'body'> & {
  json?: unknown
}

type Pane = 'sessions' | 'chat' | 'new'

class ApiError extends Error {
  status: number

  constructor(status: number, message: string) {
    super(message)
    this.status = status
  }
}

function App() {
  const [token, setToken] = useState(readStoredToken)

  const saveToken = (nextToken: string) => {
    window.localStorage.setItem(tokenStorageKey, nextToken)
    setToken(nextToken)
  }

  const resetToken = () => {
    window.localStorage.removeItem(tokenStorageKey)
    setToken('')
  }

  if (!token) {
    return <TokenScreen onSave={saveToken} />
  }

  return <Workbench token={token} onResetToken={resetToken} />
}

function Workbench({
  token,
  onResetToken,
}: {
  token: string
  onResetToken: () => void
}) {
  const apiFetch = useApi(token)
  const [state, setState] = useState<ClientState>(() => emptyClientState())
  const [selectedSessionId, setSelectedSessionId] = useState<SessionId | null>(
    null,
  )
  const [pane, setPane] = useState<Pane>('sessions')
  const [creating, setCreating] = useState(false)
  const [loadingOverview, setLoadingOverview] = useState(true)
  const [overviewError, setOverviewError] = useState<string | null>(null)
  const [sendingSessionId, setSendingSessionId] = useState<SessionId | null>(
    null,
  )

  const applyEvent = useCallback((event: Event) => {
    setState((current) => foldEvent(current, event))
  }, [])

  const refreshOverview = useCallback(async () => {
    setLoadingOverview(true)
    setOverviewError(null)

    try {
      const [sessionsPayload, runnersPayload] = await Promise.all([
        apiFetch<unknown>('/api/sessions'),
        apiFetch<unknown>('/api/runners'),
      ])
      const sessions = collectionFromPayload<Session>(
        sessionsPayload,
        'sessions',
      )
      const runners = collectionFromPayload<RunnerStatusInfo>(
        runnersPayload,
        'runners',
      )

      setState((current) => setRunners(setSessions(current, sessions), runners))
      setSelectedSessionId((current) => current ?? sessions[0]?.id ?? null)
    } catch (error) {
      setOverviewError(errorMessage(error))
    } finally {
      setLoadingOverview(false)
    }
  }, [apiFetch])

  const refreshTurns = useCallback(
    async (sessionId: SessionId) => {
      const payload = await apiFetch<unknown>(
        `/api/sessions/${sessionId}/turns`,
      )
      const turns = collectionFromPayload<Turn>(payload, 'turns')
      setState((current) => setTurns(current, sessionId, turns))
    },
    [apiFetch],
  )

  useEffect(() => {
    const timer = window.setTimeout(() => {
      void refreshOverview()
    }, 0)

    return () => window.clearTimeout(timer)
  }, [refreshOverview])

  useEffect(() => {
    if (!selectedSessionId) {
      return undefined
    }

    const timer = window.setTimeout(() => {
      void refreshTurns(selectedSessionId).catch((error) => {
        setOverviewError(errorMessage(error))
      })
    }, 0)

    return () => window.clearTimeout(timer)
  }, [refreshTurns, selectedSessionId])

  useEventSource(eventUrl('/api/events', token), applyEvent)
  useEventSource(
    selectedSessionId
      ? eventUrl(`/api/sessions/${selectedSessionId}/events`, token)
      : null,
    applyEvent,
  )

  const selectedSession =
    state.sessions.find((session) => session.id === selectedSessionId) ?? null
  const selectedTurns = selectedSessionId
    ? state.turnsBySession[selectedSessionId] ?? []
    : []
  const sessionTurnMap = state.turnsBySession

  const selectSession = (sessionId: SessionId) => {
    setSelectedSessionId(sessionId)
    setCreating(false)
    setPane('chat')
  }

  const openNewSession = () => {
    setCreating(true)
    setPane('new')
  }

  const closeMainPane = () => {
    setCreating(false)
    setPane('sessions')
  }

  const createSession = async (request: CreateSessionRequest) => {
    const payload = await apiFetch<unknown>('/api/sessions', {
      method: 'POST',
      json: request,
    })
    const session = singleFromPayload<Session>(payload, 'session')

    if (session?.id) {
      setState((current) => setSessions(current, [session, ...current.sessions]))
      setSelectedSessionId(session.id)
      setCreating(false)
      setPane('chat')
      return
    }

    await refreshOverview()
    setCreating(false)
    setPane('chat')
  }

  const sendTurn = async (sessionId: SessionId, input: string) => {
    setSendingSessionId(sessionId)

    try {
      const payload = await apiFetch<unknown>(
        `/api/sessions/${sessionId}/turns`,
        {
          method: 'POST',
          json: { input },
        },
      )
      const turn = singleFromPayload<Turn>(payload, 'turn')

      if (turn?.id) {
        setState((current) =>
          foldEvent(current, {
            type: 'turn_queued',
            turn,
          }),
        )
      } else {
        await refreshTurns(sessionId)
      }
    } finally {
      setSendingSessionId(null)
    }
  }

  const mainPane = creating ? (
    <NewSessionForm onCancel={closeMainPane} onCreate={createSession} />
  ) : selectedSession ? (
    <ChatView
      liveChunks={state.liveChunks}
      onBack={closeMainPane}
      onSend={(input) => sendTurn(selectedSession.id, input)}
      sending={sendingSessionId === selectedSession.id}
      session={selectedSession}
      turns={selectedTurns}
    />
  ) : (
    <EmptyPane onNewSession={openNewSession} />
  )

  return (
    <div className="flex min-h-svh flex-col bg-[#080b10] text-slate-100">
      <div className="grid min-h-0 flex-1 md:grid-cols-[22rem_minmax(0,1fr)]">
        <aside
          className={`${pane === 'sessions' ? 'flex' : 'hidden'} min-h-0 flex-col border-r border-slate-800 bg-[#0c1118] md:flex`}
        >
          <SessionsPanel
            loading={loadingOverview}
            onChangeToken={onResetToken}
            onNewSession={openNewSession}
            onRefresh={refreshOverview}
            onSelect={selectSession}
            selectedSessionId={selectedSessionId}
            sessionTurnMap={sessionTurnMap}
            sessions={state.sessions}
          />
          {overviewError ? (
            <div className="border-t border-rose-500/30 bg-rose-500/10 px-4 py-3 text-sm text-rose-100">
              {overviewError}
            </div>
          ) : null}
        </aside>

        <main
          className={`${pane === 'sessions' ? 'hidden' : 'flex'} min-h-0 flex-col bg-[#090d13] md:flex`}
        >
          {mainPane}
        </main>
      </div>
      <RunnersStrip runners={state.runners} />
    </div>
  )
}

function TokenScreen({ onSave }: { onSave: (token: string) => void }) {
  const [value, setValue] = useState('')
  const [error, setError] = useState<string | null>(null)

  const submit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    const token = value.trim()

    if (!token) {
      setError('Enter the bearer token.')
      return
    }

    onSave(token)
  }

  return (
    <main className="flex min-h-svh items-center justify-center bg-[#080b10] px-4 text-slate-100">
      <form
        className="w-full max-w-sm rounded-md border border-slate-800 bg-[#0e141d] p-5 shadow-2xl shadow-black/30"
        onSubmit={submit}
      >
        <div className="mb-5">
          <p className="text-xs font-semibold uppercase text-emerald-300">
            ceilidh
          </p>
          <h1 className="mt-2 text-2xl font-semibold text-white">
            Bearer token
          </h1>
        </div>

        <label className="block text-sm font-medium text-slate-300">
          Token
          <input
            autoComplete="off"
            autoFocus
            className="mt-2 w-full rounded-md border border-slate-700 bg-slate-950 px-3 py-2 text-base text-slate-100 outline-none transition focus:border-emerald-300"
            onChange={(event) => {
              setValue(event.target.value)
              setError(null)
            }}
            type="password"
            value={value}
          />
        </label>

        {error ? <p className="mt-3 text-sm text-rose-300">{error}</p> : null}

        <button
          className="mt-5 inline-flex h-10 w-full items-center justify-center rounded-md bg-emerald-300 px-4 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200 focus:outline-none focus:ring-2 focus:ring-emerald-200 focus:ring-offset-2 focus:ring-offset-slate-950"
          type="submit"
        >
          Continue
        </button>
      </form>
    </main>
  )
}

function SessionsPanel({
  loading,
  onChangeToken,
  onNewSession,
  onRefresh,
  onSelect,
  selectedSessionId,
  sessionTurnMap,
  sessions,
}: {
  loading: boolean
  onChangeToken: () => void
  onNewSession: () => void
  onRefresh: () => void
  onSelect: (sessionId: SessionId) => void
  selectedSessionId: SessionId | null
  sessionTurnMap: Record<SessionId, Turn[]>
  sessions: Session[]
}) {
  return (
    <>
      <div className="flex items-center justify-between gap-3 border-b border-slate-800 px-4 py-3">
        <div>
          <h1 className="text-lg font-semibold text-white">Sessions</h1>
          <p className="text-xs text-slate-500">ceilidh</p>
        </div>
        <div className="flex items-center gap-2">
          <button
            aria-label="Refresh sessions"
            className="inline-flex h-9 min-w-16 items-center justify-center rounded-md border border-slate-700 bg-slate-900 px-3 text-sm font-medium text-slate-200 transition hover:border-slate-500"
            onClick={onRefresh}
            type="button"
          >
            {loading ? <span className="spinner" /> : 'Reload'}
          </button>
          <button
            className="inline-flex h-9 items-center justify-center gap-2 rounded-md bg-emerald-300 px-3 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200"
            onClick={onNewSession}
            type="button"
          >
            <span aria-hidden="true">+</span>
            New
          </button>
        </div>
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto">
        {sessions.length === 0 && !loading ? (
          <div className="px-4 py-8 text-sm text-slate-400">
            No sessions yet.
          </div>
        ) : null}

        <div className="divide-y divide-slate-800/80">
          {sessions.map((session) => {
            const turns = sessionTurnMap[session.id] ?? []
            const runState = sessionRunState(turns)

            return (
              <button
                className={`block w-full px-4 py-3 text-left transition hover:bg-slate-900/80 ${
                  selectedSessionId === session.id ? 'bg-slate-900' : ''
                }`}
                key={session.id}
                onClick={() => onSelect(session.id)}
                type="button"
              >
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0">
                    <div className="truncate text-sm font-semibold text-slate-100">
                      {session.title}
                    </div>
                    <div className="mt-1 flex flex-wrap items-center gap-2 text-xs text-slate-400">
                      <LaneBadge lane={session.lane} />
                      <span>{session.runner_affinity ?? 'unassigned'}</span>
                    </div>
                  </div>
                  <RunStateDot state={runState} />
                </div>
                <div className="mt-2 text-xs text-slate-500">
                  {formatDateTime(session.updated_at)}
                </div>
              </button>
            )
          })}
        </div>
      </div>

      <div className="border-t border-slate-800 px-4 py-3">
        <button
          className="text-sm font-medium text-slate-400 transition hover:text-slate-100"
          onClick={onChangeToken}
          type="button"
        >
          Change token
        </button>
      </div>
    </>
  )
}

function NewSessionForm({
  onCancel,
  onCreate,
}: {
  onCancel: () => void
  onCreate: (request: CreateSessionRequest) => Promise<void>
}) {
  const [title, setTitle] = useState('')
  const [harness, setHarness] = useState<Harness>('claude-code')
  const [model, setModel] = useState(defaultModelForHarness('claude-code'))
  const [effort, setEffort] = useState('')
  const [repoUrl, setRepoUrl] = useState('')
  const [baseBranch, setBaseBranch] = useState('')
  const [allowPush, setAllowPush] = useState(false)
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    const cleanedTitle = title.trim()

    if (!cleanedTitle) {
      setError('Title is required.')
      return
    }

    setSubmitting(true)
    setError(null)

    const lane: Lane = {
      harness,
      model: model.trim() || defaultModelForHarness(harness),
      ...optionalField('effort', effort),
    }
    const request: CreateSessionRequest = {
      title: cleanedTitle,
      lane,
      profile: {
        ...optionalField('repo_url', repoUrl),
        ...optionalField('base_branch', baseBranch),
        allow_push: allowPush,
      },
    }

    try {
      await onCreate(request)
    } catch (createError) {
      setError(errorMessage(createError))
    } finally {
      setSubmitting(false)
    }
  }

  const changeHarness = (nextHarness: Harness) => {
    const oldDefault = defaultModelForHarness(harness)
    setHarness(nextHarness)

    if (!model.trim() || model.trim() === oldDefault) {
      setModel(defaultModelForHarness(nextHarness))
    }
  }

  return (
    <section className="flex min-h-0 flex-1 flex-col">
      <div className="flex items-center gap-3 border-b border-slate-800 px-4 py-3">
        <button
          className="inline-flex h-9 items-center justify-center rounded-md border border-slate-700 bg-slate-900 px-3 text-sm font-medium text-slate-200 md:hidden"
          onClick={onCancel}
          type="button"
        >
          {'<'} Back
        </button>
        <div>
          <h1 className="text-lg font-semibold text-white">New session</h1>
          <p className="text-xs text-slate-500">Route and workspace</p>
        </div>
      </div>

      <form
        className="mx-auto grid w-full max-w-3xl gap-4 overflow-y-auto p-4 md:grid-cols-2 md:p-6"
        onSubmit={submit}
      >
        <label className="grid gap-2 text-sm font-medium text-slate-300 md:col-span-2">
          Title
          <input
            autoFocus
            className="form-field"
            onChange={(event) => {
              setTitle(event.target.value)
              setError(null)
            }}
            value={title}
          />
        </label>

        <label className="grid gap-2 text-sm font-medium text-slate-300">
          Harness
          <select
            className="form-field"
            onChange={(event) => changeHarness(event.target.value as Harness)}
            value={harness}
          >
            <option value="claude-code">claude-code</option>
            <option value="codex">codex</option>
            <option value="mock">mock</option>
          </select>
        </label>

        <label className="grid gap-2 text-sm font-medium text-slate-300">
          Model
          <input
            className="form-field"
            onChange={(event) => setModel(event.target.value)}
            value={model}
          />
        </label>

        <label className="grid gap-2 text-sm font-medium text-slate-300">
          Effort
          <input
            className="form-field"
            onChange={(event) => setEffort(event.target.value)}
            placeholder="optional"
            value={effort}
          />
        </label>

        <label className="grid gap-2 text-sm font-medium text-slate-300">
          Base branch
          <input
            className="form-field"
            onChange={(event) => setBaseBranch(event.target.value)}
            placeholder="optional"
            value={baseBranch}
          />
        </label>

        <label className="grid gap-2 text-sm font-medium text-slate-300 md:col-span-2">
          Repo URL
          <input
            className="form-field"
            onChange={(event) => setRepoUrl(event.target.value)}
            placeholder="optional"
            type="url"
            value={repoUrl}
          />
        </label>

        <label className="flex items-center gap-3 rounded-md border border-slate-800 bg-slate-950/60 px-3 py-3 text-sm font-medium text-slate-200 md:col-span-2">
          <input
            checked={allowPush}
            className="h-4 w-4 accent-emerald-300"
            onChange={(event) => setAllowPush(event.target.checked)}
            type="checkbox"
          />
          Allow push
        </label>

        {error ? (
          <div className="rounded-md border border-rose-500/30 bg-rose-500/10 px-3 py-2 text-sm text-rose-100 md:col-span-2">
            {error}
          </div>
        ) : null}

        <div className="flex items-center justify-end gap-2 md:col-span-2">
          <button
            className="hidden h-10 items-center rounded-md border border-slate-700 bg-slate-900 px-4 text-sm font-medium text-slate-200 transition hover:border-slate-500 md:inline-flex"
            onClick={onCancel}
            type="button"
          >
            Cancel
          </button>
          <button
            className="inline-flex h-10 w-full items-center justify-center gap-2 rounded-md bg-emerald-300 px-4 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200 disabled:cursor-not-allowed disabled:bg-slate-600 disabled:text-slate-300 md:w-auto"
            disabled={submitting}
            type="submit"
          >
            {submitting ? <span className="spinner dark" /> : null}
            Create
          </button>
        </div>
      </form>
    </section>
  )
}

function ChatView({
  liveChunks,
  onBack,
  onSend,
  sending,
  session,
  turns,
}: {
  liveChunks: Record<TurnId, string>
  onBack: () => void
  onSend: (input: string) => Promise<void>
  sending: boolean
  session: Session
  turns: Turn[]
}) {
  const [input, setInput] = useState('')
  const [error, setError] = useState<string | null>(null)
  const latestRunState = sessionRunState(turns)
  const turnInFlight = turns.some((turn) => inFlightStatuses.has(turn.status))
  const disabled = sending || turnInFlight

  const submit = async () => {
    const trimmed = input.trim()

    if (!trimmed || disabled) {
      return
    }

    setError(null)

    try {
      await onSend(trimmed)
      setInput('')
    } catch (sendError) {
      setError(errorMessage(sendError))
    }
  }

  const keyDown = (event: KeyboardEvent<HTMLTextAreaElement>) => {
    if (event.key === 'Enter' && (event.metaKey || event.ctrlKey)) {
      event.preventDefault()
      void submit()
    }
  }

  return (
    <section className="flex min-h-0 flex-1 flex-col">
      <header className="flex items-center justify-between gap-3 border-b border-slate-800 bg-[#0c1118] px-4 py-3">
        <div className="flex min-w-0 items-center gap-3">
          <button
            className="inline-flex h-9 items-center justify-center rounded-md border border-slate-700 bg-slate-900 px-3 text-sm font-medium text-slate-200 md:hidden"
            onClick={onBack}
            type="button"
          >
            {'<'} Back
          </button>
          <div className="min-w-0">
            <div className="flex min-w-0 items-center gap-2">
              <RunStateDot state={latestRunState} />
              <h1 className="truncate text-base font-semibold text-white">
                {session.title}
              </h1>
            </div>
            <div className="mt-1 flex flex-wrap items-center gap-2 text-xs text-slate-400">
              <LaneBadge lane={session.lane} />
              <span>{session.runner_affinity ?? 'unassigned'}</span>
              <span>{formatDateTime(session.updated_at)}</span>
            </div>
          </div>
        </div>
      </header>

      <div className="min-h-0 flex-1 overflow-y-auto px-3 py-4 md:px-6">
        {turns.length === 0 ? (
          <div className="mx-auto mt-12 max-w-md rounded-md border border-slate-800 bg-slate-950/50 p-4 text-sm text-slate-400">
            No turns yet.
          </div>
        ) : null}

        <div className="mx-auto flex max-w-4xl flex-col gap-4">
          {turns.map((turn) => (
            <TurnBlock key={turn.id} liveText={liveChunks[turn.id]} turn={turn} />
          ))}
        </div>
      </div>

      <form
        className="border-t border-slate-800 bg-[#0c1118] p-3 md:p-4"
        onSubmit={(event) => {
          event.preventDefault()
          void submit()
        }}
      >
        <div className="mx-auto max-w-4xl">
          {error ? (
            <div className="mb-2 rounded-md border border-rose-500/30 bg-rose-500/10 px-3 py-2 text-sm text-rose-100">
              {error}
            </div>
          ) : null}

          <div className="flex items-end gap-2">
            <textarea
              className="min-h-24 flex-1 resize-none rounded-md border border-slate-700 bg-slate-950 px-3 py-2 text-base leading-6 text-slate-100 outline-none transition placeholder:text-slate-600 focus:border-emerald-300 disabled:cursor-not-allowed disabled:opacity-60"
              disabled={disabled}
              onChange={(event) => setInput(event.target.value)}
              onKeyDown={keyDown}
              placeholder={
                turnInFlight ? 'Turn in flight' : 'Message for the next turn'
              }
              value={input}
            />
            <button
              className="inline-flex h-11 min-w-24 items-center justify-center gap-2 rounded-md bg-emerald-300 px-4 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200 disabled:cursor-not-allowed disabled:bg-slate-600 disabled:text-slate-300"
              disabled={disabled || !input.trim()}
              type="submit"
            >
              {sending || turnInFlight ? <span className="spinner dark" /> : null}
              Send
            </button>
          </div>
        </div>
      </form>
    </section>
  )
}

function TurnBlock({ liveText, turn }: { liveText?: string; turn: Turn }) {
  const waiting =
    inFlightStatuses.has(turn.status) && !liveText && !turn.envelope

  return (
    <article className="grid gap-2">
      <div className="flex justify-end">
        <div className="max-w-[88%] rounded-md bg-sky-500 px-3 py-2 text-sm leading-6 text-white shadow-lg shadow-sky-950/20 md:max-w-[72%]">
          <div className="whitespace-pre-wrap">{turn.input}</div>
        </div>
      </div>

      {turn.envelope ? <EnvelopeBubble turn={turn} /> : null}

      {turn.error ? (
        <div className="flex justify-start">
          <div className="max-w-[88%] rounded-md border border-rose-500/30 bg-rose-500/10 px-3 py-2 text-sm leading-6 text-rose-100 md:max-w-[72%]">
            {turn.error}
          </div>
        </div>
      ) : null}

      {liveText ? (
        <div className="flex justify-start">
          <div className="max-w-[88%] rounded-md border border-slate-700 bg-slate-900 px-3 py-2 text-sm leading-6 text-slate-100 md:max-w-[72%]">
            <div className="mb-2 flex items-center gap-2 text-xs font-medium text-emerald-300">
              <span className="spinner" />
              Working
            </div>
            <div className="whitespace-pre-wrap">{liveText}</div>
          </div>
        </div>
      ) : null}

      {waiting ? (
        <div className="flex justify-start">
          <div className="inline-flex items-center gap-2 rounded-md border border-slate-700 bg-slate-900 px-3 py-2 text-sm text-slate-300">
            <span className="spinner" />
            {statusLabel(turn.status)}
          </div>
        </div>
      ) : null}
    </article>
  )
}

function EnvelopeBubble({ turn }: { turn: Turn }) {
  const envelope = turn.envelope
  const questions = envelope?.questions ?? []

  if (!envelope) {
    return null
  }

  return (
    <div className="flex justify-start">
      <div className="max-w-[88%] rounded-md border border-slate-700 bg-slate-900 px-3 py-3 text-sm leading-6 text-slate-100 shadow-lg shadow-black/20 md:max-w-[72%]">
        <strong className="block text-base text-white">{envelope.headline}</strong>
        {envelope.body_markdown ? (
          <div className="markdown mt-3">
            <ReactMarkdown>{envelope.body_markdown}</ReactMarkdown>
          </div>
        ) : null}

        {questions.length > 0 ? (
          <div className="mt-3 rounded-md border border-amber-300/30 bg-amber-300/10 p-3">
            <div className="mb-2 text-xs font-semibold uppercase text-amber-200">
              Questions
            </div>
            <div className="grid gap-3">
              {questions.map((question) => (
                <div key={question.text}>
                  <div className="text-sm font-medium text-amber-50">
                    {question.text}
                  </div>
                  {question.recommendation ? (
                    <div className="mt-1 text-xs text-amber-100/80">
                      Recommendation: {question.recommendation}
                    </div>
                  ) : null}
                </div>
              ))}
            </div>
          </div>
        ) : null}

        <div className="mt-3 flex flex-wrap gap-2 text-xs text-slate-500">
          <span>turn {turn.seq}</span>
          {turn.commit ? <span>{turn.commit.slice(0, 12)}</span> : null}
          {turn.finished_at ? <span>{formatDateTime(turn.finished_at)}</span> : null}
        </div>
      </div>
    </div>
  )
}

function EmptyPane({ onNewSession }: { onNewSession: () => void }) {
  return (
    <section className="flex min-h-0 flex-1 items-center justify-center p-4">
      <div className="w-full max-w-sm rounded-md border border-slate-800 bg-slate-950/50 p-4 text-center">
        <h1 className="text-lg font-semibold text-white">No session selected</h1>
        <button
          className="mt-4 inline-flex h-10 items-center justify-center gap-2 rounded-md bg-emerald-300 px-4 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200"
          onClick={onNewSession}
          type="button"
        >
          <span aria-hidden="true">+</span>
          New session
        </button>
      </div>
    </section>
  )
}

function RunnersStrip({ runners }: { runners: RunnerStatusInfo[] }) {
  return (
    <footer className="flex min-h-11 items-center gap-2 overflow-x-auto border-t border-slate-800 bg-[#080b10] px-3 py-2">
      {runners.length === 0 ? (
        <span className="rounded-md border border-slate-800 bg-slate-950 px-3 py-1 text-xs text-slate-500">
          No runners
        </span>
      ) : (
        runners.map((runner) => (
          <span
            className="inline-flex shrink-0 items-center gap-2 rounded-md border border-slate-800 bg-slate-950 px-3 py-1 text-xs text-slate-300"
            key={runner.runner}
          >
            <span
              className={`h-2 w-2 rounded-full ${
                runner.online ? 'bg-emerald-300' : 'bg-slate-600'
              }`}
            />
            <span className="max-w-44 truncate">{runner.runner}</span>
            <span className="text-slate-500">
              {runner.active_turns ?? 0} active
            </span>
          </span>
        ))
      )}
    </footer>
  )
}

function LaneBadge({ lane }: { lane: Lane }) {
  return (
    <span className="inline-flex max-w-full items-center rounded-md border border-cyan-300/20 bg-cyan-300/10 px-2 py-0.5 text-xs font-medium text-cyan-100">
      <span className="truncate">
        {lane.harness}/{lane.model}
      </span>
    </span>
  )
}

function RunStateDot({ state }: { state: 'working' | 'queued' | 'idle' }) {
  const className =
    state === 'working'
      ? 'bg-sky-300 shadow-[0_0_0_4px_rgba(125,211,252,0.12)]'
      : state === 'queued'
        ? 'bg-amber-300 shadow-[0_0_0_4px_rgba(252,211,77,0.12)]'
        : 'bg-slate-600'

  return (
    <span
      aria-label={state}
      className={`mt-1 inline-block h-2.5 w-2.5 shrink-0 rounded-full ${className}`}
      title={state}
    />
  )
}

function useApi(token: string) {
  return useCallback(
    async <T,>(path: string, init: ApiRequestInit = {}): Promise<T> => {
      const headers = new Headers(init.headers)
      headers.set('Authorization', `Bearer ${token}`)

      const request: RequestInit = {
        ...init,
        headers,
      }

      if (init.json !== undefined) {
        headers.set('Content-Type', 'application/json')
        request.body = JSON.stringify(init.json)
      }

      const response = await fetch(path, request)
      const body = await response.text()

      if (!response.ok) {
        throw new ApiError(response.status, responseError(response, body))
      }

      if (!body) {
        return undefined as T
      }

      return JSON.parse(body) as T
    },
    [token],
  )
}

function useEventSource(url: string | null, onEvent: (event: Event) => void) {
  const stableOnEvent = useMemo(() => onEvent, [onEvent])

  useEffect(() => {
    if (!url) {
      return undefined
    }

    let closed = false
    let retryMs = 1_000
    let retryTimer: number | null = null
    let source: EventSource | null = null

    const clearRetry = () => {
      if (retryTimer !== null) {
        window.clearTimeout(retryTimer)
        retryTimer = null
      }
    }

    const connect = () => {
      if (closed) {
        return
      }

      source = new EventSource(url)
      const handleMessage = (message: MessageEvent) => {
        const event = parseEvent(message.data)

        if (event) {
          stableOnEvent(event)
        }
      }

      source.onopen = () => {
        retryMs = 1_000
      }
      source.onmessage = handleMessage
      for (const eventName of eventNames) {
        source.addEventListener(eventName, handleMessage as EventListener)
      }
      source.onerror = () => {
        source?.close()

        if (closed) {
          return
        }

        clearRetry()
        retryTimer = window.setTimeout(connect, retryMs)
        retryMs = Math.min(retryMs * 2, 15_000)
      }
    }

    connect()

    return () => {
      closed = true
      clearRetry()
      source?.close()
    }
  }, [stableOnEvent, url])
}

function eventUrl(path: string, token: string) {
  return `${path}?token=${encodeURIComponent(token)}`
}

function parseEvent(data: string): Event | null {
  try {
    const value = JSON.parse(data)

    if (isRecord(value) && typeof value.type === 'string') {
      return value as Event
    }
  } catch {
    return null
  }

  return null
}

function collectionFromPayload<T>(payload: unknown, key: string): T[] {
  if (Array.isArray(payload)) {
    return payload as T[]
  }

  if (isRecord(payload) && Array.isArray(payload[key])) {
    return payload[key] as T[]
  }

  return []
}

function singleFromPayload<T>(payload: unknown, key: string): T | null {
  if (isRecord(payload) && isRecord(payload[key])) {
    return payload[key] as T
  }

  if (isRecord(payload)) {
    return payload as T
  }

  return null
}

function responseError(response: Response, body: string): string {
  const fallback =
    response.status === 409
      ? 'A turn is already in flight for this session.'
      : `Request failed with status ${response.status}.`

  if (!body) {
    return fallback
  }

  try {
    const payload = JSON.parse(body)

    if (isRecord(payload)) {
      const message = payload.message ?? payload.error

      if (typeof message === 'string' && message.trim()) {
        return message
      }
    }
  } catch {
    return response.status === 409 ? fallback : body
  }

  return fallback
}

function errorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    return error.message
  }

  if (error instanceof Error) {
    return error.message
  }

  return 'Request failed.'
}

function sessionRunState(turns: Turn[]): 'working' | 'queued' | 'idle' {
  const latestTurn = turns.at(-1)

  if (!latestTurn) {
    return 'idle'
  }

  if (latestTurn.status === 'queued') {
    return 'queued'
  }

  if (latestTurn.status === 'claimed' || latestTurn.status === 'working') {
    return 'working'
  }

  return 'idle'
}

function statusLabel(status: Turn['status']) {
  switch (status) {
    case 'queued':
      return 'Queued'
    case 'claimed':
      return 'Claimed'
    case 'working':
      return 'Working'
    case 'done':
      return 'Done'
    case 'error':
      return 'Error'
    case 'capped':
      return 'Capped'
  }
}

function defaultModelForHarness(harness: Harness): string {
  switch (harness) {
    case 'claude-code':
      return 'opus'
    case 'codex':
      return 'gpt-5'
    case 'mock':
      return 'mock'
  }
}

function optionalField<K extends string>(
  key: K,
  value: string,
): Partial<Record<K, string>> {
  const trimmed = value.trim()
  return trimmed ? ({ [key]: trimmed } as Partial<Record<K, string>>) : {}
}

function formatDateTime(value: string | null | undefined) {
  if (!value) {
    return 'never'
  }

  const date = new Date(value)

  if (Number.isNaN(date.getTime())) {
    return value
  }

  return new Intl.DateTimeFormat(undefined, {
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    month: 'short',
  }).format(date)
}

function readStoredToken() {
  return window.localStorage.getItem(tokenStorageKey) ?? ''
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}

export default App
