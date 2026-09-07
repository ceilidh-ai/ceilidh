import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type FormEvent,
  type KeyboardEvent,
} from 'react'
import ReactMarkdown from 'react-markdown'
import type {
  CallerConfig,
  CreateSessionRequest,
  Envelope,
  Event,
  Harness,
  Lane,
  ModelChoice,
  RepoList,
  RepoOwner,
  RunnerStatusInfo,
  Session,
  SessionId,
  Turn,
  TurnId,
  TurnStatus,
} from './protocol'
import {
  applyTurn,
  removeSession,
  upsertSession,
  emptyClientState,
  foldEvent,
  setRunners,
  setSessions,
  setTurns,
  type ClientState,
} from './state'

const tokenStorageKey = 'ceilidh.token'
const inFlightStatuses = new Set(['queued', 'claimed', 'working'])
const maxTreeDepth = 3
const sessionHydrationLimit = 40
const otherModelKey = '__other__'
const emptyConfig: CallerConfig = { default_repo_url: null, models: [] }
const emptyRepos: RepoList = { owners: [], available: false }
const otherRepoKey = '__other__'
const dotClasses: Record<TurnStatus, string> = {
  queued: 'animate-pulse bg-amber-300 shadow-[0_0_0_4px_rgba(252,211,77,0.14)]',
  claimed: 'animate-pulse bg-sky-300 shadow-[0_0_0_4px_rgba(125,211,252,0.14)]',
  working: 'animate-pulse bg-sky-300 shadow-[0_0_0_4px_rgba(125,211,252,0.14)]',
  done: 'bg-emerald-400',
  error: 'bg-rose-400',
  capped: 'bg-rose-400',
  cancelled: 'bg-slate-400',
}
const eventNames = [
  'turn_queued',
  'turn_claimed',
  'chunk',
  'turn_done',
  'turn_error',
  'turn_cancelled',
  'session_created',
  'session_updated',
  'session_deleted',
  'runner_status',
]

type ApiRequestInit = Omit<RequestInit, 'body'> & {
  json?: unknown
}

type Pane = 'sessions' | 'chat' | 'new'

type ComposerNote = {
  kind: 'error' | 'notice'
  text: string
}

type SessionNode = {
  session: Session
  depth: number
  childCount: number
}

class ApiError extends Error {
  status: number

  constructor(status: number, message: string) {
    super(message)
    this.status = status
  }
}

type Entry =
  | { kind: 'checking' }
  | { kind: 'cookie'; email: string }
  | { kind: 'token'; token: string; google: boolean }
  | { kind: 'login'; google: boolean }

/** A login cookie wins; then a stored token; then the login screen, which
 * offers Google when the caller has it and the token field either way. */
function App() {
  const [entry, setEntry] = useState<Entry>({ kind: 'checking' })

  useEffect(() => {
    let cancelled = false
    const decide = async () => {
      const me = await fetch('/api/me', { credentials: 'same-origin' }).catch(
        () => null,
      )
      if (me?.ok) {
        const body = (await me.json().catch(() => null)) as {
          email?: string | null
        } | null
        if (body?.email) {
          if (!cancelled) {
            setEntry({ kind: 'cookie', email: body.email })
          }
          return
        }
      }
      const config = await fetch('/auth/config').catch(() => null)
      const google: boolean = Boolean(
        config?.ok &&
          ((await config.json().catch(() => null)) as {
            google?: boolean
          } | null)?.google === true,
      )
      const token = readStoredToken()
      if (!cancelled) {
        setEntry(
          token ? { kind: 'token', token, google } : { kind: 'login', google },
        )
      }
    }
    void decide()
    return () => {
      cancelled = true
    }
  }, [])

  const saveToken = (nextToken: string) => {
    window.localStorage.setItem(tokenStorageKey, nextToken)
    setEntry({ kind: 'token', token: nextToken, google: false })
  }

  const signOut = async () => {
    window.localStorage.removeItem(tokenStorageKey)
    await fetch('/auth/logout', {
      method: 'POST',
      credentials: 'same-origin',
    }).catch(() => null)
    window.location.reload()
  }

  switch (entry.kind) {
    case 'checking':
      return (
        <main className="flex min-h-svh items-center justify-center bg-[#080b10]">
          <span className="spinner" />
        </main>
      )
    case 'login':
      return <TokenScreen google={entry.google} onSave={saveToken} />
    case 'token':
      return (
        <Workbench
          onResetToken={signOut}
          signOutLabel="Change token"
          token={entry.token}
        />
      )
    case 'cookie':
      return (
        <Workbench
          onResetToken={signOut}
          signOutLabel={`Sign out ${entry.email}`}
          token=""
        />
      )
  }
}

function Workbench({
  token,
  onResetToken,
  signOutLabel,
}: {
  token: string
  onResetToken: () => void
  signOutLabel: string
}) {
  const apiFetch = useApi(token)
  const [state, setState] = useState<ClientState>(() => emptyClientState())
  const [selectedSessionId, setSelectedSessionId] = useState<SessionId | null>(
    null,
  )
  const [pane, setPane] = useState<Pane>('sessions')
  const [creating, setCreating] = useState(false)
  const [config, setConfig] = useState<CallerConfig | null>(null)
  const [repos, setRepos] = useState<RepoList>(emptyRepos)
  const [showArchived, setShowArchived] = useState(false)
  const [selecting, setSelecting] = useState(false)
  const [picked, setPicked] = useState<Set<SessionId>>(() => new Set())
  const [archivingPicked, setArchivingPicked] = useState(false)
  const [loadingOverview, setLoadingOverview] = useState(true)
  const [overviewError, setOverviewError] = useState<string | null>(null)
  const [sendingSessionId, setSendingSessionId] = useState<SessionId | null>(
    null,
  )

  const applyEvent = useCallback((event: Event) => {
    setState((current) => foldEvent(current, event))
  }, [])

  // Every row carries a status dot, so the list needs turns for sessions the
  // operator has not opened. Bounded, and the stream keeps them fresh after.
  const hydrateTurns = useCallback(
    async (targets: Session[]) => {
      const loaded = await Promise.all(
        targets.map(async (session) => {
          try {
            const payload = await apiFetch<unknown>(
              `/api/sessions/${session.id}/turns`,
            )

            return {
              sessionId: session.id,
              turns: collectionFromPayload<Turn>(payload, 'turns'),
            }
          } catch {
            return null
          }
        }),
      )

      setState((current) =>
        loaded.reduce(
          (carried, entry) =>
            entry ? setTurns(carried, entry.sessionId, entry.turns) : carried,
          current,
        ),
      )
    },
    [apiFetch],
  )

  const refreshOverview = useCallback(async () => {
    setLoadingOverview(true)
    setOverviewError(null)

    try {
      const [configPayload, reposPayload, sessionsPayload, runnersPayload] =
        await Promise.all([
          // A config the caller cannot serve still leaves the form usable
          // through its free-text model path.
          apiFetch<unknown>('/api/config').catch(() => null),
          apiFetch<unknown>('/api/repos').catch(() => null),
          apiFetch<unknown>('/api/sessions?include=archived'),
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

      setConfig(configFromPayload(configPayload))
      setRepos(reposFromPayload(reposPayload))
      setState((current) => setRunners(setSessions(current, sessions), runners))
      setSelectedSessionId((current) => current ?? sessions[0]?.id ?? null)
      await hydrateTurns(sessions.slice(0, sessionHydrationLimit))
    } catch (error) {
      setOverviewError(errorMessage(error))
    } finally {
      setLoadingOverview(false)
    }
  }, [apiFetch, hydrateTurns])

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

  const selectedSession =
    state.sessions.find((session) => session.id === selectedSessionId) ?? null
  // Archived sessions stay out of the way unless asked for; a child of a
  // visible session stays visible so the tree never loses a branch.
  const visibleSessions = showArchived
    ? state.sessions
    : state.sessions.filter((session) => session.status !== 'archived')
  const archivedCount = state.sessions.length - visibleSessions.length

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

  const setSessionArchived = async (sessionId: SessionId, archived: boolean) => {
    const payload = await apiFetch<unknown>(
      `/api/sessions/${sessionId}/${archived ? 'archive' : 'unarchive'}`,
      { method: 'POST' },
    )
    const session = singleFromPayload<Session>(payload, 'session')
    if (session?.id) {
      setState((current) => upsertSession(current, session))
    } else {
      await refreshOverview()
    }
  }

  const toggleSelecting = () => {
    setSelecting((value) => !value)
    setPicked(new Set())
  }

  const togglePicked = (sessionId: SessionId) => {
    setPicked((current) => {
      const next = new Set(current)
      if (next.has(sessionId)) {
        next.delete(sessionId)
      } else {
        next.add(sessionId)
      }
      return next
    })
  }

  // Archive every picked session that is still active. Archiving a parent
  // already takes its sub-agents, so a picked child under a picked parent
  // simply comes back as already archived.
  const archivePicked = async () => {
    const targets = state.sessions.filter(
      (session) => picked.has(session.id) && session.status !== 'archived',
    )
    if (targets.length === 0) {
      return
    }
    setArchivingPicked(true)
    try {
      for (const target of targets) {
        try {
          await setSessionArchived(target.id, true)
        } catch (error) {
          setOverviewError(errorMessage(error))
        }
      }
      setPicked(new Set())
      setSelecting(false)
    } finally {
      setArchivingPicked(false)
    }
  }

  const deleteSession = async (sessionId: SessionId) => {
    await apiFetch<void>(`/api/sessions/${sessionId}`, { method: 'DELETE' })
    setState((current) => removeSession(current, sessionId))
    setSelectedSessionId(null)
    setPane('sessions')
  }

  const cancelTurn = async (sessionId: SessionId, turnId: TurnId) => {
    const payload = await apiFetch<unknown>(
      `/api/sessions/${sessionId}/turns/${turnId}/cancel`,
      { method: 'POST' },
    )
    const turn = singleFromPayload<Turn>(payload, 'turn')

    if (turn?.id) {
      setState((current) => applyTurn(current, turn))
      return
    }

    await refreshTurns(sessionId)
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
    config ? (
      <NewSessionForm
        config={config}
        onCancel={closeMainPane}
        onCreate={createSession}
        repos={repos}
      />
    ) : (
      <section className="flex min-h-0 flex-1 items-center justify-center p-4">
        <span className="spinner" />
      </section>
    )
  ) : selectedSession ? (
    <ChatView
      childSessions={childrenOf(state.sessions, selectedSession.id)}
      liveChunks={state.liveChunks}
      onArchive={(archived) => setSessionArchived(selectedSession.id, archived)}
      onBack={closeMainPane}
      onDelete={() => deleteSession(selectedSession.id)}
      onCancel={(turnId) => cancelTurn(selectedSession.id, turnId)}
      onOpenSession={selectSession}
      onSend={(input) => sendTurn(selectedSession.id, input)}
      parentSession={parentOf(state.sessions, selectedSession)}
      sending={sendingSessionId === selectedSession.id}
      session={selectedSession}
      sessionTurnMap={sessionTurnMap}
      turns={selectedTurns}
    />
  ) : (
    <EmptyPane onNewSession={openNewSession} />
  )

  return (
    <div className="flex h-svh flex-col overflow-hidden bg-[#080b10] text-slate-100">
      <div className="grid min-h-0 min-w-0 flex-1 md:grid-cols-[22rem_minmax(0,1fr)]">
        <aside
          className={`${pane === 'sessions' ? 'flex' : 'hidden'} min-h-0 min-w-0 flex-col border-r border-slate-800 bg-[#0c1118] md:flex`}
        >
          <SessionsPanel
            loading={loadingOverview}
            onChangeToken={onResetToken}
            changeTokenLabel={signOutLabel}
            onNewSession={openNewSession}
            onRefresh={refreshOverview}
            onSelect={selectSession}
            archivedCount={archivedCount}
            archivingPicked={archivingPicked}
            onArchivePicked={archivePicked}
            onToggleArchived={() => setShowArchived((value) => !value)}
            onTogglePicked={togglePicked}
            onToggleSelecting={toggleSelecting}
            picked={picked}
            selecting={selecting}
            selectedSessionId={selectedSessionId}
            sessionTurnMap={sessionTurnMap}
            sessions={visibleSessions}
            showArchived={showArchived}
          />
          {overviewError ? (
            <div className="border-t border-rose-500/30 bg-rose-500/10 px-4 py-3 text-sm text-rose-100">
              {overviewError}
            </div>
          ) : null}
        </aside>

        <main
          className={`${pane === 'sessions' ? 'hidden' : 'flex'} min-h-0 min-w-0 flex-col bg-[#090d13] md:flex`}
        >
          {mainPane}
        </main>
      </div>
      <RunnersStrip runners={state.runners} />
    </div>
  )
}

function TokenScreen({
  google,
  onSave,
}: {
  google: boolean
  onSave: (token: string) => void
}) {
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
            {google ? 'Sign in' : 'Bearer token'}
          </h1>
        </div>

        {google ? (
          <>
            <a
              className="inline-flex h-11 w-full items-center justify-center rounded-md bg-white px-4 text-sm font-semibold text-slate-900 transition hover:bg-slate-200"
              href="/auth/login"
            >
              Sign in with Google
            </a>
            <p className="mt-5 text-xs uppercase tracking-wide text-slate-500">
              or use a token
            </p>
          </>
        ) : null}

        <label className="block text-sm font-medium text-slate-300">
          Token
          <input
            autoComplete="off"
            autoFocus
            className="mt-2 h-11 w-full rounded-md border border-slate-700 bg-slate-950 px-3 py-2 text-[16px] text-slate-100 outline-none transition focus:border-emerald-300"
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
          className="mt-5 inline-flex h-11 w-full items-center justify-center rounded-md bg-emerald-300 px-4 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200 focus:outline-none focus:ring-2 focus:ring-emerald-200 focus:ring-offset-2 focus:ring-offset-slate-950"
          type="submit"
        >
          Continue
        </button>
      </form>
    </main>
  )
}

function SessionsPanel({
  archivedCount,
  archivingPicked,
  changeTokenLabel,
  loading,
  onArchivePicked,
  onTogglePicked,
  onToggleSelecting,
  picked,
  selecting,
  onChangeToken,
  onNewSession,
  onRefresh,
  onSelect,
  onToggleArchived,
  selectedSessionId,
  sessionTurnMap,
  sessions,
  showArchived,
}: {
  archivedCount: number
  archivingPicked: boolean
  changeTokenLabel: string
  loading: boolean
  onArchivePicked: () => Promise<void>
  onTogglePicked: (sessionId: SessionId) => void
  onToggleSelecting: () => void
  picked: Set<SessionId>
  selecting: boolean
  onChangeToken: () => void
  onToggleArchived: () => void
  showArchived: boolean
  onNewSession: () => void
  onRefresh: () => void
  onSelect: (sessionId: SessionId) => void
  selectedSessionId: SessionId | null
  sessionTurnMap: Record<SessionId, Turn[]>
  sessions: Session[]
}) {
  const nodes = useMemo(() => buildSessionTree(sessions), [sessions])
  const now = useNow(30_000)

  return (
    <>
      <div className="flex items-center justify-between gap-3 border-b border-slate-800 px-4 py-3">
        <div>
          <h1 className="text-lg font-semibold text-white">Sessions</h1>
          <p className="text-xs text-slate-500">ceilidh</p>
        </div>
        <div className="flex items-center gap-2">
          <button
            aria-pressed={selecting}
            className={`inline-flex h-11 items-center justify-center rounded-md border px-3 text-sm font-medium transition ${
              selecting
                ? 'border-emerald-300 bg-emerald-300/10 text-emerald-100'
                : 'border-slate-700 bg-slate-900 text-slate-200 hover:border-slate-500'
            }`}
            onClick={onToggleSelecting}
            type="button"
          >
            {selecting ? 'Done' : 'Select'}
          </button>
          <button
            aria-label="Refresh sessions"
            className="inline-flex h-11 min-w-16 items-center justify-center rounded-md border border-slate-700 bg-slate-900 px-3 text-sm font-medium text-slate-200 transition hover:border-slate-500"
            onClick={onRefresh}
            type="button"
          >
            {loading ? <span className="spinner" /> : 'Reload'}
          </button>
          <button
            className="inline-flex h-11 items-center justify-center gap-2 rounded-md bg-emerald-300 px-4 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200"
            onClick={onNewSession}
            type="button"
          >
            <span aria-hidden="true">+</span>
            New
          </button>
        </div>
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto overscroll-contain">
        {sessions.length === 0 && !loading ? (
          <div className="px-4 py-8 text-sm text-slate-400">
            No sessions yet.
          </div>
        ) : null}

        <div className="divide-y divide-slate-800/80">
          {nodes.map((node) => (
            <SessionRow
              key={node.session.id}
              node={node}
              now={now}
              onSelect={selecting ? onTogglePicked : onSelect}
              picked={selecting ? picked.has(node.session.id) : null}
              selected={!selecting && selectedSessionId === node.session.id}
              turns={sessionTurnMap[node.session.id]}
            />
          ))}
        </div>
      </div>

      {selecting ? (
        <div className="flex items-center justify-between gap-3 border-t border-slate-800 bg-slate-950/60 px-4 py-2">
          <span className="text-sm text-slate-400">
            {picked.size === 0
              ? 'Tap sessions to pick them'
              : `${picked.size} picked`}
          </span>
          <button
            className="inline-flex h-11 items-center justify-center gap-2 rounded-md border border-slate-700 bg-slate-900 px-4 text-sm font-semibold text-slate-100 transition hover:border-slate-500 disabled:cursor-not-allowed disabled:opacity-50"
            disabled={picked.size === 0 || archivingPicked}
            onClick={() => void onArchivePicked()}
            type="button"
          >
            {archivingPicked ? <span className="spinner" /> : null}
            Archive {picked.size > 0 ? picked.size : ''}
          </button>
        </div>
      ) : null}

      <div className="flex items-center justify-between border-t border-slate-800 px-4 py-2">
        <button
          className="inline-flex h-11 items-center text-sm font-medium text-slate-400 transition hover:text-slate-100"
          onClick={onChangeToken}
          type="button"
        >
          {changeTokenLabel}
        </button>
        {archivedCount > 0 || showArchived ? (
          <button
            className="inline-flex h-11 items-center text-sm font-medium text-slate-400 transition hover:text-slate-100"
            onClick={onToggleArchived}
            type="button"
          >
            {showArchived
              ? 'Hide archived'
              : `Show ${archivedCount} archived`}
          </button>
        ) : null}
      </div>
    </>
  )
}

function SessionRow({
  node,
  now,
  onSelect,
  picked,
  selected,
  turns,
}: {
  node: SessionNode
  now: number
  onSelect: (sessionId: SessionId) => void
  /** null when not in selection mode; otherwise whether this row is picked. */
  picked: boolean | null
  selected: boolean
  turns: Turn[] | undefined
}) {
  const { childCount, depth, session } = node

  return (
    <button
      aria-pressed={picked ?? undefined}
      className={`flex min-h-14 w-full items-start gap-2 py-3 pr-4 text-left transition hover:bg-slate-900/80 ${
        selected || picked ? 'bg-slate-900' : ''
      }`}
      onClick={() => onSelect(session.id)}
      style={{ paddingLeft: `${1 + depth * 1.1}rem` }}
      type="button"
    >
      {picked !== null ? (
        <span
          aria-hidden="true"
          className={`mt-0.5 inline-flex h-5 w-5 shrink-0 items-center justify-center rounded border text-xs ${
            picked
              ? 'border-emerald-300 bg-emerald-300 text-slate-950'
              : 'border-slate-600 text-transparent'
          }`}
        >
          &#10003;
        </span>
      ) : null}
      {depth > 0 ? (
        <span aria-hidden="true" className="mt-0.5 text-xs text-slate-600">
          &#9492;
        </span>
      ) : null}

      <span className="min-w-0 flex-1">
        <span className="flex items-start justify-between gap-2">
          <span
            className={`min-w-0 truncate text-sm font-semibold ${
              session.status === 'archived' ? 'text-slate-500' : 'text-slate-100'
            }`}
          >
            {session.title}
          </span>
          {session.status === 'archived' ? (
            <span className="shrink-0 rounded border border-slate-700 px-1.5 py-0.5 text-[10px] uppercase tracking-wide text-slate-500">
              archived
            </span>
          ) : (
            <TurnStatusDot status={latestTurnStatus(turns)} />
          )}
        </span>
        <span className="mt-1 flex min-w-0 flex-wrap items-center gap-2 text-xs text-slate-400">
          <LaneBadge lane={session.lane} />
          {childCount > 0 ? (
            <span className="text-slate-500">
              {childCount} sub-agent{childCount === 1 ? '' : 's'}
            </span>
          ) : null}
          <span className="text-slate-500">
            {formatRelative(session.updated_at, now)}
          </span>
        </span>
      </span>
    </button>
  )
}

function NewSessionForm({
  config,
  onCancel,
  onCreate,
  repos,
}: {
  repos: RepoList
  config: CallerConfig
  onCancel: () => void
  onCreate: (request: CreateSessionRequest) => Promise<void>
}) {
  const groups = useMemo(() => groupModels(config.models), [config.models])
  const [title, setTitle] = useState('')
  const [choiceKey, setChoiceKey] = useState(
    () => firstModelKey(config.models) ?? otherModelKey,
  )
  const [harness, setHarness] = useState<Harness>('claude-code')
  const [model, setModel] = useState(() =>
    defaultModelForHarness('claude-code'),
  )
  const [effort, setEffort] = useState('')
  const defaultRepo = config.default_repo_url ?? ''
  const knownRepo = findRepo(repos, defaultRepo)
  // The picker only appears when the caller has a GitHub token; without one
  // the free-text field is the whole story, exactly as before.
  const [ownerLogin, setOwnerLogin] = useState(
    () => knownRepo?.owner ?? repos.owners[0]?.login ?? '',
  )
  const [repoKey, setRepoKey] = useState(
    () => knownRepo?.url ?? (repos.available ? otherRepoKey : ''),
  )
  const [repoUrl, setRepoUrl] = useState(() => defaultRepo)
  const [baseBranch, setBaseBranch] = useState('')
  const [pushChoice, setPushChoice] = useState<boolean | null>(null)
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const choice = config.models.find((item) => modelKey(item) === choiceKey)
  const custom = !choice
  const owner = repos.owners.find((item) => item.login === ownerLogin)
  const pickedRepo =
    repos.available && repoKey !== otherRepoKey
      ? owner?.repos.find((item) => item.url === repoKey)
      : undefined
  const effectiveRepoUrl = pickedRepo ? pickedRepo.url : repoUrl
  const repoSet = effectiveRepoUrl.trim().length > 0
  // A scratch workspace has nothing to push to, so the toggle only bites when
  // a repository is set, and it is on by default when one is.
  const allowPush = repoSet && (pushChoice ?? true)

  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    const cleanedTitle = title.trim()

    if (!cleanedTitle) {
      setError('Title is required.')
      return
    }

    const cleanedModel = custom ? model.trim() : choice.model

    if (!cleanedModel) {
      setError('Model is required.')
      return
    }

    const lane: Lane = {
      harness: custom ? harness : choice.harness,
      model: cleanedModel,
      ...(custom ? optionalField('effort', effort) : {}),
    }
    const request: CreateSessionRequest = {
      title: cleanedTitle,
      lane,
      profile: {
        ...optionalField('repo_url', effectiveRepoUrl),
        ...optionalField('base_branch', baseBranch),
        allow_push: allowPush,
      },
    }

    setSubmitting(true)
    setError(null)

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
          className="inline-flex h-11 items-center justify-center rounded-md border border-slate-700 bg-slate-900 px-3 text-sm font-medium text-slate-200 md:hidden"
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
        className="mx-auto grid w-full max-w-2xl content-start gap-4 overflow-y-auto overscroll-contain p-4 md:p-6"
        onSubmit={submit}
      >
        <label className="grid gap-2 text-sm font-medium text-slate-300">
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
          Model
          <select
            className="form-field"
            onChange={(event) => setChoiceKey(event.target.value)}
            value={choiceKey}
          >
            {groups.map((group) => (
              <optgroup key={group.vendor} label={group.vendor}>
                {group.choices.map((item) => (
                  <option key={modelKey(item)} value={modelKey(item)}>
                    {item.label}
                  </option>
                ))}
              </optgroup>
            ))}
            <option value={otherModelKey}>Other model...</option>
          </select>
          {choice ? (
            <span className="text-xs text-slate-500">
              {vendorForHarness(choice.harness)} / {choice.model}
            </span>
          ) : null}
        </label>

        {custom ? (
          <div className="grid gap-4 rounded-md border border-slate-800 bg-slate-950/40 p-3 md:grid-cols-2">
            <label className="grid gap-2 text-sm font-medium text-slate-300">
              Harness
              <select
                className="form-field"
                onChange={(event) =>
                  changeHarness(event.target.value as Harness)
                }
                value={harness}
              >
                <option value="claude-code">claude-code</option>
                <option value="codex">codex</option>
                <option value="cursor">cursor</option>
              </select>
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

            <label className="grid gap-2 text-sm font-medium text-slate-300 md:col-span-2">
              Model id
              <input
                autoCapitalize="off"
                autoCorrect="off"
                className="form-field"
                onChange={(event) => setModel(event.target.value)}
                spellCheck={false}
                value={model}
              />
            </label>
          </div>
        ) : null}

        {repos.available ? (
          <div className="grid gap-3 md:grid-cols-2">
            <label className="grid gap-2 text-sm font-medium text-slate-300">
              Owner
              <select
                className="form-field"
                onChange={(event) => {
                  const nextOwner = event.target.value
                  setOwnerLogin(nextOwner)
                  const first = repos.owners.find(
                    (item) => item.login === nextOwner,
                  )?.repos[0]
                  setRepoKey(first?.url ?? otherRepoKey)
                }}
                value={ownerLogin}
              >
                {repos.owners.map((item) => (
                  <option key={item.login} value={item.login}>
                    {item.login}
                  </option>
                ))}
              </select>
            </label>

            <label className="grid gap-2 text-sm font-medium text-slate-300">
              Repository
              <select
                className="form-field"
                onChange={(event) => setRepoKey(event.target.value)}
                value={repoKey}
              >
                {(owner?.repos ?? []).map((item) => (
                  <option key={item.url} value={item.url}>
                    {item.name}
                  </option>
                ))}
                <option value={otherRepoKey}>Other or none...</option>
              </select>
            </label>
          </div>
        ) : null}

        {!repos.available || repoKey === otherRepoKey ? (
          <label className="grid gap-2 text-sm font-medium text-slate-300">
            {repos.available ? 'Repository URL' : 'Repository'}
            <input
              autoCapitalize="off"
              autoCorrect="off"
              className="form-field"
              inputMode="url"
              onChange={(event) => setRepoUrl(event.target.value)}
              placeholder="empty for a scratch workspace"
              spellCheck={false}
              value={repoUrl}
            />
          </label>
        ) : null}

        <label className="grid gap-2 text-sm font-medium text-slate-300">
          Base branch
          <input
            autoCapitalize="off"
            autoCorrect="off"
            className="form-field"
            onChange={(event) => setBaseBranch(event.target.value)}
            placeholder="optional, repo default otherwise"
            spellCheck={false}
            value={baseBranch}
          />
        </label>

        <label
          className={`flex min-h-11 items-center gap-3 rounded-md border border-slate-800 bg-slate-950/60 px-3 py-3 text-sm font-medium ${
            repoSet ? 'text-slate-200' : 'text-slate-500'
          }`}
        >
          <input
            checked={allowPush}
            className="h-5 w-5 accent-emerald-300"
            disabled={!repoSet}
            onChange={(event) => setPushChoice(event.target.checked)}
            type="checkbox"
          />
          Push every turn
        </label>

        {error ? (
          <div className="rounded-md border border-rose-500/30 bg-rose-500/10 px-3 py-2 text-sm text-rose-100">
            {error}
          </div>
        ) : null}

        <div className="flex items-center justify-end gap-2">
          <button
            className="hidden h-11 items-center rounded-md border border-slate-700 bg-slate-900 px-4 text-sm font-medium text-slate-200 transition hover:border-slate-500 md:inline-flex"
            onClick={onCancel}
            type="button"
          >
            Cancel
          </button>
          <button
            className="inline-flex h-11 w-full items-center justify-center gap-2 rounded-md bg-emerald-300 px-4 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200 disabled:cursor-not-allowed disabled:bg-slate-600 disabled:text-slate-300 md:w-auto"
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
  childSessions,
  liveChunks,
  onArchive,
  onBack,
  onCancel,
  onDelete,
  onOpenSession,
  onSend,
  parentSession,
  sending,
  session,
  sessionTurnMap,
  turns,
}: {
  childSessions: Session[]
  liveChunks: Record<TurnId, string>
  onArchive: (archived: boolean) => Promise<void>
  onBack: () => void
  onCancel: (turnId: TurnId) => Promise<void>
  onDelete: () => Promise<void>
  onOpenSession: (sessionId: SessionId) => void
  onSend: (input: string) => Promise<void>
  parentSession: Session | null
  sending: boolean
  session: Session
  sessionTurnMap: Record<SessionId, Turn[]>
  turns: Turn[]
}) {
  const [input, setInput] = useState('')
  const [note, setNote] = useState<ComposerNote | null>(null)
  const [cancelling, setCancelling] = useState(false)
  const [housekeeping, setHousekeeping] = useState(false)
  const archived = session.status === 'archived'
  const bottom = useRef<HTMLDivElement | null>(null)

  const housekeep = async (action: () => Promise<void>) => {
    if (housekeeping) {
      return
    }
    setHousekeeping(true)
    setNote(null)
    try {
      await action()
    } catch (error) {
      setNote(composerNote(error))
    } finally {
      setHousekeeping(false)
    }
  }

  const confirmDelete = () => {
    const extra =
      childSessions.length > 0
        ? ` and its ${childSessions.length} sub-agent session${childSessions.length === 1 ? '' : 's'}`
        : ''
    const ok = window.confirm(
      `Delete "${session.title}"${extra}? The turns and replies go now and the workspace on the seat is removed within ten minutes. Anything pushed stays on its branch.`,
    )
    if (ok) {
      void housekeep(onDelete)
    }
  }
  const inFlightTurn = turns.find((turn) => inFlightStatuses.has(turn.status))
  const now = useNow(inFlightTurn ? 1_000 : 60_000)
  const queuedAhead = turns.filter((turn) => turn.status === 'queued').length
  // Typing at a working agent is how you steer it: the message queues and
  // runs next, so the composer never locks.
  const disabled = sending
  const repo = shortRepo(session.profile.repo_url)
  const liveLength = turns.reduce(
    (total, turn) => total + (liveChunks[turn.id]?.length ?? 0),
    0,
  )

  useEffect(() => {
    bottom.current?.scrollIntoView({ block: 'end' })
  }, [liveLength, session.id, turns.length])

  const submit = async () => {
    const trimmed = input.trim()

    if (!trimmed || disabled) {
      return
    }

    setNote(null)

    try {
      await onSend(trimmed)
      setInput('')
    } catch (sendError) {
      setNote(composerNote(sendError))
    }
  }

  const cancel = async () => {
    if (!inFlightTurn || cancelling) {
      return
    }

    setCancelling(true)
    setNote(null)

    try {
      await onCancel(inFlightTurn.id)
    } catch (cancelError) {
      setNote(composerNote(cancelError))
    } finally {
      setCancelling(false)
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
      <header className="border-b border-slate-800 bg-[#0c1118] px-4 py-3">
        <div className="flex min-w-0 items-start gap-3">
          <button
            className="inline-flex h-11 shrink-0 items-center justify-center rounded-md border border-slate-700 bg-slate-900 px-3 text-sm font-medium text-slate-200 md:hidden"
            onClick={onBack}
            type="button"
          >
            {'<'} Back
          </button>
          <div className="min-w-0 flex-1">
            <div className="flex min-w-0 items-center gap-2">
              <TurnStatusDot
                className="mt-0"
                status={latestTurnStatus(turns)}
              />
              <h1 className="truncate text-base font-semibold text-white">
                {session.title}
              </h1>
            </div>
            <div className="mt-1 flex min-w-0 flex-wrap items-center gap-2 text-xs text-slate-400">
              <LaneBadge lane={session.lane} />
              {repo ? <span className="truncate">{repo}</span> : null}
              <span>{session.runner_affinity ?? 'unassigned'}</span>
              {parentSession ? (
                <button
                  className="inline-flex items-center gap-1 rounded-md border border-slate-700 px-2 py-1 text-xs text-slate-300 transition hover:border-slate-500 hover:text-white"
                  onClick={() => onOpenSession(parentSession.id)}
                  type="button"
                >
                  <span aria-hidden="true">&#8593;</span>
                  <span className="max-w-40 truncate">
                    {parentSession.title}
                  </span>
                </button>
              ) : null}
              {archived ? (
                <span className="rounded border border-slate-700 px-1.5 py-0.5 text-[10px] uppercase tracking-wide text-slate-500">
                  archived
                </span>
              ) : null}
            </div>
          </div>
          <div className="flex shrink-0 items-center gap-2">
            {archived ? (
              <>
                <button
                  className="inline-flex h-11 items-center justify-center rounded-md border border-slate-700 bg-slate-900 px-3 text-sm font-medium text-slate-200 transition hover:border-slate-500 disabled:opacity-60"
                  disabled={housekeeping}
                  onClick={() => void housekeep(() => onArchive(false))}
                  type="button"
                >
                  Unarchive
                </button>
                <button
                  className="inline-flex h-11 items-center justify-center rounded-md border border-rose-400/40 bg-rose-500/10 px-3 text-sm font-medium text-rose-100 transition hover:border-rose-300 disabled:opacity-60"
                  disabled={housekeeping}
                  onClick={confirmDelete}
                  type="button"
                >
                  Delete
                </button>
              </>
            ) : (
              <button
                aria-label="Archive session"
                className="inline-flex h-11 items-center justify-center rounded-md border border-slate-700 bg-slate-900 px-3 text-sm font-medium text-slate-200 transition hover:border-slate-500 disabled:opacity-60"
                disabled={housekeeping}
                onClick={() => void housekeep(() => onArchive(true))}
                type="button"
              >
                Archive
              </button>
            )}
          </div>
        </div>
      </header>

      {childSessions.length > 0 ? (
        <div className="flex shrink-0 gap-2 overflow-x-auto border-b border-slate-800 bg-[#0c1118] px-4 py-2">
          {childSessions.map((child) => (
            <button
              className="inline-flex min-h-11 shrink-0 items-center gap-2 rounded-full border border-slate-700 bg-slate-900 px-3 text-xs font-medium text-slate-200 transition hover:border-slate-500"
              key={child.id}
              onClick={() => onOpenSession(child.id)}
              type="button"
            >
              <TurnStatusDot
                className="mt-0"
                status={latestTurnStatus(sessionTurnMap[child.id])}
              />
              <span className="max-w-40 truncate">{child.title}</span>
            </button>
          ))}
        </div>
      ) : null}

      <div className="min-h-0 flex-1 overflow-y-auto overscroll-contain px-3 py-4 md:px-6">
        {turns.length === 0 ? (
          <div className="mx-auto mt-12 max-w-md rounded-md border border-slate-800 bg-slate-950/50 p-4 text-sm text-slate-400">
            No turns yet.
          </div>
        ) : null}

        <div className="mx-auto flex max-w-4xl flex-col gap-5">
          {turns.map((turn) => (
            <TurnBlock
              key={turn.id}
              liveText={liveChunks[turn.id]}
              now={now}
              turn={turn}
            />
          ))}
        </div>
        <div ref={bottom} />
      </div>

      <form
        className="shrink-0 border-t border-slate-800 bg-[#0c1118] px-3 pt-3 pb-[calc(0.75rem+env(safe-area-inset-bottom))] md:px-4"
        onSubmit={(event) => {
          event.preventDefault()
          void submit()
        }}
      >
        <div className="mx-auto max-w-4xl">
          {note ? (
            <div
              className={`mb-2 rounded-md border px-3 py-2 text-sm ${
                note.kind === 'error'
                  ? 'border-rose-500/30 bg-rose-500/10 text-rose-100'
                  : 'border-amber-300/30 bg-amber-300/10 text-amber-100'
              }`}
            >
              {note.text}
            </div>
          ) : null}

          <textarea
            className="min-h-20 w-full resize-none rounded-md border border-slate-700 bg-slate-950 px-3 py-2 text-[16px] leading-6 text-slate-100 outline-none transition placeholder:text-slate-600 focus:border-emerald-300 disabled:cursor-not-allowed disabled:opacity-60"
            disabled={disabled}
            onChange={(event) => setInput(event.target.value)}
            onKeyDown={keyDown}
            placeholder={
              inFlightTurn
                ? 'Steer it: this runs as the next turn'
                : 'Message for the next turn'
            }
            value={input}
          />

          {queuedAhead > 0 ? (
            <p className="mt-2 text-xs text-slate-400">
              {queuedAhead === 1
                ? '1 message waiting to run next'
                : `${queuedAhead} messages waiting, in order`}
            </p>
          ) : null}

          <div className="mt-2 flex items-center justify-end gap-2">
            {inFlightTurn ? (
              <button
                className="inline-flex h-11 flex-1 items-center justify-center gap-2 rounded-md border border-rose-400/40 bg-rose-500/10 px-4 text-sm font-semibold text-rose-100 transition hover:border-rose-300 disabled:cursor-not-allowed disabled:opacity-60 md:flex-none"
                disabled={cancelling || inFlightTurn.cancel_requested}
                onClick={() => void cancel()}
                type="button"
              >
                {cancelling ? <span className="spinner" /> : null}
                {inFlightTurn.cancel_requested ? 'Cancelling' : 'Cancel'}
              </button>
            ) : null}
            <button
              className="inline-flex h-11 flex-1 items-center justify-center gap-2 rounded-md bg-emerald-300 px-4 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200 disabled:cursor-not-allowed disabled:bg-slate-600 disabled:text-slate-300 md:flex-none md:min-w-28"
              disabled={disabled || !input.trim()}
              type="submit"
            >
              {sending ? <span className="spinner dark" /> : null}
              {inFlightTurn ? 'Queue' : 'Send'}
            </button>
          </div>
        </div>
      </form>
    </section>
  )
}

function TurnBlock({
  liveText,
  now,
  turn,
}: {
  liveText?: string
  now: number
  turn: Turn
}) {
  const inFlight = inFlightStatuses.has(turn.status)
  const failed = turn.status === 'error' || turn.status === 'capped'
  // A cancel with nothing to say is already covered by the status line below.
  const cancelled = turn.status === 'cancelled' && Boolean(turn.error)

  return (
    <article className="grid min-w-0 grid-cols-[minmax(0,1fr)] gap-2">
      <div className="flex min-w-0 justify-end">
        <div className="max-w-[88%] min-w-0 rounded-md bg-sky-500 px-3 py-2 text-sm leading-6 text-white shadow-lg shadow-sky-950/20 md:max-w-[72%]">
          <div className="wrap-anywhere whitespace-pre-wrap">{turn.input}</div>
        </div>
      </div>

      {turn.envelope ? <EnvelopeBubble envelope={turn.envelope} /> : null}

      {liveText ? (
        <div className="flex min-w-0 justify-start">
          <div className="max-w-[88%] min-w-0 rounded-md border border-slate-700 bg-slate-900 px-3 py-2 text-sm leading-6 text-slate-100 md:max-w-[72%]">
            <div className="mb-2 flex items-center gap-2 text-xs font-medium text-emerald-300">
              <span className="spinner" />
              Working
            </div>
            <div className="wrap-anywhere whitespace-pre-wrap">{liveText}</div>
          </div>
        </div>
      ) : null}

      {failed || cancelled ? (
        <div className="flex min-w-0 justify-start">
          <div
            className={`max-w-[88%] min-w-0 rounded-md border px-3 py-2 text-sm leading-6 wrap-anywhere md:max-w-[72%] ${
              failed
                ? 'border-rose-500/30 bg-rose-500/10 text-rose-100'
                : 'border-slate-700 bg-slate-900/60 text-slate-400'
            }`}
          >
            {turn.error ?? statusLabel(turn.status)}
          </div>
        </div>
      ) : null}

      <div className="flex flex-wrap items-center gap-2 px-1 text-[11px] text-slate-500">
        <TurnStatusDot className="mt-0" status={turn.status} />
        <span>
          {turn.cancel_requested && inFlight
            ? 'Cancelling'
            : statusLabel(turn.status)}
        </span>
        {inFlight ? (
          <span>{formatElapsed(turn.started_at ?? turn.created_at, now)}</span>
        ) : null}
        {turn.commit ? (
          <span className="font-mono text-slate-600">
            {turn.commit.slice(0, 7)}
          </span>
        ) : null}
      </div>
    </article>
  )
}

function EnvelopeBubble({ envelope }: { envelope: Envelope }) {
  const questions = envelope.questions ?? []

  return (
    <div className="flex min-w-0 justify-start">
      <div className="max-w-[88%] min-w-0 rounded-md border border-slate-700 bg-slate-900 px-3 py-3 text-sm leading-6 text-slate-100 shadow-lg shadow-black/20 md:max-w-[72%]">
        <strong className="block text-base wrap-anywhere text-white">
          {envelope.headline}
        </strong>
        {envelope.body_markdown ? (
          <div className="markdown mt-3 min-w-0">
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
                  <div className="text-sm font-medium wrap-anywhere text-amber-50">
                    {question.text}
                  </div>
                  {question.recommendation ? (
                    <div className="mt-1 text-xs wrap-anywhere text-amber-100/80">
                      Recommendation: {question.recommendation}
                    </div>
                  ) : null}
                </div>
              ))}
            </div>
          </div>
        ) : null}
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
          className="mt-4 inline-flex h-11 items-center justify-center gap-2 rounded-md bg-emerald-300 px-4 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200"
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
    <footer className="order-first flex min-h-11 shrink-0 items-center gap-2 overflow-x-auto border-b border-slate-800 bg-[#080b10] px-3 py-2 md:order-none md:border-t md:border-b-0 md:pb-[calc(0.5rem+env(safe-area-inset-bottom))]">
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
    <span className="inline-flex min-w-0 max-w-full items-center gap-1 rounded-md border border-cyan-300/20 bg-cyan-300/10 px-1.5 py-0.5 text-[11px] font-medium text-cyan-100">
      <span className="shrink-0">{vendorForHarness(lane.harness)}</span>
      <span className="truncate text-cyan-200/70">{lane.model}</span>
    </span>
  )
}

function TurnStatusDot({
  className = 'mt-1',
  status,
}: {
  className?: string
  status: TurnStatus | null
}) {
  const tone = status ? dotClasses[status] : 'bg-slate-700'
  const label = status ? statusLabel(status) : 'No turns'

  return (
    <span
      aria-label={label}
      className={`inline-block h-2.5 w-2.5 shrink-0 rounded-full ${tone} ${className}`}
      title={label}
    />
  )
}

/** A clock that re-renders on a tick, so relative times stay honest. */
function useNow(intervalMs: number) {
  const [now, setNow] = useState(() => Date.now())

  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), intervalMs)

    return () => window.clearInterval(timer)
  }, [intervalMs])

  return now
}

function useApi(token: string) {
  return useCallback(
    async <T,>(path: string, init: ApiRequestInit = {}): Promise<T> => {
      const headers = new Headers(init.headers)
      if (token) {
        headers.set('Authorization', `Bearer ${token}`)
      }

      const request: RequestInit = {
        ...init,
        headers,
        credentials: 'same-origin',
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
  // Signed in with a cookie, the browser sends it on the stream too, so the
  // token never has to ride in a URL.
  return token ? `${path}?token=${encodeURIComponent(token)}` : path
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

/** Rows in render order: every root, each followed by its sub-agent subtree. */
function buildSessionTree(sessions: Session[]): SessionNode[] {
  const known = new Set(sessions.map((session) => session.id))
  const children = new Map<SessionId, Session[]>()
  const roots: Session[] = []

  for (const session of sessions) {
    const parentId = session.parent_id
    const nested =
      typeof parentId === 'string' &&
      parentId !== session.id &&
      known.has(parentId)

    if (nested) {
      const bucket = children.get(parentId)

      if (bucket) {
        bucket.push(session)
      } else {
        children.set(parentId, [session])
      }
    } else {
      roots.push(session)
    }
  }

  const nodes: SessionNode[] = []
  const placed = new Set<SessionId>()

  const walk = (session: Session, depth: number) => {
    if (placed.has(session.id)) {
      return
    }

    placed.add(session.id)
    const kids = children.get(session.id) ?? []
    nodes.push({ childCount: kids.length, depth, session })

    for (const kid of kids) {
      walk(kid, Math.min(depth + 1, maxTreeDepth))
    }
  }

  for (const root of roots) {
    walk(root, 0)
  }

  // A parent cycle would otherwise drop rows off the list entirely.
  for (const session of sessions) {
    walk(session, 0)
  }

  return nodes
}

function parentOf(sessions: Session[], session: Session): Session | null {
  const parentId = session.parent_id

  if (typeof parentId !== 'string') {
    return null
  }

  return sessions.find((item) => item.id === parentId) ?? null
}

function childrenOf(sessions: Session[], parentId: SessionId): Session[] {
  return sessions.filter((session) => session.parent_id === parentId)
}

/** owner/repo, the only part of a clone URL worth a phone header. */
function shortRepo(url: string | null | undefined): string | null {
  const trimmed = (url ?? '').trim().replace(/\.git$/, '')

  if (!trimmed) {
    return null
  }

  const parts = trimmed.split(/[/:]/).filter(Boolean)

  return parts.length >= 2 ? parts.slice(-2).join('/') : trimmed
}

function composerNote(error: unknown): ComposerNote {
  const conflict = error instanceof ApiError && error.status === 409

  return { kind: conflict ? 'notice' : 'error', text: errorMessage(error) }
}

function formatElapsed(since: string | null | undefined, now: number) {
  const at = since ? Date.parse(since) : Number.NaN

  if (Number.isNaN(at)) {
    return ''
  }

  const seconds = Math.max(0, Math.round((now - at) / 1_000))

  if (seconds < 60) {
    return `${seconds}s`
  }

  const minutes = Math.floor(seconds / 60)

  if (minutes < 60) {
    return `${minutes}m ${String(seconds % 60).padStart(2, '0')}s`
  }

  return `${Math.floor(minutes / 60)}h ${String(minutes % 60).padStart(2, '0')}m`
}

function latestTurnStatus(turns: Turn[] | undefined): TurnStatus | null {
  return turns?.at(-1)?.status ?? null
}

function modelKey(choice: ModelChoice): string {
  return `${choice.harness}::${choice.model}`
}

function firstModelKey(models: ModelChoice[]): string | null {
  const first = models[0]
  return first ? modelKey(first) : null
}

/** Vendor groups in the order the caller listed them. */
function groupModels(models: ModelChoice[]) {
  const groups: { vendor: string; choices: ModelChoice[] }[] = []

  for (const choice of models) {
    const group = groups.find((item) => item.vendor === choice.vendor)

    if (group) {
      group.choices.push(choice)
    } else {
      groups.push({ choices: [choice], vendor: choice.vendor })
    }
  }

  return groups
}

/** The repository the caller prefills, if the picker happens to carry it. */
function findRepo(repos: RepoList, url: string) {
  if (!repos.available || !url) {
    return undefined
  }

  const wanted = url.replace(/\.git$/, '')
  for (const owner of repos.owners) {
    for (const repo of owner.repos) {
      if (repo.url.replace(/\.git$/, '') === wanted) {
        return repo
      }
    }
  }
  return undefined
}

function reposFromPayload(payload: unknown): RepoList {
  if (!isRecord(payload) || payload.available !== true) {
    return emptyRepos
  }

  const owners = collectionFromPayload<RepoOwner>(payload.owners, 'owners')
  return { owners, available: owners.length > 0 }
}

function configFromPayload(payload: unknown): CallerConfig {
  if (!isRecord(payload)) {
    return emptyConfig
  }

  return {
    default_repo_url:
      typeof payload.default_repo_url === 'string'
        ? payload.default_repo_url
        : null,
    models: collectionFromPayload<ModelChoice>(payload.models, 'models'),
  }
}

function vendorForHarness(harness: Harness): string {
  switch (harness) {
    case 'claude-code':
      return 'Anthropic'
    case 'codex':
      return 'OpenAI'
    case 'cursor':
      return 'Cursor'
    case 'mock':
      return 'Mock'
    default:
      return harness
  }
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
    case 'cancelled':
      return 'Cancelled'
  }
}

function defaultModelForHarness(harness: Harness): string {
  switch (harness) {
    case 'claude-code':
      return 'opus'
    case 'codex':
      return 'gpt-5.6-sol'
    case 'cursor':
      return 'composer-2.5'
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

/** Short relative age for a list row: now, 4m, 3h, 2d, then a date. */
function formatRelative(value: string | null | undefined, now: number) {
  if (!value) {
    return 'never'
  }

  const at = Date.parse(value)

  if (Number.isNaN(at)) {
    return value
  }

  const seconds = Math.max(0, Math.round((now - at) / 1_000))

  if (seconds < 45) {
    return 'now'
  }

  const minutes = Math.round(seconds / 60)

  if (minutes < 60) {
    return `${minutes}m`
  }

  const hours = Math.round(minutes / 60)

  if (hours < 24) {
    return `${hours}h`
  }

  const days = Math.round(hours / 24)

  if (days < 7) {
    return `${days}d`
  }

  return formatDateTime(value)
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
