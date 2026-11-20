import express from 'express'
import { app } from 'electron'
import { Server } from '@modelcontextprotocol/sdk/server/index.js'
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js'
import type { Transport } from '@modelcontextprotocol/sdk/shared/transport.js'
import { isInitializeRequest } from '@modelcontextprotocol/sdk/types.js'
import { randomUUID, timingSafeEqual } from 'node:crypto'
import { buildMcpServer, type McpServerOptions } from './server.js'
import { loadOrInitAuth, saveAuth, rotateToken, type McpAuth } from './auth.js'
import { listenLoopback, loopbackHosts } from './bind.js'
import { NO_MCP_LOG, type McpLogEntryInput } from './withLog.js'
import { PROJECT_CLOSED_MESSAGE } from './toolResult.js'
import type { AgentConnection, AgentConnectionSnapshot } from '../../shared/agent-activity.js'
import type { CompanionRouteDeps } from '../companion/routes.js'
import type { TsActorHost } from '../state/ts-actor-host.js'

type Backend = import('@weftcut/core').Backend

export interface McpInfoView {
  bind: string
  url: string
  bearer_token: string
}

export interface McpHost {
  getInfo(): McpInfoView
  connectionSnapshot(): AgentConnectionSnapshot
  resetToken(): string
  /** The backend and actor must belong to this project opening for the endpoint's lifetime. */
  registerProject(backend: Backend, host: TsActorHost, options?: ProjectMcpOptions): ProjectMcpEndpoint
  /** notifications/resources/list_changed to sessions on the app endpoint. */
  notifyResourceListChanged(): void
  close(): Promise<void>
}

/** When a session counts as ABANDONED and is closed by the host, which is what
 *  releases its work session and history lock (`transport.onclose`).
 *
 *  Streamable HTTP has no liveness of its own: the SDK ends a session only on a
 *  client `DELETE`, which a client that exits, crashes or calls plain `close()`
 *  never sends, so its work session and lock would outlive it until the app
 *  restarted. The signal used instead is the client's
 *  standalone SSE stream (the GET every SDK client opens right after
 *  `initialize`): while it is open the client is alive, however long it thinks;
 *  once it has been CLOSED for `graceMs` the client is gone (a reconnecting
 *  client re-opens it within seconds). A client that never opened one is judged
 *  by requests alone and closed after `idleMs` without any. Both are long enough
 *  that a live client is never cut mid-thought, and the SDK client does not
 *  recover a closed session by itself. */
export interface SessionReaperOptions {
  /** How long a session's SSE stream may stay closed, with no request arriving
   *  either, before the session is. */
  graceMs?: number
  /** How long a session with NO stream may go without a request. */
  idleMs?: number
  /** How often to look. */
  sweepMs?: number
}
const REAPER_DEFAULTS: Required<SessionReaperOptions> = { graceMs: 90_000, idleMs: 30 * 60_000, sweepMs: 15_000 }

/** The companion page on this listener (`main/companion/`): its routes, and
 *  the link `open_side_preview` hands out, built from this host's own URL. */
export interface McpHostCompanion {
  mount(app: express.Express, deps: CompanionRouteDeps): void
  link(mcpUrl: string): string
}

/** Host-level seams, forwarded verbatim to every per-session `buildMcpServer`. */
export type McpHostOptions = Omit<McpServerOptions, 'companionLink' | 'projectAvailable'> & {
  sessionReaper?: SessionReaperOptions
  companion?: McpHostCompanion
}

type ProjectMcpOptions = Omit<McpServerOptions, 'companionLink' | 'connectionId' | 'getTsHost' | 'projectAvailable' | 'version'>

export interface ProjectMcpEndpoint {
  getInfo(): McpInfoView
  connectionSnapshot(): AgentConnectionSnapshot
  notifyResourceListChanged(): void
  close(): Promise<void>
}

type ProjectRoute = { backend: Backend; host: TsActorHost; options: ProjectMcpOptions }

export async function startMcpHost(backend: Backend, opts: McpHostOptions = {}): Promise<McpHost> {
  let auth: McpAuth = loadOrInitAuth()
  // package.json's version, which is what every session reports in `initialize`.
  const version = opts.version ?? app.getVersion()
  const projects = new Map<string, ProjectRoute>()
  const sessions = new Map<string, { transport: StreamableHTTPServerTransport; server: Server; projectId: string | undefined }>()
  const connections = new Map<string, AgentConnection>()
  /** Per session: whether its standalone SSE stream is open, when that last
   *  changed, and when the last request arrived — the liveness the reaper below
   *  reads. `never` = the client has not opened a stream (yet). Kept here rather
   *  than on the connection record, which exists only once the client has sent
   *  `notifications/initialized`: a client that never completes the handshake
   *  still holds a session and must still be judged. */
  const streams = new Map<string, { state: 'never' | 'open' | 'closed'; changedAt: number; lastRequestAt: number; openToken: number }>()
  // Which GET a close belongs to: two on one session interleave, and only the
  // one that set the record may change it back.
  let streamSeq = 0
  const reaper = { ...REAPER_DEFAULTS, ...(opts.sessionReaper ?? {}) }
  let available = true
  const log = opts.log ?? NO_MCP_LOG

  /** One transport-lifecycle row. Every row this producer writes is `Mcp`, so
   *  the category is fixed here and callers name only what varies. Swallows:
   *  these fire from inside SDK transport callbacks, where a throw would skip
   *  the session cleanup that follows it. */
  const emitLifecycle = (
    level: McpLogEntryInput['level'],
    source: McpLogEntryInput['source'],
    message: string,
    details?: Record<string, unknown>,
    sink = log,
  ): void => {
    try {
      sink.emit({ level, category: { kind: 'Mcp' }, source, message, ...(details ? { details } : {}) })
    } catch (err) {
      console.error('[mcp] lifecycle log emit failed', err)
    }
  }

  const appExpress = express()
  // `/mcp` only: the companion routes parse their own small bodies, and only
  // past their key check (`companion/routes.ts`).
  appExpress.use('/mcp', express.json({ limit: '50mb' }))

  // Constant-time compare: a plain `!==` leaks timing. Not a real attack surface
  // for a 256-bit localhost token, but timingSafeEqual is the correct form.
  const bearerOk = (header: string | undefined): boolean => {
    if (typeof header !== 'string') return false
    const got = Buffer.from(header)
    const want = Buffer.from(`Bearer ${auth.token}`)
    return got.length === want.length && timingSafeEqual(got, want)
  }
  appExpress.use('/mcp', (req, res, next) => {
    if (!bearerOk(req.headers.authorization)) {
      // The highest-value row this producer writes: a loopback port probed by
      // another local process, or by a page the user visited, is otherwise
      // completely invisible. `user_agent` is what tells those two apart. The
      // Authorization header is never recorded — not here, not in `details`.
      emitLifecycle('warn', { kind: 'System' }, 'MCP request rejected: unauthorized', {
        method: req.method,
        user_agent: req.headers['user-agent'] ?? null,
      })
      res
        .status(401)
        .json({ jsonrpc: '2.0', error: { code: -32001, message: 'unauthorized' }, id: null })
      return
    }
    next()
  })

  // The companion page brings its own guards (`companion/routes.ts`): its key
  // opens nothing under `/mcp`, and the bearer above opens none of its data.
  opts.companion?.mount(appExpress, {
    rejected: (details) => emitLifecycle('warn', { kind: 'System' }, 'Companion request rejected', details),
  })

  appExpress.all(['/mcp', '/mcp/projects/:projectId'], async (req, res) => {
    const projectId = typeof req.params.projectId === 'string' ? req.params.projectId : undefined
    const project = projectId === undefined ? undefined : projects.get(projectId)
    const projectClosed = () => res.status(404).json({
      jsonrpc: '2.0', error: { code: -32000, message: PROJECT_CLOSED_MESSAGE, data: { error: 'ProjectClosed' } }, id: null,
    })
    if (projectId !== undefined && project === undefined) {
      projectClosed()
      return
    }
    const sid = req.headers['mcp-session-id'] as string | undefined
    const session = sid ? sessions.get(sid) : undefined
    if (session && session.projectId === projectId) {
      const client = connections.get(sid!)
      if (client) client.last_activity_at = new Date().toISOString()
      const live = streams.get(sid!)
      if (live) live.lastRequestAt = Date.now()
      // The standalone SSE stream is the session's liveness: note it opening,
      // and note the socket closing under it — that, not a DELETE, is how a
      // client that simply went away is seen (see `SessionReaperOptions`).
      //
      // Only a GET the transport actually turns into a stream counts. One it
      // rejects (406 without the SSE `Accept`, 409 for a second stream, 400 on
      // the protocol version, 403 on the Host) is neither an opening nor a
      // closing, so the record goes back to what it said — otherwise a client
      // that asked wrongly once would read as streaming forever, and nothing
      // could ever judge it gone.
      if (req.method === 'GET' && live) {
        const prev = { state: live.state, changedAt: live.changedAt }
        const token = ++streamSeq
        live.state = 'open'; live.changedAt = Date.now(); live.openToken = token
        res.on('close', () => {
          const cur = streams.get(sid!)
          if (cur === undefined || cur.openToken !== token) return
          if (res.statusCode !== 200) { cur.state = prev.state; cur.changedAt = prev.changedAt; return }
          if (cur.state === 'open') { cur.state = 'closed'; cur.changedAt = Date.now() }
        })
      }
      // Existing session — route straight through.
      await session.transport.handleRequest(req, res, req.body)
      return
    }
    // No usable existing transport: only allow a fresh initialize request.
    if (!sid && isInitializeRequest(req.body)) {
      const connectionId = randomUUID()
      const getTsHost = project ? () => project.host : opts.getTsHost
      const sessionLog = project ? project.options.log ?? NO_MCP_LOG : log
      const projectAvailable = () => projectId === undefined || projects.get(projectId) === project
      const newServer = buildMcpServer(project?.backend ?? backend, {
        ...(project?.options ?? opts), ...(getTsHost ? { getTsHost } : {}), connectionId, version,
        ...(projectId !== undefined ? { projectAvailable } : companionLink === null ? {} : { companionLink }),
      })
      const transport = new StreamableHTTPServerTransport({
        sessionIdGenerator: () => connectionId,
        onsessioninitialized: (id) => {
          streams.set(id, { state: 'never', changedAt: Date.now(), lastRequestAt: Date.now(), openToken: 0 })
        },
        // DNS-rebinding defense-in-depth: a malicious web page the user visits
        // can POST to our loopback port, so reject requests whose Host header
        // isn't our bind. The 256-bit bearer is still the primary gate; Origin
        // is left unrestricted so non-browser MCP clients (no/odd Origin) work.
        enableDnsRebindingProtection: true,
        allowedHosts: loopbackHosts(port),
      })
      transport.onclose = () => {
        const sessionId = transport.sessionId
        streams.delete(connectionId)
        sessions.delete(connectionId)
        connections.delete(connectionId)
        getTsHost?.()?.agent?.end('disconnected', connectionId)
        // `Debug`, not `Info`: a client reconnecting is routine, and at `Info` it
        // would flood the console's default filter.
        emitLifecycle('debug', { kind: 'System' }, 'MCP client disconnected',
          sessionId ? { session_id: sessionId } : undefined, sessionLog)
      }
      // Set BEFORE connect, for two reasons. `Protocol.connect` CHAINS an
      // existing onerror/onclose rather than replacing it, so ours has to be
      // in place first; and this is the only seam the DNS-rebinding rejection
      // has — the SDK writes its own 403 from inside `handleRequest`, so the
      // express handler above never sees one.
      transport.onerror = (err: Error) => {
        const detail = typeof err?.message === 'string' ? err.message : String(err)
        // LANDMINE: the discriminator is the SDK's own wording
        // (`server/webStandardStreamableHttp.js` → `Invalid Host header: <value>`).
        // Matching a message beats copying `allowedHosts` into our middleware,
        // where the copy would drift from the transport that actually enforces
        // it — the cost is that an SDK rewording demotes the rejection into the
        // generic row below rather than losing it. The sibling
        // `Invalid Origin header:` path is unreachable: `allowedOrigins` is
        // deliberately left unset (see the transport options above).
        if (detail.startsWith('Invalid Host header:')) {
          emitLifecycle('warn', { kind: 'System' }, 'MCP request rejected: host not allowed', { error: detail }, sessionLog)
          return
        }
        // `onerror` is a superset hook — the transport reports every protocol
        // fault through it (unacceptable Accept, unparseable JSON, unknown
        // session). Its own row, so a rejected Host is never inferred from one
        // of those. `Warn`, not `Error`: the request failed, the app did not.
        emitLifecycle('warn', { kind: 'System' }, 'MCP transport error', { error: detail }, sessionLog)
      }
      // `getClientVersion()` is populated while the `initialize` REQUEST is
      // handled, which precedes the `notifications/initialized` that fires this
      // — so the row carries the real agent name, not `undefined`.
      newServer.oninitialized = () => {
        if (!sessions.has(connectionId) || !projectAvailable()) return
        const client = newServer.getClientVersion()
        const now = new Date().toISOString()
        connections.set(connectionId, { id: connectionId, client: client?.name ?? 'MCP', version: client?.version ?? '', connected_at: now, last_activity_at: now })
        const who = client ? `${client.name}/${client.version ?? '?'}` : 'unknown'
        emitLifecycle('info', { kind: 'System' }, `MCP client connected: ${who}`, {
          ...(client ? { client_info: client } : {}),
          ...(transport.sessionId ? { session_id: transport.sessionId } : {}),
        }, sessionLog)
      }
      // Cast: the SDK declares Transport.onclose as a getter typed
      // `(() => void) | undefined`, which TS won't accept against the optional
      // `onclose?: () => void` of the `Transport` interface under
      // `exactOptionalPropertyTypes`. Runtime-compatible; narrow to Transport.
      await newServer.connect(transport as Transport)
      if (!available || !projectAvailable()) {
        await transport.close()
        projectClosed()
        return
      }
      sessions.set(connectionId, { transport, server: newServer, projectId })
      try {
        await transport.handleRequest(req, res, req.body)
      } finally {
        if (!transport.sessionId) await transport.close()
      }
      return
    }
    // Non-init request with no/stale session — reject without allocating.
    res
      .status(400)
      .json({ jsonrpc: '2.0', error: { code: -32000, message: 'Bad Request: no valid session ID' }, id: null })
  })

  // The reaper: close every session whose client is judged gone, which fires
  // its `onclose` and with it the work-session and lock release. `unref` so a
  // host with no clients never keeps the process alive on its own.
  const sweep = (): void => {
    const now = Date.now()
    for (const [sid, { transport, projectId }] of sessions) {
      const stream = streams.get(sid)
      if (!stream) continue
      const idleMs = now - stream.lastRequestAt
      // A closed stream is the signal and a request is the counter-signal: a
      // client whose stream dropped but who keeps calling is not gone.
      const gone = stream.state === 'closed'
        ? now - stream.changedAt > reaper.graceMs && idleMs > reaper.graceMs
        : stream.state === 'never' && idleMs > reaper.idleMs
      if (!gone) continue
      emitLifecycle('info', { kind: 'System' }, 'MCP client gone: session closed', {
        session_id: sid, client: connections.get(sid)?.client ?? null, reason: stream.state === 'closed' ? 'stream_closed' : 'idle', idle_ms: idleMs,
      }, projectId === undefined ? log : projects.get(projectId)?.options.log ?? NO_MCP_LOG)
      // Best-effort: a transport already half-closed must not stop the sweep.
      transport.close().catch(() => {})
    }
  }
  const sweeper = setInterval(sweep, reaper.sweepMs)
  sweeper.unref()

  // The stored port is only a hint; listenLoopback explains why it goes
  // stale and why re-picking is safe for connected clients.
  const { server: http, port } = await listenLoopback(appExpress, auth.port)
  auth = { ...auth, port }
  saveAuth(auth)

  const url = `http://127.0.0.1:${port}/mcp`
  const companionLink = opts.companion?.link(url) ?? null
  console.log(`[mcp] listening ${url}`)
  // Pre-workspace by construction — the host binds at `app.whenReady()` — so
  // this row exists only because the producer-side deferred queue holds it until
  // a workspace opens (`src/main/deferredLog.ts`).
  emitLifecycle('info', { kind: 'System' }, `MCP host listening on 127.0.0.1:${port}`, { port })
  // The connect snippet embeds the bearer token. Print it ONLY in unpackaged
  // (dev / e2e) runs — never in a packaged build, where stdout/log capture on a
  // shared machine could leak the token. The token is always available to the
  // UI via the get_mcp_info IPC.
  if (!app.isPackaged) {
    console.log(
      `[mcp] connect: ${JSON.stringify({
        mcpServers: { weftcut: { url, headers: { Authorization: `Bearer ${auth.token}` } } },
      })}`,
    )
  }

  const info = (endpoint: string): McpInfoView => ({ bind: `127.0.0.1:${port}`, url: endpoint, bearer_token: auth.token })
  const snapshot = (endpoint: string, active: boolean, projectId?: string): AgentConnectionSnapshot => ({
    available: active, url: endpoint,
    connections: [...connections.values()].filter(c => sessions.get(c.id)?.projectId === projectId),
  })
  const notify = (projectId?: string): void => {
    for (const session of sessions.values()) {
      if (session.projectId === projectId) session.server.sendResourceListChanged().catch(() => {})
    }
  }
  const closeSessions = async (projectId?: string) => {
    await Promise.all([...sessions.values()]
      .filter(session => projectId === undefined || session.projectId === projectId)
      .map(({ transport }) => transport.close().catch(() => {})))
  }

  return {
    connectionSnapshot: () => snapshot(url, available),
    getInfo: () => info(url),
    resetToken(): string {
      auth = rotateToken(auth)
      // `source = User`: the one lifecycle row a person causes rather than a
      // client. Neither the retired nor the new token appears in it.
      emitLifecycle('info', { kind: 'User' }, 'MCP bearer token rotated')
      return auth.token
    },
    registerProject(projectBackend, host, options = {}) {
      if (!available) throw new Error('MCP host is closed')
      const id = randomUUID()
      const endpoint = `${url}/projects/${id}`
      projects.set(id, { backend: projectBackend, host, options })
      return {
        getInfo: () => info(endpoint),
        connectionSnapshot: () => snapshot(endpoint, available && projects.has(id), id),
        notifyResourceListChanged: () => notify(id),
        async close() {
          projects.delete(id)
          await closeSessions(id)
        },
      }
    },
    notifyResourceListChanged: () => notify(),
    async close(): Promise<void> {
      available = false
      projects.clear()
      clearInterval(sweeper)
      await closeSessions()
      http.close()
    },
  }
}
