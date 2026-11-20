import { describe, it, expect, vi, afterEach } from 'vitest'
import http from 'node:http'
import { readFileSync } from 'node:fs'
import { Client } from '@modelcontextprotocol/sdk/client/index.js'
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js'
import { AgentActivityService } from '../agent/activity'
import { createActor } from '../state/actor'
import { uuidV7Gen } from '../state/ids'
import { blankProject, rootComposition } from '../state/model'
import type { TsActorHost } from '../state/ts-actor-host'

// `mcp/index.ts` reads `app.isPackaged` to decide whether to print the connect
// snippet; packaged keeps the bearer token out of the test output.
vi.mock('electron', () => ({ app: { isPackaged: true, getVersion: () => '0.0.0-test' } }))
// A fixed token, and no <userData> read/write: the real auth store needs a
// userData path and this spec only needs "the right bearer" to be knowable.
vi.mock('./auth.js', () => ({
  loadOrInitAuth: () => ({ token: 'test-token', port: 0 }),
  saveAuth: () => {},
  rotateToken: (a: { token: string; port: number }) => ({ ...a, token: 'next-token' }),
}))

import { startMcpHost, type McpHost } from './index'
import type { McpLogDeps, McpLogEntryInput } from './withLog'

type Backend = import('@weftcut/core').Backend

const INITIALIZE = {
  jsonrpc: '2.0',
  id: 1,
  method: 'initialize',
  params: {
    protocolVersion: '2025-06-18',
    capabilities: {},
    clientInfo: { name: 'vitest-client', version: '9.9.9' },
  },
}

const hosts: McpHost[] = []
const sockets: http.ClientRequest[] = []

afterEach(async () => {
  // Destroy the client side first: an initialize POST is answered with an SSE
  // stream that stays open, so the runner would otherwise wait on it.
  for (const req of sockets.splice(0)) req.destroy()
  for (const host of hosts.splice(0)) await host.close()
  vi.restoreAllMocks()
})

function collector(): { rows: McpLogEntryInput[]; deps: McpLogDeps } {
  const rows: McpLogEntryInput[] = []
  return { rows, deps: { emit: (e) => { rows.push(e) }, currentWorkspace: () => null } }
}

/** Start a host over a backend no lifecycle row ever reaches — only the six
 *  request handlers touch it, and this spec issues no tool calls. */
async function start(deps: McpLogDeps): Promise<{ host: McpHost; port: number }> {
  const host = await startMcpHost({} as Backend, { log: deps })
  hosts.push(host)
  const bind = host.getInfo().bind
  return { host, port: Number(bind.slice(bind.lastIndexOf(':') + 1)) }
}

/** One POST to /mcp, resolved on the response head. The transport answers a
 *  request-bearing POST with an open SSE stream, and every assertion below needs
 *  only the status and the session id it carries. */
function post(
  port: number,
  body: unknown,
  headers: Record<string, string>,
  path = '/mcp',
): Promise<{ status: number; sessionId: string | undefined }> {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify(body)
    const req = http.request(
      {
        host: '127.0.0.1',
        port,
        path,
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'content-length': Buffer.byteLength(payload),
          accept: 'application/json, text/event-stream',
          ...headers,
        },
      },
      (res) => {
        res.resume()
        resolve({ status: res.statusCode ?? 0, sessionId: res.headers['mcp-session-id'] as string | undefined })
      },
    )
    sockets.push(req)
    req.on('error', reject)
    req.end(payload)
  })
}

describe('project MCP endpoints', () => {
  function createProject(name: string) {
    const idGen = uuidV7Gen()
    const actor = createActor({ initial: blankProject(idGen, name), idGen, clock: () => '<TS>' })
    const agent = new AgentActivityService(actor, vi.fn())
    agent.start()
    const host = {
      actor, agent,
      openedProject: () => ({ dir: `/projects/${name}` }),
      mcpCall: (tool: string, args: string, client?: string) => actor.mcpCall(tool, args, client),
    } satisfies Pick<TsActorHost, 'actor' | 'agent' | 'openedProject' | 'mcpCall'>
    return {
      host: host as TsActorHost, actor, agent,
      trackLabels: () => rootComposition(actor.snapshot()).tracks.map(track => track.label),
    }
  }

  async function connect(url: string, name: string) {
    const client = new Client({ name, version: '1.0.0' })
    const transport = new StreamableHTTPClientTransport(new URL(url), {
      requestInit: { headers: { authorization: 'Bearer test-token' } },
    })
    await client.connect(transport)
    return { client, transport }
  }

  it('keeps concurrent edits, undo, and disconnect on their original projects', async () => {
    const catalog = readFileSync('fixtures/mcp/rust-catalog-snapshot.json', 'utf8')
    const backend = { mcpCatalog: async () => catalog } as Backend
    const host = await startMcpHost(backend)
    hosts.push(host)
    const first = createProject('first')
    const second = createProject('second')
    const firstEndpoint = host.registerProject(backend, first.host)
    const secondEndpoint = host.registerProject(backend, second.host)
    const [a, b] = await Promise.all([
      connect(firstEndpoint.getInfo().url, 'first-agent'),
      connect(secondEndpoint.getInfo().url, 'second-agent'),
    ])

    try {
      const [sessionA, sessionB] = await Promise.all([
        a.client.callTool({ name: 'begin_agent_session', arguments: { reason: 'Edit first' } }),
        b.client.callTool({ name: 'begin_agent_session', arguments: { reason: 'Edit second' } }),
      ])
      expect(sessionA.isError).toBeFalsy()
      expect(sessionB.isError).toBeFalsy()
      const [editA, editB] = await Promise.all([
        a.client.callTool({ name: 'add_track', arguments: { label: 'Track A' } }),
        b.client.callTool({ name: 'add_track', arguments: { label: 'Track B' } }),
      ])
      expect(editA.isError).toBeFalsy()
      expect(editB.isError).toBeFalsy()
      expect(first.trackLabels()).toContain('Track A')
      expect(first.trackLabels()).not.toContain('Track B')
      expect(second.trackLabels()).toContain('Track B')
      expect(second.trackLabels()).not.toContain('Track A')

      const secondState = second.actor.snapshot()
      const undo = await a.client.callTool({ name: 'undo' })
      expect(undo.isError).toBeFalsy()
      expect(first.trackLabels()).not.toContain('Track A')
      expect(second.actor.snapshot()).toBe(secondState)

      expect(firstEndpoint.connectionSnapshot().connections).toMatchObject([{ client: 'first-agent' }])
      expect(secondEndpoint.connectionSnapshot().connections).toMatchObject([{ client: 'second-agent' }])
      expect(host.connectionSnapshot().connections).toEqual([])
      const targetA = new URL(firstEndpoint.getInfo().url)
      const targetB = new URL(secondEndpoint.getInfo().url)
      const sessionId = a.transport.sessionId!
      const ping = { jsonrpc: '2.0', id: 9, method: 'ping' }
      const headers = { authorization: 'Bearer test-token', 'mcp-session-id': sessionId }
      expect((await post(Number(targetB.port), ping, headers, targetB.pathname)).status).toBe(400)

      const reopen = await a.client.callTool({ name: 'open_project', arguments: { path: '/projects/second' } })
      expect(reopen.structuredContent).toMatchObject({ error: 'ProjectConnectionBound' })

      await firstEndpoint.close()
      expect((await post(Number(targetA.port), ping, headers, targetA.pathname)).status).toBe(404)
      expect((await post(Number(targetA.port), INITIALIZE, { authorization: 'Bearer test-token' }, targetA.pathname)).status).toBe(404)
      expect(first.agent.snapshot().session).toBeNull()
      expect(second.agent.snapshot().session?.reason).toBe('Edit second')
      const edit = await b.client.callTool({ name: 'add_track', arguments: { label: 'Still editing B' } })
      expect(edit.isError).toBeFalsy()
      expect(second.trackLabels()).toContain('Still editing B')
    } finally {
      await Promise.all([a.client.close(), b.client.close()])
      first.agent.stop()
      second.agent.stop()
    }
  })
})

/** The rows arrive from SDK callbacks a microtask or two behind the HTTP
 *  response, so wait on the row rather than on the reply. */
async function rowWith(rows: McpLogEntryInput[], message: string): Promise<McpLogEntryInput> {
  const deadline = Date.now() + 2000
  let found = rows.find((r) => r.message === message)
  while (!found && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 10))
    found = rows.find((r) => r.message === message)
  }
  expect(found, `no row \`${message}\` in ${JSON.stringify(rows.map((r) => r.message))}`).toBeDefined()
  return found!
}

describe('MCP host lifecycle rows', () => {
  it('records the bind, which no workspace can yet be open for', async () => {
    const { rows, deps } = collector()
    const { port } = await start(deps)

    const bound = await rowWith(rows, `MCP host listening on 127.0.0.1:${port}`)
    expect(bound.level).toBe('info')
    expect(bound.source).toEqual({ kind: 'System' })
    expect(bound.category).toEqual({ kind: 'Mcp' })
  })

  it('records a bad bearer as its own Warn row', async () => {
    const { rows, deps } = collector()
    const { port } = await start(deps)

    const reply = await post(port, INITIALIZE, { authorization: 'Bearer wrong', 'user-agent': 'prober/1' })

    expect(reply.status).toBe(401)
    const rejected = await rowWith(rows, 'MCP request rejected: unauthorized')
    expect(rejected.level).toBe('warn')
    expect(rejected.source).toEqual({ kind: 'System' })
    expect(rejected.details).toMatchObject({ method: 'POST', user_agent: 'prober/1' })
    // The producer owns keeping the credential out of `message`; redact_and_cap
    // only ever sees `details`.
    expect(JSON.stringify(rows)).not.toContain('test-token')
  })

  it('records a disallowed Host distinctly from a bad bearer', async () => {
    const { rows, deps } = collector()
    const { port } = await start(deps)

    // Authenticated, so the 401 gate passes and a transport is built — which is
    // the only reason the Host check runs at all.
    const reply = await post(port, INITIALIZE, { authorization: 'Bearer test-token', host: 'rebind.example' })

    expect(reply.status).toBe(403)
    const rejected = await rowWith(rows, 'MCP request rejected: host not allowed')
    expect(rejected.level).toBe('warn')
    expect(rejected.details).toMatchObject({ error: 'Invalid Host header: rebind.example' })
    expect(rows.some((r) => r.message === 'MCP request rejected: unauthorized')).toBe(false)
  })

  it('names the real client on connect and stays quiet at Info on disconnect', async () => {
    const { rows, deps } = collector()
    const { host, port } = await start(deps)

    const init = await post(port, INITIALIZE, { authorization: 'Bearer test-token' })
    expect(init.sessionId).toBeTruthy()
    // `oninitialized` fires on the notification, not on the initialize request.
    const ack = await post(
      port,
      { jsonrpc: '2.0', method: 'notifications/initialized' },
      { authorization: 'Bearer test-token', 'mcp-session-id': init.sessionId! },
    )
    expect(ack.status).toBe(202)

    const connected = await rowWith(rows, 'MCP client connected: vitest-client/9.9.9')
    expect(connected.level).toBe('info')
    expect(connected.details).toMatchObject({
      client_info: { name: 'vitest-client', version: '9.9.9' },
      session_id: init.sessionId,
    })

    await host.close()
    const gone = await rowWith(rows, 'MCP client disconnected')
    expect(gone.level).toBe('debug')
    expect(gone.details).toMatchObject({ session_id: init.sessionId })
  })

  it('records a rotation without recording either token', async () => {
    const { rows, deps } = collector()
    const { host } = await start(deps)

    const next = host.resetToken()

    const rotated = await rowWith(rows, 'MCP bearer token rotated')
    expect(rotated.level).toBe('info')
    expect(rotated.source).toEqual({ kind: 'User' })
    const dump = JSON.stringify(rows)
    expect(dump).not.toContain(next)
    expect(dump).not.toContain('test-token')
  })
})

describe('a session whose client is gone is closed by the host', () => {
  // Streamable HTTP ends a session only on the client's DELETE, which a client
  // that exits never sends, so the host judges liveness itself — by the client's
  // SSE stream, and for a client that never opened one, by requests — and closes
  // the session, which fires `onclose` and with it the work-session release.
  // Without this a lost connection holds its work session until the app restarts.
  it('a client that never opened a stream and went idle is closed, with a row saying why', async () => {
    const { rows, deps } = collector()
    const host = await startMcpHost({} as Backend, { log: deps, sessionReaper: { idleMs: 60, graceMs: 60, sweepMs: 10 } })
    hosts.push(host)
    const bind = host.getInfo().bind
    const port = Number(bind.slice(bind.lastIndexOf(':') + 1))
    const init = await post(port, INITIALIZE, { authorization: 'Bearer test-token' })
    expect(init.status).toBe(200)
    expect(init.sessionId).toBeTruthy()

    const gone = await rowWith(rows, 'MCP client gone: session closed')
    expect(gone.level).toBe('info')
    expect(gone.details).toMatchObject({ session_id: init.sessionId, reason: 'idle' })
    // `onclose` ran: the ordinary disconnect row followed, and the session id is
    // refused from here on.
    await rowWith(rows, 'MCP client disconnected')
    const stale = await post(port, { jsonrpc: '2.0', id: 2, method: 'ping' }, { authorization: 'Bearer test-token', 'mcp-session-id': init.sessionId! })
    expect(stale.status).toBe(400)
  })

  // Real timers. A ping comes every fifth of the idle window, so only a stall
  // of four fifths of it makes a live client look silent; the pings outlast
  // the window, so staying alive is what they prove.
  const KEPT_ALIVE = { windowMs: 2000, paceMs: 400, pings: 8 }

  it('a client that keeps making requests is never judged gone', async () => {
    const { rows, deps } = collector()
    const host = await startMcpHost({} as Backend, { log: deps, sessionReaper: { idleMs: KEPT_ALIVE.windowMs, graceMs: KEPT_ALIVE.windowMs, sweepMs: 10 } })
    hosts.push(host)
    const bind = host.getInfo().bind
    const port = Number(bind.slice(bind.lastIndexOf(':') + 1))
    const init = await post(port, INITIALIZE, { authorization: 'Bearer test-token' })
    for (let i = 0; i < KEPT_ALIVE.pings; i++) {
      await new Promise((r) => setTimeout(r, KEPT_ALIVE.paceMs))
      await post(port, { jsonrpc: '2.0', id: 10 + i, method: 'ping' }, { authorization: 'Bearer test-token', 'mcp-session-id': init.sessionId! })
    }
    expect(rows.some((r) => r.message === 'MCP client gone: session closed')).toBe(false)
  }, 4 * KEPT_ALIVE.windowMs)

  it('a GET the transport refuses does not make the session look alive', async () => {
    // A GET without the SSE `Accept` is answered 406 and never becomes a
    // stream. Counting it as one would pin the session "streaming" forever and
    // nothing could judge the client gone — the failure this reaper exists for.
    const { rows, deps } = collector()
    const host = await startMcpHost({} as Backend, { log: deps, sessionReaper: { idleMs: 60, graceMs: 60, sweepMs: 10 } })
    hosts.push(host)
    const bind = host.getInfo().bind
    const port = Number(bind.slice(bind.lastIndexOf(':') + 1))
    const init = await post(port, INITIALIZE, { authorization: 'Bearer test-token' })
    await new Promise<void>((resolve) => {
      const req = http.request({ host: '127.0.0.1', port, path: '/mcp', method: 'GET', headers: { authorization: 'Bearer test-token', 'mcp-session-id': init.sessionId!, accept: 'application/json' } }, (res) => {
        expect(res.statusCode).not.toBe(200)
        res.resume(); res.on('end', () => resolve())
      })
      sockets.push(req)
      req.on('error', () => resolve())
      req.end()
    })
    const gone = await rowWith(rows, 'MCP client gone: session closed')
    expect(gone.details).toMatchObject({ session_id: init.sessionId, reason: 'idle' })
  })

  it('a client whose stream dropped and then went silent is closed, the row naming the stream', async () => {
    // The headline case: an SDK client that exits takes its SSE stream with it
    // and sends nothing more.
    const { rows, deps } = collector()
    const host = await startMcpHost({} as Backend, { log: deps, sessionReaper: { idleMs: 60_000, graceMs: 60, sweepMs: 10 } })
    hosts.push(host)
    const bind = host.getInfo().bind
    const port = Number(bind.slice(bind.lastIndexOf(':') + 1))
    const init = await post(port, INITIALIZE, { authorization: 'Bearer test-token' })
    await new Promise<void>((resolve) => {
      const req = http.request({ host: '127.0.0.1', port, path: '/mcp', method: 'GET', headers: { authorization: 'Bearer test-token', 'mcp-session-id': init.sessionId!, accept: 'text/event-stream' } }, (res) => { res.destroy(); resolve() })
      sockets.push(req)
      req.on('error', () => resolve())
      req.end()
    })
    const gone = await rowWith(rows, 'MCP client gone: session closed')
    expect(gone.details).toMatchObject({ session_id: init.sessionId, reason: 'stream_closed' })
  })

  it('a client whose stream dropped but who keeps making requests is not judged gone either', async () => {
    const { rows, deps } = collector()
    const host = await startMcpHost({} as Backend, { log: deps, sessionReaper: { idleMs: KEPT_ALIVE.windowMs, graceMs: KEPT_ALIVE.windowMs, sweepMs: 10 } })
    hosts.push(host)
    const bind = host.getInfo().bind
    const port = Number(bind.slice(bind.lastIndexOf(':') + 1))
    const init = await post(port, INITIALIZE, { authorization: 'Bearer test-token' })
    // Open the standalone stream, then drop its socket: the session's stream is now `closed`.
    await new Promise<void>((resolve) => {
      const req = http.request({ host: '127.0.0.1', port, path: '/mcp', method: 'GET', headers: { authorization: 'Bearer test-token', 'mcp-session-id': init.sessionId!, accept: 'text/event-stream' } }, (res) => { res.destroy(); resolve() })
      sockets.push(req)
      req.on('error', () => resolve())
      req.end()
    })
    for (let i = 0; i < KEPT_ALIVE.pings; i++) {
      await new Promise((r) => setTimeout(r, KEPT_ALIVE.paceMs))
      await post(port, { jsonrpc: '2.0', id: 20 + i, method: 'ping' }, { authorization: 'Bearer test-token', 'mcp-session-id': init.sessionId! })
    }
    expect(rows.some((r) => r.message === 'MCP client gone: session closed')).toBe(false)
  }, 4 * KEPT_ALIVE.windowMs)
})
