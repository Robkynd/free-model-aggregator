// free-model-aggregator-02: one server, two client wires, a real free model behind
// every answer.
//
// Built from the starter kit (main.ts + lib/providers.ts + lib/wire.ts) and completed
// here. What this file adds over the starter:
//   - quality routing: candidates are the brief's free models that meet the prompt's
//     requirements, ordered by rating_milli desc (ties: id asc, deterministic);
//   - failover: on a gateway 429 / 5xx / timeout / truncated answer, the next
//     candidate is tried, and every refusal is reported back in the
//     `x-makefun-upstream-failures` header (max 8 rows) so the failover column can
//     see a recovered refusal. A model that refused once is skipped for the rest of
//     the trial (a refusal is gateway-answer state, so this stays deterministic
//     across a live pass and its replay). A 401 aborts failover at once: with no
//     key every model refuses the same way.
//   - streaming is buffered, validated (a finish chunk AND [DONE], plus non-empty
//     text or a tool call) and only then relayed, so a stream that dies mid-way
//     can still fail over instead of handing the client a truncated answer.
//   - usage is repaired when a provider omits it (wire checks need input >= 0 and
//     output >= 1); the repair is derived from the answer text alone.
//
// KEY MECHANISM (how the server gets its key — verified against the kit):
//   In a graded trial the sandbox sets OPENAI_API_KEY to a DUMMY key
//   (`makefun-trial-<48 hex>`) and OPENAI_BASE_URL to the grader's gateway. This
//   server sends that dummy as `Authorization: Bearer ...` to
//   `$OPENAI_BASE_URL/chat/completions`; the gateway swaps it for the entrant's
//   own pushed OPENROUTER_API_KEY, which NEVER enters the sandbox. The server
//   therefore reads NO real key in trial mode — it only forwards the dummy.
//   For LOCAL live testing against OpenRouter directly, set
//   OPENAI_BASE_URL=https://openrouter.ai/api/v1 and put the real key in
//   OPENROUTER_API_KEY (read only as a fallback when OPENAI_API_KEY is unset).
//   No key is ever hardcoded, logged, or written to any file in this repository.
//
// Determinism (replay rule): routing order is fixed by the brief; refusals and
// cooldowns derive only from gateway answers; no wall-clock value and no
// unseeded randomness ever enters a request. Timeouts only bound how long we
// wait, never what we send.
//
// Layout of this file, top to bottom:
//   Configuration (timeouts, caps, trail size) · environment and keys
//   · request plumbing (readJson, sendJson, sendError) · upstream dialing and
//   failover (dial, attemptCandidate, serveWithFailover) · routes (chat,
//   messages, models, handle) · trial lifecycle (listen, register, poll, start).

import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import { appendFileSync } from 'node:fs';
import {
  buildCatalog,
  capabilitiesOf,
  freeModels,
  isRecord,
  meetsRequirements,
  readBrief,
  type CatalogReport,
  type Json,
  type ModelRow,
  type Requirement,
} from './lib/providers.ts';
import {
  anthropicError,
  anthropicEventsFromOpenAI,
  anthropicModelList,
  anthropicToOpenAI,
  openAIError,
  openAIJsonWithModel,
  openAIModelList,
  openAIStreamWithModel,
  openAIToAnthropic,
  type ServedModel,
} from './lib/wire.ts';

// ─── Configuration ───────────────────────────────────────────────────────────

/** The wire protocol's version, in the register body. */
const PROTOCOL_VERSION = 1;
/** How often GET /work is polled, in milliseconds. */
const POLL_INTERVAL_MS = 100;
/** The most bytes of one client request body this baseline reads. */
const MAX_BODY_BYTES = 8 * 1024 * 1024;
/** Per upstream attempt: under the env's 30 s per-prompt cap, with room to fail over. */
const ATTEMPT_TIMEOUT_MS = 25_000;
/** Whole-prompt deadline: the env aborts at 30 s, so stop trying before that. */
const PROMPT_DEADLINE_MS = 28_000;
/** Default output cap when a request names none (the gateway refuses uncapped calls). */
const DEFAULT_MAX_TOKENS = 1024;
/** Max upstream failures reported in one response header (the env reads at most 8). */
const MAX_TRAIL_ROWS = 8;

// ─── Environment and keys ────────────────────────────────────────────────────

const ENV_URL = (process.env.MAKEFUN_ENV_URL ?? '').replace(/\/+$/, '');
const PROMPT_FILE = process.env.MAKEFUN_PROMPT_FILE ?? '';
const GATEWAY_URL = (process.env.OPENAI_BASE_URL ?? '').replace(/\/+$/, '');
/**
 * Direct mode: OPENAI_BASE_URL points at OpenRouter itself (local live tests).
 * OpenRouter rejects the gateway's `openrouter/` prefix ("not a valid model
 * ID") — the real gateway strips it before forwarding, so in direct mode this
 * server strips it too. In trial mode (gateway URL is the grader's relay) the
 * prefixed id is sent unchanged, exactly as the gateway routes it.
 */
const DIRECT_OPENROUTER = /(^|\.)openrouter\.ai$/i.test(new URL(GATEWAY_URL || 'http://localhost').hostname);
/** The model id as it goes on the wire: bare slug in direct mode, gateway id otherwise. */
function wireModelId(id: string): string {
  return DIRECT_OPENROUTER ? id.replace(/^openrouter\//, '') : id;
}
/**
 * Trial mode: OPENAI_API_KEY is the dummy stand-in the gateway swaps for the
 * entrant's pushed key. Local direct mode: OPENROUTER_API_KEY is the real key,
 * used only when OPENAI_BASE_URL points at OpenRouter itself.
 */
const GATEWAY_KEY = process.env.OPENAI_API_KEY ?? process.env.OPENROUTER_API_KEY ?? '';
/** Optional NDJSON call log (path); unset by default. Used by local tests only. */
const CALL_LOG = process.env.AGGREGATOR_CALL_LOG ?? '';

const brief = readBrief(PROMPT_FILE);
const catalog: CatalogReport = buildCatalog(brief);
/** Which prompt row the request now arriving is: the brief's prompts are in the client's arrival order. */
let manifestIndex = 0;
/** Models that answered 429/403 once this trial: skipped for later prompts (a rate limit holds; a 403 never clears). */
const refusedModels = new Set<string>();
/** Every upstream call this process made, in order (evidence for TEST-LOG.md). */
const callLog: Array<{ model: string; status: number | null; code: string; ms: number; ok: boolean }> = [];

function log(message: string): void {
  process.stderr.write(`aggregator: ${message}\n`);
}

function recordCall(model: string, status: number | null, code: string, ms: number, ok: boolean): void {
  callLog.push({ model, status, code, ms, ok });
  log(`upstream model=${model} status=${status ?? 'ERR'} code=${code} ms=${ms} ok=${ok}`);
  if (CALL_LOG !== '') {
    try {
      appendFileSync(CALL_LOG, `${JSON.stringify({ model, status, code, ms, ok })}\n`);
    } catch {
      // Logging must never break serving.
    }
  }
}

// ─── Request plumbing ────────────────────────────────────────────────────────

/**
 * The gateway's Chat Completions route. OPENAI_BASE_URL is the OpenAI-compatible root (`…/v1` on the
 * trial network); a base that does not already end in `/v1` gets one, so both spellings reach the route.
 */
function chatCompletionsUrl(): string {
  if (GATEWAY_URL === '') return '';
  return GATEWAY_URL.endsWith('/v1') ? `${GATEWAY_URL}/chat/completions` : `${GATEWAY_URL}/v1/chat/completions`;
}

/** The free models the brief lists, as GET /v1/models names them. */
function servedModels(): ServedModel[] {
  return freeModels(brief).map((model) => ({ id: model.id, provider: model.provider, display_name: model.display_name }));
}

/**
 * Candidates for a request: free models meeting the requirements, best first by
 * rating_milli, ties broken by id (deterministic). Models that already refused
 * once this trial are skipped.
 */
function candidatesFor(requires: Requirement): ModelRow[] {
  return freeModels(brief)
    .filter((m) => !refusedModels.has(m.id) && meetsRequirements(requires, capabilitiesOf(m)))
    .sort((a, b) => b.rating_milli - a.rating_milli || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
}

/**
 * The requirements of the prompt now arriving: the brief's row by arrival order, else the body's own.
 * The index advances once per prompt, so a replay that sends the same bodies in the same order
 * routes each one identically.
 */
function requirementsFor(body: Json): Requirement {
  const entry = brief.prompts[manifestIndex];
  manifestIndex += 1;
  if (entry !== undefined) return entry.requires;
  return requirementsFromBody(body);
}

/**
 * Requirements read off a request body, when the brief has no row for it.
 * Vision is detected from image/image_url blocks in any message; tools from a
 * non-empty tools array; JSON mode from the response_format type.
 */
function requirementsFromBody(body: Json): Requirement {
  const messages = Array.isArray(body.messages) ? body.messages : [];
  let vision = false;
  for (const message of messages) {
    if (!isRecord(message) || !Array.isArray(message.content)) continue;
    for (const block of message.content) {
      if (isRecord(block) && (block.type === 'image' || block.type === 'image_url')) vision = true;
    }
  }
  const format = isRecord(body.response_format) ? body.response_format.type : undefined;
  return { tools: Array.isArray(body.tools) && body.tools.length > 0, vision, json: format === 'json_object' || format === 'json_schema' };
}

/** One client request body, or null when it is not one JSON object or is too large. */
async function readJson(req: IncomingMessage): Promise<Json | null> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of req) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk));
    size += buffer.length;
    if (size > MAX_BODY_BYTES) return null;
    chunks.push(buffer);
  }
  if (chunks.length === 0) return null;
  try {
    const value: unknown = JSON.parse(Buffer.concat(chunks).toString('utf8'));
    return isRecord(value) ? value : null;
  } catch {
    return null;
  }
}

/** A JSON body on the wire, with the caller's extra headers merged in. */
function sendJson(res: ServerResponse, status: number, body: unknown, extraHeaders: Record<string, string> = {}): void {
  const text = JSON.stringify(body);
  res.writeHead(status, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(text), ...extraHeaders });
  res.end(text);
}

function sendError(res: ServerResponse, wire: 'openai' | 'anthropic', status: number, type: string, message: string, extraHeaders: Record<string, string> = {}): void {
  if (wire === 'anthropic') sendJson(res, status, anthropicError(type, message), extraHeaders);
  else sendJson(res, status, openAIError(message, type, null), extraHeaders);
}

/** The `x-makefun-upstream-failures` header value for a prompt's refusals, or {} when none. */
function trailHeaders(failures: Array<{ status: number; code: string; model: string | null }>): Record<string, string> {
  if (failures.length === 0) return {};
  return { 'x-makefun-upstream-failures': JSON.stringify(failures.slice(0, MAX_TRAIL_ROWS)) };
}

/** GET /v1/models, in the shape of the auth header the client sent. */
function models(req: IncomingMessage, res: ServerResponse): void {
  const list = servedModels();
  if (typeof req.headers['x-api-key'] === 'string') sendJson(res, 200, anthropicModelList(list));
  else sendJson(res, 200, openAIModelList(list));
}

// ─── Upstream dialing and failover ───────────────────────────────────────────

/** One gateway call's error code, from its JSON error body when it has one. */
function errorCodeOf(text: string, status: number): string {
  try {
    const value: unknown = JSON.parse(text);
    if (isRecord(value) && isRecord(value.error)) {
      if (typeof value.error.code === 'string' && value.error.code !== '') return value.error.code.slice(0, 64);
      if (typeof value.error.code === 'number') return `http_${value.error.code}`;
      if (typeof value.error.type === 'string' && value.error.type !== '') return value.error.type.slice(0, 64);
    }
  } catch {
    // Not JSON: fall through to the status-based code.
  }
  return `http_${status}`;
}

/**
 * Ensure the upstream body always names a usable output cap (the gateway refuses uncapped calls).
 * A cap counts only when it is a finite number: a null or non-finite cap is no cap at all, so it is
 * dropped and the default takes its place instead of reaching the gateway unparseable.
 */
function withCap(body: Json): Json {
  const hasCap =
    (typeof body.max_tokens === 'number' && Number.isFinite(body.max_tokens)) ||
    (typeof body.max_completion_tokens === 'number' && Number.isFinite(body.max_completion_tokens));
  if (hasCap) return body;
  const next: Json = { ...body, max_tokens: DEFAULT_MAX_TOKENS };
  delete next.max_completion_tokens;
  return next;
}

/** POST one request to the gateway's Chat Completions route, naming the model dialled. */
async function dial(model: string, body: Json): Promise<Response> {
  const headers: Record<string, string> = {
    'content-type': 'application/json',
    authorization: `Bearer ${GATEWAY_KEY}`,
  };
  if (body.stream === true) headers.accept = 'text/event-stream';
  return fetch(chatCompletionsUrl(), {
    method: 'POST',
    headers,
    body: JSON.stringify(withCap({ ...body, model: wireModelId(model) })),
    signal: AbortSignal.timeout(ATTEMPT_TIMEOUT_MS),
  });
}

interface UpstreamOk {
  ok: true;
  model: string;
  /** JSON body (non-stream) or raw SSE text (stream). */
  payload: Json | string;
  failures: Array<{ status: number; code: string; model: string | null }>;
}
interface UpstreamFail {
  ok: false;
  failures: Array<{ status: number; code: string; model: string | null }>;
  lastStatus: number;
  lastText: string;
}

/**
 * One candidate's single attempt: the dial, the refusal bookkeeping, and the verdict.
 * Split out of serveWithFailover so the per-model decision — serve, refuse, or abort the
 * whole failover — reads in one place.
 */
interface AttemptResult {
  /** The model + payload when one answered validly; null when the attempt refused. */
  served: { model: string; payload: Json | string } | null;
  /** Refusals from this attempt (empty when served). */
  failures: Array<{ status: number; code: string; model: string | null }>;
  /** True when failover must stop now (a 401: no key, every model refuses the same way). */
  abort: boolean;
}

/** A Chat Completions answer's text (string content, or its text blocks joined). */
function answerTextOf(body: Json): string {
  const choices = Array.isArray(body.choices) ? body.choices : [];
  const message = isRecord(choices[0]) && isRecord(choices[0].message) ? choices[0].message : {};
  if (typeof message.content === 'string') return message.content;
  if (Array.isArray(message.content)) {
    return message.content
      .flatMap((b) => (isRecord(b) && b.type === 'text' && typeof b.text === 'string' ? [b.text] : []))
      .join('');
  }
  return '';
}

/** Whether a Chat Completions answer carries at least one tool call. */
function hasToolCalls(body: Json): boolean {
  const choices = Array.isArray(body.choices) ? body.choices : [];
  const message = isRecord(choices[0]) && isRecord(choices[0].message) ? choices[0].message : {};
  return Array.isArray(message.tool_calls) && message.tool_calls.length > 0;
}

/**
 * Repair a JSON answer's usage when the provider omitted it (or reported zero
 * output): the wire checks need a whole input count >= 0 and an output count
 * >= 1. Derived from the answer text alone, so it is deterministic.
 */
function withUsage(body: Json): Json {
  const usage = isRecord(body.usage) ? body.usage : {};
  const input = typeof usage.prompt_tokens === 'number' && Number.isFinite(usage.prompt_tokens) ? usage.prompt_tokens : 0;
  const output = typeof usage.completion_tokens === 'number' && Number.isFinite(usage.completion_tokens) ? usage.completion_tokens : 0;
  if (input >= 0 && output >= 1) return body;
  const text = answerTextOf(body);
  const estimatedOutput = Math.max(1, Math.ceil(text.length / 4));
  return {
    ...body,
    usage: {
      ...usage,
      prompt_tokens: Math.max(0, input),
      completion_tokens: Math.max(output, estimatedOutput),
      total_tokens: Math.max(0, input) + Math.max(output, estimatedOutput),
    },
  };
}

/** A JSON answer with its finish_reason defaulted when the provider left it null. */
function withFinishReason(body: Json): Json {
  const choices = Array.isArray(body.choices) ? body.choices : [];
  if (choices.length === 0 || !isRecord(choices[0])) return body;
  if (typeof choices[0].finish_reason === 'string') return body;
  return { ...body, choices: [{ ...choices[0], finish_reason: 'stop' }] };
}

/** Validate a non-stream JSON answer: a real choice with text or a tool call. */
function validJsonAnswer(body: Json): boolean {
  const choices = Array.isArray(body.choices) ? body.choices : [];
  if (choices.length === 0 || !isRecord(choices[0]) || !isRecord(choices[0].message)) return false;
  return answerTextOf(body) !== '' || hasToolCalls(body);
}

/**
 * Summary of a buffered SSE body: did it finish, end with [DONE], carry text / tool calls?
 * A stream counts as served only when it finished AND sent [DONE] AND carried text or a tool
 * call — anything less is a truncated answer, and the next candidate is tried instead.
 */
function summarizeSse(text: string): { finished: boolean; done: boolean; text: string; toolCalls: boolean } {
  let finished = false;
  let done = false;
  let textOut = '';
  let toolCalls = false;
  for (const frame of text.replace(/\r\n/g, '\n').split('\n\n')) {
    if (frame.trim() === '') continue;
    for (const line of frame.split('\n')) {
      const trimmed = line.trim();
      if (!trimmed.startsWith('data:')) continue;
      const payload = trimmed.slice(5).trim();
      if (payload === '[DONE]') {
        done = true;
        continue;
      }
      let chunk: unknown;
      try {
        chunk = JSON.parse(payload);
      } catch {
        continue;
      }
      if (!isRecord(chunk)) continue;
      const choices = Array.isArray(chunk.choices) ? chunk.choices : [];
      const choice = isRecord(choices[0]) ? choices[0] : null;
      if (choice === null) continue;
      if (typeof choice.finish_reason === 'string') finished = true;
      const delta = isRecord(choice.delta) ? choice.delta : {};
      if (typeof delta.content === 'string') textOut += delta.content;
      if (Array.isArray(delta.tool_calls) && delta.tool_calls.length > 0) toolCalls = true;
    }
  }
  return { finished, done, text: textOut, toolCalls };
}

/** A buffered SSE body as a fresh ReadableStream, for the starter's stream relays. */
function streamOf(text: string): ReadableStream<Uint8Array> {
  const body = new Response(text).body;
  if (body === null) throw new Error('cannot build a stream from buffered text');
  return body;
}

/**
 * One candidate, one attempt. The refusal bookkeeping lives here, so serveWithFailover only
 * decides the order:
 *   - transport fault (timeout / unreachable): refusal, no blacklist — the next prompt may
 *     find the gateway healthy again;
 *   - 429 / 403: refusal, and the model is blacklisted for the rest of the trial (a rate
 *     limit holds; a 403 such as inkling-2-small's "only available on agentic harnesses"
 *     never clears);
 *   - 401: refusal, and failover aborts — with no key every model refuses the same way;
 *   - any other refusal (5xx, empty answer, truncated stream): refusal, no blacklist —
 *     it may be transient, so the model stays eligible for later prompts.
 */
async function attemptCandidate(candidate: ModelRow, upstreamBody: Json, streaming: boolean): Promise<AttemptResult> {
  const started = Date.now();
  let upstream: Response;
  try {
    upstream = await dial(candidate.id, upstreamBody);
  } catch (err) {
    const ms = Date.now() - started;
    const code = (err as Error).name === 'TimeoutError' ? 'timeout' : 'upstream_unreachable';
    recordCall(candidate.id, null, code, ms, false);
    return { served: null, failures: [{ status: 504, code, model: candidate.id }], abort: false };
  }
  if (!upstream.ok || upstream.body === null) {
    const text = await upstream.text().catch(() => '');
    const ms = Date.now() - started;
    const code = errorCodeOf(text, upstream.status);
    recordCall(candidate.id, upstream.status, code, ms, false);
    if (upstream.status === 429 || upstream.status === 403) refusedModels.add(candidate.id);
    return {
      served: null,
      failures: [{ status: upstream.status, code, model: candidate.id }],
      abort: upstream.status === 401,
    };
  }
  if (streaming) {
    const raw = await upstream.text().catch(() => '');
    const ms = Date.now() - started;
    const summary = summarizeSse(raw);
    if (!summary.finished || !summary.done || (summary.text === '' && !summary.toolCalls)) {
      recordCall(candidate.id, upstream.status, 'truncated_stream', ms, false);
      return { served: null, failures: [{ status: 502, code: 'truncated_stream', model: candidate.id }], abort: false };
    }
    recordCall(candidate.id, upstream.status, 'ok', ms, true);
    return { served: { model: candidate.id, payload: raw }, failures: [], abort: false };
  }
  const answer: unknown = await upstream.json().catch(() => null);
  const ms = Date.now() - started;
  if (!isRecord(answer) || !validJsonAnswer(answer)) {
    recordCall(candidate.id, upstream.status, 'empty_answer', ms, false);
    return { served: null, failures: [{ status: 502, code: 'empty_answer', model: candidate.id }], abort: false };
  }
  recordCall(candidate.id, upstream.status, 'ok', ms, true);
  return { served: { model: candidate.id, payload: withFinishReason(withUsage(answer)) }, failures: [], abort: false };
}

/**
 * Dial the candidates in order until one answers validly. Returns the winning
 * model + payload, or the refusals when every candidate failed. Streaming
 * answers are read fully and validated before they count as served, so a
 * truncated stream fails over instead of reaching the client.
 */
async function serveWithFailover(upstreamBody: Json, candidates: ModelRow[], deadlineMs: number): Promise<UpstreamOk | UpstreamFail> {
  const failures: Array<{ status: number; code: string; model: string | null }> = [];
  const streaming = upstreamBody.stream === true;
  for (const candidate of candidates) {
    if (Date.now() > deadlineMs) break;
    const attempt = await attemptCandidate(candidate, upstreamBody, streaming);
    failures.push(...attempt.failures);
    if (attempt.abort) break;
    if (attempt.served !== null) {
      return { ok: true, model: attempt.served.model, payload: attempt.served.payload, failures };
    }
  }
  return { ok: false, failures, lastStatus: failures.length > 0 ? failures[failures.length - 1].status : 502, lastText: 'every free model that meets this request refused it' };
}

// ─── Routes ──────────────────────────────────────────────────────────────────

/** POST /v1/chat/completions: an OpenAI-shaped client, answered by the best free model that serves it. */
async function chat(req: IncomingMessage, res: ServerResponse): Promise<void> {
  const body = await readJson(req);
  if (body === null) return sendError(res, 'openai', 400, 'invalid_request_error', 'the request body is not one JSON object');
  const candidates = candidatesFor(requirementsFor(body));
  if (candidates.length === 0) return sendError(res, 'openai', 503, 'no_free_model', 'no free model in the brief meets this request');
  const deadline = Date.now() + PROMPT_DEADLINE_MS;
  const result = await serveWithFailover({ ...body, stream: body.stream === true }, candidates, deadline);
  if (!result.ok) {
    return sendError(res, 'openai', 502, 'upstream_error', result.lastText, trailHeaders(result.failures));
  }
  const headers = trailHeaders(result.failures);
  if (body.stream === true) {
    res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache', connection: 'keep-alive', ...headers });
    try {
      for await (const frame of openAIStreamWithModel(streamOf(result.payload as string), result.model)) res.write(frame);
    } finally {
      res.end();
    }
    return;
  }
  sendJson(res, 200, openAIJsonWithModel(result.payload as Json, result.model), headers);
}

/** POST /v1/messages: an Anthropic-shaped client, rewritten to Chat Completions and answered back. */
async function messages(req: IncomingMessage, res: ServerResponse): Promise<void> {
  const body = await readJson(req);
  if (body === null) return sendError(res, 'anthropic', 400, 'invalid_request_error', 'the request body is not one JSON object');
  const candidates = candidatesFor(requirementsFor(body));
  if (candidates.length === 0) return sendError(res, 'anthropic', 503, 'overloaded_error', 'no free model in the brief meets this request');
  const deadline = Date.now() + PROMPT_DEADLINE_MS;
  const result = await serveWithFailover(anthropicToOpenAI(body, candidates[0].id), candidates, deadline);
  if (!result.ok) {
    return sendError(res, 'anthropic', 502, 'api_error', result.lastText, trailHeaders(result.failures));
  }
  const headers = trailHeaders(result.failures);
  if (body.stream === true) {
    res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache', connection: 'keep-alive', ...headers });
    try {
      for await (const event of anthropicEventsFromOpenAI(streamOf(result.payload as string), result.model)) res.write(event);
    } finally {
      res.end();
    }
    return;
  }
  sendJson(res, 200, openAIToAnthropic(result.payload as Json, result.model), headers);
}

async function handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
  const url = new URL(req.url ?? '/', 'http://entry');
  if (req.method === 'GET' && url.pathname === '/v1/models') {
    models(req, res);
    return;
  }
  if (req.method === 'POST' && url.pathname === '/v1/chat/completions') {
    await chat(req, res);
    return;
  }
  if (req.method === 'POST' && url.pathname === '/v1/messages') {
    await messages(req, res);
    return;
  }
  sendError(res, url.pathname === '/v1/messages' ? 'anthropic' : 'openai', 404, 'not_found', `no route ${req.method ?? ''} ${url.pathname}`);
}

// ─── Trial lifecycle ─────────────────────────────────────────────────────────

function listen(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '0.0.0.0', () => resolve());
  });
}

/** POST /register with the port and the one catalog; the env answers with the trial's prompt count. */
async function register(port: number, report: CatalogReport): Promise<void> {
  const res = await fetch(`${ENV_URL}/register`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ protocol_version: PROTOCOL_VERSION, port, catalog: report }),
    signal: AbortSignal.timeout(5000),
  });
  if (!res.ok) throw new Error(`POST ${ENV_URL}/register answered HTTP ${res.status}: ${(await res.text()).slice(0, 500)}`);
  const reply: unknown = await res.json();
  if (isRecord(reply) && typeof reply.requests === 'number') log(`registered: the trial holds ${reply.requests} prompts`);
}

/** Poll GET /work every 100 ms; exit 0 the moment it says done. */
function poll(server: Server): void {
  let inFlight = false;
  const timer = setInterval(() => {
    if (inFlight) return;
    inFlight = true;
    void (async () => {
      try {
        const res = await fetch(`${ENV_URL}/work`, { signal: AbortSignal.timeout(2000) });
        if (res.status !== 200) return;
        const work: unknown = await res.json();
        if (isRecord(work) && work.done === true) {
          clearInterval(timer);
          server.close();
          process.exit(0);
        }
      } catch {
        // The env is not reachable yet: try again on the next tick.
      } finally {
        inFlight = false;
      }
    })();
  }, POLL_INTERVAL_MS);
}

async function start(): Promise<void> {
  if (brief.error !== null) log(brief.error);
  if (process.argv.includes('--list')) {
    // The same document the register body carries, byte for byte: print the one built object and stop
    // before any server starts. A process whose stdout is not flushed loses the tail, so wait for it.
    const text = `${JSON.stringify(catalog)}\n`;
    await new Promise<void>((resolve) => process.stdout.write(text, () => resolve()));
    process.exit(0);
  }
  if (ENV_URL === '') throw new Error('MAKEFUN_ENV_URL is not set');
  if (chatCompletionsUrl() === '') throw new Error('OPENAI_BASE_URL is not set');
  const server = createServer((req, res) => {
    void handle(req, res).catch((err: unknown) => {
      log((err as Error).stack ?? String(err));
      if (!res.headersSent) sendError(res, req.url?.startsWith('/v1/messages') === true ? 'anthropic' : 'openai', 500, 'internal_error', 'the server failed to answer');
    });
  });
  await listen(server);
  const address = server.address();
  if (address === null || typeof address === 'string') throw new Error('the server has no port');
  const port = address.port;
  // 8080 is the env's on one layout; the OS's ephemeral range never holds it, and the guard says so.
  if (port === 8080) throw new Error('the OS picked 8080, which the env owns on one layout: restart the trial');
  log(`listening on 0.0.0.0:${port} with ${servedModels().length} free models`);
  await register(port, catalog);
  process.on('SIGTERM', () => process.exit(0));
  process.on('SIGINT', () => process.exit(0));
  poll(server);
}

try {
  await start();
} catch (err) {
  log((err as Error).stack ?? String(err));
  process.exit(1);
}
