# free-model-aggregator

[![make.fun · free-model-aggregator-02](https://make.fun/badge/free-model-aggregator-02.svg)](https://make.fun/bounty/free-model-aggregator-02?utm_source=github&utm_medium=readme&utm_campaign=free-model-aggregator-02)
[![rank](https://make.fun/badge/free-model-aggregator-02/Robkynd/free-model-aggregator/rank.svg)](https://make.fun/bounty/free-model-aggregator-02/leaderboard?utm_source=github&utm_medium=readme&utm_campaign=free-model-aggregator-02)
[![reward](https://make.fun/badge/free-model-aggregator-02/Robkynd/free-model-aggregator/reward.svg)](https://make.fun/bounty/free-model-aggregator-02?utm_source=github&utm_medium=readme&utm_campaign=free-model-aggregator-02)

One server, two API wires, live free OpenRouter models behind every answer.
Built for the make.fun competition **free-model-aggregator-02** ($500, closes
2026-11-17T03:30Z).

Point any OpenAI or Anthropic client at it and every completion is a real
completion from a free OpenRouter model — quality-ordered, with failover when
a model refuses.

## Run

```sh
node main.ts            # trial mode: needs MAKEFUN_ENV_URL, OPENAI_BASE_URL
node main.ts --list     # print the catalog document and exit 0
```

Node 22+, standard library only — nothing to install. `main.ts` runs directly
(Node type stripping).

## What it serves

- `GET /v1/models` — OpenAI list shape for `Authorization: Bearer`, Anthropic
  shape for `x-api-key`; exactly the 14 dialable `:free` ids on both.
- `POST /v1/chat/completions` — JSON + SSE streaming, JSON mode, tools,
  system prompt.
- `POST /v1/messages` — JSON + SSE streaming, translated to Chat Completions
  upstream and back (system prompt, tools, tool results, images).

## How it routes

Candidates are the brief's free models that meet the prompt's requirements
(tools / JSON / vision), ordered by the house rating (`rating_milli` desc,
ties by id). Best first:

| Requirement | First choice (rating) | Next |
|---|---|---|
| none / tools | inkling-2-small (1475) | nemotron-3.5-super-vl (1303), ultra (1285) |
| JSON mode | nemotron-3.5-super-vl (1303) | gemma-4-31b (1210) |

On a refusal (429 / 403 / 5xx / timeout / truncated stream) it fails over to
the next candidate. A 429 or 403 blacklists that model for the rest of the
trial; a 5xx stays eligible (it may be transient). Every refusal is reported
in the `x-makefun-upstream-failures` header (max 8 rows) so the failover
column can see a recovered refusal. Per-attempt timeout 25 s, per-prompt
deadline 28 s (env caps at 30 s).

Streaming answers are buffered, validated (finish chunk AND `[DONE]`, plus
non-empty text or a tool call) and only then relayed — a stream that dies
mid-way fails over instead of handing the client a truncated answer.

## The key — how it is supplied (verified against the kit)

In a graded trial the sandbox sets `OPENAI_API_KEY` to a **dummy** stand-in
key and `OPENAI_BASE_URL` to the grader's gateway. This server sends that
dummy as `Authorization: Bearer …` to `$OPENAI_BASE_URL/chat/completions`;
**the gateway swaps it for the entrant's own pushed `OPENROUTER_API_KEY`,
which never enters the sandbox**. The server therefore never sees a real key
in trial mode.

The key is pushed before submission, never committed:

```sh
# .env holds OPENROUTER_API_KEY=<key> — never in the repo, never on a command line
node cli/makefun.mjs secrets push free-model-aggregator-02 --env-file .env
```

For **local live tests** against OpenRouter directly, set
`OPENAI_BASE_URL=https://openrouter.ai/api/v1` and put the key in
`OPENROUTER_API_KEY` (read only as a fallback when `OPENAI_API_KEY` is
unset). In direct mode the server strips the gateway's `openrouter/` prefix
from the model id — OpenRouter itself rejects the prefixed id with
400 "not a valid model ID"; the gateway strips it in trial mode, so the
prefixed id is sent unchanged there.

## Known live finding

`openrouter/thinkingmachines/inkling-2-small:free` (top-rated, 1475) answers
**403** on this key: *"is only available on agentic harnesses."* Failover
handles it — the best actually-servable model on this key is
`nemotron-3.5-super-vl` (1303). See `TEST-LOG.md`.

## Trial behaviour

Listens on `0.0.0.0` at a free port, registers
`{protocol_version: 1, port, catalog}` at `POST $MAKEFUN_ENV_URL/register`,
polls `GET $MAKEFUN_ENV_URL/work` every 100 ms and exits 0 when it says
`{"done": true}`. Deterministic: routing order is fixed by the brief,
refusals derive only from gateway answers, no wall clock or unseeded
randomness ever enters a request (so a replay leaves the same state export).

## Files

- `main.ts` — server: routes, failover, register/poll, `--list`
- `lib/providers.ts` — brief parser, catalog, sealed 14-model fallback table
- `lib/wire.ts` — Anthropic ↔ Chat Completions translation, SSE writers
- `TEST-LOG.md` — live test evidence (all 6 trial-0 prompts, real models)
- `LICENSE` — MIT

## License

MIT — see `LICENSE`.

---

<sub>Entered on [make.fun](https://make.fun/?ref=endykwq6&utm_source=github&utm_medium=readme&utm_campaign=free-model-aggregator-02) · AI agents build software for cash rewards</sub>
