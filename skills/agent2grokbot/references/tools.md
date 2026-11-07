# Wire reference

Read only for raw MCP calls, callback debugging or configuration changes. Tool
parameters and return shapes are in `SKILL.md`.

## Endpoint and authentication

`POST <base>/mcp` with `Accept: application/json, text/event-stream` and MCP 2.0
(`2026-07-28`); legacy `2025-11-25` handshake is accepted. Add
`Authorization: Bearer <A2G_ADMIN_TOKEN>` when set. `GET /health` needs no
token. Do not hard-code `MCP-Protocol-Version`; the SDK detects the revision.
`AdminTokenMiddleware` guards `/mcp` only, so SSE is never buffered.

Raw calls need `Mcp-Method: tools/call` and `Mcp-Name: <tool>`, plus `_meta`
carrying `protocolVersion`, `clientInfo` and `clientCapabilities`. The response
is JSON, or SSE whose final `data:` frame holds the result.

## Peer object

```json
{"id":"grokbot","name":"Grok Bot","capabilities":"...","enabled":true,
 "lanes":[{"lane_id":"...","url":"...","has_secret":true,
           "secret_fingerprint":"9f48edf5","busy_task_id":null}]}
```

`busy_task_id` non-null means the lane is occupied and the next task queues.
Secrets are never returned.

## Task lifecycle

`queued` -> `dispatched` -> `running` -> `completed` | `failed` | `stale` |
`cancelled`. A handle reports any in-flight state as `working`. Read `result`
on success, `error` otherwise. `heartbeat_interval_s` is mutable: the Bot can
extend or shorten it per callback.

## Callbacks (Bot to server)

`POST /cb/{task_id}` with the per-task `Bearer <callback_token>` and
`X-A2G-Signature: t=<unix>,v1=<hex>`, where hex is
`HMAC_SHA256(callback_token, "<t>." + raw_body_bytes)`. Replay window 300s;
`(task_id, event_id)` dedupe; `410 Gone` after a terminal state.

| `type` | Extra fields | Effect |
| --- | --- | --- |
| `heartbeat` | `ts` | Refresh liveness, increment `heartbeat_count` |
| `progress` | `text` | Refresh liveness, emit `notifications/progress` |
| `result` | `result` | Terminal `completed`, release lane |
| `failed` | `error` | Terminal `failed`, release lane |

Every callback carries `task_id` and a unique `event_id`. Any heartbeat or
progress callback may declare `next_heartbeat_s` or `next_heartbeat_at` in the
body or via `X-A2G-Next-Heartbeat-S` / `X-A2G-Next-Heartbeat-At`. Body wins
over header, relative over absolute, clamped to 1..86400 seconds; the server
echoes `next_heartbeat_s` and marks `stale` after `A2G_MISS_THRESHOLD` times
that interval. Only `result` completes a task.

## Configuration

| Variable | Default | Effect |
| --- | --- | --- |
| `A2G_ADMIN_TOKEN` | empty | Require Bearer on `/mcp` |
| `A2G_PUBLIC_BASE_URL` | `http://127.0.0.1:7583` | Callback prefix and the single allowed Host |
| `A2G_HEARTBEAT_INTERVAL_S` | `300` | Initial liveness cadence |
| `A2G_MISS_THRESHOLD` | `2` | Missed intervals before `stale` |
| `A2G_SSE_MAX_S` | `55` | Deadline before a handle is returned |
| `A2G_ALLOW_PRIVATE_URLS` | `1` | `0` blocks private webhook hosts |
