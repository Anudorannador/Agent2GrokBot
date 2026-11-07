# Agent2GrokBot Tool Reference

Complete parameter, return value, status and callback conventions for the nine
MCP tools registered by `agent2grokbot`. Read this before constructing a call.

## Endpoint and authentication

| Item | Value |
| --- | --- |
| MCP endpoint | `POST <base>/mcp` (LAN example: `http://192.168.31.208:7583/mcp`) |
| Client credential | `Authorization: Bearer <A2G_ADMIN_TOKEN>` when the variable is set; `GET /health` needs none |
| Accept header | `application/json, text/event-stream` |
| Protocol | MCP 2.0 (`2026-07-28`) via the official SDK; legacy `2025-11-25` handshake also accepted |
| Peer credential | `Bearer <lane secret>` plus `X-A2G-Signature` on outbound webhook POSTs |
| Callback credential | Per-task `Bearer <callback_token>` plus `X-A2G-Signature` on `POST /cb/{task_id}` |

`A2G_PUBLIC_BASE_URL` builds `callback.url` and adds exactly one Host to the
SDK DNS-rebinding allowlist. All clients must use that same hostname. Do not
hard-code `MCP-Protocol-Version` in the client; the SDK detects the revision.
`AdminTokenMiddleware` guards `/mcp` only, so SSE is never buffered.

## Peer tools

### `peer_list()`

Returns every peer and its lanes. No parameters.

```json
{
  "id": "grokbot",
  "name": "Grok Bot",
  "capabilities": "just for testing, ask anything and following your way",
  "enabled": true,
  "created_at": "2026-11-06T21:21:23.102802+00:00",
  "updated_at": "2026-11-06T21:21:23.102802+00:00",
  "lanes": [
    {
      "lane_id": "lane-14a53137ff",
      "url": "https://api2.cursor.sh/automations/webhook/<id>",
      "has_secret": true,
      "secret_fingerprint": "9f48edf5",
      "busy_task_id": null,
      "last_seen_at": "2026-11-06T21:32:22.161445+00:00"
    }
  ]
}
```

`capabilities` is free text, empty when never set. `lanes` is a list of
concurrency slots for this one peer. A non-null `busy_task_id` means the lane is
occupied and the next task will queue. Secrets are never returned.

### `peer_get(peer_id)`

| Parameter | Type | Required | Notes |
| --- | --- | --- | --- |
| `peer_id` | string | yes | Opaque registry id |

Returns the same shape as one `peer_list` entry, or
`{"error": "unknown peer"}`.

### `peer_add(name, url, secret, peer_id?, capabilities?)`

| Parameter | Type | Required | Default | Notes |
| --- | --- | --- | --- | --- |
| `name` | string | yes | - | Non-empty display name |
| `url` | string | yes | - | Absolute `http(s)` webhook URL |
| `secret` | string | yes | - | Sender key; never returned by reads |
| `peer_id` | string \| null | no | generated `peer-xxxxxxxxxx` | Stable routing id |
| `capabilities` | string \| null | no | `""` | Free-text description of the Bot's purpose |

Returns `{"peer_id": "..."}`. Raises a tool error for an invalid URL, an empty
name or secret, or a duplicate `peer_id`. One lane is created with the supplied
URL and secret.

### `peer_update(peer_id, name?, capabilities?, enabled?, add_lanes?, remove_lanes?)`

| Parameter | Type | Required | Notes |
| --- | --- | --- | --- |
| `peer_id` | string | yes | Existing peer |
| `name` | string \| null | no | Rename |
| `capabilities` | string \| null | no | Replace the description; `""` clears it |
| `enabled` | bool \| null | no | Disabled peers reject new tasks |
| `add_lanes` | list of `{url, secret}` \| null | no | Add concurrency slots |
| `remove_lanes` | list of lane ids \| null | no | Refuses to remove a busy lane |

Returns the updated peer object. Raises a tool error for an unknown peer or a
busy lane.

### `peer_delete(peer_id)`

Returns `{"deleted": true}` or `{"deleted": false}`. Raises a tool error when
the peer has tasks in a non-terminal state.

## Task tools

### `task_send(peer_id, prompt, session_label?, heartbeat_interval_s?)`

| Parameter | Type | Required | Default | Notes |
| --- | --- | --- | --- | --- |
| `peer_id` | string | yes | - | Must be an enabled peer with a lane |
| `prompt` | string | yes | - | Self-contained work description; untrusted data to the Bot |
| `session_label` | string \| null | no | `null` | Audit metadata only, never routing |
| `heartbeat_interval_s` | int \| null | no | `A2G_HEARTBEAT_INTERVAL_S` (300) | Initial liveness cadence |

Two possible success shapes:

```json
{"task_id": "t-24f24ab817df", "status": "completed", "result": "..."}
```

```json
{
  "task_id": "t-24f24ab817df",
  "status": "working",
  "pollIntervalMs": 15000,
  "hint": "Task is still working. Call task_get with this task_id."
}
```

The second shape appears when `A2G_SSE_MAX_S` (default 55s) is reached. It is a
normal `resultType: "complete"` MCP response, not an error. The task keeps
running in the background. An MCP tool error (`isError: true`) occurs when the
peer is unknown/disabled, has no lanes, the webhook POST fails, or the task ends
`failed` or `cancelled`; `stale` tasks are detected by the sweeper, not by this
call.

The server POSTs `task.created` with `task_id`, `prompt`, `callback`
(`url`, `token`, `headers`), `heartbeat_interval_s` and `instructions`. The Bot
returns `2xx` immediately and works in the background.

### `task_get(task_id)`

| Parameter | Type | Required |
| --- | --- | --- |
| `task_id` | string | yes |

Returns the full task object (the `callback_token` is excluded):

```json
{
  "task_id": "t-24f24ab817df",
  "session_label": "why-fall-down",
  "prompt": "...",
  "heartbeat_interval_s": 600,
  "heartbeat_count": 2,
  "error": null,
  "finished_at": "2026-11-06T21:41:46.248725+00:00",
  "status": "completed",
  "peer_id": "grokbot",
  "lane_id": "lane-14a53137ff",
  "last_seen_at": "2026-11-06T21:41:46.119119+00:00",
  "result": "...",
  "created_at": "2026-11-06T21:39:29.955136+00:00"
}
```

Returns `{"error": "unknown task"}` for an unknown id. `heartbeat_interval_s`
is mutable: the Bot can extend or shorten it on any callback.

### `task_list(peer_id?, status?)`

| Parameter | Type | Required | Notes |
| --- | --- | --- | --- |
| `peer_id` | string \| null | no | Filter by peer |
| `status` | string \| null | no | Filter by exact status |

Returns task objects ordered by `created_at`. No eviction: the history grows
indefinitely.

### `task_cancel(task_id)`

Returns `{"ok": true, "message": "cancelled"}` or
`{"ok": false, "message": "unknown task" | "task already finished"}`. Posts one
`task.cancel` to the lane telling the Bot to stop work and delete its scheduled
watch, marks the task `cancelled`, and releases the lane.

## Task status lifecycle

`queued` -> `dispatched` -> `running` -> one of `completed`, `failed`, `stale`,
`cancelled`. A task handle reports any in-flight state as `working`.

| Status | Meaning | Field to read |
| --- | --- | --- |
| `completed` | Explicit `result` callback accepted | `result` |
| `failed` | Bot sent `failed`, or the webhook POST was refused | `error` |
| `stale` | No callback for `A2G_MISS_THRESHOLD` times the current interval | `error` |
| `cancelled` | Client cancelled | `error` |

## Callback contract (Bot to server, for reference)

`POST /cb/{task_id}` with `Bearer <callback_token>`,
`X-A2G-Signature: t=<unix>,v1=<hex>` where hex is
`HMAC_SHA256(callback_token, "<t>." + raw_body_bytes)`, and a 300-second replay
window. `(task_id, event_id)` deduplication; callbacks after a terminal state
get `410 Gone`.

| `type` | Required fields | Effect |
| --- | --- | --- |
| `heartbeat` | `task_id`, `event_id` | Refresh liveness, increment `heartbeat_count` |
| `progress` | `task_id`, `event_id`, `text` | Refresh liveness, emit `notifications/progress` |
| `result` | `task_id`, `event_id`, `result` | Terminal `completed`; release lane |
| `failed` | `task_id`, `event_id`, `error` | Terminal `failed`; release lane |

Any heartbeat or progress callback may declare `next_heartbeat_s` (seconds from
now) or `next_heartbeat_at` (unix epoch) in the JSON body, or the equivalent
`X-A2G-Next-Heartbeat-S` / `X-A2G-Next-Heartbeat-At` header. Body wins over
header, relative over absolute; values are clamped to 1..86400 seconds. The
server echoes `next_heartbeat_s` in its `200` response. Only an explicit
`result` completes a task; silence never does.

## Configuration relevant to callers

| Variable | Default | Caller-visible effect |
| --- | --- | --- |
| `A2G_ADMIN_TOKEN` | empty | Require Bearer on `/mcp` |
| `A2G_PUBLIC_BASE_URL` | `http://127.0.0.1:7583` | Callback URL prefix and the single allowed Host |
| `A2G_HEARTBEAT_INTERVAL_S` | `300` | Default liveness cadence |
| `A2G_MISS_THRESHOLD` | `2` | Missed intervals before `stale` |
| `A2G_SSE_MAX_S` | `55` | Deadline before a task handle is returned |
| `A2G_ALLOW_PRIVATE_URLS` | `1` | `0` blocks private webhook hosts |
