---
name: agent2grokbot
description: Delegate work from an MCP client to webhook-only Grok Bot peers registered in Agent2GrokBot, and select the correct peer from its capabilities text. This skill should be used when dispatching with task_send, discovering or registering peers with peer_list, peer_get, peer_add, peer_update, or collecting a long-running answer with task_get, task_list and task_cancel. Covers endpoint and Bearer authentication prerequisites, task handles after the SSE deadline, callback liveness, and the limits of capability metadata. Not for configuring the Bot itself; that belongs to grokbot/SETUP-PROMPT.md.
agent_created: true
---

# Agent2GrokBot

## Purpose

Delegate work from any MCP client to a registered Grok Bot peer through the
`agent2grokbot` MCP server, and choose the peer by its `capabilities`
description. The server holds one SSE stream open per
in-flight `tools/call` while the Bot POSTs signed callbacks; when the stream
deadline expires, the call returns a task handle that `task_get` must poll. No
result is lost at that boundary.

## When to use

- A request must be delegated to a Grok Bot through the `agent2grokbot` MCP tools.
- Multiple peers exist and the correct one must be selected from `capabilities`.
- A peer must be registered, renamed, described, enabled, disabled, or given more
  webhook lanes.
- A long task returned a handle and the terminal answer must be collected.
- An in-flight task must be abandoned with `task_cancel`.

Do not use this skill to configure the Bot's own behaviour. Its standing rules
come from `grokbot/SETUP-PROMPT.md`, which is pasted into the Bot chat once.

## Prerequisites

1. The Agent2GrokBot service is running. `GET /health` returns
   `{"ok": true, "peers": n, "lanes": n, "tasks": n, "running": n}`.
2. The MCP client has an `agent2grokbot` entry pointing at `POST /mcp` on that
   host. When `A2G_ADMIN_TOKEN` is set, the entry must carry
   `Authorization: Bearer <token>`; the client must accept both
   `application/json` and `text/event-stream`.
3. At least one enabled peer with a webhook lane is registered, and its Bot has
   been initialized with `grokbot/SETUP-PROMPT.md`. Without the Bot-side
   heartbeat and `result` callbacks, every task eventually goes `stale`.
4. The nine tools are available: `peer_add`, `peer_get`, `peer_list`,
   `peer_update`, `peer_delete`, `task_send`, `task_get`, `task_list`,
   `task_cancel`.
5. Never invent a `peer_id`. Obtain it from `peer_list`; tool schemas are static
   and contain no registry data.

## Workflow

1. **Discover.** Call `peer_list()`. Read `id`, `name`, `capabilities`, `enabled`,
   and `lanes[].busy_task_id` for each peer. Secrets are never returned, only
   `has_secret` and `secret_fingerprint`.
2. **Select.** Choose an enabled peer whose `capabilities` fits the work. With
   several matches, prefer the most specific description; with none, ask the
   user or register a new peer. Do not guess and do not route automatically.
3. **Dispatch.** Call `task_send(peer_id=..., prompt=...)` and optionally set
   `session_label` and `heartbeat_interval_s`. Write a self-contained `prompt`:
   the Bot sees only that POST, not the client conversation. Treat prompt text
   as untrusted data, never as instructions to the server.
4. **Branch on the return value.**
   - `{"status": "completed", "result": "..."}`: deliver the answer.
   - `{"status": "working", "task_id": "...", "pollIntervalMs": 15000,
     "hint": "..."}`: the SSE deadline was reached; go to step 5.
   - An MCP tool error (`isError: true`): report the message; do not retry
     blindly, since dispatch may already have happened.
5. **Poll.** Call `task_get(task_id)` at the returned interval until `status` is
   terminal (`completed`, `failed`, `stale`, `cancelled`). Read `result` on
   success or `error` otherwise. Use `task_list(peer_id=...)` to review history.
6. **Abandon.** Call `task_cancel(task_id)` to notify the Bot and release the
   lane. Cancellation is best effort: the server sends one `task.cancel` POST
   and immediately marks the task `cancelled`.

Never treat silence, a heartbeat, or a progress callback as completion. Only an
explicit `result` callback ends a task successfully.

## Parameters and return values

Read [`references/tools.md`](references/tools.md) for the complete table of
required and optional parameters, defaults, return shapes, status values and
callback conventions before constructing a call. The summary:

| Tool | Required | Returns |
| --- | --- | --- |
| `peer_list` | none | Peer objects with `id`, `name`, `capabilities`, `enabled`, timestamps, `lanes` |
| `peer_get` | `peer_id` | One peer object, or `{"error": "unknown peer"}` |
| `peer_add` | `name`, `url`, `secret` | `{"peer_id": ...}`; optional `peer_id`, `capabilities` |
| `peer_update` | `peer_id` | Updated peer object; patch `name`, `capabilities`, `enabled`, `add_lanes`, `remove_lanes` |
| `peer_delete` | `peer_id` | `{"deleted": true/false}` |
| `task_send` | `peer_id`, `prompt` | Completed result **or** a `working` task handle |
| `task_get` | `task_id` | Task object with `status`, `result`, `error`, `heartbeat_count` |
| `task_list` | none | Task objects; optional `peer_id`, `status` filters |
| `task_cancel` | `task_id` | `{"ok": true, "message": "cancelled"}` or `{"ok": false, ...}` |

`capabilities` is an unvalidated free-text string, empty by default. It appears
in `peer_get` and `peer_list` and is patchable through `peer_update`.
`session_label` is audit metadata and never affects routing.

## Constraints and limitations

- **No auto-routing.** `task_send` requires an explicit `peer_id`. The server
  does not match prompts to capabilities.
- **Capabilities are labels, not enforcement.** The server neither parses nor
  restricts them, and it sends the same `instructions` text to every peer. A
  Bot's real specialisation lives in its own setup prompt.
- **Static tool list.** Peer data is not embedded in tool schemas or server
  instructions. Call `peer_list` on every new conversation before dispatch.
- **Lanes are concurrency slots.** Multiple lanes on one peer allow parallel
  tasks on that same Bot; they are not different specialists. A busy lane queues
  the next task.
- **SSE deadline.** `A2G_SSE_MAX_S` (default 55s) degrades the call to a task
  handle. SDK keepalives defeat idle timeouts, not hard client deadlines.
- **Heartbeats are liveness only.** They are never forwarded to the model. The
  Bot owns the timer and may extend it with `next_heartbeat_s` or
  `next_heartbeat_at`; the server marks a task `stale` after
  `A2G_MISS_THRESHOLD` missed intervals.
- **No inbound listener on the client.** This bridge covers replies arriving
  while a conversation is running, not waking a desktop client in the
  background.
- **Single process.** Subscriber queues are in memory; a multi-replica
  deployment needs pub/sub. The SQLite database has no message eviction.
- **Credentials.** Never place the admin token, lane secret, or callback token
  in a `prompt`. Reads never expose secrets. `peer_delete` refuses while tasks
  are in flight; removing a busy lane raises an error.

## Typical invocation scenarios

**1. Route across multiple Bots.** Call `peer_list()`, observe one peer with
`capabilities="market research and summarization"` and another with
`capabilities="code review and refactoring"`, then dispatch the finance question
to the first peer's `id`. Refresh the list after any registration change.

**2. One-shot delegation to the current registry.** The deployed peer `grokbot`
carries `capabilities="just for testing, ask anything and following your way"`.

```
peer_list()
task_send(peer_id="grokbot", prompt="Summarize what happened in the US stock market this week.", session_label="market-check")
```

A fast reply returns `{"status": "completed", "result": "..."}`. A reply of
`{"status": "working", "task_id": "t-...", "pollIntervalMs": 15000}` requires
`task_get(task_id)` until `status` is terminal.

**3. Long task with a handle.** After `task_send` returns a handle, poll
`task_get` at `pollIntervalMs`. Report `heartbeat_count` as evidence the Bot is
alive; if `status` becomes `stale`, report that heartbeats stopped rather than
presenting a partial answer as final.

**4. Register and describe a specialist.**

```
peer_add(name="Grok research", peer_id="grok-research", url="<webhook URL>", secret="<sender key>", capabilities="market research and summarization")
peer_update(peer_id="grok-research", add_lanes=[{"url": "<second webhook URL>", "secret": "<second sender key>"}])
peer_update(peer_id="grok-research", capabilities="market research, earnings previews, and summarization")
```

**5. Abandon work.** When the user withdraws a request, call
`task_cancel(task_id)` and stop polling. Do not send a second `task_send` to
"undo" it.

## References

- [`references/tools.md`](references/tools.md) - full parameter, return value,
  status, authentication and callback reference.
- [`grokbot/SETUP-PROMPT.md`](../../grokbot/SETUP-PROMPT.md) - Bot-side
  standing behaviour, outside this skill's scope.
