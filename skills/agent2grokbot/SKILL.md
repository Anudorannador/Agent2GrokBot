---
name: agent2grokbot
description: Dispatch work to Grok Bot peers through the Agent2GrokBot MCP server. This skill should be used when an agent needs to choose a peer by capabilities, call task_send, register peers with peer_add or peer_update, or collect a long-running result with task_get. Defines the responsibility split between the human, the Bot, the MCP server and the calling agent. Not for Bot setup or server deployment, which are already done when this skill is used.
agent_created: true
---

# Agent2GrokBot

## Purpose

Tell the calling agent how to delegate work over the `agent2grokbot` MCP tools
and which part owns what. The MCP connection and the Bot are already set up;
this skill covers neither installation nor configuration.

## Coordination

| Party | Owns | Does not do |
| --- | --- | --- |
| Human | One-time Bot setup and peer registration | Dispatch tasks |
| Grok Bot | Do the work in `prompt`; report progress and a final result | Accept inbound client connections |
| Agent2GrokBot MCP server | Peer registry, dispatch, lane allocation, liveness, SSE stream and task handles | Run the work, choose a peer, enforce capabilities |
| Calling agent (this skill) | Discover peers, choose `peer_id`, send `prompt`, poll a handle, cancel | Configure the Bot or the server |

`capabilities` is free text on each peer. The server never parses or enforces
it; selection is always the caller's decision with an explicit `peer_id`.

## Prerequisites

- The `agent2grokbot` MCP tools are connected; `peer_list()` succeeds.
- At least one enabled peer exists whose Bot is initialized. Without the Bot's
  callbacks, tasks go `stale`.
- Never invent a `peer_id`; tool schemas contain no registry data.

## Workflow

1. `peer_list()` -> read `id`, `capabilities`, `enabled`, `lanes[].busy_task_id`.
2. Pick the enabled peer whose `capabilities` fits; otherwise ask the user.
3. For files, first upload raw bytes to `POST /v1/blobs?filename=<name>` with the
   configured admin Bearer token and file MIME `Content-Type`; then pass the
   returned IDs as `task_send(..., attachment_ids=[...])`. Never Base64 files into
   `prompt`. Download result attachments from their signed `url` values.
4. `task_send(peer_id, prompt)` with a self-contained prompt.
5. `status: "completed"` -> use `result`; output files also appear as MCP
   `resource_link` content blocks. `status: "working"` -> poll `task_get(task_id)`
   until terminal; `task_cancel` to abandon. `task_get` refreshes download URLs.
6. Only an explicit `result` completes a task; heartbeats and silence do not.

## Parameters and returns

| Tool | Required | Optional | Returns |
| --- | --- | --- | --- |
| `peer_list` | - | - | Peers: `id`, `name`, `capabilities`, `enabled`, `lanes` |
| `peer_get` | `peer_id` | - | One peer or `{"error": "unknown peer"}` |
| `peer_add` | `name`, `url`, `secret` | `peer_id`, `capabilities` | `{"peer_id": ...}` |
| `peer_update` | `peer_id` | `name`, `capabilities`, `enabled`, `add_lanes`, `remove_lanes` | Updated peer |
| `peer_delete` | `peer_id` | - | `{"deleted": bool}` |
| `task_send` | `peer_id`, `prompt` | `session_label`, `heartbeat_interval_s`, `attachment_ids` | `completed` result or `working` handle; output resource links |
| `task_get` | `task_id` | - | Task with `status`, `result`, `error`; fresh attachment URLs |
| `task_list` | - | `peer_id`, `status` | Task objects |
| `task_cancel` | `task_id` | - | `{"ok": bool, "message": ...}` |
| `contract_get` | - | `version`, `full` | Delta a peer on `version` still owes, or every release when `full` |
| `contract_status` | - | - | Per-peer `protocol_version`, `up_to_date`, `pending_versions` |
| `contract_broadcast` | - | `peer_id`, `dry_run` | Per-peer delivery result |

A handle is `{"status":"working","task_id":...,"pollIntervalMs":15000,"hint":...}`.
Terminal statuses: `completed`, `failed`, `stale`, `cancelled`. Never put tokens
or secrets in `prompt`.

## Limits

- No auto-routing; `capabilities` is a label, not a policy.
- Lanes are concurrency slots of one peer, not different specialists.
- A long call returns a handle; the task keeps running in the background.
- Heartbeats are liveness only, never forwarded to the model.
- Reads never return secrets; `peer_delete` refuses in-flight peers.
- Blob URLs expire; call `task_get` for fresh output URLs. Input IDs can be bound
  only once, and output IDs must belong to the same task.
- Binary storage is not automatically evicted; do not treat it as permanent storage.

## Scenarios

- Current `grokbot` peer (`just for testing, ask anything and following your way`): `peer_list()` -> `task_send(peer_id="grokbot", prompt="...")` -> poll if `working`.
- Send audio/video: upload each file to `/v1/blobs` -> `task_send(...,
  attachment_ids=[...])` -> follow result `resource_link` URLs.
- Two specialists: choose by `capabilities`, dispatch to that `id`.
- Register one: `peer_add(..., capabilities="...")`; refine with `peer_update`.
- Withdraw a request: `task_cancel(task_id)`, then stop polling.
- The server was upgraded: `contract_status()` first. `up_to_date: false` means
  that peer is still on old behaviour, so `contract_broadcast(peer_id=...)`
  before debugging a task against the new semantics. Never ask the user to
  re-paste the Bot prompt; the push event replaces it.
