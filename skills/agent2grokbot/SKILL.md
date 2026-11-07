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
3. `task_send(peer_id, prompt)` with a self-contained prompt.
4. `status: "completed"` -> use `result`. `status: "working"` -> poll
   `task_get(task_id)` until terminal; `task_cancel` to abandon.
5. Only an explicit `result` completes a task; heartbeats and silence do not.

## Parameters and returns

| Tool | Required | Optional | Returns |
| --- | --- | --- | --- |
| `peer_list` | - | - | Peers: `id`, `name`, `capabilities`, `enabled`, `lanes` |
| `peer_get` | `peer_id` | - | One peer or `{"error": "unknown peer"}` |
| `peer_add` | `name`, `url`, `secret` | `peer_id`, `capabilities` | `{"peer_id": ...}` |
| `peer_update` | `peer_id` | `name`, `capabilities`, `enabled`, `add_lanes`, `remove_lanes` | Updated peer |
| `peer_delete` | `peer_id` | - | `{"deleted": bool}` |
| `task_send` | `peer_id`, `prompt` | `session_label`, `heartbeat_interval_s` | `completed` result or `working` handle |
| `task_get` | `task_id` | - | Task with `status`, `result`, `error` |
| `task_list` | - | `peer_id`, `status` | Task objects |
| `task_cancel` | `task_id` | - | `{"ok": bool, "message": ...}` |

A handle is `{"status":"working","task_id":...,"pollIntervalMs":15000,"hint":...}`.
Terminal statuses: `completed`, `failed`, `stale`, `cancelled`. Never put tokens
or secrets in `prompt`.

## Limits

- No auto-routing; `capabilities` is a label, not a policy.
- Lanes are concurrency slots of one peer, not different specialists.
- A long call returns a handle; the task keeps running in the background.
- Heartbeats are liveness only, never forwarded to the model.
- Reads never return secrets; `peer_delete` refuses in-flight peers.

## Scenarios

- Current `grokbot` peer (`just for testing, ask anything and following your way`): `peer_list()` -> `task_send(peer_id="grokbot", prompt="...")` -> poll if `working`.
- Two specialists: choose by `capabilities`, dispatch to that `id`.
- Register one: `peer_add(..., capabilities="...")`; refine with `peer_update`.
- Withdraw a request: `task_cancel(task_id)`, then stop polling.
