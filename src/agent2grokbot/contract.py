"""Versioned wire contract shared by the server and every Grok Bot peer.

Every change to the webhook payload, the callback schema or the blob API gets an
entry here, including the exact text a Bot must fold into its standing prompt.
Bots learn about changes without a human re-pasting anything:

1. `task.created` carries `protocol_version` always, plus a `contract` block
   whenever that peer has not yet acknowledged the current version.
2. `contract.updated` is a push-only webhook event sent by `contract_broadcast`,
   so a Bot can update its standing prompt even with no task in flight.
3. A Bot acks by echoing `protocol_version` in any callback, or by POSTing to
   the `ack_url` supplied with the broadcast.

Entries are append-only and must never be edited after release; a mistake is
corrected by adding the next version.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Change:
    """One released wire-contract version and the migration text it implies."""

    version: str
    released: str
    summary: str
    added: tuple[str, ...] = ()
    changed: tuple[str, ...] = ()
    removed: tuple[str, ...] = ()
    prompt_patch: str = ""

    def as_dict(self) -> dict:
        return {
            "version": self.version,
            "released": self.released,
            "summary": self.summary,
            "added": list(self.added),
            "changed": list(self.changed),
            "removed": list(self.removed),
            "prompt_patch": self.prompt_patch,
        }


# Append only. The last entry is the current contract.
CONTRACT: tuple[Change, ...] = (
    Change(
        version="0.3.0",
        released="2026-11-06",
        summary="Baseline webhook task lifecycle with text-only results.",
        added=(
            "task.created webhook with task_id, prompt, callback and instructions",
            "heartbeat, progress, result and failed callbacks",
            "task.cancel webhook and next_heartbeat_s / next_heartbeat_at hints",
        ),
        prompt_patch=(
            "Heartbeat between milestones, send one terminal result callback when "
            "the work finishes, and treat silence as failure, not completion."
        ),
    ),
    Change(
        version="0.4.0",
        released="2026-11-11",
        summary="Binary attachments move out of webhook JSON into a blob API.",
        added=(
            "task.created.attachments[] input descriptors with expiring signed urls",
            "task.created.blob_upload {url, headers} for streaming output files",
            "result.attachments[] [{blob_id}] and MCP resource links for outputs",
            "POST /v1/tasks/{task_id}/blobs authenticated by the callback token",
        ),
        changed=(
            "result may be either a string or {text, attachments}",
            "callbacks now also accept a protocol_version field",
        ),
        removed=(
            "Base64 or raw file bytes inside prompt, result or any callback JSON",
        ),
        prompt_patch=(
            "Treat task.created.attachments[] as input files: GET each signed url "
            "before it expires and read the bytes from disk, never from JSON. For "
            "every output file, compute the lowercase hex SHA-256, sign the UTF-8 "
            "string '<task_id>.<sha256>' with the callback token using the normal "
            "X-A2G-Signature scheme, then POST the raw bytes to blob_upload.url "
            "with the filename query parameter, Content-Type, X-A2G-SHA256 and that "
            "signature. Finish with result {text, attachments:[{blob_id}]}. Never "
            "put file bytes or base64 into webhook JSON or MCP payloads."
        ),
    ),
    Change(
        version="0.5.0",
        released="2026-11-11",
        summary="Self-describing contract channel so Bots migrate without a re-paste.",
        added=(
            "task.created.protocol_version on every dispatch",
            "task.created.contract delta block, present only while the peer is behind",
            "contract.updated webhook event with contract.prompt_patch and ack_url",
            "POST /v1/contract/ack authenticated by a peer-scoped signed ticket",
        ),
        changed=(
            "peers table records protocol_version and contract_seen_at per peer",
        ),
        removed=(),
        prompt_patch=(
            "Keep your standing prompt equal to the current protocol version. When a "
            "task.created or contract.updated payload contains a contract block, "
            "apply contract.prompt_patch to your own instructions, then acknowledge "
            "by POSTing {\"protocol_version\": \"<contract.current_version>\"} to "
            "contract.ack_url (or by echoing protocol_version in any callback body). "
            "Echo protocol_version in every heartbeat and result callback so the "
            "server stops resending deltas. A contract block that names new "
            "behaviour overrides any earlier instruction with which it conflicts."
        ),
    ),
)


def _key(version: str) -> tuple[int, ...]:
    """Order versions numerically so 0.10.0 sorts after 0.9.0."""
    parts = []
    for piece in str(version).strip().lstrip("vV").split("."):
        digits = "".join(ch for ch in piece if ch.isdigit())
        parts.append(int(digits) if digits else 0)
    return tuple(parts)


def current() -> Change:
    return CONTRACT[-1]


def current_version() -> str:
    return current().version


def is_behind(version: str | None) -> bool:
    """True when `version` is unknown, malformed or older than the current one."""
    if not version:
        return True
    return _key(version) < _key(current_version())


def is_newer(version: str | None, than: str | None) -> bool:
    """True when `version` outranks `than`. Unknown values never win."""
    if not version:
        return False
    if not than:
        return True
    return _key(version) > _key(than)


def since(version: str | None) -> list[Change]:
    """Every change a peer on `version` still has to absorb, oldest first."""
    if not version:
        return list(CONTRACT)
    floor = _key(version)
    return [item for item in CONTRACT if _key(item.version) > floor]


def delta(version: str | None) -> dict:
    """The migration payload: what changed and the text to fold into the prompt."""
    pending = since(version)
    return {
        "your_version": version or None,
        "current_version": current_version(),
        "up_to_date": not pending,
        "changes": [item.as_dict() for item in pending],
        "prompt_patch": "\n\n".join(
            item.prompt_patch for item in pending if item.prompt_patch),
    }


def history() -> list[dict]:
    return [item.as_dict() for item in CONTRACT]
