"""FastAPI application: MCP 2.0 via the official SDK, plus webhook callbacks.

The SDK owns JSON-RPC, version negotiation, tool schemas, the legacy handshake,
SSE framing, progress notifications and SSE keepalives. This module only wires
FastAPI, registers tools and handles the peer's inbound callbacks.
"""

from __future__ import annotations

import asyncio
import time
import urllib.parse
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.context import Context
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.transport_security import TransportSecuritySettings

from . import __version__
from .store import Config, Store


class AdminTokenMiddleware:
    """Pure ASGI guard; a BaseHTTPMiddleware would buffer the SSE stream."""

    def __init__(self, app, token: str):
        self.app = app
        self.token = token

    async def __call__(self, scope, receive, send):
        if self.token and scope["type"] == "http" and scope["path"].startswith("/mcp"):
            headers = dict(scope["headers"])
            if headers.get(b"authorization") != ("Bearer " + self.token).encode():
                await JSONResponse({"error": "unauthorized"}, status_code=401)(
                    scope, receive, send)
                return
        await self.app(scope, receive, send)


def create_app(config: Config | None = None,
               http_client: httpx.AsyncClient | None = None) -> FastAPI:
    cfg = config or Config()
    store = Store(cfg, http_client)
    mcp = MCPServer("agent2grokbot", version=__version__,
                    instructions="Dispatch tasks to registered webhook peers.")

    # ------------------------------------------------------------------ tools

    @mcp.tool()
    async def peer_add(name: str, url: str, secret: str,
                       peer_id: str | None = None) -> dict:
        """Register a webhook peer. Returns the new peer id."""
        try:
            return {"peer_id": await store.peer_add(name, url, secret, peer_id)}
        except ValueError as exc:
            raise ToolError(str(exc)) from exc

    @mcp.tool()
    async def peer_get(peer_id: str) -> dict:
        """Read one peer and its lanes. Secrets are never returned."""
        peer = await store.peer_get(peer_id)
        return peer or {"error": "unknown peer"}

    @mcp.tool()
    async def peer_list() -> list[dict]:
        """List all peers and their lanes. Secrets are never returned."""
        return await store.peer_list()

    @mcp.tool()
    async def peer_update(peer_id: str, name: str | None = None,
                          enabled: bool | None = None,
                          add_lanes: list[dict] | None = None,
                          remove_lanes: list[str] | None = None) -> dict:
        """Update a peer's name, enabled flag, or webhook lanes."""
        patch: dict[str, Any] = {}
        if name is not None:
            patch["name"] = name
        if enabled is not None:
            patch["enabled"] = enabled
        if add_lanes:
            patch["add_lanes"] = add_lanes
        if remove_lanes:
            patch["remove_lanes"] = remove_lanes
        try:
            return await store.peer_update(peer_id, patch)
        except (ValueError, KeyError) as exc:
            raise ToolError(str(exc)) from exc

    @mcp.tool()
    async def peer_delete(peer_id: str) -> dict:
        """Delete a peer. Refuses while tasks are in flight."""
        try:
            return {"deleted": await store.peer_delete(peer_id)}
        except ValueError as exc:
            raise ToolError(str(exc)) from exc

    @mcp.tool()
    async def task_send(peer_id: str, prompt: str, ctx: Context,
                        session_label: str | None = None,
                        heartbeat_interval_s: int | None = None) -> dict:
        """Dispatch a task to a peer and stream its result. Returns a task handle if the deadline is reached."""
        try:
            tid, _ = await store.task_create(peer_id, prompt, session_label,
                                             heartbeat_interval_s)
        except ValueError as exc:
            raise ToolError(str(exc)) from exc
        mailbox = store.subscribe(tid)
        await store.dispatch_queued(peer_id)
        deadline = time.monotonic() + cfg.sse_max_s
        try:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return await store.handle(tid)
                try:
                    event = await asyncio.wait_for(mailbox.get(),
                                                   timeout=min(0.5, remaining))
                except asyncio.TimeoutError:
                    continue
                kind = event["type"]
                if kind == "progress":
                    # No-op when the client did not supply a progressToken.
                    await ctx.report_progress(1, 1, event.get("text", ""))
                    continue
                if kind in ("heartbeat", "dispatched"):
                    continue
                if event.get("is_error"):
                    raise ToolError(str(event.get("error") or kind))
                return {"task_id": tid, "status": kind, "result": event.get("result")}
        finally:
            store.unsubscribe(tid, mailbox)

    @mcp.tool()
    async def task_get(task_id: str) -> dict:
        """Read the current status and result of a task."""
        return await store.task_get(task_id) or {"error": "unknown task"}

    @mcp.tool()
    async def task_list(peer_id: str | None = None,
                        status: str | None = None) -> list[dict]:
        """List tasks, optionally filtered by peer_id or status."""
        return await store.task_list(peer_id, status)

    @mcp.tool()
    async def task_cancel(task_id: str) -> dict:
        """Cancel a task, tell the peer to delete its ScheduleRoutine, release the lane."""
        return await store.task_cancel(task_id)

    # --------------------------------------------------------------- transport

    parsed = urllib.parse.urlparse(cfg.public_base)
    host = parsed.hostname or "127.0.0.1"
    security = TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=["127.0.0.1:*", "localhost:*", "[::1]:*", "%s:*" % host],
        allowed_origins=["http://127.0.0.1:*", "http://localhost:*",
                         "http://[::1]:*", "%s://%s:*" % (parsed.scheme, host)],
    )
    mcp_app = mcp.streamable_http_app(
        streamable_http_path="/mcp", transport_security=security)
    session_manager = mcp._lowlevel_server._session_manager

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await store.open()
        async with session_manager.run():
            try:
                yield
            finally:
                await store.close()

    app = FastAPI(title="agent2grokbot", version=__version__, lifespan=lifespan)
    app.state.store = store
    if cfg.admin_token:
        app.add_middleware(AdminTokenMiddleware, token=cfg.admin_token)

    @app.get("/health")
    async def health() -> dict:
        return await store.stats()

    @app.post("/cb/{task_id}")
    async def callback(task_id: str, request: Request) -> JSONResponse:
        raw = await request.body()
        code, payload = await store.callback(task_id, dict(request.headers), raw)
        return JSONResponse(status_code=code, content=payload)

    app.mount("/", mcp_app)
    return app
