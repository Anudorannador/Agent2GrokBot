"""Async test harness: in-process FastAPI app plus a mocked Grok Bot webhook."""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
import time

import httpx

from agent2grokbot import create_app
from agent2grokbot.store import Config, sign

BASE = "http://127.0.0.1:7583"
V2 = "2026-07-28"
LEGACY = "2025-11-25"
META = {
    "io.modelcontextprotocol/protocolVersion": V2,
    "io.modelcontextprotocol/clientInfo": {"name": "test", "version": "1"},
    "io.modelcontextprotocol/clientCapabilities": {},
}


def headers(method: str, name: str | None = None) -> dict:
    hdrs = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "MCP-Protocol-Version": V2,
        "Mcp-Method": method,
    }
    if name:
        hdrs["Mcp-Name"] = name
    return hdrs


def result_text(parsed: dict | None) -> str:
    return parsed["result"]["content"][0]["text"] if parsed else ""


def result_json(parsed: dict | None) -> dict:
    return json.loads(result_text(parsed))


def parse_response(response: httpx.Response) -> dict | None:
    """Read either a JSON response or the final SSE data frame."""
    if "text/event-stream" not in response.headers.get("content-type", ""):
        return json.loads(response.text) if response.text else None
    result = None
    for line in response.text.splitlines():
        if line.startswith("data: "):
            obj = json.loads(line[6:])
            if "result" in obj or "error" in obj:
                result = obj
    return result


class Harness:
    """A full app whose outbound webhook calls are handled in-process."""

    def __init__(self, *, mode="ok", work=0.25, heartbeat=0.05, sse_max_s=5,
                 miss_threshold=2, heartbeat_interval_s=1, admin_token="",
                 bot_key="bot-key"):
        self.mode = mode
        self.work = work
        self.heartbeat = heartbeat
        self.bot_key = bot_key
        self.received: list[dict] = []
        self.cancelled: list[str] = []
        self._pending: set[asyncio.Task] = set()
        self.config = Config(
            db=os.path.join(tempfile.mkdtemp(prefix="a2g-"), "test.db"),
            public_base=BASE,
            admin_token=admin_token,
            master_secret="test-master-secret",
            heartbeat_interval_s=heartbeat_interval_s,
            miss_threshold=miss_threshold,
            sse_max_s=sse_max_s,
            allow_private_urls=True,
        )
        self.bot_client = httpx.AsyncClient(
            transport=httpx.MockTransport(self._bot_handler))
        self.app = create_app(self.config, http_client=self.bot_client)
        self.store = self.app.state.store
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app), base_url=BASE)
        self._lifespan = None

    async def _bot_handler(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        if request.headers.get("Authorization") != "Bearer " + self.bot_key:
            return httpx.Response(401, json={"error": "bad key"})
        self.received.append(body)
        if body.get("event") == "task.cancel":
            self.cancelled.append(body.get("task_id"))
            return httpx.Response(200, json={"accepted": True})
        task = asyncio.create_task(self._work(body))
        self._pending.add(task)
        task.add_done_callback(self._pending.discard)
        return httpx.Response(200, json={"accepted": True})

    async def _callback(self, task: dict, obj: dict):
        cb = task["callback"]
        raw = json.dumps(obj).encode()
        await self.client.post(cb["url"], content=raw, headers={
            "Content-Type": "application/json",
            "Authorization": cb["headers"]["Authorization"],
            "X-A2G-Signature": sign(cb["token"], raw),
        })

    async def _work(self, task: dict):
        tid = task["task_id"]
        started = time.monotonic()
        tick = 0
        while time.monotonic() - started < self.work:
            await asyncio.sleep(self.heartbeat)
            tick += 1
            await self._callback(task, {"type": "heartbeat", "task_id": tid,
                                        "event_id": "%s-hb-%d" % (tid, tick),
                                        "ts": int(time.time())})
        if self.mode == "progress":
            await self._callback(task, {"type": "progress", "task_id": tid,
                                        "event_id": tid + "-p", "text": "halfway"})
        if self.mode in ("ok", "progress"):
            await self._callback(task, {"type": "result", "task_id": tid,
                                        "event_id": tid + "-r",
                                        "result": "done: " + task["prompt"]})
        elif self.mode == "fail":
            await self._callback(task, {"type": "failed", "task_id": tid,
                                        "event_id": tid + "-f", "error": "bot failed"})
        # mode "silent" deliberately sends no terminal callback.

    async def __aenter__(self):
        self._lifespan = self.app.router.lifespan_context(self.app)
        await self._lifespan.__aenter__()
        return self

    async def __aexit__(self, *exc):
        for task in list(self._pending):
            task.cancel()
        self._pending.clear()
        await self.client.aclose()
        await self._lifespan.__aexit__(*exc)
        await self.bot_client.aclose()

    async def rpc(self, method: str, params: dict | None = None,
                  rpc_id: int = 1, extra_headers: dict | None = None,
                  version: str | None = V2) -> httpx.Response:
        payload = {"jsonrpc": "2.0", "id": rpc_id, "method": method}
        if params is not None:
            payload["params"] = params
        hdrs = headers(method, (params or {}).get("name"))
        if version is None:
            hdrs.pop("MCP-Protocol-Version", None)
            hdrs.pop("Mcp-Method", None)
        elif version != V2:
            hdrs["MCP-Protocol-Version"] = version
        hdrs.update(extra_headers or {})
        return await self.client.post("/mcp", json=payload, headers=hdrs)

    async def call_tool(self, name: str, arguments: dict | None = None,
                        progress_token: str | None = None,
                        rpc_id: int = 1) -> tuple[httpx.Response, dict | None]:
        meta = dict(META)
        if progress_token:
            meta["progressToken"] = progress_token
        response = await self.rpc("tools/call", {
            "name": name, "arguments": arguments or {}, "_meta": meta}, rpc_id=rpc_id)
        return response, parse_response(response)

    async def add_peer(self, lanes: int = 1, peer_id: str = "grok") -> str:
        pid = await self.store.peer_add("Grok", "http://127.0.0.1:9/hook",
                                        self.bot_key, peer_id)
        if lanes > 1:
            await self.store.peer_update(pid, {"add_lanes": [
                {"url": "http://127.0.0.1:9/hook", "secret": self.bot_key}
                for _ in range(lanes - 1)]})
        return pid

    async def wait_until(self, predicate, timeout: float = 5.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if await predicate():
                return True
            await asyncio.sleep(0.02)
        return False
