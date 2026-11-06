"""End-to-end async flows: one-call results, SSE progress, lanes and deadlines."""

import asyncio
import json
import unittest

from sqlmodel import update

from agent2grokbot.store import Task, now, sign

from . import support


class E2ETests(unittest.IsolatedAsyncioTestCase):
    async def make(self, **kwargs):
        h = support.Harness(**kwargs)
        await h.__aenter__()
        return h

    async def test_task_send_returns_result_in_one_call(self):
        h = await self.make(work=0.2, heartbeat=0.04)
        try:
            pid = await h.add_peer()
            response, parsed = await h.call_tool(
                "task_send", {"peer_id": pid, "prompt": "one call"})
            self.assertEqual(response.status_code, 200)
            payload = support.result_json(parsed)
            self.assertEqual(payload["status"], "completed")
            self.assertEqual(payload["result"], "done: one call")
        finally:
            await h.__aexit__(None, None, None)

    async def test_outbound_payload_carries_schedule_routine_instruction(self):
        h = await self.make(work=0.15, heartbeat=0.04)
        try:
            pid = await h.add_peer()
            _, parsed = await h.call_tool("task_send", {
                "peer_id": pid, "prompt": "inspect", "heartbeat_interval_s": 7})
            created = [m for m in h.received if m.get("event") == "task.created"]
            self.assertEqual(len(created), 1)
            payload = created[0]
            self.assertIn("ScheduleRoutine", payload["instructions"])
            self.assertIn("next_heartbeat_s", payload["instructions"])
            self.assertIn("next_heartbeat_at", payload["instructions"])
            self.assertEqual(payload["heartbeat_interval_s"], 7)
            self.assertIn("/cb/%s" % payload["task_id"], payload["callback"]["url"])
            self.assertTrue(payload["callback"]["token"])
            self.assertEqual(support.result_json(parsed)["result"], "done: inspect")
        finally:
            await h.__aexit__(None, None, None)

    async def test_three_concurrent_sessions_get_their_own_result(self):
        h = await self.make(work=0.3, heartbeat=0.05)
        try:
            pid = await h.add_peer(lanes=3)
            results = await asyncio.gather(*[
                h.call_tool("task_send", {
                    "peer_id": pid, "prompt": "job-%d" % i,
                    "session_label": "session-%d" % i}, rpc_id=i)
                for i in range(3)])
            for i, (response, parsed) in enumerate(results):
                self.assertEqual(response.status_code, 200)
                self.assertEqual(support.result_json(parsed)["result"], "done: job-%d" % i)
            ids = {m["task_id"] for m in h.received if m.get("event") == "task.created"}
            self.assertEqual(len(ids), 3)
        finally:
            await h.__aexit__(None, None, None)

    async def test_sse_progress_frame_and_final_response(self):
        h = await self.make(mode="progress", work=0.25, heartbeat=0.05)
        try:
            pid = await h.add_peer()
            response, parsed = await h.call_tool(
                "task_send", {"peer_id": pid, "prompt": "stream"},
                progress_token="p1")
            self.assertEqual(response.status_code, 200)
            self.assertIn("text/event-stream", response.headers.get("content-type", ""))
            frames = [line[6:] for line in response.text.splitlines()
                      if line.startswith("data: ")]
            self.assertTrue(any("notifications/progress" in f and "halfway" in f
                                for f in frames), frames)
            self.assertEqual(parsed["result"]["resultType"], "complete")
            self.assertEqual(support.result_json(parsed)["result"], "done: stream")
        finally:
            await h.__aexit__(None, None, None)

    async def test_deadline_returns_handle_then_task_get_collects_result(self):
        h = await self.make(work=1.0, heartbeat=0.05, sse_max_s=0.3)
        try:
            pid = await h.add_peer()
            _, parsed = await h.call_tool(
                "task_send", {"peer_id": pid, "prompt": "long job"})
            handle = support.result_json(parsed)
            self.assertEqual(handle["status"], "working")
            self.assertIn("pollIntervalMs", handle)
            tid = handle["task_id"]
            self.assertTrue(await h.wait_until(
                lambda: self._completed(h, tid), timeout=6))
            _, final = await h.call_tool("task_get", {"task_id": tid})
            self.assertEqual(support.result_json(final)["result"], "done: long job")
        finally:
            await h.__aexit__(None, None, None)

    async def _completed(self, h, tid):
        return (await h.store.task_get(tid))["status"] == "completed"

    async def test_callback_route_honours_header_reschedule(self):
        h = await self.make(mode="silent", work=2.0, heartbeat=0.2,
                            miss_threshold=2, heartbeat_interval_s=5)
        try:
            pid = await h.add_peer()
            tid, token = await h.store.task_create(pid, "batch fix", None, 5)
            await h.store.x(update(Task).where(Task.task_id == tid).values(
                status="running", last_seen_at=now()))
            raw = json.dumps({"type": "heartbeat", "task_id": tid,
                              "event_id": "hb-1"}).encode()
            response = await h.client.post("/cb/%s" % tid, content=raw, headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer " + token,
                "X-A2G-Signature": sign(token, raw),
                "X-A2G-Next-Heartbeat-S": "1800",
            })
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["next_heartbeat_s"], 1800)
            self.assertEqual((await h.store.task_get(tid))["heartbeat_interval_s"], 1800)
            self.assertEqual(await h.store.sweep_once(), 0)
        finally:
            await h.__aexit__(None, None, None)

    async def test_unreachable_peer_fails_fast(self):
        h = await self.make(bot_key="other-key")
        try:
            pid = await h.store.peer_add("Dead", "http://127.0.0.1:1/hook", "bot-key")
            _, parsed = await h.call_tool("task_send", {"peer_id": pid, "prompt": "hello"})
            self.assertTrue(parsed["result"]["isError"])
            self.assertIn("HTTP 401", support.result_text(parsed))
        finally:
            await h.__aexit__(None, None, None)

    async def test_failed_callback_closes_with_error(self):
        h = await self.make(mode="fail", work=0.15, heartbeat=0.04)
        try:
            pid = await h.add_peer()
            _, parsed = await h.call_tool(
                "task_send", {"peer_id": pid, "prompt": "break it"})
            self.assertTrue(parsed["result"]["isError"])
            self.assertIn("bot failed", support.result_text(parsed))
        finally:
            await h.__aexit__(None, None, None)

    async def test_cancel_tool_closes_in_flight_call(self):
        h = await self.make(mode="silent", work=3.0, heartbeat=0.1)
        try:
            pid = await h.add_peer()
            pending = asyncio.create_task(h.call_tool(
                "task_send", {"peer_id": pid, "prompt": "cancel me"}))
            self.assertTrue(await h.wait_until(
                lambda: self._running(h), timeout=4))
            tasks = await h.store.task_list()
            _, cancelled = await h.call_tool("task_cancel", {"task_id": tasks[0]["task_id"]})
            self.assertTrue(support.result_json(cancelled)["ok"])
            _, parsed = await pending
            self.assertTrue(parsed["result"]["isError"])
            self.assertIn("cancelled", support.result_text(parsed))
        finally:
            await h.__aexit__(None, None, None)

    async def _running(self, h):
        tasks = await h.store.task_list()
        return bool(tasks) and tasks[0]["status"] == "running"


if __name__ == "__main__":
    unittest.main()
