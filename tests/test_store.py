"""Async state, security, lane allocation and callback lifecycle."""

import json
import unittest
from datetime import datetime, timedelta, timezone

from sqlmodel import select, update

from agent2grokbot.store import (MAX_HEARTBEAT_S, PeerLane, Task,
                                 next_heartbeat_interval, now, sign, verify)

from . import support


def callback_headers(row, body, signed=True):
    raw = json.dumps(body).encode()
    token = row.callback_token
    return {
        "Authorization": "Bearer " + token,
        "Content-Type": "application/json",
        "X-A2G-Signature": sign(token, raw) if signed else "t=0,v1=00",
    }, raw


class StoreTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def harness():
        return support.Harness(mode="silent", work=2.0, heartbeat=0.2,
                               miss_threshold=100)

    async def test_peer_crud_masks_secret(self):
        async with self.harness() as h:
            store = h.store
            pid = await store.peer_add("Grok", "http://127.0.0.1:9/hook",
                                       "top-secret", "grok-main")
            self.assertEqual(pid, "grok-main")
            peer = await store.peer_get(pid)
            self.assertEqual(peer["name"], "Grok")
            self.assertNotIn("secret", peer["lanes"][0])
            self.assertNotIn("top-secret", json.dumps(peer))
            self.assertTrue(peer["lanes"][0]["has_secret"])

            updated = await store.peer_update(pid, {
                "name": "Grok 2", "enabled": False,
                "add_lanes": [{"url": "http://127.0.0.1:10/hook", "secret": "second"}],
            })
            self.assertEqual(len(updated["lanes"]), 2)
            self.assertFalse(updated["enabled"])
            self.assertEqual(len(await store.peer_list()), 1)

            lane = updated["lanes"][1]["lane_id"]
            await store.peer_update(pid, {"remove_lanes": [lane]})
            self.assertEqual(len((await store.peer_get(pid))["lanes"]), 1)
            self.assertTrue(await store.peer_delete(pid))
            self.assertEqual(await store.peer_list(), [])

    async def test_peer_delete_refuses_in_flight(self):
        async with self.harness() as h:
            pid = await h.store.peer_add("Grok", "http://127.0.0.1:9/hook", "k")
            await h.store.task_create(pid, "work", None, None)
            with self.assertRaises(ValueError):
                await h.store.peer_delete(pid)

    async def test_signature_roundtrip_and_tamper(self):
        raw = b'{"type":"result"}'
        header = sign("s3cret", raw)
        self.assertTrue(verify("s3cret", raw, header))
        self.assertFalse(verify("s3cret", raw + b" ", header))
        self.assertFalse(verify("wrong", raw, header))
        self.assertFalse(verify("s3cret", raw, None))

    async def test_next_heartbeat_interval_parsing(self):
        at = 1000.0
        self.assertEqual(next_heartbeat_interval({"next_heartbeat_s": 900}, {}, at), 900)
        self.assertEqual(next_heartbeat_interval({"next_heartbeat_at": at + 600}, {}, at), 600)
        self.assertEqual(next_heartbeat_interval(
            {}, {"x-a2g-next-heartbeat-s": "45"}, at), 45)
        self.assertEqual(next_heartbeat_interval(
            {}, {"x-a2g-next-heartbeat-at": str(at + 120)}, at), 120)
        self.assertEqual(next_heartbeat_interval(
            {"next_heartbeat_s": 10}, {"x-a2g-next-heartbeat-s": "45"}, at), 10)
        self.assertEqual(next_heartbeat_interval(
            {"next_heartbeat_s": 999999}, {}, at), MAX_HEARTBEAT_S)
        self.assertEqual(next_heartbeat_interval(
            {"next_heartbeat_at": at - 500}, {}, at), 1)
        self.assertIsNone(next_heartbeat_interval({}, {}, at))
        self.assertIsNone(next_heartbeat_interval({"next_heartbeat_s": "bad"}, {}, at))
        self.assertIsNone(next_heartbeat_interval(
            {"next_heartbeat_s": float("nan")}, {}, at))

    async def test_peer_can_reschedule_next_heartbeat(self):
        h = support.Harness(mode="silent", work=2.0, heartbeat=0.2,
                            miss_threshold=2, heartbeat_interval_s=5)
        async with h:
            store = h.store
            pid = await store.peer_add("Grok", "http://127.0.0.1:9/hook", "k")
            tid, _ = await store.task_create(pid, "batch fix", None, 5)
            await store.x(update(Task).where(Task.task_id == tid).values(
                status="running", last_seen_at=now()))
            row = await store.task_row(tid)

            body = {"type": "heartbeat", "task_id": tid, "event_id": "hb-1",
                    "next_heartbeat_s": 1800}
            hdrs, raw = callback_headers(row, body)
            code, payload = await store.callback(tid, hdrs, raw)
            self.assertEqual(code, 200)
            self.assertEqual(payload["next_heartbeat_s"], 1800)
            self.assertEqual((await store.task_get(tid))["heartbeat_interval_s"], 1800)
            self.assertEqual(await store.sweep_once(), 0,
                             "a long batch must not be marked stale")

            hdrs2, raw2 = callback_headers(
                row, {"type": "progress", "task_id": tid, "event_id": "p1"})
            hdrs2["X-A2G-Next-Heartbeat-S"] = "60"
            await store.callback(tid, hdrs2, raw2)
            self.assertEqual((await store.task_get(tid))["heartbeat_interval_s"], 60)
            old = (datetime.now(timezone.utc) - timedelta(seconds=600)).isoformat()
            await store.x(update(Task).where(Task.task_id == tid).values(
                last_seen_at=old))
            self.assertEqual(await store.sweep_once(), 1)
            self.assertEqual((await store.task_get(tid))["status"], "stale")

    async def test_callback_token_is_stable_and_task_specific(self):
        async with self.harness() as h:
            a = h.store.callback_token("t-1")
            self.assertEqual(a, h.store.callback_token("t-1"))
            self.assertNotEqual(a, h.store.callback_token("t-2"))

    async def test_event_dedupe(self):
        async with self.harness() as h:
            self.assertTrue(await h.store.event_record("t-1", "e1", "heartbeat", "{}"))
            self.assertFalse(await h.store.event_record("t-1", "e1", "heartbeat", "{}"))
            self.assertTrue(await h.store.event_record("t-2", "e1", "heartbeat", "{}"))

    async def test_callback_auth_dedupe_and_terminal(self):
        async with self.harness() as h:
            store = h.store
            pid = await store.peer_add("Grok", "http://127.0.0.1:9/hook", "k")
            tid, _ = await store.task_create(pid, "work", None, None)
            await store.x(update(Task).where(Task.task_id == tid).values(
                status="running", last_seen_at=now()))
            row = await store.task_row(tid)

            body = {"type": "heartbeat", "task_id": tid, "event_id": "hb-1"}
            hdrs, raw = callback_headers(row, body)
            self.assertEqual((await store.callback(tid, hdrs, raw))[0], 200)
            self.assertTrue((await store.callback(tid, hdrs, raw))[1].get("duplicate"))
            self.assertEqual((await store.task_get(tid))["heartbeat_count"], 1)

            self.assertEqual((await store.callback(
                tid, dict(hdrs, Authorization="Bearer wrong"), raw))[0], 401)
            unsigned, raw2 = callback_headers(
                row, {"type": "progress", "task_id": tid, "event_id": "p1"}, signed=False)
            self.assertEqual((await store.callback(tid, unsigned, raw2))[0], 401)
            mismatch, raw3 = callback_headers(
                row, {"type": "heartbeat", "task_id": "other", "event_id": "x"})
            self.assertEqual((await store.callback(tid, mismatch, raw3))[0], 400)

            done, raw4 = callback_headers(
                row, {"type": "result", "task_id": tid, "event_id": "r1",
                      "result": "the answer"})
            self.assertEqual((await store.callback(tid, done, raw4))[0], 200)
            self.assertEqual((await store.task_get(tid))["status"], "completed")
            self.assertEqual((await store.task_get(tid))["result"], "the answer")
            self.assertEqual((await store.callback(tid, done, raw4))[0], 410)

    async def test_lane_allocation_queue_and_release(self):
        async with self.harness() as h:
            store = h.store
            pid = await h.add_peer(lanes=2)
            ids = [(await store.task_create(pid, "task %d" % i, None, None))[0]
                   for i in range(3)]
            await store.dispatch_queued(pid)
            statuses = [(await store.task_get(t))["status"] for t in ids]
            self.assertEqual(statuses.count("running"), 2, statuses)
            self.assertEqual(statuses.count("queued"), 1, statuses)

            await store.terminal(ids[0], "completed", result="early")
            self.assertTrue(await h.wait_until(
                lambda: self._status(store, ids[2]), timeout=4))
            freed = await store.q(select(PeerLane).where(
                PeerLane.lane_id == (await store.task_get(ids[0]))["lane_id"]))
            self.assertEqual(freed[0].busy_task_id, ids[2])

    @staticmethod
    async def _status(store, tid):
        return (await store.task_get(tid))["status"] == "running"

    async def test_cancel_posts_delete_instruction_and_releases(self):
        async with self.harness() as h:
            pid = await h.add_peer()
            tid, _ = await h.store.task_create(pid, "work", None, None)
            await h.store.dispatch_queued(pid)
            self.assertEqual((await h.store.task_get(tid))["status"], "running")
            result = await h.store.task_cancel(tid)
            self.assertTrue(result["ok"])
            self.assertEqual((await h.store.task_get(tid))["status"], "cancelled")
            self.assertIn(tid, h.cancelled)
            cancel = [m for m in h.received if m.get("event") == "task.cancel"]
            self.assertTrue(cancel)
            self.assertIn("ScheduleRoutine", cancel[0]["instructions"])

    async def test_stale_sweep_uses_task_interval(self):
        async with self.harness() as h:
            pid = await h.store.peer_add("Grok", "http://127.0.0.1:9/hook", "k")
            tid, _ = await h.store.task_create(pid, "work", None, 5)
            old = (datetime.now(timezone.utc) - timedelta(seconds=600)).isoformat()
            await h.store.x(update(Task).where(Task.task_id == tid).values(
                status="running", last_seen_at=old))
            self.assertEqual(await h.store.sweep_once(), 1)
            self.assertEqual((await h.store.task_get(tid))["status"], "stale")
            self.assertIn("no heartbeat", (await h.store.task_get(tid))["error"])

    async def test_health_reports_state(self):
        async with self.harness() as h:
            response = await h.client.get("/health")
            self.assertEqual(response.status_code, 200)
            self.assertTrue(response.json()["ok"])


if __name__ == "__main__":
    unittest.main()
