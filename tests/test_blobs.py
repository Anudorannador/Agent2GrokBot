"""Binary attachment transport: streaming, authentication and MCP resource links."""

import asyncio
import hashlib
import json
import os
import unittest

from sqlmodel import update

from agent2grokbot.store import Task, sign

from . import support


class BlobTests(unittest.IsolatedAsyncioTestCase):
    async def test_round_trip_uses_references_and_mcp_resource_links(self):
        h = support.Harness(mode="silent", work=5.0, heartbeat=0.05, sse_max_s=5)
        async with h:
            pid = await h.add_peer()
            source = b"audio bytes that never enter webhook JSON"
            uploaded = await h.client.post(
                "/v1/blobs?filename=input.mp3", content=source,
                headers={"Content-Type": "audio/mpeg"})
            self.assertEqual(uploaded.status_code, 201, uploaded.text)
            input_id = uploaded.json()["blob_id"]
            self.assertEqual(uploaded.json()["sha256"], hashlib.sha256(source).hexdigest())
            self.assertTrue(os.path.isfile(os.path.join(h.store.blob_root, input_id)))
            self.assertEqual(os.path.dirname(h.store.blob_root),
                             os.path.dirname(os.path.abspath(h.config.db)))

            pending = asyncio.create_task(h.call_tool("task_send", {
                "peer_id": pid, "prompt": "transcribe", "attachment_ids": [input_id]}))

            async def dispatched():
                return any(item.get("event") == "task.created" for item in h.received)

            self.assertTrue(await h.wait_until(dispatched))
            created = next(item for item in h.received
                           if item.get("event") == "task.created")
            tid = created["task_id"]
            raw = json.dumps(created).encode()
            self.assertNotIn(source, raw)
            self.assertEqual(len(created["attachments"]), 1)
            attachment = created["attachments"][0]
            self.assertEqual(attachment["blob_id"], input_id)
            self.assertEqual(attachment["mime_type"], "audio/mpeg")
            downloaded = await h.client.get(attachment["url"])
            self.assertEqual(downloaded.status_code, 200)
            self.assertEqual(downloaded.content, source)

            output = b"generated audio"
            digest = hashlib.sha256(output).hexdigest()
            token = created["callback"]["token"]
            signature = sign(token, ("%s.%s" % (tid, digest)).encode())
            produced = await h.client.post(
                "/v1/tasks/%s/blobs?filename=result.mp3" % tid, content=output,
                headers={
                    "Content-Type": "audio/mpeg",
                    "Authorization": "Bearer " + token,
                    "X-A2G-SHA256": digest,
                    "X-A2G-Signature": signature,
                })
            self.assertEqual(produced.status_code, 201, produced.text)
            output_id = produced.json()["blob_id"]
            await h._callback(created, {
                "type": "result", "task_id": tid, "event_id": tid + "-result",
                "result": {"text": "done", "attachments": [{"blob_id": output_id}]},
            })

            _, parsed = await pending
            payload = support.result_json(parsed)
            self.assertEqual(payload["status"], "completed")
            self.assertEqual(payload["result"]["text"], "done")
            result_attachment = payload["result"]["attachments"][0]
            self.assertEqual(result_attachment["blob_id"], output_id)
            links = [item for item in parsed["result"]["content"]
                     if item["type"] == "resource_link"]
            self.assertEqual(len(links), 1)
            self.assertEqual(links[0]["mimeType"], "audio/mpeg")
            self.assertEqual(links[0]["uri"], result_attachment["url"])
            final = await h.client.get(links[0]["uri"])
            self.assertEqual(final.status_code, 200)
            self.assertEqual(final.content, output)

            _, fetched = await h.call_tool("task_get", {"task_id": tid})
            refreshed = support.result_json(fetched)["result"]["attachments"][0]
            self.assertEqual(refreshed["blob_id"], output_id)
            self.assertIn("ticket=", refreshed["url"])

    async def test_upload_limit_and_admin_authentication(self):
        h = support.Harness(admin_token="admin-secret")
        h.config.blob_max_bytes = 4
        async with h:
            denied = await h.client.post("/v1/blobs", content=b"1234")
            self.assertEqual(denied.status_code, 401)
            headers = {"Authorization": "Bearer admin-secret"}
            accepted = await h.client.post("/v1/blobs", content=b"1234", headers=headers)
            self.assertEqual(accepted.status_code, 201)
            too_large = await h.client.post("/v1/blobs", content=b"12345", headers=headers)
            self.assertEqual(too_large.status_code, 413)
            self.assertEqual(len(os.listdir(h.store.blob_root)), 1)

    async def test_result_rejects_blob_from_another_task(self):
        h = support.Harness(mode="silent", work=5.0, heartbeat=0.2)
        async with h:
            pid = await h.add_peer()
            first, first_token = await h.store.task_create(pid, "first", None, None)
            second, _ = await h.store.task_create(pid, "second", None, None)
            await h.store.x(update(Task).where(Task.task_id == first).values(
                status="running"))
            data = b"private output"
            digest = hashlib.sha256(data).hexdigest()
            response = await h.client.post(
                "/v1/tasks/%s/blobs" % first, content=data,
                headers={"Authorization": "Bearer " + first_token,
                         "X-A2G-SHA256": digest,
                         "X-A2G-Signature": sign(first_token,
                                                ("%s.%s" % (first, digest)).encode())})
            self.assertEqual(response.status_code, 201, response.text)
            blob_id = response.json()["blob_id"]
            row = await h.store.task_row(second)
            raw = json.dumps({"type": "result", "task_id": second, "event_id": "bad",
                              "result": {"attachments": [blob_id]}}).encode()
            rejected = await h.client.post("/cb/%s" % second, content=raw, headers={
                "Authorization": "Bearer " + row.callback_token,
                "X-A2G-Signature": sign(row.callback_token, raw)})
            self.assertEqual(rejected.status_code, 400)
            self.assertEqual((await h.store.task_get(second))["status"], "queued")


if __name__ == "__main__":
    unittest.main()
