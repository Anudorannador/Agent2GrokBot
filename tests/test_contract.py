"""Contract channel: registry semantics, webhook deltas, ack and broadcast."""

from __future__ import annotations

import json
import unittest
import urllib.parse

from agent2grokbot import contract
from agent2grokbot.store import sign

from . import support


class ContractRegistryTest(unittest.TestCase):
    def test_current_is_the_last_entry(self):
        self.assertEqual(contract.current_version(), contract.CONTRACT[-1].version)

    def test_versions_order_numerically_not_lexically(self):
        self.assertTrue(contract.is_newer("0.10.0", "0.9.0"))
        self.assertFalse(contract.is_newer("0.9.0", "0.10.0"))

    def test_unknown_version_owes_every_change(self):
        self.assertEqual(len(contract.since(None)), len(contract.CONTRACT))
        self.assertTrue(contract.is_behind(None))

    def test_current_version_owes_nothing(self):
        self.assertEqual(contract.since(contract.current_version()), [])
        self.assertFalse(contract.is_behind(contract.current_version()))

    def test_every_release_carries_prompt_text(self):
        for item in contract.CONTRACT:
            self.assertTrue(item.prompt_patch.strip(), item.version)
            self.assertTrue(item.summary.strip(), item.version)

    def test_removals_are_recorded_not_silent(self):
        baseline = [item for item in contract.CONTRACT if item.version == "0.4.0"][0]
        self.assertTrue(baseline.removed, "0.4.0 dropped inline base64 and must say so")

    def test_delta_shape(self):
        value = contract.delta("0.3.0")
        self.assertEqual(value["your_version"], "0.3.0")
        self.assertFalse(value["up_to_date"])
        self.assertEqual([item["version"] for item in value["changes"]],
                         ["0.4.0", "0.5.0"])
        self.assertIn("blob_upload", value["prompt_patch"])


class ContractFlowTest(unittest.IsolatedAsyncioTestCase):
    async def _post_callback(self, harness, task_id: str, body: dict):
        row = await harness.store.task_row(task_id)
        raw = json.dumps(body).encode()
        return await harness.client.post("/cb/%s" % task_id, content=raw, headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer " + row.callback_token,
            "X-A2G-Signature": sign(row.callback_token, raw),
        })

    async def _received(self, harness, count: int = 1) -> bool:
        return len(harness.received) >= count

    async def _has_contract(self, harness) -> bool:
        return len(harness.contracts) >= 1

    async def test_first_dispatch_carries_the_delta(self):
        async with support.Harness() as h:
            await h.add_peer()
            await h.call_tool("task_send", {"peer_id": "grok", "prompt": "hi"})
            self.assertTrue(await h.wait_until(lambda: self._received(h)))
            created = h.received[0]
            self.assertEqual(created["protocol_version"], contract.current_version())
            block = created["contract"]
            self.assertFalse(block["up_to_date"])
            self.assertIn("ack_url", block)
            self.assertIn("prompt_patch", block)
            self.assertTrue(block["changes"])

    async def test_contract_block_disappears_once_acknowledged(self):
        async with support.Harness() as h:
            peer_id = await h.add_peer()
            await h.call_tool("task_send", {"peer_id": "grok", "prompt": "one"})
            self.assertTrue(await h.wait_until(lambda: self._received(h)))
            self.assertIn("contract", h.received[0])

            await h.store.record_protocol(peer_id, contract.current_version())
            status = await h.store.contract_status()
            self.assertTrue(status["peers"][0]["up_to_date"])

            await h.call_tool("task_send", {"peer_id": "grok", "prompt": "two"}, rpc_id=2)
            self.assertTrue(await h.wait_until(lambda: self._received(h, 2)))
            self.assertNotIn("contract", h.received[1])

    async def test_heartbeat_version_echo_advances_the_peer(self):
        async with support.Harness(mode="silent") as h:
            peer_id = await h.add_peer()
            # Dispatch directly: task_send would block for the full SSE window.
            task_id, _ = await h.store.task_create("grok", "slow", None, None)
            await h.store.dispatch_queued("grok")
            self.assertTrue(await h.wait_until(lambda: self._received(h)))

            await self._post_callback(h, task_id, {
                "type": "heartbeat", "task_id": task_id, "event_id": "hb-1",
                "protocol_version": "0.3.0"})
            status = await h.store.contract_status()
            self.assertEqual(status["peers"][0]["protocol_version"], "0.3.0")
            self.assertFalse(status["peers"][0]["up_to_date"])

            await self._post_callback(h, task_id, {
                "type": "heartbeat", "task_id": task_id, "event_id": "hb-2",
                "protocol_version": contract.current_version()})
            status = await h.store.contract_status()
            self.assertEqual(status["peers"][0]["protocol_version"],
                             contract.current_version())
            self.assertTrue(status["peers"][0]["up_to_date"])
            self.assertEqual(status["peers"][0]["peer_id"], peer_id)

    async def test_stale_version_echo_never_rewinds(self):
        async with support.Harness(mode="silent") as h:
            peer_id = await h.add_peer()
            await h.store.record_protocol(peer_id, contract.current_version())
            advanced = await h.store.record_protocol(peer_id, "0.3.0")
            self.assertFalse(advanced)
            status = await h.store.contract_status()
            self.assertEqual(status["peers"][0]["protocol_version"],
                             contract.current_version())

    async def test_ack_endpoint_accepts_a_signed_ticket(self):
        async with support.Harness() as h:
            peer_id = await h.add_peer()
            ack_url = h.store.contract_ack_url(peer_id)
            parsed = urllib.parse.urlparse(ack_url)
            query = urllib.parse.parse_qs(parsed.query)
            response = await h.client.post(parsed.path, params={
                "peer_id": peer_id, "expires": query["expires"][0],
                "ticket": query["ticket"][0]},
                content=json.dumps({"protocol_version": contract.current_version()}))
            self.assertEqual(response.status_code, 200, response.text)
            self.assertTrue(response.json()["up_to_date"])

    async def test_ack_endpoint_rejects_a_forged_ticket(self):
        async with support.Harness() as h:
            peer_id = await h.add_peer()
            ack_url = h.store.contract_ack_url(peer_id)
            query = urllib.parse.parse_qs(urllib.parse.urlparse(ack_url).query)
            response = await h.client.post("/v1/contract/ack", params={
                "peer_id": peer_id, "expires": query["expires"][0],
                "ticket": "0" * 64})
            self.assertEqual(response.status_code, 401)

    async def test_ack_never_rewinds_a_newer_record(self):
        async with support.Harness() as h:
            peer_id = await h.add_peer()
            await h.store.record_protocol(peer_id, contract.current_version())
            ack_url = h.store.contract_ack_url(peer_id)
            parsed = urllib.parse.urlparse(ack_url)
            query = urllib.parse.parse_qs(parsed.query)
            response = await h.client.post(parsed.path, params={
                "peer_id": peer_id, "expires": query["expires"][0],
                "ticket": query["ticket"][0]},
                content=json.dumps({"protocol_version": "0.3.0"}))
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["protocol_version"],
                             contract.current_version())
            self.assertTrue(response.json()["up_to_date"])

    async def test_version_keys_treat_padding_as_equal(self):
        # "0.5" and "0.5.0" are the same release, so neither outranks the other.
        self.assertFalse(contract.is_newer("0.5.0", "0.5"))
        self.assertFalse(contract.is_newer("0.5", "0.5.0"))

    async def test_broadcast_dry_run_sends_nothing(self):
        async with support.Harness() as h:
            await h.add_peer()
            value = await h.store.contract_broadcast(dry_run=True)
            self.assertTrue(value["dry_run"])
            self.assertFalse(value["peers"][0]["sent"])
            self.assertIn("would_send", value["peers"][0])
            self.assertEqual(h.received, [])

    async def test_broadcast_pushes_contract_updated(self):
        async with support.Harness() as h:
            await h.add_peer()
            value = await h.store.contract_broadcast()
            self.assertTrue(value["peers"][0]["sent"])
            self.assertTrue(await h.wait_until(lambda: self._has_contract(h)))
            event = h.contracts[0]
            self.assertEqual(event["event"], "contract.updated")
            self.assertEqual(event["protocol_version"], contract.current_version())
            self.assertIn("ack_url", event["contract"])
            self.assertIn("not a task", event["instructions"])

    async def test_contract_webhook_omits_the_task_header(self):
        async with support.Harness() as h:
            await h.add_peer()
            await h.store.contract_broadcast()
            self.assertTrue(await h.wait_until(lambda: self._has_contract(h)))
            sent = h.received_headers[0]
            self.assertNotIn("x-a2g-task-id", {k.lower() for k in sent})
            self.assertIn("x-a2g-signature", {k.lower() for k in sent})

    async def test_tools_are_exposed_over_mcp(self):
        async with support.Harness() as h:
            await h.add_peer()
            response = await h.rpc("tools/list", {"_meta": support.META})
            names = {tool["name"] for tool in
                     support.parse_response(response)["result"]["tools"]}
            self.assertLessEqual(
                {"contract_get", "contract_status", "contract_broadcast"}, names)

            _, parsed = await h.call_tool("contract_get", {"version": "0.3.0"})
            self.assertFalse(support.result_json(parsed)["up_to_date"])

            _, parsed = await h.call_tool(
                "contract_get", {"full": True}, rpc_id=2)
            self.assertEqual(len(support.result_json(parsed)["history"]),
                             len(contract.CONTRACT))

            _, parsed = await h.call_tool(
                "contract_broadcast", {"dry_run": True}, rpc_id=3)
            self.assertTrue(support.result_json(parsed)["dry_run"])


if __name__ == "__main__":
    unittest.main()
