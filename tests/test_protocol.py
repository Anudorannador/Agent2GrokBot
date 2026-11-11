"""MCP 2.0 wire shapes, SDK-owned version errors and the legacy handshake."""

import unittest

from . import support


class ProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def test_discover_shape(self):
        async with support.Harness() as h:
            response = await h.rpc("server/discover", {"_meta": support.META})
            self.assertEqual(response.status_code, 200)
            result = support.parse_response(response)["result"]
            self.assertEqual(result["resultType"], "complete")
            self.assertIn("2026-07-28", result["supportedVersions"])
            self.assertEqual(
                result["_meta"]["io.modelcontextprotocol/serverInfo"]["name"],
                "agent2grokbot")
            self.assertIn("capabilities", result)
            self.assertIn("ttlMs", result)
            self.assertIn("cacheScope", result)

    async def test_tools_list_and_generated_schemas(self):
        async with support.Harness() as h:
            response = await h.rpc("tools/list", {"_meta": support.META})
            result = support.parse_response(response)["result"]
            self.assertEqual(result["resultType"], "complete")
            tools = {t["name"]: t for t in result["tools"]}
            self.assertEqual(set(tools), {
                "peer_add", "peer_get", "peer_list", "peer_update", "peer_delete",
                "task_send", "task_get", "task_list", "task_cancel",
                "contract_get", "contract_status", "contract_broadcast"})
            schema = tools["task_send"]["inputSchema"]["properties"]
            self.assertIn("peer_id", schema)
            self.assertIn("prompt", schema)
            self.assertIn("heartbeat_interval_s", schema)
            self.assertIn("attachment_ids", schema)
            self.assertNotIn("ctx", schema, "Context is injected, not advertised")
            for name in ("peer_add", "peer_update"):
                self.assertIn("capabilities",
                              tools[name]["inputSchema"]["properties"])

    async def test_capabilities_are_writable_and_discoverable_over_mcp(self):
        text = "just for testing, ask anything and following your way"
        async with support.Harness() as h:
            _, added = await h.call_tool("peer_add", {
                "name": "Grok", "url": "http://127.0.0.1:9/hook",
                "secret": h.bot_key, "peer_id": "grokbot",
                "capabilities": text})
            self.assertEqual(support.result_json(added)["peer_id"], "grokbot")

            _, listed = await h.call_tool("peer_list")
            payload = support.result_json(listed)
            peers = payload if isinstance(payload, list) else [payload]
            self.assertEqual(peers[0]["capabilities"], text)

            _, updated = await h.call_tool("peer_update", {
                "peer_id": "grokbot", "capabilities": "market research only"})
            self.assertEqual(support.result_json(updated)["capabilities"],
                             "market research only")

    async def test_tools_call_is_complete(self):
        async with support.Harness() as h:
            response, parsed = await h.call_tool("peer_list")
            self.assertEqual(response.status_code, 200)
            self.assertEqual(parsed["result"]["resultType"], "complete")
            self.assertFalse(parsed["result"]["isError"])

    async def test_unsupported_version(self):
        async with support.Harness() as h:
            meta = dict(support.META,
                        **{"io.modelcontextprotocol/protocolVersion": "1999-01-01"})
            response = await h.rpc("tools/list", {"_meta": meta},
                                   version="1999-01-01")
            self.assertEqual(response.status_code, 400)
            error = support.parse_response(response)["error"]
            self.assertEqual(error["code"], -32022)
            self.assertEqual(error["message"], "Unsupported protocol version")
            self.assertIn("2026-07-28", error["data"]["supported"])

    async def test_header_mismatch(self):
        async with support.Harness() as h:
            response = await h.rpc("tools/list", {"_meta": support.META},
                                   extra_headers={"Mcp-Method": "server/discover"})
            self.assertEqual(response.status_code, 400)
            self.assertEqual(support.parse_response(response)["error"]["code"], -32020)

    async def test_missing_meta_envelope(self):
        async with support.Harness() as h:
            response = await h.rpc("tools/list", {})
            self.assertEqual(response.status_code, 400)
            self.assertEqual(support.parse_response(response)["error"]["code"], -32602)

    async def test_unknown_method(self):
        async with support.Harness() as h:
            response = await h.rpc("does/not/exist", {"_meta": support.META})
            self.assertEqual(response.status_code, 404)
            self.assertEqual(support.parse_response(response)["error"]["code"], -32601)

    async def test_legacy_initialize_never_claims_v2(self):
        async with support.Harness() as h:
            response = await h.rpc("initialize", {
                "protocolVersion": "2025-11-25", "capabilities": {},
                "clientInfo": {"name": "legacy", "version": "1"}}, version=None)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(
                support.parse_response(response)["result"]["protocolVersion"],
                "2025-11-25")

    async def test_health(self):
        async with support.Harness() as h:
            response = await h.client.get("/health")
            self.assertEqual(response.status_code, 200)
            self.assertTrue(response.json()["ok"])

    async def test_extra_allowed_host_for_tailscale(self):
        h = support.Harness(allowed_hosts="100.64.0.9:*")
        async with h:
            response = await h.rpc("tools/list", {"_meta": support.META},
                                  extra_headers={"Host": "100.64.0.9:7583"})
            self.assertEqual(response.status_code, 200)

    async def test_admin_token_guard(self):
        async with support.Harness(admin_token="test-admin") as h:
            denied = await h.rpc("tools/list", {"_meta": support.META})
            self.assertEqual(denied.status_code, 401)
            allowed = await h.rpc("tools/list", {"_meta": support.META},
                                  extra_headers={"Authorization": "Bearer test-admin"})
            self.assertEqual(allowed.status_code, 200)


if __name__ == "__main__":
    unittest.main()
