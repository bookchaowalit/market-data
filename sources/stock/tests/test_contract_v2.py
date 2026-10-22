"""Offline check of book-stock-data against data-product-contracts-v2.yaml (frozen 2026-08-04).

The registry lives in the private architecture-docs repo, so the frozen values this product
must meet are copied here: port 8102, schema_version stock.v1, the GET endpoints, the
envelope keys and the allowed data_status values. The API runs in-process on a loopback
ephemeral port over a temporary Bronze lake seeded from the offline sample (no network).
"""

from __future__ import annotations

import json
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest import mock
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

from test_api_free_only import SAMPLE_QUOTES, _lake_deps_available

from book_stock import config
from book_stock.api import DataProductHandler
from book_stock.store import seed_lake_from_quotes

V2_PORT = 8102
V2_SCHEMA = "stock.v1"
V2_ENVELOPE_KEYS = (
    "schema_version",
    "source",
    "retrieved_at",
    "data_status",
    "items",
    "next_cursor",
)
V2_DATA_STATUS = {
    "ok",
    "empty",
    "stale",
    "malformed",
    "not_found",
    "forbidden",
    "error",
    "accepted",
}
V2_POLICY_DEFAULTS = {
    "FREE_ONLY": True,
    "ALLOW_PAID_PROVIDERS": False,
    "ALLOW_EXTERNAL_WRITES": False,
    "ALLOW_REFRESH": False,
}


class FrozenMapTest(unittest.TestCase):
    def test_port_schema_and_loopback_host(self) -> None:
        self.assertEqual(int(config.API_PORT), V2_PORT)
        self.assertEqual(config.SCHEMA_VERSION, V2_SCHEMA)
        self.assertEqual(config.API_HOST, "127.0.0.1")

    def test_policy_defaults(self) -> None:
        for name, value in V2_POLICY_DEFAULTS.items():
            self.assertIs(getattr(config, name), value, name)


@unittest.skipUnless(
    _lake_deps_available(), "shared data_lake runtime required (uv sync --all-extras)"
)
class EnvelopeContractTest(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        lake_uri = str(Path(tmp.name) / "lake")
        seed_lake_from_quotes(SAMPLE_QUOTES, data_lake_uri=lake_uri)
        for patcher in (
            mock.patch.object(config, "DATA_LAKE_URI", lake_uri),
            mock.patch.object(config, "DATA_DIR", Path(tmp.name)),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)
        server = ThreadingHTTPServer(("127.0.0.1", 0), DataProductHandler)
        self.addCleanup(server.server_close)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.shutdown)
        self.base = f"http://127.0.0.1:{server.server_address[1]}"

    def call(self, path: str, method: str = "GET") -> tuple[int, dict]:
        request = Request(self.base + path, method=method)
        try:
            with urlopen(request, timeout=10) as response:
                return response.status, json.loads(response.read())
        except HTTPError as error:
            return error.code, json.loads(error.read())

    def assert_envelope(self, body: dict) -> None:
        for key in V2_ENVELOPE_KEYS:
            self.assertIn(key, body)
        self.assertEqual(body["schema_version"], V2_SCHEMA)
        self.assertEqual(body["source"], config.REPO_NAME)
        self.assertIn(body["data_status"], V2_DATA_STATUS)
        self.assertIsInstance(body["items"], list)

    def test_required_get_endpoints_return_v2_envelopes(self) -> None:
        status, health = self.call("/healthz")
        self.assertEqual(status, 200)
        self.assertIn(health["data_status"], V2_DATA_STATUS)
        for path in ("/v1/metadata", "/v1/records", "/v1/history"):
            status, body = self.call(path)
            self.assertEqual(status, 200, path)
            self.assert_envelope(body)
        status, records = self.call("/v1/records")
        self.assertGreater(len(records["items"]), 0)
        record_id = records["items"][0]["record_id"]
        status, one = self.call("/v1/records/" + quote(str(record_id), safe=""))
        self.assertEqual(status, 200)
        self.assert_envelope(one)
        self.assertEqual(one["items"][0]["record_id"], record_id)

    def test_unknown_record_and_refresh_use_v2_statuses(self) -> None:
        status, missing = self.call("/v1/records/no-such-record")
        self.assertEqual(status, 404)
        self.assert_envelope(missing)
        self.assertEqual(missing["data_status"], "not_found")
        status, refresh = self.call("/v1/refresh", method="POST")
        self.assertEqual(status, 403)
        self.assert_envelope(refresh)
        self.assertEqual(refresh["data_status"], "forbidden")


if __name__ == "__main__":
    unittest.main()
