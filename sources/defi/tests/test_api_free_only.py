"""Offline tests for book-defi-data free-only API + policy."""
from __future__ import annotations

import json
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest import mock
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from book_defi import config, lake, policy
from book_defi.api import DataProductHandler
from book_defi.http_client import UpstreamError, request_json
from book_defi.store import (
    get_record,
    load_csv_projection,
    load_history,
    load_records,
    seed_fixtures,
    seed_lake_from_pools,
)


FIXTURE_ROOT = Path(__file__).resolve().parents[1] / "fixtures"

SAMPLE_POOLS = [
    {
        "pool": "pool-aave-usdc",
        "chain": "Ethereum",
        "project": "aave-v3",
        "symbol": "USDC",
        "tvlUsd": 1000000,
        "apy": 5.5,
        "apyBase": 5.0,
        "apyReward": 0.5,
        "rewardTokens": [],
        "category": "Lending",
        "url": "https://example.com/aave",
    },
    {
        "pool": "pool-compound-dai",
        "chain": "Ethereum",
        "project": "compound",
        "symbol": "DAI",
        "tvlUsd": 500000,
        "apy": 4.2,
        "apyBase": 4.2,
        "apyReward": 0,
        "rewardTokens": [],
        "category": "Lending",
        "url": "https://example.com/compound",
    },
]


def _set_data_dir(path: Path):
    return mock.patch.object(config, "DATA_DIR", path)


def _set_lake_uri(uri: str):
    return mock.patch.object(config, "DATA_LAKE_URI", uri)


def _set_read_mode(mode: str, fallback: str = "error"):
    return mock.patch.multiple(
        config,
        LAKE_READ_MODE=mode,
        LAKE_READ_FALLBACK=fallback,
    )


def _lake_deps_available() -> bool:
    try:
        import duckdb  # noqa: F401
        import pyarrow  # noqa: F401
    except ImportError:
        return False
    return lake.find_solo_empire_root() is not None


@unittest.skipUnless(_lake_deps_available(), "pyarrow/duckdb + monorepo data_lake required")
class StoreLakeTests(unittest.TestCase):
    def test_bronze_records_load(self):
        with tempfile.TemporaryDirectory() as tmp:
            lake_uri = str(Path(tmp) / "lake")
            seed_lake_from_pools(SAMPLE_POOLS, data_lake_uri=lake_uri)
            with _set_lake_uri(lake_uri):
                payload = load_records()
            self.assertEqual(payload["source_kind"], "bronze_parquet")
            self.assertGreaterEqual(len(payload["items"]), 1)
            item = payload["items"][0]
            self.assertTrue(item.get("lineage_chain") or item.get("chain"))
            self.assertTrue(item.get("lineage_protocol") or item.get("protocol"))

    def test_iceberg_and_parquet_reads_have_same_items(self):
        with tempfile.TemporaryDirectory() as tmp:
            lake_uri = str(Path(tmp) / "lake")
            seed_lake_from_pools(SAMPLE_POOLS, data_lake_uri=lake_uri)

            def read_catalog_rows(contract, dataset, *, data_lake_uri=None, sql=None):
                return lake.read_bronze_rows(
                    dataset, data_lake_uri=data_lake_uri, sql=sql
                )

            with _set_lake_uri(lake_uri), _set_read_mode("parquet"):
                expected_yields = load_records()
                expected_history = load_history()
            with _set_lake_uri(lake_uri), _set_read_mode("iceberg"), mock.patch(
                "data_lake.product_store.read_iceberg_rows",
                side_effect=read_catalog_rows,
            ):
                actual_yields = load_records()
                actual_history = load_history()

            self.assertEqual(actual_yields["items"], expected_yields["items"])
            self.assertEqual(actual_history["items"], expected_history["items"])
            self.assertEqual(actual_yields["source_kind"], "iceberg_rest_duckdb")
            self.assertEqual(actual_history["source_kind"], "iceberg_rest_duckdb")
            self.assertEqual(actual_yields["read_mode"], "iceberg")

    def test_iceberg_mode_fallback_is_explicit(self):
        with tempfile.TemporaryDirectory() as tmp:
            lake_uri = str(Path(tmp) / "lake")
            seed_lake_from_pools(SAMPLE_POOLS, data_lake_uri=lake_uri)
            with _set_lake_uri(lake_uri), _set_read_mode("iceberg", "parquet"), mock.patch(
                "data_lake.product_store.read_iceberg_rows",
                side_effect=lake.LakeUnavailable("catalog unavailable"),
            ):
                payload = load_records()

            self.assertGreaterEqual(len(payload["items"]), 1)
            self.assertEqual(payload["source_kind"], "bronze_parquet_fallback")
            self.assertIn("catalog unavailable", payload["fallback_reason"])

    def test_iceberg_mode_fails_closed_by_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            lake_uri = str(Path(tmp) / "lake")
            seed_lake_from_pools(SAMPLE_POOLS, data_lake_uri=lake_uri)
            with _set_lake_uri(lake_uri), _set_read_mode("iceberg", "error"), mock.patch(
                "data_lake.product_store.read_iceberg_rows",
                side_effect=lake.LakeUnavailable("catalog credentials unavailable"),
            ):
                payload = load_records()

            self.assertEqual(payload["data_status"], "error")
            self.assertEqual(payload["items"], [])
            self.assertEqual(payload["source_kind"], "iceberg_rest_duckdb")
            self.assertIn("catalog credentials unavailable", payload["error"])

    def test_csv_does_not_feed_load_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            data_dir = root / "data"
            lake_uri = str(root / "empty-lake")
            Path(lake_uri).mkdir()
            with mock.patch.object(config, "FIXTURES_DIR", FIXTURE_ROOT):
                seed_fixtures(data_dir)
            self.assertGreaterEqual(
                len(load_csv_projection(kind="records", data_dir=data_dir)["items"]), 1
            )
            with _set_lake_uri(lake_uri), _set_data_dir(data_dir):
                payload = load_records(data_dir)
            self.assertEqual(payload["items"], [])

    def test_history_dataset(self):
        with tempfile.TemporaryDirectory() as tmp:
            lake_uri = str(Path(tmp) / "lake")
            seed_lake_from_pools(SAMPLE_POOLS, data_lake_uri=lake_uri)
            with _set_lake_uri(lake_uri):
                hist = load_history()
            self.assertIn("defi_yields_history", hist["path"])
            self.assertGreaterEqual(len(hist["items"]), 1)

    def test_invalid_record_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            lake_uri = str(Path(tmp) / "lake")
            seed_lake_from_pools(SAMPLE_POOLS, data_lake_uri=lake_uri)
            with _set_lake_uri(lake_uri):
                self.assertIsNone(get_record("no-such-pool"))


@unittest.skipUnless(_lake_deps_available(), "pyarrow/duckdb + monorepo data_lake required")
class CsvProjectionOnlyTests(unittest.TestCase):
    def test_csv_projection_helper(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            with mock.patch.object(config, "FIXTURES_DIR", FIXTURE_ROOT):
                seed_fixtures(data_dir)
            payload = load_csv_projection(kind="records", data_dir=data_dir)
            self.assertEqual(payload["source_kind"], "csv_projection")


class PolicyTests(unittest.TestCase):
    def test_defillama_public_allowed(self):
        self.assertTrue(policy.evaluate_provider("defillama_public").allowed)

    def test_pro_blocked(self):
        self.assertFalse(policy.evaluate_provider("defillama_pro").allowed)

    def test_external_writes_blocked(self):
        with mock.patch.object(config, "FREE_ONLY", True), mock.patch.object(
            config, "ALLOW_EXTERNAL_WRITES", False
        ):
            with self.assertRaises(PermissionError):
                policy.require_external_writes("telegram")


class HttpClientTests(unittest.TestCase):
    def test_blocks_pro(self):
        with self.assertRaises(PermissionError):
            request_json("GET", "https://example.invalid", provider="defillama_pro")

    def test_timeout(self):
        class Boom:
            def request(self, *args, **kwargs):
                raise TimeoutError("timed out")

        fake = mock.Mock()
        fake.request = Boom().request
        with mock.patch("book_defi.http_client.httpx", fake), mock.patch.object(
            config, "MAX_RETRIES", 0
        ):
            with self.assertRaises(UpstreamError) as ctx:
                request_json(
                    "GET",
                    "https://example.invalid/timeout",
                    provider="defillama_public",
                    sleep=lambda _: None,
                )
            self.assertEqual(ctx.exception.kind, "timeout")


@unittest.skipUnless(_lake_deps_available(), "pyarrow/duckdb + monorepo data_lake required")
class ApiContractTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.data_dir = root / "data"
        self.data_dir.mkdir()
        self.lake_uri = str(root / "lake")
        seed_lake_from_pools(
            SAMPLE_POOLS, data_lake_uri=self.lake_uri, data_dir=self.data_dir
        )
        self.assertFalse((self.data_dir / config.RECORDS_FILE).exists())
        self.data_patch = _set_data_dir(self.data_dir)
        self.lake_patch = _set_lake_uri(self.lake_uri)
        self.read_patch = _set_read_mode("parquet", "error")
        self.data_patch.start()
        self.lake_patch.start()
        self.read_patch.start()
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), DataProductHandler)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.lake_patch.stop()
        self.data_patch.stop()
        self.read_patch.stop()
        self.tmp.cleanup()

    def _get(self, path: str):
        try:
            with urlopen(f"http://127.0.0.1:{self.port}{path}", timeout=5) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except HTTPError as e:
            return e.code, json.loads(e.read().decode("utf-8"))

    def _post(self, path: str):
        req = Request(f"http://127.0.0.1:{self.port}{path}", data=b"{}", method="POST")
        req.add_header("Content-Type", "application/json")
        try:
            with urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except HTTPError as e:
            return e.code, json.loads(e.read().decode("utf-8"))

    def test_metadata_lake_first(self):
        status, body = self._get("/v1/metadata")
        self.assertEqual(status, 200)
        meta = body["items"][0]
        self.assertEqual(meta["storage_model"], "lake_first_bronze_duckdb")
        self.assertEqual(meta["records_source_kind"], "bronze_parquet")
        self.assertEqual(meta["shared_adapter"], "data_lake.product_adapter")
        self.assertEqual(meta["lake_dataset_yields"], "defi_yields")
        self.assertEqual(meta["read_mode"], "parquet")
        self.assertEqual(meta["read_fallback"], "error")

    def test_metadata_reports_iceberg_mode(self):
        def read_catalog_rows(contract, dataset, *, data_lake_uri=None, sql=None):
            return lake.read_bronze_rows(
                dataset, data_lake_uri=data_lake_uri, sql=sql
            )

        with _set_read_mode("iceberg"), mock.patch(
            "data_lake.product_store.read_iceberg_rows",
            side_effect=read_catalog_rows,
        ):
            status, body = self._get("/v1/metadata")

        self.assertEqual(status, 200)
        meta = body["items"][0]
        self.assertEqual(meta["storage_model"], "lake_first_iceberg_rest_duckdb")
        self.assertEqual(meta["records_source_kind"], "iceberg_rest_duckdb")
        self.assertEqual(meta["history_source_kind"], "iceberg_rest_duckdb")
        self.assertEqual(meta["read_mode"], "iceberg")

    def test_api_serves_without_csv(self):
        self.assertFalse(any(self.data_dir.glob("*.csv")))
        status, body = self._get("/v1/records")
        self.assertEqual(status, 200)
        self.assertGreaterEqual(len(body["items"]), 1)
        (self.data_dir / config.RECORDS_FILE).write_text(
            "pool_id,chain,project,symbol,tvl_usd,apy,apy_base,apy_reward,reward_tokens,category,url,scraped_at\n"
            "FAKE,Ethereum,fake,FAKE,1,1,1,0,,x,https://x,2099-01-01 00:00:00\n",
            encoding="utf-8",
        )
        status, body = self._get("/v1/records")
        pools = {item["pool_id"] for item in body["items"]}
        self.assertIn("pool-aave-usdc", pools)
        self.assertNotIn("FAKE", pools)

    def test_refresh_forbidden(self):
        status, body = self._post("/v1/refresh")
        self.assertEqual(status, 403)


class SmokeCompileTests(unittest.TestCase):
    def test_modules_import(self):
        from book_defi import api, config as cfg, store

        self.assertTrue(cfg.FREE_ONLY)
        self.assertEqual(cfg.LAKE_DOMAIN, "market")
        self.assertEqual(cfg.LAKE_DATASET_YIELDS, "defi_yields")
        self.assertEqual(cfg.SCHEMA_VERSION, "defi.v1")
        self.assertEqual(int(cfg.API_PORT), 8104)
        self.assertEqual(cfg.LAKE_READ_MODE, "parquet")
        self.assertEqual(cfg.LAKE_READ_FALLBACK, "error")
        self.assertTrue(hasattr(store, "load_csv_projection"))
        self.assertTrue(hasattr(api, "main"))


if __name__ == "__main__":
    unittest.main()
