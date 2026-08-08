"""Lake-first pilot tests for book-stock-data (shared product_adapter)."""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from book_stock import config, lake
from book_stock.ingest import project_prices_csv, run_live_ingest
from book_stock.store import load_records, seed_lake_from_quotes


SAMPLE_QUOTES = [
    {
        "symbol": "AAPL",
        "price": 190.5,
        "prev_close": 189.0,
        "change": 1.5,
        "change_pct": 0.79,
        "currency": "USD",
        "exchange": "NMS",
        "timestamp": "2026-08-01 16:00:00",
    },
    {
        "symbol": "MSFT",
        "price": 420.0,
        "prev_close": 418.0,
        "change": 2.0,
        "change_pct": 0.48,
        "currency": "USD",
        "exchange": "NMS",
        "timestamp": "2026-08-01 16:00:00",
    },
]


def _monorepo_lake_root():
    try:
        return lake.find_solo_empire_root()
    except (ImportError, ModuleNotFoundError):
        return None


@unittest.skipUnless(_monorepo_lake_root() is not None, "shared data_lake adapter not found")
class NormalizeTests(unittest.TestCase):
    def test_quote_records_include_id(self):
        records = lake.quote_records_from_api(SAMPLE_QUOTES)
        self.assertEqual(len(records), 2)
        ids = {r["id"] for r in records}
        self.assertEqual(ids, {"AAPL", "MSFT"})
        for row in records:
            self.assertIn("event_time", row)


@unittest.skipUnless(_monorepo_lake_root() is not None, "shared data_lake adapter not found")
class LakeFirstOrderingTests(unittest.TestCase):
    def test_csv_not_written_when_lake_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            landing = json.dumps({"responses": {}}).encode("utf-8")

            def boom(**_k):
                raise lake.LakeIngestError("simulated lake failure")

            with mock.patch.object(lake, "ingest_to_lake", side_effect=boom):
                with mock.patch(
                    "book_stock.ingest.fetch_quotes_raw",
                    return_value=(landing, SAMPLE_QUOTES, {}),
                ):
                    with self.assertRaises(lake.LakeIngestError):
                        run_live_ingest(
                            symbols=["AAPL", "MSFT"],
                            output_dir=out,
                            write_history=False,
                            project_csv=True,
                        )
            self.assertFalse((out / "stock_prices.csv").exists())

    def test_csv_after_successful_lake(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            landing = json.dumps({"responses": {"AAPL": "{}"}}).encode("utf-8")
            order: list[str] = []

            def fake_ingest(**kwargs):
                order.append(f"lake:{kwargs['dataset']}")
                return {
                    "status": "success",
                    "run_id": "test-run",
                    "record_count": len(kwargs["records"]),
                    "raw_key": "landing/test",
                    "bronze_key": "bronze/test",
                    "manifest_key": "control/test",
                    "data_lake": {"uri": "file:///tmp/lake"},
                }

            def fake_prices(*_a, **_k):
                order.append("csv_prices")
                return out / "stock_prices.csv"

            def fake_history(*_a, **_k):
                order.append("csv_history")
                return out / "stock_history.csv"

            with mock.patch.object(lake, "ingest_to_lake", side_effect=fake_ingest):
                with mock.patch.object(
                    lake, "write_lineage", return_value=out / "lake_lineage.json"
                ):
                    with mock.patch(
                        "book_stock.ingest.fetch_quotes_raw",
                        return_value=(landing, SAMPLE_QUOTES, {}),
                    ):
                        with mock.patch(
                            "book_stock.ingest.project_prices_csv",
                            side_effect=fake_prices,
                        ):
                            with mock.patch(
                                "book_stock.ingest.project_history_csv",
                                side_effect=fake_history,
                            ):
                                run_live_ingest(
                                    symbols=["AAPL", "MSFT"],
                                    output_dir=out,
                                    write_history=True,
                                    project_csv=True,
                                )
            self.assertEqual(
                order,
                [
                    f"lake:{config.LAKE_DATASET_PRICES}",
                    f"lake:{config.LAKE_DATASET_HISTORY}",
                    "csv_prices",
                    "csv_history",
                ],
            )


@unittest.skipUnless(
    _monorepo_lake_root() is not None,
    "Solo Empire monorepo with data_lake not found",
)
class RealLakeIngestTests(unittest.TestCase):
    def test_ingest_landing_bronze_manifest_and_lineage(self):
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            self.skipTest("pyarrow not installed")

        with tempfile.TemporaryDirectory() as tmp:
            lake_uri = str(Path(tmp) / "lake")
            out = Path(tmp) / "projection"
            out.mkdir()
            raw = json.dumps(
                {"provider": "yahoo_public", "responses": {"AAPL": "{}"}},
                separators=(",", ":"),
            ).encode("utf-8")
            records = lake.quote_records_from_api(SAMPLE_QUOTES)
            result = lake.ingest_to_lake(
                raw=raw,
                records=records,
                dataset=config.LAKE_DATASET_PRICES,
                data_lake_uri=lake_uri,
                metadata={"test": True},
            )
            self.assertEqual(result["status"], "success")
            self.assertEqual(result["record_count"], 2)
            self.assertTrue(result["raw_key"].startswith("landing/"))
            self.assertTrue(result["bronze_key"].startswith("bronze/"))
            self.assertIn("stock_prices", result["bronze_key"])
            self.assertTrue(result["manifest_key"].startswith("control/"))

            lake_root = Path(lake_uri)
            self.assertEqual((lake_root / result["raw_key"]).read_bytes(), raw)
            # Lineage pointer
            lineage_path = lake.write_lineage(
                result, dataset=config.LAKE_DATASET_PRICES, data_dir=out
            )
            lineage = json.loads(lineage_path.read_text(encoding="utf-8"))
            self.assertEqual(lineage["source"], config.LAKE_SOURCE)
            self.assertEqual(lineage["domain"], config.LAKE_DOMAIN)
            self.assertIn(config.LAKE_DATASET_PRICES, lineage["datasets"])

    def test_replay_from_landing(self):
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            self.skipTest("pyarrow not installed")

        with tempfile.TemporaryDirectory() as tmp:
            lake_uri = str(Path(tmp) / "lake")
            raw = b'{"provider":"yahoo_public","responses":{"AAPL":"{\\"ok\\":true}"}}'
            records = lake.quote_records_from_api(SAMPLE_QUOTES[:1])
            result = lake.ingest_to_lake(
                raw=raw,
                records=records,
                dataset=config.LAKE_DATASET_PRICES,
                data_lake_uri=lake_uri,
            )
            replayed = lake.landing_object_bytes(
                result["raw_key"], data_lake_uri=lake_uri
            )
            self.assertEqual(replayed, raw)

    def test_api_load_without_csv(self):
        try:
            import duckdb  # noqa: F401
            import pyarrow  # noqa: F401
        except ImportError:
            self.skipTest("duckdb/pyarrow not installed")

        with tempfile.TemporaryDirectory() as tmp:
            lake_uri = str(Path(tmp) / "lake")
            seed_lake_from_quotes(SAMPLE_QUOTES, data_lake_uri=lake_uri)
            with mock.patch.object(config, "DATA_LAKE_URI", lake_uri):
                payload = load_records()
            self.assertEqual(payload["source_kind"], "bronze_parquet")
            self.assertGreaterEqual(len(payload["items"]), 2)
            project_prices_csv(SAMPLE_QUOTES, Path(tmp) / "csv-only")
            # Still bronze-backed
            with mock.patch.object(config, "DATA_LAKE_URI", lake_uri):
                again = load_records()
            self.assertEqual(again["source_kind"], "bronze_parquet")


class SharedAdapterTests(unittest.TestCase):
    def test_uses_shared_product_adapter(self):
        root = _monorepo_lake_root()
        if root is None:
            self.skipTest("not under monorepo")
        self.assertTrue(
            (root / "infra" / "scripts" / "data_lake" / "product_adapter.py").is_file()
        )
        self.assertEqual(config.LAKE_DOMAIN, "market")
        self.assertEqual(config.LAKE_SOURCE, "book-stock-data")


if __name__ == "__main__":
    src = Path(__file__).resolve().parents[1] / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))
    unittest.main()
