"""Lake-first tests for book-defi-data (shared adapter + pool identity)."""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from book_defi import config, lake
from book_defi.ingest import run_live_ingest
from book_defi.store import load_records, seed_lake_from_pools


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
        "category": "Lending",
        "url": "https://example.com/compound",
    },
]



def _shared_adapter_available() -> bool:
    return lake.shared_runtime_available()


class IdentityAndDedupeTests(unittest.TestCase):
    def test_stable_pool_id_prefers_native(self):
        pid = lake.stable_pool_id({"pool": "abc-123", "chain": "Ethereum"})
        self.assertEqual(pid, "abc-123")

    def test_stable_pool_id_fallback_hash(self):
        pid = lake.stable_pool_id(
            {"chain": "Arbitrum", "project": "gmx", "symbol": "USDC"}
        )
        self.assertTrue(pid.startswith("hash:"))
        # Same inputs → same id
        again = lake.stable_pool_id(
            {"chain": "Arbitrum", "project": "gmx", "symbol": "USDC"}
        )
        self.assertEqual(pid, again)

    @unittest.skipUnless(
        _shared_adapter_available(),
        "shared data_lake runtime not installed (pip install -e .[lake])",
    )
    def test_dedupe_same_pool_in_batch(self):
        pools = [
            {**SAMPLE_POOLS[0], "apy": 5.0},
            {**SAMPLE_POOLS[0], "apy": 9.9},  # same pool, later wins
            SAMPLE_POOLS[1],
        ]
        records = lake.yield_records_from_pools(pools, dedupe=True)
        self.assertEqual(len(records), 2)
        aave = next(r for r in records if r["pool_id"] == "pool-aave-usdc")
        self.assertEqual(float(aave["apy"]), 9.9)
        for r in records:
            self.assertEqual(r["id"], r["pool_id"])
            self.assertTrue(r["lineage_chain"])
            self.assertTrue(r["lineage_protocol"])
            self.assertIn("event_time", r)


@unittest.skipUnless(
    _shared_adapter_available(),
    "shared data_lake runtime not installed (pip install -e .[lake])",
)
class LakeFirstOrderingTests(unittest.TestCase):
    def test_csv_not_written_when_lake_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            raw = json.dumps({"data": SAMPLE_POOLS}).encode("utf-8")

            def boom(**_k):
                raise lake.LakeIngestError("simulated lake failure")

            with mock.patch.object(lake, "ingest_to_lake", side_effect=boom):
                with mock.patch(
                    "book_defi.ingest.fetch_pools_raw",
                    return_value=(raw, SAMPLE_POOLS),
                ):
                    with mock.patch(
                        "book_defi.ingest.filter_pools",
                        return_value=SAMPLE_POOLS,
                    ):
                        with self.assertRaises(lake.LakeIngestError):
                            run_live_ingest(
                                chains=["Ethereum"],
                                min_apy=0,
                                output_dir=out,
                                data_lake_uri=str(out / "lake"),
                                write_history=False,
                                project_csv=True,
                            )
            self.assertFalse((out / "defi_yields.csv").exists())

    def test_csv_after_lake(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            raw = json.dumps({"data": SAMPLE_POOLS}).encode("utf-8")
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
                }

            with mock.patch.object(lake, "ingest_to_lake", side_effect=fake_ingest):
                with mock.patch.object(lake, "write_lineage", return_value=out / "l.json"):
                    with mock.patch(
                        "book_defi.ingest.fetch_pools_raw",
                        return_value=(raw, SAMPLE_POOLS),
                    ):
                        with mock.patch(
                            "book_defi.ingest.filter_pools",
                            return_value=SAMPLE_POOLS,
                        ):
                            with mock.patch(
                                "book_defi.ingest.project_yields_csv",
                                side_effect=lambda *_a, **_k: order.append("csv")
                                or (out / "defi_yields.csv"),
                            ):
                                run_live_ingest(
                                    chains=["Ethereum"],
                                    min_apy=0,
                                    output_dir=out,
                                    data_lake_uri=str(out / "lake"),
                                    write_history=False,
                                    project_csv=True,
                                )
            self.assertEqual(order[0], f"lake:{config.LAKE_DATASET_YIELDS}")
            self.assertIn("csv", order)
            self.assertLess(order.index(f"lake:{config.LAKE_DATASET_YIELDS}"), order.index("csv"))


@unittest.skipUnless(
    lake.shared_runtime_available(),
    "shared data_lake runtime not installed (pip install -e .[lake])",
)
class RealLakeIngestTests(unittest.TestCase):
    def test_ingest_lineage_and_replay(self):
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            self.skipTest("pyarrow not installed")

        with tempfile.TemporaryDirectory() as tmp:
            lake_uri = str(Path(tmp) / "lake")
            out = Path(tmp) / "proj"
            out.mkdir()
            raw = json.dumps({"data": SAMPLE_POOLS}, separators=(",", ":")).encode("utf-8")
            records = lake.yield_records_from_pools(SAMPLE_POOLS)
            result = lake.ingest_to_lake(
                raw=raw,
                records=records,
                dataset=config.LAKE_DATASET_YIELDS,
                data_lake_uri=lake_uri,
            )
            self.assertEqual(result["status"], "success")
            self.assertEqual(result["record_count"], 2)
            self.assertIn("defi_yields", result["bronze_key"])
            self.assertEqual(
                lake.landing_object_bytes(result["raw_key"], data_lake_uri=lake_uri),
                raw,
            )
            # Replay twice → same bytes (idempotent landing)
            again = lake.landing_object_bytes(result["raw_key"], data_lake_uri=lake_uri)
            self.assertEqual(again, raw)
            lineage = json.loads(
                lake.write_lineage(
                    result, dataset=config.LAKE_DATASET_YIELDS, data_dir=out
                ).read_text(encoding="utf-8")
            )
            self.assertEqual(lineage["source"], "book-defi-data")
            self.assertEqual(lineage["domain"], "market")

    def test_dedupe_then_latest_snapshot(self):
        try:
            import duckdb  # noqa: F401
            import pyarrow  # noqa: F401
        except ImportError:
            self.skipTest("deps missing")

        with tempfile.TemporaryDirectory() as tmp:
            lake_uri = str(Path(tmp) / "lake")
            pools = [
                {**SAMPLE_POOLS[0], "apy": 1.0},
                {**SAMPLE_POOLS[0], "apy": 8.0},
            ]
            seed_lake_from_pools(pools, data_lake_uri=lake_uri)
            with mock.patch.object(config, "DATA_LAKE_URI", lake_uri):
                payload = load_records()
            aave = [i for i in payload["items"] if i["pool_id"] == "pool-aave-usdc"]
            self.assertEqual(len(aave), 1)
            self.assertEqual(float(aave[0]["apy"]), 8.0)

    def test_replay_normalized_records_stable(self):
        """Same pools → same record ids (replay identity)."""
        a = lake.yield_records_from_pools(SAMPLE_POOLS)
        b = lake.yield_records_from_pools(SAMPLE_POOLS)
        self.assertEqual([r["id"] for r in a], [r["id"] for r in b])
        self.assertEqual([r["pool_id"] for r in a], [r["pool_id"] for r in b])


class SharedAdapterTests(unittest.TestCase):
    @unittest.skipUnless(
        lake.shared_runtime_available(),
        "shared data_lake runtime not installed (pip install -e .[lake])",
    )
    def test_contract(self):
        # The product delegates to the shared adapter (installed [lake] extra or SOLO_DATA_LAKE_ROOT).
        self.assertTrue(hasattr(lake._pa_mod(), "LakeProductContract"))
        self.assertEqual(config.LAKE_SOURCE, "book-defi-data")
        self.assertEqual(config.LAKE_DOMAIN, "market")
        self.assertEqual(config.LAKE_DATASET_YIELDS, "defi_yields")
        self.assertEqual(config.LAKE_DATASET_HISTORY, "defi_yields_history")
        self.assertEqual(config.SCHEMA_VERSION, "defi.v1")


if __name__ == "__main__":
    src = Path(__file__).resolve().parents[1] / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))
    unittest.main()
