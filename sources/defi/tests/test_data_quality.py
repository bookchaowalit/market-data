"""Data-quality guards for DeFiLlama pools: NaN/inf/negatives never pass."""

from __future__ import annotations

import contextlib
import csv
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from book_defi import ingest, lake, quality
from book_defi.fsutil import atomic_write_csv

EVENT_TIME = "2026-08-01T12:00:00Z"


def _pool(pool_id: str, **extra) -> dict:
    return {
        "pool": pool_id,
        "chain": "Ethereum",
        "project": "aave-v3",
        "symbol": "USDC",
        "tvlUsd": 1_000_000,
        "apy": 8.0,
        "apyBase": 6.0,
        "apyReward": 2.0,
        "category": "lending",
        **extra,
    }


class FilterPoolsTests(unittest.TestCase):
    def test_non_finite_or_non_numeric_metrics_are_excluded(self):
        pools = [
            _pool("ok"),
            _pool("nan-apy", apy=float("nan")),
            _pool("inf-apy", apy=float("inf")),
            _pool("str-apy", apy="high"),
            _pool("nan-tvl", tvlUsd=float("nan")),
            _pool("neg-tvl", tvlUsd=-5_000_000),
            _pool("none-apy", apy=None),
            _pool("too-high", apy=5000),
            "not-a-dict",
        ]
        kept = ingest.filter_pools(pools, ["Ethereum"], 5.0)
        self.assertEqual([p["pool"] for p in kept], ["ok"])

    def test_stablecoin_detection_tolerates_missing_symbol(self):
        pools = [_pool("a", symbol=None), _pool("b", symbol="usdc-weth")]
        self.assertEqual(
            [p["pool"] for p in ingest.detect_stablecoin_pools(pools)], ["b"]
        )

    def test_opportunities_ignore_nan_apy(self):
        opps = ingest.detect_opportunities(
            [_pool("x", apy=float("nan"), tvlUsd=10_000_000)], prev_pools={}
        )
        self.assertEqual(opps, [])


class YieldRecordQualityTests(unittest.TestCase):
    def test_non_finite_metrics_blanked_and_ids_unique(self):
        pools = json.loads(
            '[{"pool": "p1", "chain": "Base", "project": "x", "symbol": "S",'
            ' "tvlUsd": -1, "apy": NaN, "apyBase": Infinity, "apyReward": null},'
            ' {"pool": "p1", "chain": "Base", "project": "x", "symbol": "S",'
            ' "tvlUsd": 2000000, "apy": 7.5, "apyBase": 7.5, "apyReward": 0}]'
        )
        first = lake.yield_records_from_pools(pools[:1], event_time=EVENT_TIME)[0]
        self.assertEqual(
            (first["tvl_usd"], first["apy"], first["apy_base"]), ("", "", "")
        )
        self.assertIsNone(first["apy_reward"])
        records = lake.yield_records_from_pools(pools, event_time=EVENT_TIME)
        self.assertEqual([r["id"] for r in records], ["p1"])
        self.assertEqual(records[0]["apy"], 7.5)  # last occurrence wins

    def test_metric_helper(self):
        self.assertEqual(lake._metric("3.5"), "3.5")
        self.assertEqual(lake._metric(-2), -2)
        self.assertEqual(lake._metric(-2, non_negative=True), "")
        self.assertIsNone(quality.finite_number(float("-inf")))


class CliValidationTests(unittest.TestCase):
    def test_min_apy_must_be_finite_and_non_negative(self):
        for value in ("nan", "inf", "-1"):
            with self.subTest(value=value), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as ctx:
                    ingest.main(["--min-apy", value])
                self.assertEqual(ctx.exception.code, 2)

    def test_chain_and_category_lists_deduplicated(self):
        with mock.patch.object(ingest, "run_live_ingest") as run:
            self.assertEqual(
                ingest.main(
                    ["--chains", "Base, Base,Ethereum", "--categories", "dex,,dex"]
                ),
                0,
            )
        self.assertEqual(run.call_args.kwargs["chains"], ["Base", "Ethereum"])
        self.assertEqual(run.call_args.kwargs["categories"], ["dex"])


class AtomicProjectionTests(unittest.TestCase):
    def test_projection_counts_rows_and_leaves_no_temp_files(self):
        pools = [_pool("p1"), _pool("p1", apy=9.0), _pool("p2")]
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                ingest.project_yields_csv(pools, out)
                ingest.project_history_csv(pools, out)
                ingest.project_history_csv(pools, out)
            self.assertIn("Projected 2 pools", buf.getvalue())
            self.assertEqual(
                sorted(p.name for p in out.iterdir()),
                ["defi_yields.csv", "defi_yields_history.csv"],
            )
            with (out / "defi_yields_history.csv").open(
                newline="", encoding="utf-8"
            ) as f:
                self.assertEqual(len(list(csv.DictReader(f))), 4)

    def test_failed_write_keeps_previous_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "out.csv"
            atomic_write_csv(path, ["a"], [{"a": 1}])
            with self.assertRaises(ValueError):
                atomic_write_csv(path, ["a"], [{"b": 2}])
            self.assertEqual(path.read_bytes(), b"a\r\n1\r\n")
            self.assertEqual([p.name for p in Path(tmp).iterdir()], ["out.csv"])


if __name__ == "__main__":
    unittest.main()
