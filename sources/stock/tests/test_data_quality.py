"""Data-quality guards and market-time handling for book-stock-data."""
from __future__ import annotations

import contextlib
import csv
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from book_stock import config, ingest, lake, quality
from book_stock.fsutil import atomic_append_csv, atomic_write_csv

EVENT_TIME = "2026-08-01T12:00:00Z"


def _quote(symbol: str, price, **extra) -> dict:
    return {
        "symbol": symbol,
        "price": price,
        "prev_close": 100.0,
        "change": 1.0,
        "change_pct": 1.0,
        "currency": "USD",
        "exchange": "NMS",
        "timestamp": "2026-08-01 16:00:00",
        **extra,
    }


def _chart(meta: dict) -> dict:
    return {"chart": {"result": [{"meta": meta}], "error": None}}


class QuoteFromChartTests(unittest.TestCase):
    def test_market_time_is_rendered_in_utc(self):
        # 2026-08-01T20:00:00Z; a local-time render would shift it by the host offset.
        quote = ingest.quote_from_chart(
            "AAPL",
            _chart({"regularMarketPrice": 101.0, "chartPreviousClose": 100.0,
                    "regularMarketTime": 1785614400, "currency": "USD"}),
        )
        self.assertEqual(quote["timestamp"], "2026-08-01 20:00:00")
        self.assertEqual(quote["change"], 1.0)
        self.assertEqual(quote["change_pct"], 1.0)

    def test_unusable_payloads_yield_empty_quote(self):
        for payload in (
            {},
            {"chart": None},
            {"chart": {"result": None}},
            {"chart": {"result": ["x"]}},
            _chart({}),
            _chart({"regularMarketPrice": None}),
            _chart({"regularMarketPrice": float("nan")}),
            _chart({"regularMarketPrice": 0}),
            _chart({"regularMarketPrice": -3}),
            [],
        ):
            with self.subTest(payload=payload):
                self.assertEqual(ingest.quote_from_chart("AAPL", payload), {})

    def test_bad_previous_close_gives_zero_change(self):
        quote = ingest.quote_from_chart(
            "AAPL", _chart({"regularMarketPrice": 5, "chartPreviousClose": float("inf")})
        )
        self.assertEqual((quote["prev_close"], quote["change"], quote["change_pct"]), (0, 0, 0))
        self.assertEqual(quote["timestamp"], "")


class QuoteRecordQualityTests(unittest.TestCase):
    def test_rejects_invalid_price_duplicates_and_blank_symbol(self):
        quotes = [
            _quote("aapl", 190.5, change_pct=float("nan")),
            _quote("AAPL", 191.0),  # duplicate after upper-casing
            _quote("MSFT", float("inf")),
            _quote("NVDA", "n/a"),
            _quote("TSLA", 0),
            _quote("", 10.0),
            _quote("AMD", "150.25"),
        ]
        records, rejected = lake.quote_records_with_report(quotes, event_time=EVENT_TIME)
        self.assertEqual([r["id"] for r in records], ["AAPL", "AMD"])
        self.assertEqual(records[0]["price"], 190.5)
        self.assertEqual(records[0]["change_pct"], "")
        self.assertEqual(
            quality.summarize_rejections(rejected),
            {"duplicate_id": 1, "invalid_price": 3, "missing_symbol": 1},
        )
        self.assertEqual(lake.quote_records_from_api(quotes, event_time=EVENT_TIME), records)

    def test_clean_quotes_matches_lake_filter(self):
        kept, rejected = ingest.clean_quotes(
            [_quote("AAPL", 1.0), _quote("AAPL", 2.0), _quote("MSFT", -1)]
        )
        self.assertEqual([(q["symbol"], q["price"]) for q in kept], [("AAPL", 1.0)])
        self.assertEqual(len(rejected), 2)


@unittest.skipUnless(
    lake.shared_runtime_available(),
    "shared data_lake runtime not installed (pip install -e .[lake])",
)
class StaleMarketTimeTests(unittest.TestCase):
    """Market-closed captures keep the newest market time and report staleness."""

    def test_older_market_time_does_not_replace_newer_price(self):
        from book_stock.store import load_records, seed_lake_from_quotes

        with tempfile.TemporaryDirectory() as tmp:
            uri = str(Path(tmp) / "lake")
            seed_lake_from_quotes(
                [_quote("AAPL", 190.5, timestamp="2026-08-01 20:00:00")], data_lake_uri=uri
            )
            # A later capture that returns an older market time (e.g. a
            # stale cache over a weekend) must not win the latest selection.
            seed_lake_from_quotes(
                [_quote("AAPL", 150.0, timestamp="2026-07-31 20:00:00")],
                data_lake_uri=uri,
                raw=json.dumps({"second": True}).encode(),
            )
            with mock.patch.object(config, "DATA_LAKE_URI", uri), \
                    mock.patch.object(config, "STALE_AFTER_HOURS", 24.0):
                payload = load_records()
        self.assertEqual(len(payload["items"]), 1)
        item = payload["items"][0]
        self.assertEqual(float(item["price"]), 190.5)
        self.assertEqual(item["event_time"], "2026-08-01T20:00:00Z")
        # The fixed 2026-08-01 market time is far older than 24h at test time.
        self.assertEqual(payload["data_status"], "stale")


class CliValidationTests(unittest.TestCase):
    def _exit_code(self, argv: list[str]) -> int:
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as ctx:
            ingest.main(argv)
        return ctx.exception.code

    def test_rejects_empty_symbols_and_bad_threshold(self):
        self.assertEqual(self._exit_code(["--symbols", " , "]), 2)
        self.assertEqual(self._exit_code(["--alert-threshold", "inf"]), 2)
        self.assertEqual(self._exit_code(["--alert-threshold", "-2"]), 2)

    def test_symbols_are_uppercased_and_deduplicated(self):
        with mock.patch.object(ingest, "run_live_ingest") as run:
            self.assertEqual(ingest.main(["--symbols", "aapl, AAPL,msft"]), 0)
        self.assertEqual(run.call_args.kwargs["symbols"], ["AAPL", "MSFT"])


class AtomicWriteTests(unittest.TestCase):
    def test_projection_rewrites_and_appends_without_temp_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            with contextlib.redirect_stdout(io.StringIO()):
                ingest.project_history_csv([_quote("AAPL", 1.0)], out)
                ingest.project_history_csv([_quote("MSFT", 2.0)], out)
                ingest.project_prices_csv([_quote("AAPL", 1.0)], out)
            with (out / "stock_history.csv").open(newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            self.assertEqual([r["symbol"] for r in rows], ["AAPL", "MSFT"])
            self.assertEqual(sorted(p.name for p in out.iterdir()), ["stock_history.csv", "stock_prices.csv"])

    def test_failed_write_keeps_previous_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "out.csv"
            atomic_write_csv(path, ["a"], [{"a": 1}])
            with self.assertRaises(ValueError):
                atomic_append_csv(path, ["a"], [{"a": 2, "unexpected": 3}])
            self.assertEqual(path.read_bytes(), b"a\r\n1\r\n")
            self.assertEqual([p.name for p in Path(tmp).iterdir()], ["out.csv"])


if __name__ == "__main__":
    unittest.main()
