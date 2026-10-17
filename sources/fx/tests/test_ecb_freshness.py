"""Deterministic ECB calendar and actual store-boundary regressions; no providers."""

import copy
import json
import importlib.util
import tempfile
import subprocess
import sys
import unittest
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from book_fx import store
from book_fx.freshness import apply_ecb_freshness, expected_rate_date


class ECBFreshnessTests(unittest.TestCase):
    def test_calendar_fixtures(self):
        fixture = Path(__file__).parents[1] / "fixtures" / "ecb_freshness.json"
        for case in json.loads(fixture.read_text()):
            with self.subTest(case=case["name"]):
                self.assertEqual(
                    expected_rate_date(datetime.fromisoformat(case["now"])),
                    date.fromisoformat(case["expected"]),
                )

    def test_requires_aware_clock(self):
        with self.assertRaises(ValueError):
            expected_rate_date(datetime(2026, 4, 7, 16))

    def test_snapshot_checks_every_current_quote_and_preserves_errors(self):
        now = datetime.fromisoformat("2026-04-06T19:00:00+00:00")
        payload = {
            "data_status": "stale",
            "items": [{"date": "2026-04-02"}],
            "latest_timestamp": "2026-04-02T00:00:00Z",
        }
        self.assertEqual(apply_ecb_freshness(payload, now=now)["data_status"], "ok")
        self.assertEqual(payload["latest_timestamp"], "2026-04-02T00:00:00Z")
        self.assertEqual(
            apply_ecb_freshness(
                {**payload, "items": [{"date": "2026-04-01"}, {"date": "2026-04-02"}]},
                now=now,
            )["data_status"],
            "stale",
        )
        for status in ("error", "malformed", "empty"):
            result = apply_ecb_freshness({**payload, "data_status": status}, now=now)
            self.assertEqual(result["data_status"], status)

    def test_invalid_and_future_dates_cannot_be_fresh(self):
        now = datetime.fromisoformat("2026-04-06T19:00:00+00:00")
        for value in (
            None,
            "",
            "garbage",
            "2026-04-31",
            "2026-04-02T00:00:00Z",
            "2026-04-07",
        ):
            with self.subTest(value=value):
                payload = {"data_status": "ok", "items": [{"date": value}]}
                self.assertEqual(
                    apply_ecb_freshness(payload, now=now)["data_status"], "malformed"
                )

    def test_same_day_arrival_before_cutoff_is_allowed(self):
        payload = {"data_status": "ok", "items": [{"date": "2026-04-07"}]}
        self.assertEqual(
            apply_ecb_freshness(
                payload, now=datetime.fromisoformat("2026-04-07T13:50:00Z")
            )["data_status"],
            "ok",
        )

    def test_history_uses_latest_date_without_expiring_old_history(self):
        payload = {
            "data_status": "stale",
            "items": [{"date": "2026-03-31"}, {"date": "2026-04-02"}],
        }
        now = datetime.fromisoformat("2026-04-06T19:00:00Z")
        self.assertEqual(
            apply_ecb_freshness(payload, now=now, history=True)["data_status"], "ok"
        )

    def test_actual_store_loaders_apply_rule_and_keep_fail_closed(self):
        payload = {
            "data_status": "stale",
            "items": [{"date": "2026-04-02"}],
            "latest_timestamp": "unchanged",
        }
        ps = SimpleNamespace(
            load_layered_dataset=mock.Mock(
                side_effect=lambda *a, **k: copy.deepcopy(payload)
            )
        )
        clock = datetime.fromisoformat("2026-04-06T19:00:00Z")
        with (
            mock.patch.object(store, "_ps_mod", return_value=ps),
            mock.patch.object(store, "_contract"),
            mock.patch.object(store, "_lake_uri", return_value="/synthetic"),
            mock.patch("book_fx.freshness._now", return_value=clock),
        ):
            for loader in (store.load_records, store.load_history):
                result = loader()
                self.assertEqual(result["data_status"], "ok")
                self.assertEqual(result["latest_timestamp"], "unchanged")
            payload["data_status"] = "malformed"
            self.assertEqual(store.load_records()["data_status"], "malformed")

    def test_real_bronze_easter_snapshot_is_current_then_expires(self):
        if any(
            importlib.util.find_spec(name) is None
            for name in ("duckdb", "pyarrow", "data_lake")
        ):
            self.skipTest("optional lake runtime required")
        with tempfile.TemporaryDirectory() as tmp:
            store.seed_lake_from_rates(
                {"base": "EUR", "date": "2026-04-02", "rates": {"USD": 1.1}},
                data_lake_uri=tmp,
            )
            with mock.patch(
                "book_fx.freshness._now",
                return_value=datetime.fromisoformat("2026-04-06T19:00:00Z"),
            ):
                self.assertEqual(
                    store.load_records(data_lake_uri=tmp)["data_status"], "ok"
                )
                self.assertEqual(
                    store.load_history(data_lake_uri=tmp)["data_status"], "ok"
                )
            with mock.patch(
                "book_fx.freshness._now",
                return_value=datetime.fromisoformat("2026-04-07T14:00:00Z"),
            ):
                self.assertEqual(
                    store.load_records(data_lake_uri=tmp)["data_status"], "stale"
                )

    def test_packaged_timezone_works_without_system_iana_database(self):
        # New process imports the actual store/API only after disabling system
        # zoneinfo paths, so an already-cached Europe/Berlin cannot mask failure.
        code = """
from importlib.metadata import version
from zoneinfo import ZoneInfo, reset_tzpath
reset_tzpath([])
ZoneInfo.clear_cache()
from book_fx import store, api
from book_fx.freshness import expected_rate_date
from datetime import datetime, date
assert version('tzdata')
assert expected_rate_date(datetime.fromisoformat('2026-01-05T14:59:59Z')) == date(2026, 1, 2)
assert expected_rate_date(datetime.fromisoformat('2026-01-05T15:00:00Z')) == date(2026, 1, 5)
assert expected_rate_date(datetime.fromisoformat('2026-07-06T13:59:59Z')) == date(2026, 7, 3)
assert expected_rate_date(datetime.fromisoformat('2026-07-06T14:00:00Z')) == date(2026, 7, 6)
print('packaged IANA timezone: store/API imports and winter/summer cutoff passed')
"""
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("winter/summer cutoff passed", result.stdout)
