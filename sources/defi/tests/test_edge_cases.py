"""Edge-case regressions: overflowing numbers, invalid optional metrics, booleans."""

from __future__ import annotations

import json
import os
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from book_defi import config, quality

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

try:
    import scrape_defi_yields as defi
except RuntimeError:  # httpx missing
    defi = None

NOW = datetime(2026, 8, 1, 12, tzinfo=timezone.utc)


def _pool(pool_id: str, **extra) -> dict:
    return {
        "pool": pool_id,
        "project": "aave-v3",
        "symbol": "USDC",
        "chain": "Ethereum",
        "tvlUsd": 5_000_000,
        "apy": 6.5,
        **extra,
    }


class QualityTests(unittest.TestCase):
    def test_huge_json_int_is_rejected_not_raised(self) -> None:
        huge = json.loads("1" + "0" * 400)
        self.assertIsNone(quality.finite_number(huge))
        self.assertIsNone(quality.non_negative_number(huge))


@unittest.skipIf(defi is None, "scripts need httpx/bs4")
class ParsePayloadTests(unittest.TestCase):
    def _parse(self, pools: list[dict]) -> list[dict]:
        return defi.parse_payload(
            {"status": "success", "data": pools},
            ["Ethereum"],
            provider_updated_at="2026-08-01T11:00:00Z",
            now=NOW,
        )

    def test_invalid_optional_metric_blanks_instead_of_aborting(self) -> None:
        rows = self._parse(
            [
                _pool("a", apyBase=float("nan")),
                _pool("b", apyReward="n/a"),
                _pool("c", apyPct1D=10**400),
            ]
        )
        self.assertEqual([r["pool_id"] for r in rows], ["a", "b", "c"])
        self.assertEqual(
            (rows[0]["apy_base"], rows[1]["apy_reward"], rows[2]["apy_1d"]),
            ("", "", ""),
        )

    def test_overflowing_required_value_drops_only_that_pool(self) -> None:
        rows = self._parse([_pool("a", tvlUsd=10**400), _pool("b")])
        self.assertEqual([r["pool_id"] for r in rows], ["b"])

    def test_string_false_is_not_a_stablecoin(self) -> None:
        rows = self._parse(
            [_pool("a", stablecoin="false"), _pool("b", stablecoin=True)]
        )
        self.assertEqual(
            {r["pool_id"]: r["stablecoin"] for r in rows}, {"a": False, "b": True}
        )


class EnvBoolTests(unittest.TestCase):
    def _parse(self, raw, default):
        with mock.patch.dict(os.environ, {"EDGE_CASE_FLAG": raw}):
            return config.env_bool("EDGE_CASE_FLAG", default)

    def test_blank_or_unknown_keeps_the_safe_default(self) -> None:
        for raw in ("", "  ", "ture", "enabled?"):
            with self.subTest(raw=raw):
                self.assertIs(self._parse(raw, True), True)
                self.assertIs(self._parse(raw, False), False)

    def test_explicit_values(self) -> None:
        for raw in ("1", "TRUE", " yes ", "on"):
            self.assertIs(self._parse(raw, False), True)
        for raw in ("0", "False", " no", "OFF"):
            self.assertIs(self._parse(raw, True), False)


if __name__ == "__main__":
    unittest.main()
