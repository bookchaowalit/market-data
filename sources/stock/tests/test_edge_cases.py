"""Edge-case regressions: overflowing numbers, null previous close, env flags."""
from __future__ import annotations

import json
import math
import os
import unittest
from unittest import mock

from book_stock import config, ingest, quality


def _chart(**meta) -> dict:
    return {"chart": {"result": [{"meta": {"regularMarketPrice": 110.0, **meta}}]}}


class QuoteEdgeCases(unittest.TestCase):
    def test_huge_json_int_is_rejected_not_raised(self) -> None:
        huge = json.loads("1" + "0" * 400)
        self.assertIsNone(quality.finite_number(huge))
        self.assertEqual(ingest.quote_from_chart("X", _chart(regularMarketPrice=huge)), {})

    def test_null_chart_previous_close_falls_back_to_previous_close(self) -> None:
        quote = ingest.quote_from_chart("X", _chart(chartPreviousClose=None, previousClose=100.0))
        self.assertEqual((quote["prev_close"], quote["change_pct"]), (100.0, 10.0))

    def test_tiny_previous_close_does_not_produce_infinite_change(self) -> None:
        quote = ingest.quote_from_chart("X", _chart(regularMarketPrice=1e300, chartPreviousClose=1e-300))
        self.assertTrue(math.isfinite(quote["change_pct"]))

    def test_non_dict_meta_is_unusable(self) -> None:
        self.assertEqual(ingest.quote_from_chart("X", {"chart": {"result": [{"meta": [1]}]}}), {})


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
