"""yield_records_from_pools(dedupe=False) keeps every occurrence (public lake helper)."""

from __future__ import annotations

import unittest

from book_defi import lake

POOL = {
    "pool": "pool-1",
    "chain": "Ethereum",
    "project": "aave-v3",
    "symbol": "USDC",
    "tvlUsd": 2_000_000,
    "apy": 6.5,
}


class YieldRecordsTest(unittest.TestCase):
    def test_dedupe_false_keeps_both_occurrences(self) -> None:
        records = lake.yield_records_from_pools(
            [dict(POOL), dict(POOL, apy=7.0)], dedupe=False
        )
        self.assertEqual([record["apy"] for record in records], [6.5, 7.0])

    def test_dedupe_true_keeps_the_last_occurrence(self) -> None:
        records = lake.yield_records_from_pools(
            [dict(POOL), dict(POOL, apy=7.0)], dedupe=True
        )
        self.assertEqual([record["apy"] for record in records], [7.0])


if __name__ == "__main__":
    unittest.main()
