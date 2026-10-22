#!/usr/bin/env python3
"""Run bounded DefiLlama yield capture owned by this repository."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import traceback
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
# The cron venv installs requirements-collect.txt only, not this package.
sys.path.insert(0, str(ROOT / "src"))

from book_defi import observability  # noqa: E402
from scrape_defi_yields import DeFiYieldScraper  # noqa: E402

CHAINS = ["Ethereum", "Arbitrum", "Optimism", "Polygon", "Base"]


async def run_defi(output_dir: Path) -> list[dict[str, Any]]:
    scraper = DeFiYieldScraper(
        chains=CHAINS,
        min_apy=5.0,
        min_tvl_usd=1_000_000,
        max_apy=100_000,
        limit=200,
        min_rows=10,
        max_age_hours=24,
        output_dir=output_dir,
    )
    batch = await scraper.run()
    print(f"[run_defi] defi_yields: {batch[0].get('count') if batch else 0}")
    return batch


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run book-defi-data DefiLlama collection"
    )
    parser.add_argument("--output-dir", type=Path, default=ROOT / "data" / "exported")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    observability.init_sentry("cron")
    try:
        results = asyncio.run(run_defi(args.output_dir))
    except Exception as exc:  # noqa: BLE001 - report, keep the traceback, exit non-zero
        traceback.print_exc()
        observability.report_failure("cron capture", exc)
        return 1
    if args.json:
        print(json.dumps(results, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
