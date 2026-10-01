#!/usr/bin/env python3
"""
DeFi yields from DeFiLlama API (free, no auth).

Lake-first flow (shared product_adapter):
    DeFiLlama /pools bytes
      → landing/ (exact bytes)
      → Bronze Parquet (defi_yields + defi_yields_history) + manifest
      → optional local CSV projection under data/

Stable pool ids, chain/protocol lineage, snapshot event_time, and in-batch
dedupe are enforced in book_defi.lake.yield_records_from_pools.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


def _require_httpx():
    try:
        import httpx as _httpx
    except ImportError:
        print("ERROR: httpx required for live ingestion. Install: pip install httpx")
        raise SystemExit(1)
    return _httpx


class _HttpxProxy:
    def __getattr__(self, name):
        return getattr(_require_httpx(), name)


httpx = _HttpxProxy()

PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = PROJECT_ROOT / "data"

try:
    from . import config as _dp_config
    from .policy import (
        evaluate_provider,
        require_provider,
        require_external_writes,
        external_writes_allowed,
    )
    from .store import seed_fixtures
    from . import lake as _lake
    from .fsutil import atomic_append_csv, atomic_write_csv
    from .quality import finite_number
except ImportError:  # pragma: no cover
    _dp_config = None
    _lake = None

    def evaluate_provider(name):
        class D:
            allowed = True
            status = "free"
            reason = ""

        return D()

    def require_provider(name):
        return evaluate_provider(name)

    def require_external_writes(action):
        return None

    def external_writes_allowed():
        return False

    def seed_fixtures(data_dir=None):
        return None


DEFILLAMA_BASE = "https://yields.llama.fi"
STABLECOINS = ["USDT", "USDC", "DAI", "BUSD", "FRAX", "LUSD"]
DEFAULT_CHAINS = ["Ethereum", "Arbitrum", "Optimism", "Polygon", "Base", "BSC"]
DEFAULT_MIN_APY = 5.0


def fetch_pools_raw() -> tuple[bytes, list]:
    """Fetch pools; return exact response bytes + pool list."""
    require_provider("defillama_public")
    resp = httpx.get(f"{DEFILLAMA_BASE}/pools", timeout=60)
    resp.raise_for_status()
    raw = getattr(resp, "content", None) or json.dumps(resp.json()).encode("utf-8")
    data = resp.json()
    pools = data.get("data", []) if isinstance(data, dict) else []
    return raw, pools


def fetch_pools() -> list:
    try:
        _raw, pools = fetch_pools_raw()
        return pools
    except Exception as e:  # noqa: BLE001
        print(f"  ERROR: Failed to fetch pools: {e}")
        return []


def filter_pools(
    pools: list,
    chains: list,
    min_apy: float,
    categories: list = None,
) -> list:
    """Keep pools on ``chains`` with a finite APY in [min_apy, 1000] and TVL >= $100k.

    NaN/inf/non-numeric APY or TVL values are excluded: NaN compares false
    against every bound, so without the check it slipped through the filter.
    """
    filtered = []
    for pool in pools:
        if not isinstance(pool, dict):
            continue
        if chains and pool.get("chain", "") not in chains:
            continue
        apy = finite_number(pool.get("apy") or 0)
        if apy is None or apy < min_apy:
            continue
        tvl = finite_number(pool.get("tvlUsd") or 0)
        if tvl is None or tvl < 100000:
            continue
        if apy > 1000:
            continue
        if categories:
            pool_cat = pool.get("category", "")
            if pool_cat not in categories:
                continue
        filtered.append(pool)
    return filtered


def detect_stablecoin_pools(pools: list) -> list:
    stable_pools = []
    for pool in pools:
        symbol = str(pool.get("symbol") or "").upper()
        for sc in STABLECOINS:
            if sc in symbol:
                apy = pool.get("apy", 0) or 0
                if apy >= 5.0:
                    stable_pools.append(pool)
                    break
    return stable_pools


def detect_opportunities(pools: list, prev_pools: dict) -> list:
    opportunities = []
    for pool in pools:
        pool_id = pool.get("pool", "")
        apy = finite_number(pool.get("apy")) or 0.0
        tvl = finite_number(pool.get("tvlUsd")) or 0.0
        if pool_id not in prev_pools and apy >= 20 and tvl >= 500000:
            opportunities.append(
                {
                    "type": "NEW_HIGH_YIELD",
                    "pool": pool,
                    "reason": f"New pool with {apy:.1f}% APY, TVL ${tvl:,.0f}",
                }
            )
        elif pool_id in prev_pools:
            prev_apy = prev_pools[pool_id]
            if prev_apy > 0 and apy > prev_apy * 1.5 and apy >= 15:
                opportunities.append(
                    {
                        "type": "APY_SPIKE",
                        "pool": pool,
                        "reason": f"APY jumped from {prev_apy:.1f}% to {apy:.1f}%",
                    }
                )
    return opportunities


def load_previous_pools(output_dir: Optional[Path] = None) -> dict:
    history_file = Path(output_dir or OUTPUT_DIR) / "defi_yields_history.csv"
    pools = {}
    if history_file.exists():
        with open(history_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                pool_id = row.get("pool_id", "")
                try:
                    pools[pool_id] = float(row.get("apy", 0))
                except (ValueError, TypeError):
                    pass
    return pools


def _projection_timestamp() -> str:
    # UTC: readers (product_store.parse_ts / _freshness) treat naive stamps as
    # UTC, so a host-local stamp looked hours fresher (or older) than it was.
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def project_yields_csv(pools: list, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / "defi_yields.csv"
    fieldnames = [
        "pool_id",
        "chain",
        "project",
        "symbol",
        "tvl_usd",
        "apy",
        "apy_base",
        "apy_reward",
        "reward_tokens",
        "category",
        "url",
        "scraped_at",
    ]
    scraped = _projection_timestamp()
    records = (
        _lake.yield_records_from_pools(pools, dedupe=True)
        if _lake is not None
        else []
    )
    # Prefer normalized records for stable pool_id projection.
    rows: list[dict[str, Any]] = []
    if records:
        for r in records:
            rows.append(
                {
                    "pool_id": r.get("pool_id", ""),
                    "chain": r.get("chain", ""),
                    "project": r.get("project", ""),
                    "symbol": r.get("symbol", ""),
                    "tvl_usd": r.get("tvl_usd", 0),
                    "apy": r.get("apy", 0),
                    "apy_base": r.get("apy_base", 0),
                    "apy_reward": r.get("apy_reward", 0),
                    "reward_tokens": r.get("reward_tokens", ""),
                    "category": r.get("category", ""),
                    "url": r.get("url", ""),
                    "scraped_at": scraped,
                }
            )
    else:
        for pool in pools:
            rows.append(
                {
                    "pool_id": pool.get("pool", ""),
                    "chain": pool.get("chain", ""),
                    "project": pool.get("project", ""),
                    "symbol": pool.get("symbol", "")[:30],
                    "tvl_usd": pool.get("tvlUsd", 0),
                    "apy": pool.get("apy", 0),
                    "apy_base": pool.get("apyBase", 0),
                    "apy_reward": pool.get("apyReward", 0),
                    "reward_tokens": ",".join(pool.get("rewardTokens", []) or []),
                    "category": pool.get("category", ""),
                    "url": pool.get("url", ""),
                    "scraped_at": scraped,
                }
            )
    atomic_write_csv(filepath, fieldnames, rows)
    print(f"  Projected {len(rows)} pools → {filepath}")
    return filepath


def project_history_csv(pools: list, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / "defi_yields_history.csv"
    fieldnames = [
        "pool_id",
        "chain",
        "project",
        "symbol",
        "tvl_usd",
        "apy",
        "apy_base",
        "apy_reward",
        "category",
        "scraped_at",
    ]
    scraped = _projection_timestamp()
    records = (
        _lake.yield_records_from_pools(pools, dedupe=True)
        if _lake is not None
        else []
    )
    rows: list[dict[str, Any]] = []
    if records:
        for r in records:
            rows.append(
                {
                    "pool_id": r.get("pool_id", ""),
                    "chain": r.get("chain", ""),
                    "project": r.get("project", ""),
                    "symbol": r.get("symbol", ""),
                    "tvl_usd": r.get("tvl_usd", 0),
                    "apy": r.get("apy", 0),
                    "apy_base": r.get("apy_base", 0),
                    "apy_reward": r.get("apy_reward", 0),
                    "category": r.get("category", ""),
                    "scraped_at": scraped,
                }
            )
    else:
        for pool in pools:
            rows.append(
                {
                    "pool_id": pool.get("pool", ""),
                    "chain": pool.get("chain", ""),
                    "project": pool.get("project", ""),
                    "symbol": pool.get("symbol", "")[:30],
                    "tvl_usd": pool.get("tvlUsd", 0),
                    "apy": pool.get("apy", 0),
                    "apy_base": pool.get("apyBase", 0),
                    "apy_reward": pool.get("apyReward", 0),
                    "category": pool.get("category", ""),
                    "scraped_at": scraped,
                }
            )
    atomic_append_csv(filepath, fieldnames, rows)
    print(f"  Projected +{len(rows)} history rows → {filepath}")
    return filepath


def save_yields(pools: list, output_dir: Optional[Path] = None):
    return project_yields_csv(pools, Path(output_dir or OUTPUT_DIR))


def append_history(pools: list, output_dir: Optional[Path] = None):
    return project_history_csv(pools, Path(output_dir or OUTPUT_DIR))


def _lake_ingest_yields(
    raw: bytes,
    pools: list,
    *,
    data_lake_uri: Optional[str],
    output_dir: Path,
    dataset: str,
    dataset_role: str,
) -> dict[str, Any]:
    if _lake is None or _dp_config is None:
        raise RuntimeError("Lake adapter unavailable in this runtime")
    records = _lake.yield_records_from_pools(pools, dedupe=True)
    if not records:
        raise _lake.LakeIngestError("No yield records after normalize/dedupe")
    chains = sorted({str(r.get("chain") or "") for r in records if r.get("chain")})
    protocols = sorted(
        {str(r.get("protocol") or "") for r in records if r.get("protocol")}
    )
    result = _lake.ingest_to_lake(
        raw=raw,
        records=records,
        dataset=dataset,
        data_lake_uri=data_lake_uri,
        metadata={
            "endpoint": "yields.llama.fi/pools",
            "dataset_role": dataset_role,
            "chains": chains[:50],
            "protocols_sample": protocols[:50],
            "pool_count": len(records),
        },
    )
    _lake.write_lineage(result, dataset=dataset, data_dir=output_dir)
    print(
        f"  Lake {dataset}: run_id={result.get('run_id')} "
        f"records={result.get('record_count')} bronze={result.get('bronze_key')}"
    )
    return result


def run_live_ingest(
    *,
    chains: list[str],
    min_apy: float,
    output_dir: Path,
    categories: Optional[list[str]] = None,
    write_history: bool = True,
    data_lake_uri: Optional[str] = None,
    project_csv: bool = True,
) -> dict[str, Any]:
    print(
        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
        "DeFi Yield Scraper (lake-first)"
    )
    print(f"  Chains: {chains} | Min APY: {min_apy}%")
    if data_lake_uri:
        print(f"  Data lake: {data_lake_uri}")
    elif _lake is not None:
        print(f"  Data lake: {_lake.default_data_lake_uri()}")

    print("  Fetching pools from DeFiLlama...")
    raw, all_pools = fetch_pools_raw()
    print(f"  Total pools: {len(all_pools)}")
    if not all_pools:
        raise RuntimeError("No pools fetched.")

    filtered = filter_pools(all_pools, chains, min_apy, categories)
    print(f"  Filtered: {len(filtered)} pools (>{min_apy}% APY, >$100k TVL)")

    stable_pools = detect_stablecoin_pools(filtered)
    if stable_pools:
        print(f"\n  *** HIGH-YIELD STABLECOIN POOLS ({len(stable_pools)}) ***")
        stable_pools.sort(key=lambda x: x.get("apy", 0) or 0, reverse=True)
        for pool in stable_pools[:10]:
            print(
                f"    {pool.get('project', ''):15s} | "
                f"{pool.get('symbol', '')[:20]:20s} | "
                f"{pool.get('chain', ''):10s} | APY: {pool.get('apy', 0):.1f}% | "
                f"TVL: ${pool.get('tvlUsd', 0):,.0f}"
            )

    prev_pools = load_previous_pools(output_dir) if project_csv else {}
    opportunities = detect_opportunities(filtered, prev_pools)
    if opportunities:
        print(f"\n  *** YIELD OPPORTUNITIES ({len(opportunities)}) ***")
        for opp in opportunities[:10]:
            pool = opp["pool"]
            print(
                f"    [{opp['type']}] {pool.get('project', ''):15s} | "
                f"{pool.get('chain', ''):10s} | APY: {pool.get('apy', 0):.1f}% | "
                f"{opp['reason']}"
            )

    # Durable write before CSV (fail-closed).
    lake_yields = _lake_ingest_yields(
        raw,
        filtered,
        data_lake_uri=data_lake_uri,
        output_dir=output_dir,
        dataset=_dp_config.LAKE_DATASET_YIELDS,
        dataset_role="snapshot",
    )
    lake_history = None
    if write_history:
        # Separate landing identity for history dataset (same pools, history role).
        history_raw = json.dumps(
            {
                "provider": "defillama_public",
                "dataset": _dp_config.LAKE_DATASET_HISTORY,
                "parent_run_id": lake_yields.get("run_id"),
                "data": filtered,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
        lake_history = _lake_ingest_yields(
            history_raw,
            filtered,
            data_lake_uri=data_lake_uri,
            output_dir=output_dir,
            dataset=_dp_config.LAKE_DATASET_HISTORY,
            dataset_role="history",
        )

    if project_csv:
        project_yields_csv(filtered, output_dir)
        if write_history:
            project_history_csv(filtered, output_dir)

    print("\n  TOP YIELDS BY CHAIN:")
    by_chain: dict[str, list] = {}
    for pool in filtered:
        chain = pool.get("chain", "Unknown")
        by_chain.setdefault(chain, []).append(pool)
    for chain in chains:
        chain_pools = by_chain.get(chain, [])
        if chain_pools:
            chain_pools.sort(key=lambda x: x.get("apy", 0) or 0, reverse=True)
            top = chain_pools[0]
            print(
                f"    {chain:12s}: {top.get('project', ''):15s} | "
                f"{top.get('symbol', '')[:20]:20s} | APY: {top.get('apy', 0):.1f}%"
            )

    print(f"\n  Total: {len(filtered)} pools tracked")
    print("  Done (lake durable; CSV is projection only).")
    return {
        "pools": len(filtered),
        "lake_yields": lake_yields,
        "lake_history": lake_history,
        "projected_csv": project_csv,
    }


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Scrape DeFi yields (lake-first; CSV is projection)"
    )
    parser.add_argument(
        "--chains",
        default=",".join(DEFAULT_CHAINS),
        help="Comma-separated chains",
    )
    parser.add_argument(
        "--min-apy",
        type=float,
        default=DEFAULT_MIN_APY,
        help="Minimum APY to track",
    )
    parser.add_argument(
        "--categories",
        default="",
        help="Comma-separated categories (lending,staking,dex,etc)",
    )
    parser.add_argument(
        "--no-history",
        action="store_true",
        help="Skip defi_yields_history lake write and CSV append",
    )
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR))
    parser.add_argument("--data-lake-uri", default=None)
    parser.add_argument(
        "--no-project-csv",
        action="store_true",
        help="Skip local CSV projection after lake write",
    )
    parser.add_argument("--fixture", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    # Chain names are case-sensitive on DeFiLlama ("Ethereum"), so only trim
    # and de-duplicate; an empty list means "all chains".
    chains = list(dict.fromkeys(c.strip() for c in args.chains.split(",") if c.strip()))
    categories = list(
        dict.fromkeys(c.strip() for c in args.categories.split(",") if c.strip())
    ) or None
    if not math.isfinite(args.min_apy) or args.min_apy < 0:
        parser.error("--min-apy must be a finite, non-negative number")
    output_dir = Path(args.output_dir)

    if getattr(args, "fixture", False) or getattr(args, "dry_run", False):
        out = Path(args.output_dir)
        out.mkdir(parents=True, exist_ok=True)
        if _dp_config is not None:
            _dp_config.DATA_DIR = out
        seed_fixtures(out)
        print(
            "["
            + datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            + "] Fixture mode: seeded local projection under "
            + str(out)
        )
        print("  No upstream providers contacted; no lake write.")
        return 0

    try:
        run_live_ingest(
            chains=chains,
            min_apy=args.min_apy,
            output_dir=output_dir,
            categories=categories,
            write_history=not args.no_history,
            data_lake_uri=args.data_lake_uri,
            project_csv=not args.no_project_csv,
        )
    except Exception as exc:  # noqa: BLE001
        if _lake is not None and isinstance(
            exc, (_lake.LakeUnavailable, _lake.LakeIngestError)
        ):
            print(
                f"ERROR: lake ingest failed before projection: {exc}",
                file=sys.stderr,
            )
            return 2
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


class DeFiYieldScraper:
    """Wrapper class for scheduler compatibility (lake-first)."""

    def __init__(self, chains=None, min_apy=5.0, categories=None, **kwargs):
        self.chains = chains or DEFAULT_CHAINS
        self.min_apy = min_apy
        self.categories = categories
        self.data_lake_uri = kwargs.get("data_lake_uri")
        self.output_dir = Path(kwargs.get("output_dir") or OUTPUT_DIR)

    async def run(self, **kwargs):
        result = run_live_ingest(
            chains=self.chains,
            min_apy=self.min_apy,
            output_dir=Path(kwargs.get("output_dir") or self.output_dir),
            categories=self.categories,
            write_history=True,
            data_lake_uri=kwargs.get("data_lake_uri", self.data_lake_uri),
            project_csv=True,
        )
        return [{"source": "defi_yields", "count": result["pools"], "lake": True}]


if __name__ == "__main__":
    raise SystemExit(main())
