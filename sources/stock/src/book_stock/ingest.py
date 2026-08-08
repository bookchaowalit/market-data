#!/usr/bin/env python3
"""
Stock portfolio prices via Yahoo Finance public chart API (free, best-effort).

Lake-first flow (shared product_adapter contract with book-crypto-data):
    Yahoo API bytes
      → Object Storage landing/ (exact bytes)
      → Bronze Parquet (stock_prices + stock_history) + manifest
      → optional local CSV projection under data/

Usage:
    python -m book_stock.ingest
    python -m book_stock.ingest --symbols AAPL,MSFT,GOOGL
    python -m book_stock.ingest --alert-threshold 2
    python -m book_stock.ingest --no-history
    python -m book_stock.ingest --data-lake-uri /path/to/data/lake
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime
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


YAHOO_BASE = "https://query1.finance.yahoo.com/v8/finance/chart"

DEFAULT_SYMBOLS = [
    "AAPL",
    "MSFT",
    "GOOGL",
    "AMZN",
    "NVDA",
    "META",
    "TSLA",
    "AMD",
    "CRM",
    "PLTR",
    "SPY",
    "QQQ",
    "BTC-USD",
    "ETH-USD",
    "^SET50",
]


def fetch_quote_raw(symbol: str) -> tuple[bytes, dict]:
    """Fetch one symbol. Returns (exact_response_bytes, normalized_quote)."""
    require_provider("yahoo_public")
    url = f"{YAHOO_BASE}/{symbol}"
    params = {"interval": "1d", "range": "5d"}
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = httpx.get(url, params=params, headers=headers, timeout=15)
    resp.raise_for_status()
    raw = getattr(resp, "content", None) or json.dumps(resp.json()).encode("utf-8")
    data = resp.json()
    result = data.get("chart", {}).get("result", [])
    if not result:
        return raw, {}
    meta = result[0].get("meta", {})
    price = meta.get("regularMarketPrice", 0)
    prev_close = meta.get("chartPreviousClose", meta.get("previousClose", 0))
    change = price - prev_close if prev_close else 0
    change_pct = (change / prev_close * 100) if prev_close else 0
    ts = ""
    if meta.get("regularMarketTime"):
        ts = datetime.fromtimestamp(meta.get("regularMarketTime", 0)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    quote = {
        "symbol": symbol,
        "price": price,
        "prev_close": prev_close,
        "change": round(change, 4),
        "change_pct": round(change_pct, 2),
        "currency": meta.get("currency", "USD"),
        "exchange": meta.get("exchangeName", ""),
        "timestamp": ts,
    }
    return raw, quote


def fetch_quotes_raw(symbols: list[str]) -> tuple[bytes, list[dict], dict[str, bytes]]:
    """Fetch all symbols; bundle exact per-symbol response bytes as landing JSON."""
    quotes: list[dict] = []
    raw_by_symbol: dict[str, bytes] = {}
    for sym in symbols:
        try:
            raw, q = fetch_quote_raw(sym)
            raw_by_symbol[sym] = raw
            if q:
                quotes.append(q)
        except Exception as e:  # noqa: BLE001
            print(f"  Warning: Failed to fetch {sym}: {e}")
    # Landing payload preserves exact response bodies per symbol (UTF-8 text).
    landing_obj = {
        "provider": "yahoo_public",
        "fetched_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "symbols": list(symbols),
        "responses": {
            sym: raw.decode("utf-8", errors="replace") for sym, raw in raw_by_symbol.items()
        },
    }
    landing_bytes = json.dumps(
        landing_obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return landing_bytes, quotes, raw_by_symbol


def fetch_quotes(symbols: list) -> list:
    """Backward-compatible quote list fetch."""
    _raw, quotes, _bodies = fetch_quotes_raw(symbols)
    return quotes


def _projection_timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def project_prices_csv(quotes: list[dict], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / "stock_prices.csv"
    fieldnames = [
        "symbol",
        "price",
        "prev_close",
        "change",
        "change_pct",
        "currency",
        "exchange",
        "timestamp",
        "scraped_at",
    ]
    scraped = _projection_timestamp()
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for q in quotes:
            writer.writerow({**q, "scraped_at": scraped})
    print(f"  Projected {len(quotes)} quotes → {filepath}")
    return filepath


def project_history_csv(quotes: list[dict], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / "stock_history.csv"
    fieldnames = [
        "symbol",
        "price",
        "prev_close",
        "change",
        "change_pct",
        "currency",
        "exchange",
        "timestamp",
        "scraped_at",
    ]
    scraped = _projection_timestamp()
    file_exists = filepath.exists()
    with open(filepath, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        for q in quotes:
            writer.writerow({**q, "scraped_at": scraped})
    print(f"  Projected +{len(quotes)} history rows → {filepath}")
    return filepath


# Backward-compatible aliases
def save_prices(quotes: list, output_dir: Optional[Path] = None):
    return project_prices_csv(quotes, Path(output_dir or OUTPUT_DIR))


def append_history(quotes: list, output_dir: Optional[Path] = None):
    return project_history_csv(quotes, Path(output_dir or OUTPUT_DIR))


def load_previous_prices(output_dir: Optional[Path] = None) -> dict:
    history_file = Path(output_dir or OUTPUT_DIR) / "stock_history.csv"
    prices = {}
    if history_file.exists():
        with open(history_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    prices[row["symbol"]] = float(row.get("price", 0))
                except (TypeError, ValueError, KeyError):
                    continue
    return prices


def print_alerts(quotes: list, threshold: float, prev_prices: dict):
    print("\n  Alerts:")
    alerted = False
    for q in quotes:
        sym = q["symbol"]
        pct = q["change_pct"]
        if abs(pct) >= threshold:
            direction = "UP" if pct > 0 else "DOWN"
            print(f"    *** {sym}: {direction} {pct:+.2f}% (${q['price']:.2f}) ***")
            alerted = True
        if sym in prev_prices and prev_prices[sym] > 0:
            multi_change = (q["price"] - prev_prices[sym]) / prev_prices[sym] * 100
            if abs(multi_change) >= threshold * 2:
                direction = "RISING" if multi_change > 0 else "FALLING"
                print(f"    *** {sym}: {direction} {multi_change:+.2f}% since last check ***")
                alerted = True
    if not alerted:
        print(f"    No alerts (all moves < {threshold}%)")


def _lake_ingest_prices(
    raw: bytes,
    quotes: list[dict],
    *,
    data_lake_uri: Optional[str],
    output_dir: Path,
) -> dict[str, Any]:
    if _lake is None or _dp_config is None:
        raise RuntimeError("Lake adapter unavailable in this runtime")
    records = _lake.quote_records_from_api(quotes)
    result = _lake.ingest_to_lake(
        raw=raw,
        records=records,
        dataset=_dp_config.LAKE_DATASET_PRICES,
        data_lake_uri=data_lake_uri,
        metadata={"endpoint": "v8/finance/chart", "dataset_role": "snapshot"},
    )
    _lake.write_lineage(
        result, dataset=_dp_config.LAKE_DATASET_PRICES, data_dir=output_dir
    )
    print(
        f"  Lake stock_prices: run_id={result.get('run_id')} "
        f"records={result.get('record_count')} bronze={result.get('bronze_key')}"
    )
    return result


def _lake_ingest_history(
    quotes: list[dict],
    *,
    data_lake_uri: Optional[str],
    output_dir: Path,
    parent_raw_sha_prefix: str = "",
) -> dict[str, Any]:
    if _lake is None or _dp_config is None:
        raise RuntimeError("Lake adapter unavailable in this runtime")
    records = _lake.quote_records_from_api(quotes)
    # Separate landing identity for the history dataset write.
    history_doc = {
        "provider": "yahoo_public",
        "dataset": _dp_config.LAKE_DATASET_HISTORY,
        "parent_prices_batch": parent_raw_sha_prefix,
        "quotes": quotes,
    }
    raw = json.dumps(
        history_doc, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    result = _lake.ingest_to_lake(
        raw=raw,
        records=records,
        dataset=_dp_config.LAKE_DATASET_HISTORY,
        data_lake_uri=data_lake_uri,
        metadata={"endpoint": "v8/finance/chart", "dataset_role": "history"},
    )
    _lake.write_lineage(
        result, dataset=_dp_config.LAKE_DATASET_HISTORY, data_dir=output_dir
    )
    print(
        f"  Lake stock_history: run_id={result.get('run_id')} "
        f"records={result.get('record_count')} bronze={result.get('bronze_key')}"
    )
    return result


def run_live_ingest(
    *,
    symbols: list[str],
    output_dir: Path,
    alert_threshold: float = 3.0,
    write_history: bool = True,
    data_lake_uri: Optional[str] = None,
    project_csv: bool = True,
) -> dict[str, Any]:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Stock Price Scraper (lake-first)")
    print(f"  Symbols: {len(symbols)} | Alert: >{alert_threshold}%")
    if data_lake_uri:
        print(f"  Data lake: {data_lake_uri}")
    elif _lake is not None:
        print(f"  Data lake: {_lake.default_data_lake_uri()}")

    landing_bytes, quotes, _bodies = fetch_quotes_raw(symbols)
    print(f"  Got {len(quotes)} quotes")
    if not quotes:
        raise RuntimeError("No quotes fetched. Check network/symbols.")

    lake_prices = _lake_ingest_prices(
        landing_bytes,
        quotes,
        data_lake_uri=data_lake_uri,
        output_dir=output_dir,
    )
    lake_history = None
    if write_history:
        lake_history = _lake_ingest_history(
            quotes,
            data_lake_uri=data_lake_uri,
            output_dir=output_dir,
            parent_raw_sha_prefix=str(lake_prices.get("run_id") or ""),
        )

    if project_csv:
        project_prices_csv(quotes, output_dir)
        if write_history:
            project_history_csv(quotes, output_dir)

    prev_prices = load_previous_prices(output_dir) if project_csv else {}
    print_alerts(quotes, alert_threshold, prev_prices)

    print(f"\n  {'Symbol':12s} {'Price':>12s} {'Change':>10s} {'Currency':>8s}")
    print(f"  {'-'*12} {'-'*12} {'-'*10} {'-'*8}")
    for q in quotes:
        arrow = "+" if q["change_pct"] >= 0 else ""
        print(
            f"  {q['symbol']:12s} {q['price']:>12.2f} "
            f"{arrow}{q['change_pct']:>8.2f}% {q['currency']:>8s}"
        )
    print("\n  Done (lake durable; CSV is projection only).")
    return {
        "quotes": len(quotes),
        "lake_prices": lake_prices,
        "lake_history": lake_history,
        "projected_csv": project_csv,
    }


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Scrape stock prices (lake-first; CSV is projection)"
    )
    parser.add_argument(
        "--symbols",
        default=",".join(DEFAULT_SYMBOLS),
        help="Comma-separated stock symbols",
    )
    parser.add_argument(
        "--alert-threshold",
        type=float,
        default=3.0,
        help="Alert threshold %% for daily moves",
    )
    parser.add_argument(
        "--no-history",
        action="store_true",
        help="Skip stock_history lake write and CSV append",
    )
    parser.add_argument(
        "--output-dir",
        default=str(OUTPUT_DIR),
        help="Local projection directory (default: repo data/)",
    )
    parser.add_argument(
        "--data-lake-uri",
        default=None,
        help="Object storage / local lake URI",
    )
    parser.add_argument(
        "--no-project-csv",
        action="store_true",
        help="Skip local CSV projection after lake write",
    )
    parser.add_argument(
        "--fixture",
        action="store_true",
        help="Load offline fixtures into data/ (no upstream, no lake)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry-run: use fixtures / skip upstream network",
    )
    args = parser.parse_args(argv)

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
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
            symbols=symbols,
            output_dir=output_dir,
            alert_threshold=args.alert_threshold,
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


class StockPriceScraper:
    """Wrapper class for scheduler compatibility (lake-first)."""

    def __init__(self, symbols=None, alert_threshold=3.0, **kwargs):
        self.symbols = symbols or DEFAULT_SYMBOLS
        self.alert_threshold = alert_threshold
        self.data_lake_uri = kwargs.get("data_lake_uri")
        self.output_dir = Path(kwargs.get("output_dir") or OUTPUT_DIR)

    async def run(self, **kwargs):
        result = run_live_ingest(
            symbols=self.symbols,
            output_dir=Path(kwargs.get("output_dir") or self.output_dir),
            alert_threshold=self.alert_threshold,
            write_history=True,
            data_lake_uri=kwargs.get("data_lake_uri", self.data_lake_uri),
            project_csv=True,
        )
        return [{"source": "stocks", "count": result["quotes"], "lake": True}]


if __name__ == "__main__":
    raise SystemExit(main())
