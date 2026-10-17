#!/usr/bin/env python3
"""Capture bounded DeFi yield pools from the public DefiLlama API.

This repository is the collection producer. Lake-first ingestion and the
read-only API remain in ``src/book_defi``; this adapter captures the
validated upstream response under ``data/exported``.
"""

from __future__ import annotations

import csv
import email.utils
import json
import math
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

try:
    import httpx
except ImportError as exc:  # pragma: no cover - requirements.txt supplies httpx
    raise RuntimeError("httpx is required for DeFi yield capture") from exc


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "data" / "exported"
API_URL = "https://yields.llama.fi/pools"
SOURCE_NAME = "DefiLlama"
DEFAULT_CHAINS = ["Ethereum", "Arbitrum", "Optimism", "Polygon", "Base"]
DEFAULT_MIN_APY = 5.0
DEFAULT_MIN_TVL_USD = 1_000_000.0
DEFAULT_MAX_APY = 100_000.0
DEFAULT_LIMIT = 200
DEFAULT_MIN_ROWS = 10
DEFAULT_MAX_AGE_HOURS = 24.0
MAX_CHAINS = 10
MAX_LIMIT = 500
CHAIN_ALIASES = {"optimism": "OP Mainnet"}
KNOWN_CHAINS = {
    "Arbitrum",
    "Aurora",
    "Avalanche",
    "Base",
    "BSC",
    "Ethereum",
    "Fantom",
    "Gnosis",
    "Kava",
    "Moonbeam",
    "Near",
    "OP Mainnet",
    "Polygon",
    "Solana",
    "Sui",
}
OUTPUT_STEM_RE = re.compile(r"^[a-z0-9_]+$")

SNAPSHOT_FIELDS = [
    "captured_at",
    "pool_id",
    "project",
    "symbol",
    "chain",
    "tvl_usd",
    "apy",
    "apy_base",
    "apy_reward",
    "apy_1d",
    "apy_7d",
    "apy_30d",
    "stablecoin",
    "il_risk",
    "exposure",
    "pool_meta",
    "source",
    "source_url",
    "provider_updated_at",
]
HISTORY_FIELDS = [
    "captured_at",
    "pool_id",
    "project",
    "symbol",
    "chain",
    "tvl_usd",
    "apy",
    "provider_updated_at",
    "source",
]


def _utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _as_values(values: Iterable[str] | str | None, default: list[str]) -> list[str]:
    if values is None:
        values = default
    if isinstance(values, str):
        values = values.split(",")
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value).strip()
        if normalized and normalized.casefold() not in seen:
            result.append(normalized)
            seen.add(normalized.casefold())
    return result


def normalize_chains(values: Iterable[str] | str | None) -> list[str]:
    """Normalize configured chain names to DefiLlama's labels."""

    requested = _as_values(values, DEFAULT_CHAINS)
    if not requested or len(requested) > MAX_CHAINS:
        raise ValueError(f"chains must contain 1-{MAX_CHAINS} unique names")
    normalized: list[str] = []
    known_by_key = {chain.casefold(): chain for chain in KNOWN_CHAINS}
    for chain in requested:
        key = chain.casefold()
        canonical = CHAIN_ALIASES.get(key, known_by_key.get(key))
        if canonical is None:
            raise ValueError(f"unsupported DefiLlama chain: {chain}")
        if canonical not in normalized:
            normalized.append(canonical)
    return normalized


def _finite_number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be numeric")
    try:
        number = float(value)
    except OverflowError:  # an int beyond float range (JSON allows 1e400 digits)
        raise ValueError(f"{field} must be finite") from None
    if not math.isfinite(number):
        raise ValueError(f"{field} must be finite")
    return number


def _optional_number(value: Any, field: str) -> float | str:
    """Blank for a missing *or invalid* optional metric.

    Raising here aborted the whole capture because one pool carried a NaN or
    string ``apyBase``; required fields are validated separately.
    """
    if value is None or value == "":
        return ""
    try:
        return _finite_number(value, field)
    except ValueError:
        return ""


def _strict_bool(value: Any) -> bool:
    """True only for JSON ``true`` (or an explicit "true"/"1" string).

    ``bool("false")`` is True, which marked string-typed flags as stablecoins.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes"}
    return False


def _provider_datetime(value: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise ValueError("DefiLlama response is missing provider timestamp")
    parsed: datetime | None = None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = email.utils.parsedate_to_datetime(text)
        except (TypeError, ValueError, IndexError):
            parsed = None
    if parsed is None or parsed.tzinfo is None:
        raise ValueError("DefiLlama provider timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def normalize_provider_timestamp(value: str) -> str:
    return (
        _provider_datetime(value)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def validate_freshness(
    provider_updated_at: str,
    max_age_hours: float,
    now: datetime | None = None,
) -> str:
    """Return a canonical provider timestamp or fail closed when it is stale."""

    provider_time = _provider_datetime(provider_updated_at)
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    current = current.astimezone(timezone.utc)
    age = current - provider_time
    if age < timedelta(minutes=-5):
        raise ValueError("DefiLlama provider timestamp is in the future")
    if age > timedelta(hours=max_age_hours):
        raise ValueError(f"DefiLlama response is older than {max_age_hours:g} hours")
    return provider_time.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_payload(
    payload: Any,
    chains: Iterable[str] | str | None,
    min_apy: float = DEFAULT_MIN_APY,
    min_tvl_usd: float = DEFAULT_MIN_TVL_USD,
    max_apy: float = DEFAULT_MAX_APY,
    limit: int = DEFAULT_LIMIT,
    provider_updated_at: str = "",
    max_age_hours: float = DEFAULT_MAX_AGE_HOURS,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Validate and project DefiLlama pools into the local finance contract."""

    selected_chains = normalize_chains(chains)
    minimum_apy = _finite_number(min_apy, "min_apy")
    minimum_tvl = _finite_number(min_tvl_usd, "min_tvl_usd")
    maximum_apy = _finite_number(max_apy, "max_apy")
    if minimum_apy < 0 or maximum_apy < minimum_apy:
        raise ValueError("APY thresholds must satisfy 0 <= min_apy <= max_apy")
    if minimum_tvl <= 0:
        raise ValueError("min_tvl_usd must be greater than zero")
    if (
        isinstance(limit, bool)
        or not isinstance(limit, int)
        or not 1 <= limit <= MAX_LIMIT
    ):
        raise ValueError(f"limit must be an integer from 1 to {MAX_LIMIT}")
    maximum_age = _finite_number(max_age_hours, "max_age_hours")
    if maximum_age <= 0:
        raise ValueError("max_age_hours must be greater than zero")
    canonical_provider_time = validate_freshness(
        provider_updated_at, maximum_age, now=now
    )
    if not isinstance(payload, dict):
        raise ValueError("DefiLlama response must be a JSON object")
    if str(payload.get("status", "")).casefold() != "success":
        raise ValueError("DefiLlama response status is not success")
    data = payload.get("data")
    if not isinstance(data, list) or not data:
        raise ValueError("DefiLlama response data must be a non-empty list")

    allowed = {chain.casefold() for chain in selected_chains}
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in data:
        if not isinstance(item, dict):
            continue
        chain = str(item.get("chain") or "").strip()
        if chain.casefold() not in allowed:
            continue
        pool_id = str(item.get("pool") or "").strip()
        project = str(item.get("project") or "").strip()
        symbol = str(item.get("symbol") or "").strip()
        if not pool_id or not project or not symbol or pool_id in seen:
            continue
        try:
            tvl_usd = _finite_number(item.get("tvlUsd"), "tvlUsd")
            apy = _finite_number(item.get("apy"), "apy")
        except ValueError:
            continue
        if tvl_usd < minimum_tvl or apy < minimum_apy or apy > maximum_apy:
            continue
        seen.add(pool_id)
        rows.append(
            {
                "pool_id": pool_id,
                "project": project,
                "symbol": symbol,
                "chain": chain,
                "tvl_usd": tvl_usd,
                "apy": apy,
                "apy_base": _optional_number(item.get("apyBase"), "apyBase"),
                "apy_reward": _optional_number(item.get("apyReward"), "apyReward"),
                "apy_1d": _optional_number(item.get("apyPct1D"), "apyPct1D"),
                "apy_7d": _optional_number(item.get("apyPct7D"), "apyPct7D"),
                "apy_30d": _optional_number(item.get("apyPct30D"), "apyPct30D"),
                "stablecoin": _strict_bool(item.get("stablecoin")),
                "il_risk": str(item.get("ilRisk") or ""),
                "exposure": str(item.get("exposure") or ""),
                "pool_meta": str(item.get("poolMeta") or ""),
                "source": SOURCE_NAME,
                "source_url": API_URL,
                "provider_updated_at": canonical_provider_time,
            }
        )

    rows.sort(
        key=lambda row: (-float(row["apy"]), -float(row["tvl_usd"]), row["pool_id"])
    )
    return rows[:limit]


def fetch_pools(api_url: str = API_URL) -> tuple[bytes, dict[str, Any], str]:
    response = httpx.get(
        api_url,
        headers={
            # DefiLlama rejects python-httpx / custom UAs with a non-JSON Allow body.
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0"
            ),
            "Accept": "application/json",
        },
        timeout=60,
        follow_redirects=True,
    )
    response.raise_for_status()
    if not response.content:
        raise ValueError("DefiLlama response is empty")
    provider_updated_at = response.headers.get("date", "")
    if not provider_updated_at:
        raise ValueError("DefiLlama response is missing Date header")
    try:
        payload = response.json()
    except json.JSONDecodeError as exc:
        raise ValueError("DefiLlama response is not JSON") from exc
    if not isinstance(payload, dict) or payload.get("status") != "success":
        raise ValueError("DefiLlama response is missing a successful pools payload")
    return response.content, payload, normalize_provider_timestamp(provider_updated_at)


def write_raw(
    raw: bytes, provider_updated_at: str, output_dir: Path, stem: str
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{stem}_raw.json"
    payload = {"provider_updated_at": provider_updated_at, "payload": json.loads(raw)}
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def write_snapshot(
    rows: list[dict[str, Any]], captured_at: str, output_dir: Path, stem: str
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{stem}.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=SNAPSHOT_FIELDS, extrasaction="ignore"
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({**row, "captured_at": captured_at})
    return path


def append_history(
    rows: list[dict[str, Any]], captured_at: str, output_dir: Path, stem: str
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{stem}_history.csv"
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=HISTORY_FIELDS, extrasaction="ignore"
        )
        if not exists:
            writer.writeheader()
        for row in rows:
            writer.writerow({**row, "captured_at": captured_at})
    return path


class DeFiYieldScraper:
    """Scheduler adapter for bounded DefiLlama yield capture."""

    def __init__(
        self,
        chains: Iterable[str] | str | None = None,
        min_apy: float = DEFAULT_MIN_APY,
        min_tvl_usd: float = DEFAULT_MIN_TVL_USD,
        max_apy: float = DEFAULT_MAX_APY,
        limit: int = DEFAULT_LIMIT,
        min_rows: int = DEFAULT_MIN_ROWS,
        max_age_hours: float = DEFAULT_MAX_AGE_HOURS,
        output_stem: str = "defi_yields",
        api_url: str = API_URL,
        output_dir: str | Path | None = None,
        **_: Any,
    ) -> None:
        self.chains = normalize_chains(chains)
        self.min_apy = _finite_number(min_apy, "min_apy")
        self.min_tvl_usd = _finite_number(min_tvl_usd, "min_tvl_usd")
        self.max_apy = _finite_number(max_apy, "max_apy")
        if self.min_apy < 0 or self.max_apy < self.min_apy:
            raise ValueError("APY thresholds must satisfy 0 <= min_apy <= max_apy")
        if self.min_tvl_usd <= 0:
            raise ValueError("min_tvl_usd must be greater than zero")
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= MAX_LIMIT
        ):
            raise ValueError(f"limit must be an integer from 1 to {MAX_LIMIT}")
        if (
            isinstance(min_rows, bool)
            or not isinstance(min_rows, int)
            or not 1 <= min_rows <= limit
        ):
            raise ValueError(f"min_rows must be an integer from 1 to {limit}")
        self.max_age_hours = _finite_number(max_age_hours, "max_age_hours")
        if self.max_age_hours <= 0:
            raise ValueError("max_age_hours must be greater than zero")
        if not OUTPUT_STEM_RE.fullmatch(output_stem):
            raise ValueError(
                "output_stem must contain only lowercase letters, numbers, and underscores"
            )
        self.limit = limit
        self.min_rows = min_rows
        self.output_stem = output_stem
        self.api_url = api_url
        self.output_dir = Path(output_dir) if output_dir else OUTPUT_DIR

    async def run(self, **_: Any) -> list[dict[str, Any]]:
        raw, payload, provider_updated_at = fetch_pools(self.api_url)
        rows = parse_payload(
            payload,
            self.chains,
            min_apy=self.min_apy,
            min_tvl_usd=self.min_tvl_usd,
            max_apy=self.max_apy,
            limit=self.limit,
            provider_updated_at=provider_updated_at,
            max_age_hours=self.max_age_hours,
        )
        if len(rows) < self.min_rows:
            raise ValueError(
                f"DefiLlama capture produced only {len(rows)} pools; need at least {self.min_rows}"
            )
        captured_at = _utc_now()
        raw_path = write_raw(
            raw, provider_updated_at, self.output_dir, self.output_stem
        )
        snapshot_path = write_snapshot(
            rows, captured_at, self.output_dir, self.output_stem
        )
        history_path = append_history(
            rows, captured_at, self.output_dir, self.output_stem
        )
        print(f"[{self.output_stem}] {len(rows)} pools -> {snapshot_path}")
        return [
            {
                "source": self.output_stem,
                "count": len(rows),
                "chains": self.chains,
                "provider_updated_at": provider_updated_at,
                "output": str(snapshot_path),
                "history": str(history_path),
                "raw": str(raw_path),
            }
        ]


if __name__ == "__main__":
    import asyncio

    asyncio.run(DeFiYieldScraper().run())
