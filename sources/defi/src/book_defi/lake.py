"""Lake-first adapter for book-defi-data (shared product_adapter contract).

Extra guarantees vs FX/stock:
- stable ``source_record_id`` / ``id`` per protocol pool
- chain + protocol lineage fields on every Bronze row
- snapshot ``event_time``
- dedupe when the same pool appears twice in one fetch
"""
from __future__ import annotations

import hashlib
import importlib.util
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from . import config
from . import quality


def _load_shared():
    cur = config.PROJECT_ROOT.resolve()
    candidates = [cur, *cur.parents]
    if config.SOLO_EMPIRE_ROOT:
        # Explicit parent checkout (e.g. sibling clone) wins over the walk.
        candidates.insert(0, Path(config.SOLO_EMPIRE_ROOT).expanduser().resolve())
    for parent in candidates:
        scripts = parent / "infra" / "scripts"
        if (scripts / "data_lake" / "product_adapter.py").is_file():
            if str(scripts) not in sys.path:
                sys.path.insert(0, str(scripts))
            break
    from data_lake import product_adapter as pa  # type: ignore
    return pa


_pa = None


def _pa_mod():
    global _pa
    if _pa is None:
        _pa = _load_shared()
    return _pa


class LakeUnavailable(RuntimeError):
    pass


class LakeIngestError(RuntimeError):
    pass


def _sync_exc():
    pa = _pa_mod()
    global LakeUnavailable, LakeIngestError
    LakeUnavailable = pa.LakeUnavailable  # type: ignore
    LakeIngestError = pa.LakeIngestError  # type: ignore


def _contract():
    pa = _pa_mod()
    _sync_exc()
    return pa.LakeProductContract(
        source=config.LAKE_SOURCE,
        domain=config.LAKE_DOMAIN,
        product_schema_version=config.SCHEMA_VERSION,
        privacy_class=config.LAKE_PRIVACY_CLASS,
        retention_class=config.LAKE_RETENTION_CLASS,
        bronze_schema_version=config.LAKE_BRONZE_SCHEMA_VERSION,
        project_root=config.PROJECT_ROOT,
        data_lake_uri=config.DATA_LAKE_URI,
        solo_empire_root=config.SOLO_EMPIRE_ROOT,
        lineage_filename=config.LINEAGE_FILE,
        datasets=(config.LAKE_DATASET_YIELDS, config.LAKE_DATASET_HISTORY),
    )


def utc_now_iso() -> str:
    """UTC now as ``YYYY-MM-DDTHH:MM:SSZ`` (same format as the shared adapter).

    Kept local so projections and identity helpers work without the lake runtime.
    """
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def find_solo_empire_root(start: Optional[Path] = None) -> Optional[Path]:
    """Return the Solo Empire root, or ``None`` when the shared adapter is absent."""
    try:
        pa = _pa_mod()
    except ImportError:
        return None
    return pa.find_solo_empire_root(
        start or config.PROJECT_ROOT,
        solo_empire_root=config.SOLO_EMPIRE_ROOT,
    )


def shared_runtime_available() -> bool:
    """Return True when the shared ``data_lake`` runtime can be imported.

    Covers the installed ``[lake]`` extra (``solo-empire-data-lake``) and the
    ``SOLO_EMPIRE_ROOT`` / parent-checkout fallback used inside Solo Empire.
    """
    if importlib.util.find_spec("data_lake") is not None:
        return True
    try:
        _pa_mod()
    except ImportError:
        return False
    return True


def default_data_lake_uri(solo_root: Optional[Path] = None) -> str:
    del solo_root
    return _pa_mod().default_data_lake_uri(
        data_lake_uri=config.DATA_LAKE_URI,
        project_root=config.PROJECT_ROOT,
        solo_empire_root=config.SOLO_EMPIRE_ROOT,
    )


_SLUG_RE = re.compile(r"[^a-zA-Z0-9._-]+")


def _slug(value: str) -> str:
    text = _SLUG_RE.sub("-", str(value or "").strip().lower())
    text = re.sub(r"-+", "-", text).strip("-.")
    return text or "unknown"


def stable_pool_id(pool: dict[str, Any]) -> str:
    """Deterministic pool identity for source_record_id / dedupe.

    Preference order:
    1. DeFiLlama native ``pool`` UUID/slug when present
    2. Stable hash of chain + project + symbol + underlying tokens
    """
    native = str(pool.get("pool") or pool.get("pool_id") or "").strip()
    if native:
        # Keep native ids path-safe for /v1/records/{id}
        return native.replace("/", "_")

    chain = _slug(pool.get("chain", ""))
    project = _slug(pool.get("project") or pool.get("protocol") or "")
    symbol = _slug(pool.get("symbol", ""))
    underlying = pool.get("underlyingTokens") or pool.get("underlying") or []
    if isinstance(underlying, list):
        under = ",".join(_slug(str(t)) for t in underlying)
    else:
        under = _slug(str(underlying))
    seed = f"{chain}|{project}|{symbol}|{under}"
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]
    return f"hash:{chain}:{project}:{digest}"


def _metric(value: Any, *, non_negative: bool = False) -> Any:
    """Keep finite metric values (and ``None``); blank NaN/inf/non-numeric ones.

    With ``non_negative`` a negative value (e.g. TVL) is blanked too.
    """
    if value is None or value == "":
        return value
    number = quality.finite_number(value)
    if number is None or (non_negative and number < 0):
        return ""
    return value


def yield_records_from_pools(
    pools: list[dict[str, Any]],
    *,
    event_time: Optional[str] = None,
    dedupe: bool = True,
) -> list[dict[str, Any]]:
    """Normalize DeFiLlama pool objects into Bronze-ready records.

    When ``dedupe`` is True, later rows with the same stable pool id replace
    earlier ones within the same batch (last-write wins).
    """
    received = event_time or utc_now_iso()
    # Parse provider timestamps when present (unix seconds).
    by_id: dict[str, dict[str, Any]] = {}
    ordered: list[dict[str, Any]] = []

    for pool in pools:
        if not isinstance(pool, dict):
            continue
        pool_id = stable_pool_id(pool)
        chain = str(pool.get("chain") or "").strip()
        project = str(pool.get("project") or pool.get("protocol") or "").strip()
        symbol = str(pool.get("symbol") or "")[:30]

        row_event = received
        for key in ("timestamp", "updatedAt", "createdAt"):
            raw_ts = pool.get(key)
            if raw_ts is None:
                continue
            try:
                ts_int = int(raw_ts)
                # ms vs s
                if ts_int > 10_000_000_000:
                    ts_int = ts_int // 1000
                row_event = (
                    datetime.fromtimestamp(ts_int, tz=timezone.utc)
                    .replace(microsecond=0)
                    .isoformat()
                    .replace("+00:00", "Z")
                )
                break
            except (TypeError, ValueError, OSError, OverflowError):
                continue

        reward_tokens = pool.get("rewardTokens") or []
        if isinstance(reward_tokens, list):
            reward_str = ",".join(str(t) for t in reward_tokens)
        else:
            reward_str = str(reward_tokens)

        record = {
            "id": pool_id,
            "pool_id": pool_id,
            "chain": chain,
            "protocol": project,
            "project": project,
            "symbol": symbol,
            "tvl_usd": _metric(pool.get("tvlUsd", pool.get("tvl_usd", 0)), non_negative=True),
            "apy": _metric(pool.get("apy", 0)),
            "apy_base": _metric(pool.get("apyBase", pool.get("apy_base", 0))),
            "apy_reward": _metric(pool.get("apyReward", pool.get("apy_reward", 0))),
            "reward_tokens": reward_str,
            "category": pool.get("category", ""),
            "url": pool.get("url", ""),
            "event_time": row_event,
            # Lineage-friendly metadata kept in payload for API consumers.
            "lineage_chain": chain,
            "lineage_protocol": project,
        }
        if dedupe:
            if pool_id in by_id:
                # Replace previous occurrence (same batch re-fetch).
                by_id[pool_id] = record
            else:
                by_id[pool_id] = record
                ordered.append(pool_id)
        else:
            ordered.append(pool_id)
            by_id[f"{pool_id}#{len(ordered)}"] = record

    if dedupe:
        return [by_id[pid] for pid in ordered]
    return [by_id[k] for k in ordered]


def ingest_to_lake(
    *,
    raw: bytes,
    records: list[dict[str, Any]],
    dataset: str,
    data_lake_uri: Optional[str] = None,
    metadata: Optional[dict[str, Any]] = None,
    content_type: str = "application/json",
    input_format: str = "json",
) -> dict[str, Any]:
    _sync_exc()
    meta = {
        "provider": "defillama_public",
        "lineage_fields": ["chain", "protocol", "pool_id", "raw_object_key"],
    }
    if metadata:
        meta.update(metadata)
    return _pa_mod().ingest_to_lake(
        _contract(),
        raw=raw,
        records=records,
        dataset=dataset,
        data_lake_uri=data_lake_uri,
        metadata=meta,
        content_type=content_type,
        input_format=input_format,
        provider="defillama_public",
    )


def write_lineage(
    result: dict[str, Any],
    *,
    dataset: str,
    data_dir: Optional[Path] = None,
) -> Path:
    return _pa_mod().write_lineage(
        _contract(),
        result,
        dataset=dataset,
        data_dir=Path(data_dir or config.DATA_DIR),
    )


def load_lineage(data_dir: Optional[Path] = None) -> Optional[dict[str, Any]]:
    return _pa_mod().load_lineage(
        Path(data_dir or config.DATA_DIR),
        filename=config.LINEAGE_FILE,
    )


def bronze_dataset_dir(
    dataset: str,
    *,
    data_lake_uri: Optional[str] = None,
) -> Path:
    return _pa_mod().bronze_dataset_dir(
        _contract(), dataset, data_lake_uri=data_lake_uri
    )


def read_bronze_rows(
    dataset: str,
    *,
    data_lake_uri: Optional[str] = None,
    sql: str | None = None,
) -> list[dict[str, Any]]:
    _sync_exc()
    return _pa_mod().read_bronze_rows(
        _contract(), dataset, data_lake_uri=data_lake_uri, sql=sql
    )


def read_iceberg_rows(
    dataset: str,
    *,
    data_lake_uri: Optional[str] = None,
    sql: str | None = None,
) -> list[dict[str, Any]]:
    _sync_exc()
    return _pa_mod().read_iceberg_rows(
        _contract(), dataset, data_lake_uri=data_lake_uri, sql=sql
    )


def parse_payload_json(row: dict[str, Any]) -> dict[str, Any]:
    return _pa_mod().parse_payload_json(row)


def select_latest_bronze_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _pa_mod().select_latest_bronze_rows(rows)


def landing_object_bytes(raw_key: str, *, data_lake_uri: Optional[str] = None) -> bytes:
    _sync_exc()
    uri = data_lake_uri or default_data_lake_uri()
    return _pa_mod().landing_object_bytes(uri, raw_key)
