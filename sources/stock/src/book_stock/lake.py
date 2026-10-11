"""Lake-first adapter for book-stock-data (shared product_adapter contract)."""
from __future__ import annotations

import importlib.util
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from . import config
from . import quality


def _load_shared():
    """Import the shared product adapter.

    Uses the installed ``solo-empire-data-lake`` package (the ``[lake]`` extra).
    Falls back to a solo-data-lake checkout named by ``SOLO_DATA_LAKE_ROOT``.
    """
    if importlib.util.find_spec("data_lake") is None and config.SOLO_DATA_LAKE_ROOT:
        scripts = Path(config.SOLO_DATA_LAKE_ROOT).expanduser().resolve() / "infra" / "scripts"
        if not (scripts / "data_lake" / "product_adapter.py").is_file():
            raise ImportError(
                f"SOLO_DATA_LAKE_ROOT={config.SOLO_DATA_LAKE_ROOT} has no "
                "infra/scripts/data_lake/product_adapter.py"
            )
        if str(scripts) not in sys.path:
            sys.path.insert(0, str(scripts))
    try:
        from data_lake import product_adapter as pa  # type: ignore
    except ImportError as exc:
        raise ImportError(
            "shared data_lake runtime not found: install the [lake] extra "
            "(pip install -e .[lake]) or set SOLO_DATA_LAKE_ROOT to a "
            "solo-data-lake checkout"
        ) from exc
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
        lineage_filename=config.LINEAGE_FILE,
        datasets=(config.LAKE_DATASET_PRICES, config.LAKE_DATASET_HISTORY),
    )


def utc_now_iso() -> str:
    return _pa_mod().utc_now_iso()


def shared_runtime_available() -> bool:
    """Return True when the shared ``data_lake`` runtime can be imported.

    Covers the installed ``[lake]`` extra (``solo-empire-data-lake``) and the
    ``SOLO_DATA_LAKE_ROOT`` sibling-checkout fallback.
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
    )


def quote_records_from_api(
    quotes: list[dict[str, Any]],
    *,
    event_time: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Normalize Yahoo quote dicts into Bronze-ready records."""
    records, _ = quote_records_with_report(quotes, event_time=event_time)
    return records


def _optional_number(value: Any) -> Any:
    """Keep finite numbers (and blanks); blank NaN/inf/non-numeric values."""
    if value in ("", None):
        return ""
    return value if quality.finite_number(value) is not None else ""


def quote_records_with_report(
    quotes: list[dict[str, Any]],
    *,
    event_time: Optional[str] = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Normalize Yahoo quotes and report rejected rows.

    A quote is rejected when it has no symbol, when its price is missing,
    non-numeric, NaN/inf or not positive, or when its symbol repeats. NaN/inf
    in ``prev_close``/``change``/``change_pct`` are blanked.
    """
    received = event_time or utc_now_iso()
    records: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for q in quotes:
        symbol = str(q.get("symbol", "")).strip().upper()
        if not symbol:
            rejected.append({"id": "", "reason": "missing_symbol"})
            continue
        price = quality.finite_number(q.get("price"))
        if price is None or price <= 0:
            rejected.append({"id": symbol, "reason": "invalid_price"})
            continue
        ts = str(q.get("timestamp") or "").strip()
        # Prefer market timestamp when parseable; else ingest time.
        row_event = received
        if ts:
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
                try:
                    dt = datetime.strptime(ts, fmt).replace(tzinfo=timezone.utc)
                    row_event = dt.isoformat().replace("+00:00", "Z")
                    break
                except ValueError:
                    continue
        records.append(
            {
                "id": symbol,
                "symbol": symbol,
                "price": q.get("price", ""),
                "prev_close": _optional_number(q.get("prev_close", "")),
                "change": _optional_number(q.get("change", "")),
                "change_pct": _optional_number(q.get("change_pct", "")),
                "currency": q.get("currency", ""),
                "exchange": q.get("exchange", ""),
                "timestamp": ts,
                "event_time": row_event,
            }
        )
    records, duplicates = quality.dedupe_by_key(records)
    rejected.extend({"id": r["id"], "reason": "duplicate_id"} for r in duplicates)
    return records, rejected


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
    return _pa_mod().ingest_to_lake(
        _contract(),
        raw=raw,
        records=records,
        dataset=dataset,
        data_lake_uri=data_lake_uri,
        metadata=metadata,
        content_type=content_type,
        input_format=input_format,
        provider="yahoo_public",
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


def read_silver_rows(
    dataset: str,
    *,
    data_lake_uri: Optional[str] = None,
    sql: str | None = None,
) -> list[dict[str, Any]]:
    """Read the shared Silver Parquet projection for a product dataset."""
    _sync_exc()
    try:
        from data_lake.silver import read_silver_rows as _read_shared_silver  # type: ignore

        return _read_shared_silver(
            data_lake_uri=data_lake_uri or default_data_lake_uri(),
            domain=config.LAKE_DOMAIN,
            dataset=dataset,
            silver_schema_version="1",
            sql=sql,
        )
    except (LakeUnavailable, LakeIngestError):
        raise
    except Exception as exc:  # noqa: BLE001 - normalize shared runtime errors
        raise LakeIngestError(
            f"Silver DuckDB query failed for {dataset}: {exc}"
        ) from exc


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
    """Replay exact landing payload for lineage proof."""
    _sync_exc()
    uri = data_lake_uri or default_data_lake_uri()
    return _pa_mod().landing_object_bytes(uri, raw_key)
