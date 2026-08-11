# book-defi-data

DeFi yield pools (DeFiLlama free yields API).

Migration status: **lake-first** via shared Solo Empire
`data_lake.product_adapter` (same path as crypto / stock / fx).

Extra guarantees:

- stable `pool_id` / `source_record_id` (native DeFiLlama id or deterministic hash)
- chain + protocol lineage on every Bronze row
- snapshot `event_time`
- in-batch dedupe when the same pool is returned twice
- replay from landing bytes

## Data contract

| Field | Value |
|---|---|
| `source` | `book-defi-data` |
| `domain` | `market` |
| `dataset` (snapshot) | `defi_yields` |
| `dataset` (history) | `defi_yields_history` |
| Bronze `schema_version` | `1` |
| Product envelope | `defi.v1` |
| port | `8104` |

```text
DeFiLlama /pools
    → landing/source=book-defi-data/.../payload.json
    → bronze/domain=market/dataset=defi_yields|defi_yields_history/...
    → control/manifests/
    → HTTP API  ← DuckDB Bronze only
    → data/*.csv  (optional CLI projection)
```

Shared modules: `infra/scripts/data_lake/product_adapter.py`, `product_store.py`.

## Optional Iceberg REST catalog read pilot

DeFi uses the shared `product_store` feature flag. Direct Bronze Parquet remains
the default:

| Variable | Default | Meaning |
|---|---|---|
| `LAKE_READ_MODE` | `parquet` | Direct Bronze read; `iceberg` resolves the registered catalog table and queries it with DuckDB |
| `LAKE_READ_FALLBACK` | `error` | Fail-closed by default; `parquet` is an explicit fallback |
| `ICEBERG_CATALOG_URI` | unset | REST/SQL catalog URI from the runtime secret provider |
| `ICEBERG_WAREHOUSE_URI` | unset | Provider-issued warehouse URI/name |
| `ICEBERG_CATALOG_TOKEN` | unset | Catalog credential; never store it in the repository or API response |

The matching tables must be registered before enabling Iceberg mode:
`bronze.market_defi_yields` and `bronze.market_defi_yields_history`.

```bash
npx --yes @infisical/cli@0.43.120 run \
  --projectId=<solo-empire-project-id> --env=dev --path=/cloudflare -- \
  bash -lc 'export SOLO_EMPIRE_DATA_LAKE_URI=s3://<bucket>/<prefix>; \
    export LAKE_READ_MODE=iceberg; export LAKE_READ_FALLBACK=error; \
    PYTHONPATH=src /path/to/solo-empire/.venv/bin/python \
    -m book_defi.api --host 127.0.0.1 --port 8104'
```

Successful `/v1/metadata` reports
`storage_model=lake_first_iceberg_rest_duckdb` and both source kinds as
`iceberg_rest_duckdb`. CSV remains CLI-only and is not a fallback unless the
explicit `LAKE_READ_FALLBACK=parquet` flag is selected.

### Live R2 parity evidence

Verified on 2026-08-08 with bounded prefix
`portfolio-demo/book-defi-iceberg-001`:

| Check | Result |
|---|---|
| Tables | `bronze.market_defi_yields`, `bronze.market_defi_yields_history` |
| Rows | 2 yields + 2 history |
| Iceberg vs direct Parquet | parity passed for both datasets |
| Raw bytes and lineage | exact / passed |
| Registration retry | `already_registered` for both |
| Snapshot retry | idempotent for both |
| API storage model | `lake_first_iceberg_rest_duckdb` |
| CSV dependency | none |

The fixture is intentionally old, so `data_status=stale` is expected. The
default `LAKE_READ_MODE=parquet` remains unchanged.

## Quick start

```bash
pip install -e .
pip install -r <solo-empire>/infra/requirements-data-lake.txt

python -m book_defi.ingest --fixture
python -m book_defi.ingest --chains Ethereum,Arbitrum --min-apy 5 \
  --data-lake-uri /path/to/data/lake
python -m book_defi.api --host 127.0.0.1 --port 8104
```

Lake failure exits non-zero and does **not** update CSV.

## API

| Method | Path | Notes |
|---|---|---|
| GET | `/healthz` | Bronze status |
| GET | `/v1/metadata` | `storage_model=lake_first_bronze_duckdb` (Parquet) or `lake_first_iceberg_rest_duckdb` (Iceberg flag) |
| GET | `/v1/records` | Latest `defi_yields` (deduped by pool) |
| GET | `/v1/history` | Full `defi_yields_history` |
| POST | `/v1/refresh` | **403 by default** |

## Free-only

| Provider | Status |
|---|---|
| `defillama_public` | free |
| `defillama_pro` | blocked |

## Tests

```bash
PYTHONPATH=src /path/to/solo-empire/.venv/bin/python -m unittest discover -s tests -v
```
