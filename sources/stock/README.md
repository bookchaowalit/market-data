# book-stock-data

Stock market data product (Yahoo public chart endpoint, best-effort).

Migration status: **lake-first** using the shared Solo Empire
`data_lake.product_adapter` contract (same path as `book-crypto-data`).

- Producer: exact API bytes → landing → Bronze → manifest → optional CSV
- API: DuckDB over Bronze by default; guarded Silver parity mode (no CSV)
- PostgreSQL/ClickHouse: multi-engine lab only (not production)

Shared procedure (monorepo root):

- `docs/systems/data-lake-architecture.md`
- `learning/platform-engineering/app-cli-data-lake-playbook.md`
- `learning/platform-engineering/multi-engine-lake-query-lab.md`
- `infra/scripts/data_lake/product_adapter.py`
- `infra/scripts/data_lake/product_store.py`

## Data contract

| Field | Value |
|---|---|
| `source` | `book-stock-data` |
| `domain` | `market` |
| `dataset` (snapshot) | `stock_prices` |
| `dataset` (history) | `stock_history` |
| Bronze `schema_version` | `1` |
| Product envelope | `stock.v1` |
| `privacy_class` | `public` |
| `retention_class` | `operational` |

```text
Yahoo chart API
    → landing/source=book-stock-data/.../payload.json
    → bronze/domain=market/dataset=stock_prices|stock_history/...
    → control/manifests/
    → HTTP API  ← DuckDB query of Bronze
    → data/*.csv  (optional CLI projection only)
```

## Optional Iceberg REST catalog read pilot

Stock uses the shared `product_store` feature flag. Direct Bronze Parquet is
still the default:

| Variable | Default | Meaning |
|---|---|---|
| `LAKE_READ_MODE` | `parquet` | Direct Bronze read; `iceberg` resolves the registered catalog table and queries it with DuckDB |
| `LAKE_READ_FALLBACK` | `error` | Fail-closed by default; `parquet` is an explicit fallback |
| `ICEBERG_CATALOG_URI` | unset | REST/SQL catalog URI from the runtime secret provider |
| `ICEBERG_WAREHOUSE_URI` | unset | Provider-issued warehouse URI/name |
| `ICEBERG_CATALOG_TOKEN` | unset | Catalog credential; never store it in the repository or API response |

The matching tables must be registered before enabling Iceberg mode:
`bronze.market_stock_prices` and `bronze.market_stock_history`.

```bash
npx --yes @infisical/cli@0.43.120 run \
  --projectId=<solo-empire-project-id> --env=dev --path=/cloudflare -- \
  bash -lc 'export SOLO_EMPIRE_DATA_LAKE_URI=s3://<bucket>/<prefix>; \
    export LAKE_READ_MODE=iceberg; export LAKE_READ_FALLBACK=error; \
    PYTHONPATH=src /path/to/solo-empire/.venv/bin/python \
    -m book_stock.api --host 127.0.0.1 --port 8102'
```

Successful `/v1/metadata` reports
`storage_model=lake_first_iceberg_rest_duckdb` and both source kinds as
`iceberg_rest_duckdb`. CSV remains CLI-only and is not a fallback unless the
explicit `LAKE_READ_FALLBACK=parquet` flag is selected.

### Live R2 parity evidence

Verified on 2026-08-07 with bounded prefix
`portfolio-demo/book-stock-iceberg-001`:

| Check | Result |
|---|---|
| Tables | `bronze.market_stock_prices`, `bronze.market_stock_history` |
| Rows | 2 prices + 2 history |
| Iceberg vs direct Parquet | parity passed for both datasets |
| Raw bytes and lineage | exact / passed |
| Registration retry | `already_registered` for both |
| Snapshot retry | idempotent for both |
| API storage model | `lake_first_iceberg_rest_duckdb` |
| CSV dependency | none |

The fixture is intentionally old, so `data_status=stale` is expected. The
default `LAKE_READ_MODE=parquet` remains unchanged.

## Silver parity/serving pilot

The shared `product_store` supports `SILVER_READ_MODE=bronze|compare|silver`:

| Value | Behavior |
|---|---|
| `bronze` | Read and serve Bronze; default |
| `compare` | Read Silver and report `silver_parity`, but serve Bronze |
| `silver` | Serve current-state Silver only after parity passes; fail-closed otherwise |

Generate `stock.silver.v1` from `stock_prices` Bronze with the shared
`infra/scripts/data_lake/silver.py` command documented in the monorepo
[free-cloud runbook](../../../../../../../../../docs/operations/FREE-CLOUD-E2E-RUNBOOK.md),
then run `compare` and inspect `/v1/metadata` for
`silver_parity.status=passed`. `stock.silver.v1` is a deduplicated snapshot;
`/v1/history` remains on `stock_history` Bronze until a separate Silver history
contract exists. CSV is never used by the HTTP API.

## Quick start

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e .
# Lake writes need monorepo data-lake runtime
pip install -r <solo-empire>/infra/requirements-data-lake.txt

python -m book_stock.ingest --fixture
python -m book_stock.ingest --symbols AAPL,MSFT --data-lake-uri /path/to/data/lake
python -m book_stock.api --host 127.0.0.1 --port 8102
```

Environment:

| Variable | Purpose |
|---|---|
| `SOLO_EMPIRE_DATA_LAKE_URI` / `DATA_LAKE_URI` | Lake root |
| `SOLO_EMPIRE_ROOT` | Monorepo root if not discovered |
| `DATA_DIR` | Optional CSV projection dir |

Live ingest exits non-zero on lake failure and does **not** update CSV.

## Local read-only API

Default bind: `127.0.0.1:8102`.

| Method | Path | Notes |
|---|---|---|
| GET | `/healthz` | Bronze data status |
| GET | `/v1/metadata` | Bronze by default; reports `silver_parity` during the pilot |
| GET | `/v1/records` | Latest from Bronze or guarded `stock_prices` Silver |
| GET | `/v1/history` | Full `stock_history` Bronze |
| POST | `/v1/refresh` | **403 by default** |

## Free-only defaults

```text
FREE_ONLY=true
ALLOW_PAID_PROVIDERS=false
ALLOW_EXTERNAL_WRITES=false
API_HOST=127.0.0.1
API_PORT=8102
ALLOW_REFRESH=false
```

| Provider | Classification |
|---|---|
| `yahoo_public` | free (best-effort; unofficial) |
| paid market data APIs | blocked |

## Tests

```bash
PYTHONPATH=src /path/to/solo-empire/.venv/bin/python -m unittest discover -s tests -v
```

Coverage: contract freeze, free-only policy, lake write ordering, fail-closed,
API without CSV, Bronze/Silver parity, and lineage/replay from landing.

## Safety

- API GET handlers read Bronze via DuckDB only.
- No secrets in source or fixtures.
- Do not enable public bind or `ALLOW_REFRESH` without owner approval.
