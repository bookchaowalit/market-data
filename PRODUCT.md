# market-data

Four free-only market data products in one repository. Each source fetches a free public
API, lands the exact response bytes plus normalized Bronze Parquet in the shared lake
(through `solo-empire-data-lake`, data-lake-core), optionally projects CSV, and serves a
loopback read-only HTTP API over the lake with DuckDB.

| Product (contract id) | Source | Port | Envelope `schema_version` |
|---|---|---|---|
| `book-crypto-data` | `sources/crypto` (CoinGecko, Binance public) | 8101 | `crypto.v1` |
| `book-stock-data` | `sources/stock` (Yahoo public chart) | 8102 | `stock.v1` |
| `book-fx-data` | `sources/fx` (Frankfurter / ECB) | 8103 | `fx.v1` |
| `book-defi-data` | `sources/defi` (DefiLlama) | 8104 | `defi.v1` |

## Runtime status (verified 2026-10-22)

| What | Status |
|---|---|
| Entry points | `python -m book_<source>.ingest` / `book-<source>-data` (ingest job), `book-<source>-data-api` (loopback API), defi also `scripts/run_defi.py` (snapshot capture under `data/exported`) |
| Who schedules ingest | data-lake-pipelines `run_market_capture.py` through its Windows task control plane (`windows_scraping_scheduler.py`): crypto every 4 h, fx every 6 h, stocks daily 08:00, defi daily 07:00. Whether those Windows tasks are installed and running cannot be verified from WSL [ต้องยืนยัน] |
| Who schedules `run_defi.py` | `sources/defi/setup_cron.sh install` would add a daily 07:00 crontab entry. This WSL host's crontab has no scraper entries (2026-10-22), so it is not installed here |
| Hosted deployment | None. The APIs bind 127.0.0.1 by default; no cloud job |
| Error reporting | Sentry, opt-in: `sentry-sdk` is a dependency (and in `sources/defi/requirements-collect.txt` for the cron venv); nothing is sent without `SENTRY_DSN`. A failed ingest run sends one `error` event; a logged-and-skipped part (crypto trending, a stock symbol, fx history) sends one `warning`. Scrubbed: no PII, no frame locals, no request bodies, exception type only. No DSN is configured for these products yet [ต้องยืนยัน] |
| Lake runtime | `solo-empire-data-lake[lake]` pinned at data-lake-core `e5dbb14` (CI-verified) |
| Contract | `data-product-contracts-v2.yaml` (frozen port map, envelope keys, data_status values, policy defaults), checked offline by `sources/*/tests/test_contract_v2.py` and `test_contract_freeze.py` |
| Quality gates in CI | per source on Python 3.11 and 3.13: ruff, ruff format, mypy (gradual, src only), offline pytest with coverage gate (crypto 78, stock 79, fx 77, defi 76), compileall, CLI `--help` smoke; shared security scan |

## Product contract

- Decision or user outcome: free, reproducible market snapshots (prices, rates, yields) with lineage for the insights and portfolio dashboards.
- Consumers: market-insights-web and the portfolio frontend (catalog files listed in the registry).
- Lake layers: landing (exact bytes) → Bronze Parquet + manifest → optional Silver; CSV is a CLI projection only.
- Policy: `FREE_ONLY=true`, no paid providers, no external writes, `POST /v1/refresh` returns 403 unless explicitly enabled.
- Freshness: data older than `STALE_AFTER_HOURS` (24 h) is served as `stale`.
