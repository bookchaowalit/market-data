# market-data

Market data products in one repository: each source collects from a free public
API, writes the exact response bytes to the shared lake (landing + Bronze
Parquet through `data-lake-core`) and serves a read-only HTTP API that queries
the lake with DuckDB. CSV files are optional CLI projections only.

| Source | Folder | Package / CLI | Provider | Envelope |
|---|---|---|---|---|
| Crypto prices, trending, OHLCV, funding | [`sources/crypto`](sources/crypto) | `book_crypto` / `book-crypto-data` | CoinGecko, Binance public | `crypto.v1` |
| Stock prices | [`sources/stock`](sources/stock) | `book_stock` / `book-stock-data` | Yahoo public chart | `stock.v1` |
| FX rates | [`sources/fx`](sources/fx) | `book_fx` / `book-fx-data` | Frankfurter (ECB) | `fx.v1` |
| DeFi yields | [`sources/defi`](sources/defi) | `book_defi` / `book-defi-data` | DeFiLlama | `defi.v1` |

The data product ids (`book-crypto-data`, `book-stock-data`, `book-fx-data`,
`book-defi-data`) and package names are contract ids and stay unchanged.

## History

Merged on 2026-10-13 from `market-crypto-data`, `market-stock-data`,
`market-fx-rate-data` and `market-defi-yield-data` (now archived) with their
full git history, rewritten under `sources/<name>/`. Shared modules
(`fsutil`, `http_client`, most of `lake`, `policy`) are still duplicated per
source; extracting them is the next step.

## Develop and test

Each source is its own Python package with its own `uv.lock` (uv 0.13+). CI runs the
same steps for every source on Python 3.11 and 3.13 (matrix in `.github/workflows/ci.yml`):

```bash
cd sources/crypto
uv sync --frozen --all-extras      # lake extra (pinned data-lake-core) + dev tools
uv run ruff check . && uv run ruff format --check .
uv run mypy                        # scope and strictness in [tool.mypy]
FREE_ONLY=true uv run python -m pytest -q -rs --cov   # offline; no provider calls
```

Error reporting is opt-in: set `SENTRY_DSN` (and optionally `SENTRY_ENVIRONMENT`) for a
scheduled `python -m book_<source>.ingest` run, or for `sources/defi/scripts/run_defi.py`.
Events carry the source id, a stage label and the exception type only (no payloads, no
exception messages, no frame locals). See each source's README for its data contract,
lake layout and API, and `PRODUCT.md` for what runs today.

## License

Apache-2.0
