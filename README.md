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

Each source is its own Python package:

```bash
cd sources/crypto
python -m venv .venv && .venv/bin/pip install -e ".[lake]" pytest ruff
.venv/bin/python -m pytest -q
```

CI runs the same steps for every source (matrix in `.github/workflows/ci.yml`).
See each source's README for its data contract, lake layout and API.

## License

Apache-2.0
