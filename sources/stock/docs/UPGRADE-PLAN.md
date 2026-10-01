# Upgrade plan — book-stock-data

Score: 8/10 -> 8.5/10 — quotes with NaN/inf/non-positive prices or duplicate symbols are rejected before Bronze/CSV, market time is UTC, projections are atomic; remaining gaps are packaging polish.

## Backlog

- P0: Confirm GitHub Actions `CI` is green once this work reaches `main` (it downloads the
  pinned `solo-empire-data-lake` tarball). The workflow triggers only on `main` pushes and PRs,
  so `claude/untitled-session-bhlj06` has been verified locally only; keep it required on `main`.
- P1: When `solo-empire-data-lake` moves, bump the pinned commit in `[lake]` together with
  the other book-*-data repos (same SHA everywhere).
- P2: Add a `[project.optional-dependencies] dev` extra and a `[build-system]` table so
  `pip install -e ".[dev]"` is the single documented setup.
- P2: Surface the per-run `rejected_by_reason` counts in `/v1/metadata`.

## Done in this pass (pass 4: edge cases)

- `config.env_bool` returned False for anything but a true-ish word, so
  `FREE_ONLY=` (blank line in .env/compose) or a typo silently disabled the
  free-only guard; blank/unrecognised values now keep the safe default.
- `ingest.quote_from_chart`: a present-but-null `chartPreviousClose` hid
  `previousClose` (change reported as 0); a JSON integer price beyond float
  range raised `OverflowError` (`quality.finite_number` now returns `None`);
  a tiny previous close produced an infinite `change_pct`; a non-dict `meta`
  crashed. All now degrade to a usable quote or `{}`.
- Verified: `tests/test_edge_cases.py` (all 6 behaviours fail on the old
  code); full suite 60 passed; ruff 0.15.8 + 0.16.9.
- Bumped the `[lake]` pin `68fb5a9` -> `4c24c66` (NDJSON/BOM/U+2028/double-decode fixes); 60 passed with the new package; ruff 0.15.8 + 0.16.9 clean.

## Done in this pass (pass 3)

- New `quality` module; `lake.quote_records_with_report` rejects blank symbols,
  missing/non-numeric/NaN/inf/non-positive prices and duplicate symbols, and blanks
  non-finite optional numbers; `ingest.clean_quotes` keeps CSV in step with Bronze.
- `ingest.quote_from_chart` (pure, tested) replaces inline parsing: no crash on a
  `None` price or bad previous close, and `regularMarketTime` is now UTC (was local time
  labelled UTC in Bronze `event_time`).
- Stale/market-closed test: an older market time does not replace a newer price in the
  latest selection, and the snapshot reports `stale`.
- New `fsutil` module: price CSV replaced atomically, history appended via atomic rewrite.
- `fetch_quotes_raw` uses timezone-aware `datetime.now(timezone.utc)` (was deprecated `utcnow()`).
- CLI validation for `--symbols` and `--alert-threshold`; README Quick start uses `[lake]`.
- Verified: `pytest -q -rs` 50 passed, 0 skipped (was 40) with the `[lake]` venv; ruff
  0.15.8 and 0.16.9 clean.
- Refresh auth: `/v1/refresh` compares the bearer token with `hmac.compare_digest`
  (`_refresh_token_ok`) instead of `==`, which leaked the matching prefix
  length through timing; `tests/test_refresh_token_compare.py` pins it.
- CSV projection stamps (`_projection_timestamp`) are UTC; they were host-local
  but `product_store.parse_ts` reads naive stamps as UTC, so freshness was off
  by the host offset (`tests/test_projection_timestamp_utc.py`).
- `run_live_ingest` reads previous prices *before* appending this run to
  `stock_history.csv`; it read them afterwards, so every price was compared
  with itself and "since last check" alerts never fired
  (`tests/test_alert_history_order.py`).

## Done in pass 2

- Added a `[lake]` extra pinning `solo-empire-data-lake` at `68fb5a9` (plus pyarrow/duckdb); CI
  installs `-e ".[lake]"`, asserts `data_lake` imports, and lake tests now run instead of skipping.
- Test guards use `lake.shared_runtime_available()` (`importlib.util.find_spec("data_lake")`,
  then the `SOLO_EMPIRE_ROOT` / parent-checkout fallback) instead of `find_solo_empire_root()`.
- `[tool.ruff.lint] select = ["E4", "E7", "E9", "F"]` pins the classic rule set: unpinned
  ruff 0.16 widened its defaults and would have failed `ruff check .` in CI.
- `tests/test_fixture_headers.py` pins `fixtures/*.csv` headers to the CSV projection.
- `SharedAdapterTests` now checks the adapter via the installed runtime instead of skipping outside the parent.
- Verified: fresh venv `pip install -e ".[lake]"` from the pinned tarball — 0 skipped of 40 with `[lake]` (was 26 of 39 skipped);
  also against `pip install /home/user/solo-empire-data-lake`, with `SOLO_EMPIRE_ROOT=<parent>`
  (parent adapter wins), and without the extra (lake tests skip with a reason). ruff 0.15 and 0.16 clean.

## Done in pass 1

- `lake.find_solo_empire_root()` now returns `None` when the shared `data_lake`
  adapter cannot be imported (it raised `ModuleNotFoundError` before), and the
  adapter loader also honours `SOLO_EMPIRE_ROOT` so a sibling clone of the parent
  repo works without editing `PYTHONPATH`.
- CI: installs `pytest ruff`, runs `ruff check .` and `python -m pytest -q -rs`
  (skip reasons visible) instead of bare `unittest`.
- Fixed the remaining default-rule ruff findings; `.gitignore` covers egg-info/ruff cache.
- README "Tests" section documents the standalone and full-lake commands.
- Verified: clean venv with `pip install -e . pytest` (CI shape) and a full run with
  `SOLO_EMPIRE_ROOT=<parent>` + pyarrow/duckdb (all lake tests execute and pass).
