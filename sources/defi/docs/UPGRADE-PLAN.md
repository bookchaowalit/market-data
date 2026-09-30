# Upgrade plan — book-defi-data

Score: 8/10 -> 8.5/10 — NaN/inf/non-numeric APY or TVL no longer slips through the pool filter, bad metrics are blanked before Bronze, projections are atomic and CLI input is validated; remaining gaps are packaging polish.

## Backlog

- P0: Confirm the first CI run on GitHub is green (it now downloads the pinned
  `solo-empire-data-lake` tarball); keep it required on `main`.
- P1: When `solo-empire-data-lake` moves, bump the pinned commit in `[lake]` together with
  the other book-*-data repos (same SHA everywhere).
- P2: Add a `[project.optional-dependencies] dev` extra and a `[build-system]` table so
  `pip install -e ".[dev]"` is the single documented setup.
- P1: Coerce numeric-string `apy`/`tvlUsd` to floats in `filter_pools` output so console
  formatting (`:.1f`) cannot raise on a string value.
- P2: Record how many pools each filter rule dropped in the landing metadata.

## Done in this pass (pass 4: edge cases)

- `config.env_bool` returned False for anything but a true-ish word, so
  `FREE_ONLY=` (blank line in .env/compose) or a typo silently disabled the
  free-only guard; blank/unrecognised values now keep the safe default.
- `scripts/scrape_defi_yields.parse_payload` aborted the whole capture when
  one pool had a NaN/string optional metric (`apyBase`, `apyPct1D`, ...);
  those are now blanked. A JSON integer beyond float range raised
  `OverflowError` past the `except ValueError`; it now drops only that pool.
  `stablecoin` used `bool()`, so the string `"false"` counted as a stablecoin.
- `quality.finite_number` returns `None` instead of raising `OverflowError`
  for out-of-range JSON integers.
- Verified: `tests/test_edge_cases.py` (all 8 fail on the old code); full
  suite 54 passed; ruff 0.15.8 + 0.16.9.
- Bumped the `[lake]` pin `68fb5a9` -> `4c24c66` (NDJSON/BOM/U+2028/double-decode fixes); 54 passed with the new package; ruff 0.15.8 + 0.16.9 clean.

## Done in this pass (pass 3)

- `filter_pools`/`detect_opportunities`/`detect_stablecoin_pools` use `quality.finite_number`:
  NaN/inf/non-numeric APY or TVL is excluded (NaN previously passed every bound), and a
  `None` symbol no longer crashes stablecoin detection.
- `lake.yield_records_from_pools` blanks non-finite metrics and negative TVL before Bronze.
- New `fsutil` module: yields CSV replaced atomically, history appended via atomic rewrite;
  the projection log now counts written (de-duplicated) rows.
- CLI: `--min-apy` must be finite and >= 0; chain/category lists de-duplicated.
- README Quick start uses `pip install -e ".[lake]"`; new "Data quality" section.
- Verified: `pytest -q -rs` 45 passed, 0 skipped (was 35) with the `[lake]` venv; ruff
  0.15.8 and 0.16.9 clean.
- Refresh auth: `/v1/refresh` compares the bearer token with `hmac.compare_digest`
  (`_refresh_token_ok`) instead of `==`, which leaked the matching prefix
  length through timing; `tests/test_refresh_token_compare.py` pins it.
- CSV projection stamps (`_projection_timestamp`) are UTC; they were host-local
  but `product_store.parse_ts` reads naive stamps as UTC, so freshness was off
  by the host offset (`tests/test_projection_timestamp_utc.py`).

## Done in pass 2

- Added a `[lake]` extra pinning `solo-empire-data-lake` at `68fb5a9` (plus pyarrow/duckdb); CI
  installs `-e ".[lake]"`, asserts `data_lake` imports, and lake tests now run instead of skipping.
- Test guards use `lake.shared_runtime_available()` (`importlib.util.find_spec("data_lake")`,
  then the `SOLO_EMPIRE_ROOT` / parent-checkout fallback) instead of `find_solo_empire_root()`.
- `[tool.ruff.lint] select = ["E4", "E7", "E9", "F"]` pins the classic rule set: unpinned
  ruff 0.16 widened its defaults and would have failed `ruff check .` in CI.
- `tests/test_fixture_headers.py` pins `fixtures/*.csv` headers to the CSV projection.
- `lake.utc_now_iso` is local (same format as the adapter), so CSV projection works without the lake runtime.
- `SharedAdapterTests` now checks the adapter via the installed runtime instead of skipping outside the parent.
- Verified: fresh venv `pip install -e ".[lake]"` from the pinned tarball — 0 skipped of 36 with `[lake]` (was 19 of 35 skipped);
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
- Tests that need the adapter (`utc_now_iso`, envelope, CSV projection helper, ordering)
  now skip cleanly without it; ordering tests pass an explicit temporary lake URI.
