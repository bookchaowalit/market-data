---
id: SPEC-001
title: L2 baseline for market-data
status: approved
created: 2026-10-22
owner: bookchaowalit
---

# SPEC-001: L2 baseline

## Document control

| Field | Value |
|---|---|
| Main-only | yes (ADR-0004 v1.1: red run is local, red and green commits pushed together) |
| Maker | Claude session b-data (2026-10-22) |
| Checker / approver | independent Claude reviewer / AI approval 2026-10-24 (see Approval) |

## Problem

The Big Loop scorecard (2026-10-22) had this repo at L0: no lockfile, no type check, no
PRODUCT.md and no error reporting. Every ingest `main` catches the run failure and returns
1 or 2 with one stderr line, and partial failures (crypto trending, a stock symbol, fx
history) are printed and skipped, so a failing scheduled capture is invisible. The four
sources pinned data-lake-core `b8f8698`, which predates its CI baseline.

## Behaviour

1. **Lockfiles: one `uv.lock` per source** (`sources/<source>/uv.lock`), not a root uv
   workspace. Reasons: CI v1.1.1 installs per `working-directory` and only runs
   `uv sync --frozen --all-extras` when `uv.lock` is in that directory (a workspace keeps the
   lock and `.venv` at the root, so every job would fall back to an unlocked install); each
   source is an independent product with its own contract id, port and release cadence and is
   scheduled and installed on its own (defi's cron venv); a workspace would force one shared
   resolution across products that do not share code.
2. `solo-empire-data-lake[lake]` is pinned at data-lake-core
   `e5dbb1462e9a2c159e65565c40e1dda194699e30` (CI green: https://github.com/bookchaowalit/data-lake-core/actions/runs/39712959999),
   the same archive-URL form as career-job-market-data.
3. `mypy` runs in CI over `src/` (defi also `scripts/`), gradual; `observability.py` is strict.
   tests/ are not typed yet.
4. With `SENTRY_DSN` set, a failed ingest run sends exactly one `error` event (stage +
   exception type, tags `source_id`, `job`), a logged-and-skipped part sends one `warning`,
   a successful run sends nothing, and without a DSN Sentry is never initialised. `init` sets
   `send_default_pii=False`, `include_local_variables=False`, `max_request_body_size="never"`,
   `traces_sample_rate=0.0` and a `before_send` that drops request data and frame locals and
   replaces exception messages with the type name. `scripts/run_defi.py` reports a failed
   capture, prints the traceback and exits 1 (previously an uncaught exception, also exit 1).
5. `book_defi.lake.yield_records_from_pools(dedupe=False)` returns every occurrence.
6. Each source's API returns envelopes that meet `data-product-contracts-v2.yaml`.
7. CI fails per source when coverage drops below crypto 78, stock 79, fx 77, defi 76
   (measured 78.73 / 79.34 / 77.72 / 76.05 on 3.11 and 3.13).

## Acceptance

| AC | Given / When / Then | Test (each `sources/<source>/tests/`) |
|---|---|---|
| AC-01 | Given `SENTRY_DSN`, when `run_live_ingest` raises, then `main` returns 1 and one scrubbed `error` event (type only) is sent with the security init settings | `test_ingest_job.py::IngestJobSentryTest::test_failed_run_reaches_sentry_once` |
| AC-02 | Given `SENTRY_DSN`, when the lake write fails, then `main` returns 2 and one `error` event is sent | `test_lake_failure_exit_2_reaches_sentry_once` |
| AC-03 | Given `SENTRY_DSN`, when the run succeeds, then nothing is sent | `test_successful_run_sends_nothing` |
| AC-04 | Given no `SENTRY_DSN`, then Sentry is not initialised | `test_without_dsn_sentry_stays_off` |
| AC-05 | `before_send` removes request data, frame vars and exception text | `test_before_send_strips_frame_vars_request_and_exception_text` |
| AC-06 | A logged-and-skipped part sends one `warning` (crypto trending, stock symbol, fx history) | `test_skipped_trending_failure_is_a_warning`, `test_skipped_symbol_failure_is_a_warning`, `test_skipped_history_failure_is_a_warning` |
| AC-07 | defi cron runner: a failed capture sends one `error` event and exits 1 | defi `test_ingest_job.py::CronRunnerSentryTest::test_failed_cron_capture_reaches_sentry_and_exits_1` |
| AC-08 | `dedupe=False` keeps both occurrences; `dedupe=True` keeps the last | defi `test_yield_records.py` |
| AC-09 | Port, schema_version, loopback host, policy defaults, and every required GET endpoint plus 404 / refresh-403 envelopes match the v2 registry | `test_contract_v2.py` |
| AC-10 | CI on `main` is green for all sources on Python 3.11 and 3.13 with the coverage gates | CI run below |

## Red/green log

| AC | Red (sha · command · failure) | Green |
|---|---|---|
| AC-01..07 | `313ad0e` · `PYTHONPATH=src python -m pytest -q tests/test_ingest_job.py` (each source, detached worktree) · `AssertionError: 0 != 1`, `AssertionError: assert None is not None`, `AssertionError: Lists differ: [] != ['warning']` | https://github.com/bookchaowalit/market-data/actions/runs/39755737596 (`54b1018`, success: 8 source jobs + security) |
| AC-08 | `313ad0e` · `PYTHONPATH=src python -m pytest -q tests/test_yield_records.py` (defi) · `KeyError: 'pool-1'` (found by mypy: `ordered` held pool ids, `by_id` used `id#n` keys) | https://github.com/bookchaowalit/market-data/actions/runs/39755737596 (`54b1018`, success: 8 source jobs + security) |
| AC-09 | none (characterisation; passed first run) | https://github.com/bookchaowalit/market-data/actions/runs/39755737596 (`54b1018`, success: 8 source jobs + security) |

First push (`5804f6d`, run 39755563817) failed: CI v1.1.1's auto typecheck runs `mypy .`,
which ignores `[tool.mypy] files` and checked the untyped tests, and OSV flagged certifi
2026.7.22 (PYSEC-2026-4320) resolved from `sources/defi/requirements-collect.txt`. Fixed in
`54b1018` with `typecheck-command: mypy` and a `certifi>=2026.10.19` floor.

## Contract findings (v2 registry, offline)

- No mismatch in port map, `schema_version`, envelope keys, `data_status` values or policy
  defaults for crypto.v1 / stock.v1 / fx.v1 / defi.v1.
- `/healthz` returns a health object, not an envelope; the registry lists it as a required
  endpoint but does not say it must be an envelope [ต้องยืนยัน].
- The registry's `repository` ids (`book-crypto-data` …) are product ids; the code now lives in
  `market-data/sources/*`, which the registry `path` fields already point to.
- Bronze manifest/landing behaviour comes from data-lake-core (pinned there by
  `tests/test_lake_contract.py`).

## Schedule (planned replacement, D6)

No scheduler was changed (D6 open; `setup_cron.sh` untouched). Today:
data-lake-pipelines' Windows task control plane runs `python -m book_<source>.ingest` (crypto
`0 */4 * * *`, fx `0 */6 * * *`, stocks `0 8 * * *`, defi `0 7 * * *`), installed state not
verifiable from WSL; `sources/defi/setup_cron.sh install` would add a WSL crontab line for
`scripts/run_defi.py` at 07:00 daily, and it is not installed on this host (2026-10-22).
Planned replacement once D6 is decided: one hosted scheduled job per source on the chosen host
that runs `uv sync --frozen` then `python -m book_<source>.ingest` with the lake URI and
`SENTRY_DSN`/`SENTRY_ENVIRONMENT` set, with the same intervals; retire the duplicate defi
snapshot cron (`run_defi.py`) or fold it into the defi ingest job.

## Out of scope

Live provider calls, R2 writes, scheduler changes, the shared-module extraction noted in the
README, typing tests/, the 80% coverage floor (follow-up).

## Approval (2026-10-24)

| Field | Value |
|---|---|
| Checker | independent Claude reviewer subagent (`solo:solo-reviewer`, Sonnet, read-only, no maker context) |
| Approver | AI approval under the owner's process (approvals by the owner or AI with evidence; maker and checker are different agents). Recorded by Claude session oa-remainder on the owner's instruction "ทำที่เหลือให้หน่อย" (2026-10-24) |
| Verdict | **approved** |
| Tests run by the checker | per source with FREE_ONLY=true ALLOW_PAID_PROVIDERS=false ALLOW_EXTERNAL_WRITES=false: crypto 87, defi 66, fx 81, stock 70 passed |
| Evidence | uv.lock per source pinned to data-lake-core e5dbb14; Sentry tests in test_ingest_job.py; defi cron runner test; test_yield_records.py; test_contract_v2.py per source; coverage 78/79/77/76 matches CI. |
