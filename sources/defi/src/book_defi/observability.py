"""Opt-in Sentry for scheduled ingest runs of book-defi-data.

Off unless ``SENTRY_DSN`` is set (``sentry-sdk`` is a dependency). ``main`` turns run
failures into exit codes and logs-and-skips partial failures, so an unhandled-exception
hook never sees them; :func:`report_failure` and :func:`report_skipped` send them instead.

Security (review 2026-10-22): events carry the exception *type* and a fixed stage label,
never the exception message, provider payloads, frame locals or request bodies.
``before_send`` also scrubs any event the SDK builds itself (e.g. the excepthook).
"""

from __future__ import annotations

import os
from importlib import import_module
from types import ModuleType
from typing import Any

SOURCE_ID = "book-defi-data"

_sdk: ModuleType | None = None
_failure_reported = False


def scrub_event(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any]:
    """Drop request data and frame locals; replace exception messages with the type."""
    del hint
    event.pop("request", None)
    for value in (event.get("exception") or {}).get("values") or []:
        value["value"] = value.get("type") or "Exception"
        for frame in (value.get("stacktrace") or {}).get("frames") or []:
            frame.pop("vars", None)
    return event


def init_sentry(job: str) -> ModuleType | None:
    """Initialise Sentry for one run; return the SDK, or None when it is off."""
    global _sdk, _failure_reported
    _sdk, _failure_reported = None, False
    dsn = os.environ.get("SENTRY_DSN", "").strip()
    if not dsn:
        return None
    try:
        sdk = import_module("sentry_sdk")
    except ImportError:
        return None
    sdk.init(
        dsn=dsn,
        environment=os.environ.get("SENTRY_ENVIRONMENT", "local"),
        send_default_pii=False,
        include_local_variables=False,
        max_request_body_size="never",
        traces_sample_rate=0.0,
        before_send=scrub_event,
    )
    sdk.set_tag("source_id", SOURCE_ID)
    sdk.set_tag("job", job)
    _sdk = sdk
    return sdk


def report_failure(stage: str, exc: BaseException) -> None:
    """One error event for a failed run: stage label and exception type only."""
    global _failure_reported
    if _sdk is None:
        return
    _sdk.capture_message(
        f"{SOURCE_ID} {stage} failed: {type(exc).__name__}", level="error"
    )
    _sdk.flush(timeout=5)
    _failure_reported = True


def report_skipped(stage: str, exc: BaseException) -> None:
    """One warning event for a part of a run that was logged and skipped."""
    if _sdk is None:
        return
    _sdk.capture_message(
        f"{SOURCE_ID} {stage} skipped: {type(exc).__name__}", level="warning"
    )
    _sdk.flush(timeout=5)


def report_exit(code: int) -> int:
    """Send one error event for a non-zero exit not yet reported; return ``code``."""
    if _sdk is not None and code != 0 and not _failure_reported:
        _sdk.capture_message(
            f"{SOURCE_ID} ingest exited with code {code}", level="error"
        )
        _sdk.flush(timeout=5)
    return code
