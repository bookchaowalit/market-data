"""ECB reference-rate calendar, independent of capture/transport freshness."""
from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

FRANKFURT = ZoneInfo("Europe/Berlin")
PUBLICATION_CUTOFF = time(16)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _easter(year: int) -> date:
    """Gregorian Easter Sunday (Meeus/Jones/Butcher computus)."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    weekday_shift = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * weekday_shift) // 451
    month, day = divmod(h + weekday_shift - 7 * m + 114, 31)
    return date(year, month, day + 1)


def _publishing_day(day: date) -> bool:
    # TARGET closures only; national and ECB office holidays do not all close TARGET.
    easter = _easter(day.year)
    return day.weekday() < 5 and (day.month, day.day) not in {
        (1, 1), (5, 1), (12, 25), (12, 26),
    } and day not in {easter - timedelta(days=2), easter + timedelta(days=1)}


def expected_rate_date(now: datetime) -> date:
    """Last expected publication, with a 16:00 Frankfurt-local policy cutoff.

    ECB says *around* 16:00; this deterministic cutoff is not a provider SLA.
    """
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("ECB freshness requires a timezone-aware clock")
    local = now.astimezone(FRANKFURT)
    day = local.date()
    if local.time() < PUBLICATION_CUTOFF:
        day -= timedelta(days=1)
    while not _publishing_day(day):
        day -= timedelta(days=1)
    return day


def apply_ecb_freshness(
    payload: dict[str, Any], *, now: datetime | None = None, history: bool = False,
) -> dict[str, Any]:
    """Replace event-age status only for readable data; preserve failure states.

    Snapshot quotes must each be current. Historical datasets instead use their
    newest rate date: older history points are intentionally retained.
    """
    if payload.get("data_status") not in {"ok", "stale"} or not payload.get("items"):
        return payload
    clock = now if now is not None else _now()
    expected = expected_rate_date(clock)
    today = clock.astimezone(FRANKFURT).date()
    try:
        dates = [date.fromisoformat(item["date"]) for item in payload["items"]]
        # fromisoformat also accepts compact/week forms; the API contract is YYYY-MM-DD.
        if any(item["date"] != day.isoformat() for item, day in zip(payload["items"], dates)):
            raise ValueError("non-canonical provider rate date")
        if any(day > today for day in dates):
            raise ValueError("provider rate date is in the future")
    except (KeyError, TypeError, ValueError):
        return {**payload, "data_status": "malformed"}
    observed = max(dates) if history else min(dates)
    return {**payload, "data_status": "stale" if observed < expected else "ok"}
