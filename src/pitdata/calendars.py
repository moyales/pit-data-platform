"""Exchange trading sessions, as plain dates (D-033).

`pandas_market_calendars` is the one maintained library whose NYSE calendar
matches the real Fama-French file on every date from 1926: all 1,158
Saturday sessions before 1952-05-24, and the pre-1970 holidays that
`exchange_calendars` treats as trading days. Agreement between the two shows
they are consistent, not that either is right.

pandas stays inside this module: callers get a frozenset of `datetime.date`.
The library is imported only when sessions are needed, because importing it
takes over a second and `pit get` never needs it.
"""

from datetime import date
from functools import cache

# Our name -> the library's calendar name.
_CALENDARS = {"NYSE": "NYSE"}


def known_calendars() -> list[str]:
    return sorted(_CALENDARS)


@cache
def sessions(calendar: str, start: date, end: date) -> frozenset[date]:
    """The trading sessions of `calendar` from `start` to `end`, inclusive."""
    if calendar not in _CALENDARS:
        raise ValueError(f"unknown calendar {calendar!r}; known: {known_calendars()}")
    if end < start:
        raise ValueError(f"end {end} is before start {start}")
    import pandas_market_calendars as pmc

    days = pmc.get_calendar(_CALENDARS[calendar]).valid_days(
        start_date=start.isoformat(), end_date=end.isoformat()
    )
    # valid_days returns midnight UTC timestamps, so .date() is the session date.
    return frozenset(d.date() for d in days)
