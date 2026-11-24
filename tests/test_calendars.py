"""The NYSE calendar models the history the Fama-French file depends on (D-033).

The calendar is computed locally from rules, so these tests need no network.
Each date is a documented event; together they show the library models the
old history that `exchange_calendars` does not.
"""

from datetime import date

import pytest

from pitdata.calendars import known_calendars, sessions

NYSE = sessions("NYSE", date(1926, 7, 1), date(2026, 12, 31))


@pytest.mark.parametrize(
    ("day", "is_session", "why"),
    [
        (date(1952, 5, 17), True, "Saturday sessions before June 1952"),
        (date(1952, 5, 24), True, "the last Saturday session"),
        (date(1952, 5, 31), False, "no Saturday sessions after"),
        (date(1933, 3, 3), True, "the day before the 1933 bank holiday"),
        (date(1933, 3, 6), False, "1933 bank holiday"),
        (date(1933, 3, 14), False, "1933 bank holiday, last day"),
        (date(1933, 3, 15), True, "reopening after the bank holiday"),
        (date(1968, 6, 11), True, "Tuesday during the 1968 paperwork crisis"),
        (date(1968, 6, 12), False, "Wednesday closures in 1968"),
        (date(2001, 9, 11), False, "September 11"),
        (date(2001, 9, 17), True, "reopening after September 11"),
        (date(2012, 10, 29), False, "Hurricane Sandy"),
        (date(2024, 1, 1), False, "New Year's Day"),
        (date(2024, 1, 2), True, "an ordinary Tuesday"),
    ],
)
def test_known_history(day, is_session, why):
    assert (day in NYSE) is is_session, why


def test_sessions_are_plain_dates_within_the_range():
    days = sessions("NYSE", date(2024, 1, 1), date(2024, 1, 10))
    assert sorted(days) == [date(2024, 1, d) for d in (2, 3, 4, 5, 8, 9, 10)]
    assert all(type(d) is date for d in days)


def test_unknown_calendar_and_reversed_range_raise():
    with pytest.raises(ValueError, match="unknown calendar"):
        sessions("LSE", date(2024, 1, 1), date(2024, 1, 2))
    with pytest.raises(ValueError, match="before start"):
        sessions("NYSE", date(2024, 1, 2), date(2024, 1, 1))


def test_known_calendars():
    assert known_calendars() == ["NYSE"]
