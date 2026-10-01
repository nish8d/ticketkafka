from datetime import UTC, datetime, timedelta

from pipeline.dashboard import format_duration, pivot_stats
from pipeline.models import CATEGORIES

T0 = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)
T1 = T0 + timedelta(minutes=5)


def test_format_duration():
    assert format_duration(None) == "—"  # no tickets loaded in the last hour
    assert format_duration(0.25) == "250 ms"
    assert format_duration(12.34) == "12.3 s"
    assert format_duration(150) == "2.5 min"


def test_pivot_stats_aligns_windows_and_fills_gaps_with_zero():
    # The aggregator writes no row for a category with no tickets in a window.
    rows = [(T1, "billing", 4), (T0, "billing", 2), (T0, "technical", 1)]
    windows, series = pivot_stats(rows, CATEGORIES)
    assert windows == [T0, T1]
    assert series == {"billing": [2, 4], "technical": [1, 0], "account": [0, 0], "other": [0, 0]}
    assert list(series) == list(CATEGORIES)  # stable order, so colours don't jump between refreshes


def test_pivot_stats_keeps_unexpected_values_at_the_end():
    windows, series = pivot_stats([(T0, "refunds", 1)], ("billing",))
    assert windows == [T0]
    assert series == {"billing": [0], "refunds": [1]}


def test_pivot_stats_of_nothing():
    assert pivot_stats([], ("billing",)) == ([], {"billing": []})
