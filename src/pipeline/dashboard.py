"""What the dashboard shows, as plain functions over a Postgres connection. No Streamlit here, so the
queries and the shaping can be tested on their own; the page itself is dashboard/app.py."""
from dataclasses import dataclass
from datetime import datetime

from psycopg.rows import dict_row

HEADLINE_SQL = """
SELECT count(*),
       count(*) FILTER (WHERE created_at > now() - interval '1 hour'),
       -- Medians over tickets loaded in the last hour, so a replayed backlog doesn't skew them for ever.
       extract(epoch FROM percentile_cont(0.5) WITHIN GROUP (ORDER BY enriched_at - created_at)
                          FILTER (WHERE loaded_at > now() - interval '1 hour')),
       extract(epoch FROM percentile_cont(0.5) WITHIN GROUP (ORDER BY loaded_at - enriched_at)
                          FILTER (WHERE loaded_at > now() - interval '1 hour'))
FROM tickets
"""

# The latest `windows` window starts of one dimension, oldest first.
STATS_SQL = """
SELECT window_start, value, count
FROM ticket_stats
WHERE dimension = %(dimension)s
  AND window_start >= (SELECT min(window_start) FROM (
        SELECT DISTINCT window_start FROM ticket_stats WHERE dimension = %(dimension)s
        ORDER BY window_start DESC LIMIT %(windows)s) AS recent)
ORDER BY window_start, value
"""

SENTIMENT_SQL = "SELECT category, avg(sentiment), count(*) FROM tickets GROUP BY category ORDER BY category"

LATEST_SQL = """
SELECT created_at, customer_id, category, priority, sentiment, summary
FROM tickets ORDER BY created_at DESC LIMIT %(limit)s
"""


@dataclass(frozen=True)
class Headline:
    total: int
    last_hour: int
    classify_seconds: float | None  # enriched_at - created_at: mostly LLM time
    sink_seconds: float | None      # loaded_at - enriched_at: Kafka plus the Connect sink


def _seconds(value) -> float | None:
    return None if value is None else float(value)  # extract() returns a Decimal


def fetch_headline(conn) -> Headline:
    total, last_hour, classify, sink = conn.execute(HEADLINE_SQL).fetchone()
    return Headline(total, last_hour, _seconds(classify), _seconds(sink))


def fetch_stats(conn, dimension: str, windows: int = 24) -> list[tuple[datetime, str, int]]:
    return [tuple(row) for row in conn.execute(STATS_SQL, {"dimension": dimension, "windows": windows}).fetchall()]


def fetch_sentiment(conn) -> list[tuple[str, float, int]]:
    return [(category, float(avg), count) for category, avg, count in conn.execute(SENTIMENT_SQL).fetchall()]


def fetch_latest(conn, limit: int = 20) -> list[dict]:
    with conn.cursor(row_factory=dict_row) as cur:
        return cur.execute(LATEST_SQL, {"limit": limit}).fetchall()


def format_duration(seconds: float | None) -> str:
    if seconds is None:
        return "—"
    if seconds < 1:
        return f"{seconds * 1000:.0f} ms"
    if seconds < 60:
        return f"{seconds:.1f} s"
    return f"{seconds / 60:.1f} min"


def pivot_stats(rows, values: tuple[str, ...]) -> tuple[list[datetime], dict[str, list[int]]]:
    """(window_start, value, count) rows → the window starts, and one count series per value aligned
    on them. A value with no tickets in a window has no row (the aggregator emits none), so it gets 0."""
    windows = sorted({start for start, _, _ in rows})
    position = {start: i for i, start in enumerate(windows)}
    names = [*values, *sorted({value for _, value, _ in rows} - set(values))]
    series = {name: [0] * len(windows) for name in names}
    for start, value, count in rows:
        series[value][position[start]] = count
    return windows, series
