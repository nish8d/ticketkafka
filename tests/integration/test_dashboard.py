from datetime import UTC, datetime, timedelta

import psycopg
import pytest

from pipeline import config
from pipeline.dashboard import Headline, fetch_headline, fetch_latest, fetch_sentiment, fetch_stats

pytestmark = pytest.mark.integration


@pytest.fixture
def conn():
    try:
        conn = psycopg.connect(config.POSTGRES_DSN, connect_timeout=5)
    except psycopg.OperationalError:
        pytest.skip(f"Postgres is not reachable at {config.POSTGRES_DSN} (docker compose up -d --build)")
    # Temporary tables with the sink tables' names shadow the real ones for this session only, so the
    # queries run against known rows and live data is never touched. Nothing is committed.
    conn.execute("CREATE TEMP TABLE tickets (LIKE public.tickets INCLUDING ALL)")
    conn.execute("CREATE TEMP TABLE ticket_stats (LIKE public.ticket_stats INCLUDING ALL)")
    yield conn
    conn.rollback()
    conn.close()


def _ticket(conn, ticket_id, category, sentiment, created_at, enriched_after_s, loaded_after_s, summary="s"):
    enriched_at = created_at + timedelta(seconds=enriched_after_s)
    conn.execute(
        "INSERT INTO tickets (ticket_id, customer_id, created_at, channel, product, subject, body, tier, category,"
        " priority, sentiment, summary, enriched_at, model, kafka_topic, loaded_at)"
        " VALUES (%s, 'C-0001', %s, 'email', 'p', 's', 'b', 'free', %s, 'high', %s, %s, %s, 'stub', 't', %s)",
        (ticket_id, created_at, category, sentiment, summary, enriched_at,
         enriched_at + timedelta(seconds=loaded_after_s)))


def test_queries_on_empty_tables(conn):
    assert fetch_headline(conn) == Headline(total=0, last_hour=0, classify_seconds=None, sink_seconds=None)
    assert fetch_stats(conn, "category") == []
    assert fetch_sentiment(conn) == []
    assert fetch_latest(conn) == []


def test_headline_counts_and_medians(conn):
    now = datetime.now(UTC)
    _ticket(conn, "a", "billing", -0.5, now - timedelta(minutes=5), enriched_after_s=4, loaded_after_s=1)
    _ticket(conn, "b", "billing", 0.5, now - timedelta(minutes=4), enriched_after_s=6, loaded_after_s=3)
    _ticket(conn, "old", "technical", 0.0, now - timedelta(days=2), enriched_after_s=100, loaded_after_s=100)
    headline = fetch_headline(conn)
    assert (headline.total, headline.last_hour) == (3, 2)
    # Medians only over tickets classified in the last hour, so the 2-day-old one doesn't count.
    assert headline.classify_seconds == pytest.approx(5.0)
    assert headline.sink_seconds == pytest.approx(2.0)


def test_sentiment_and_latest(conn):
    now = datetime.now(UTC)
    _ticket(conn, "a", "billing", -0.5, now - timedelta(minutes=3), 1, 1, summary="first")
    _ticket(conn, "b", "billing", 0.1, now - timedelta(minutes=2), 1, 1, summary="second")
    _ticket(conn, "c", "technical", 0.4, now - timedelta(minutes=1), 1, 1, summary="third")
    assert [(c, round(avg, 2), n) for c, avg, n in fetch_sentiment(conn)] == [("billing", -0.2, 2), ("technical", 0.4, 1)]
    latest = fetch_latest(conn, limit=2)
    assert [row["summary"] for row in latest] == ["third", "second"]  # newest first
    assert set(latest[0]) == {"created_at", "customer_id", "category", "priority", "sentiment", "summary"}


def test_stats_returns_only_the_latest_windows_of_one_dimension(conn):
    start = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)
    for i in range(30):
        window = start + timedelta(minutes=5 * i)
        conn.execute("INSERT INTO ticket_stats (dimension, value, window_start, window_end, count)"
                     " VALUES ('category', 'billing', %s, %s, %s), ('priority', 'high', %s, %s, 1)",
                     (window, window + timedelta(minutes=5), i, window, window + timedelta(minutes=5)))
    rows = fetch_stats(conn, "category", windows=24)
    assert len(rows) == 24
    assert rows[0] == (start + timedelta(minutes=5 * 6), "billing", 6)  # oldest of the latest 24, first
    assert {value for _, value, _ in rows} == {"billing"}


def test_medians_ignore_a_backlog_loaded_just_now(conn):
    # Right after `connectors apply`, the backlog is loaded now but was enriched long ago. It must not
    # count as "classified → in Postgres" latency.
    now = datetime.now(UTC)
    _ticket(conn, "live", "billing", 0.0, now - timedelta(seconds=10), enriched_after_s=4, loaded_after_s=1)
    _ticket(conn, "backlog", "billing", 0.0, now - timedelta(days=2), enriched_after_s=5,
            loaded_after_s=2 * 24 * 3600 - 10)
    headline = fetch_headline(conn)
    assert headline.classify_seconds == pytest.approx(4.0)
    assert headline.sink_seconds == pytest.approx(1.0)
