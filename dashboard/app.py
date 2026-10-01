"""Read-only dashboard over the Postgres tables the Kafka Connect sinks fill.

  uv run streamlit run dashboard/app.py
"""
import pandas as pd
import psycopg
import streamlit as st

from pipeline import config
from pipeline.dashboard import fetch_headline, fetch_latest, fetch_sentiment, fetch_stats, format_duration, pivot_stats
from pipeline.models import CATEGORIES, DIMENSIONS, PRIORITIES

st.set_page_config(page_title="Ticket pipeline", layout="wide")
st.title("Support tickets, end to end")
st.caption("generator → validator → enricher → aggregator → Kafka Connect → Postgres · refreshes every 5 s")
dimension = st.radio("Tickets per window by", DIMENSIONS, horizontal=True)


@st.fragment(run_every="5s")
def panels(dimension: str) -> None:
    try:
        with psycopg.connect(config.POSTGRES_DSN, connect_timeout=3) as conn:
            headline = fetch_headline(conn)
            stats = fetch_stats(conn, dimension)
            sentiment = fetch_sentiment(conn)
            latest = fetch_latest(conn)
    except psycopg.OperationalError:
        st.warning("Cannot reach Postgres on localhost:5433. Start the stack: `docker compose up -d --build`.")
        return
    except psycopg.errors.UndefinedTable:
        st.warning("The sink tables don't exist; they come from `connect/sql/init.sql` on a fresh volume.")
        return
    if headline.total == 0:
        st.info("No tickets in Postgres yet. Run `uv run python -m pipeline.connectors apply` and the pipeline services.")
        return

    total, last_hour, classify, sink = st.columns(4)
    total.metric("Tickets in Postgres", f"{headline.total:,}")
    last_hour.metric("Created in the last hour", f"{headline.last_hour:,}")
    classify.metric("Generated → classified (median)", format_duration(headline.classify_seconds),
                    help="enriched_at − created_at, over tickets loaded in the last hour: mostly LLM time")
    sink.metric("Classified → in Postgres (median)", format_duration(headline.sink_seconds),
                help="loaded_at − enriched_at, over tickets loaded in the last hour: Kafka plus the Connect sink")

    st.subheader(f"Tickets per window, by {dimension}")
    windows, series = pivot_stats(stats, CATEGORIES if dimension == "category" else PRIORITIES)
    if windows:
        st.bar_chart(pd.DataFrame(series, index=pd.DatetimeIndex(windows, name="window start")))
    else:
        st.caption("No closed windows yet: the aggregator writes a window only once its grace period has passed.")

    left, right = st.columns([1, 2])
    with left:
        st.subheader("Average sentiment by category")
        frame = pd.DataFrame(sentiment, columns=["category", "sentiment", "tickets"]).set_index("category")
        st.bar_chart(frame["sentiment"])
    with right:
        st.subheader("Latest tickets")
        st.dataframe(pd.DataFrame(latest), hide_index=True)


panels(dimension)
