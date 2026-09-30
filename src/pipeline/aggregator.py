"""Stage 6: windowed ticket counts and the latest ticket per customer, with Quix Streams.

  tickets.billing, tickets.tech, tickets.enriched.other
    ├─ group_by(category) ─ tumbling window count ─┐
    ├─ group_by(priority) ─ tumbling window count ─┴─► tickets.stats
    └─ newer than the stored created_at? ────────────► customers.latest (compacted)

Unlike stages 3–5 there is no hand-written poll loop: Quix owns polling, committing (a checkpoint
every 5 s), state and shutdown. We supply the pieces below.
"""
import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from pydantic import ValidationError

from pipeline.messages import SourceRef, dlq_headers
from pipeline.models import TicketStats
from pipeline.serde import UndecodableMessage

log = logging.getLogger("aggregator")

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MS = timedelta(milliseconds=1)
# The one thing is_newer() remembers per customer (Quix scopes state to the message key).
LATEST_CREATED_MS = "latest_created_at_ms"


def _ms(dt: datetime) -> int:
    return (dt - _EPOCH) // _MS  # integer arithmetic: no float rounding at millisecond edges


def _dt(ms: int) -> datetime:
    return _EPOCH + ms * _MS


def event_time_ms(value: dict, headers, timestamp: int, timestamp_type) -> int:
    """Quix timestamp extractor: window by when the customer opened the ticket (event time), not by
    when the enricher got round to it (the Kafka timestamp, which a slow LLM pushes later)."""
    return _ms(datetime.fromisoformat(value["created_at"]))


def stats_row(dimension: str, window: dict, key: str) -> dict:
    """A closed window, as Quix emits it ({"start", "end", "value"}), as a TicketStats record.
    After group_by() the message key is the counted value itself, e.g. "billing"."""
    return TicketStats(dimension=dimension, value=key, window_start=_dt(window["start"]),
                       window_end=_dt(window["end"]), count=window["value"]).model_dump()


def stats_key(row: dict) -> str:
    return f"{row['dimension']}={row['value']}"


def is_newer(value: dict, state) -> bool:
    """customers.latest guard. Compaction keeps the *last written* message per key; this makes the
    last written one also the *latest created*, even when tickets arrive out of order."""
    created = event_time_ms(value, None, 0, None)
    if created <= state.get(LATEST_CREATED_MS, -1):
        return False
    state.set(LATEST_CREATED_MS, created)
    return True


def has_value(value) -> bool:
    if value is None:
        log.warning("skipping a tombstone (null value): the enriched topics should never contain one")
        return False
    return True


def log_late(value, key, timestamp_ms, late_by_ms, start, end, store_name, topic, partition, offset) -> bool:
    """Window on_late callback: the ticket's window already closed (end + grace has passed)."""
    log.info("late ticket %s for %s window %s..%s, %.1fs too late: not counted (%s[%s]@%s)",
             value.get("ticket_id"), key, _dt(start).isoformat(), _dt(end).isoformat(),
             late_by_ms / 1000, topic, partition, offset)
    return False  # we've logged it; don't let Quix log it again


def make_consumer_error_handler(producer, dlq_topic: str,
                                now: Callable[[], datetime] = lambda: datetime.now(UTC)):
    """Application(on_consumer_error=...). Returning True skips the message; False re-raises (crash)."""

    def on_consumer_error(exc: Exception, message, logger: logging.Logger) -> bool:
        if message is None or not isinstance(exc, (UndecodableMessage, ValidationError)):
            return False  # not the message's fault: crash, and the last checkpoint is redone on restart
        source = SourceRef(message.topic(), message.partition(), message.offset())
        errors = []
        producer.produce(dlq_topic, key=message.key(), value=message.value(),
                         headers=dlq_headers(exc, source, now()), on_delivery=lambda err, _msg: errors.append(err))
        if producer.flush(10) or errors != [None]:
            log.error("could not dead-letter %s[%s]@%s (%s); crashing so it is retried",
                      source.topic, source.partition, source.offset, errors)
            return False
        log.warning("dead-lettered %s[%s]@%s: %s", source.topic, source.partition, source.offset, exc)
        return True

    return on_consumer_error
