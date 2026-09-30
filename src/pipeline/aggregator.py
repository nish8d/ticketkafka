"""Stage 6: windowed ticket counts and the latest ticket per customer, with Quix Streams.

  tickets.billing, tickets.tech, tickets.enriched.other
    ├─ group_by(category) ─ tumbling window count ─┐
    ├─ group_by(priority) ─ tumbling window count ─┴─► tickets.stats
    └─ newer than the stored created_at? ────────────► customers.latest (compacted)

Unlike stages 3–5 there is no hand-written poll loop: Quix owns polling, committing (a checkpoint
every 5 s), state and shutdown. We supply the pieces below.
"""
import argparse
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from pydantic import ValidationError
from quixstreams import Application

from pipeline import config
from pipeline.clients import producer_config
from pipeline.messages import SourceRef, dlq_headers
from pipeline.models import DIMENSIONS, TicketStats
from pipeline.quix_serde import QuixAvroDeserializer, QuixAvroSerializer
from pipeline.serde import (
    EnrichedTicketSerde,
    TicketStatsSerde,
    UndecodableMessage,
    ensure_registered,
    load_schema,
    make_registry,
)

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


DEFAULT_STATE_DIR = Path("state")


@dataclass(frozen=True)
class Topics:
    inputs: tuple[str, ...] = config.AGGREGATOR_INPUTS
    stats: str = config.TOPIC_STATS
    latest: str = config.TOPIC_CUSTOMERS_LATEST
    dlq: str = config.TOPIC_DLQ

    def ours(self) -> list[str]:
        return [*self.inputs, self.stats, self.latest, self.dlq]


class MissingTopics(RuntimeError):
    pass


def require_topics(admin, names: list[str]) -> None:
    """Quix creates any topic it's given that doesn't exist, with its own settings. It must be allowed
    to (it creates its changelog and repartition topics that way), so check ours exist first."""
    existing = set(admin.list_topics(timeout=10).topics)
    missing = [name for name in names if name not in existing]
    if missing:
        raise MissingTopics(f"missing topics {missing}: run `uv run python -m pipeline.admin` first "
                            "(Quix would create them itself, without our settings, e.g. customers.latest uncompacted)")


def build_app(topics: Topics, registry, *, group: str, state_dir: Path, window_ms: int, grace_ms: int,
              dlq_producer) -> Application:
    app = Application(
        broker_address=config.BOOTSTRAP_SERVERS,
        consumer_group=group,
        auto_offset_reset="earliest",
        state_dir=state_dir,
        # At-least-once: offsets and state are committed together at each checkpoint (every 5 s); a
        # crash redoes the work since the last one. Exactly-once is stage 8.
        processing_guarantee="at-least-once",
        on_consumer_error=make_consumer_error_handler(dlq_producer, topics.dlq),
    )
    enriched = EnrichedTicketSerde(registry, load_schema(config.ENRICHED_SCHEMA_V1))
    stats_serde = TicketStatsSerde(registry, load_schema(config.STATS_SCHEMA_V1))
    inputs = [app.topic(name, key_deserializer="str", value_deserializer=QuixAvroDeserializer(enriched),
                        timestamp_extractor=event_time_ms) for name in topics.inputs]
    stats = app.topic(topics.stats, key_serializer="str", value_serializer=QuixAvroSerializer(stats_serde))
    latest = app.topic(topics.latest, key_serializer="str", value_serializer=QuixAvroSerializer(enriched))

    tickets = app.dataframe(inputs[0])
    for topic in inputs[1:]:
        tickets = tickets.concat(app.dataframe(topic))
    tickets = tickets.filter(has_value)

    # Already keyed by customer_id, so the state is per customer with no re-keying.
    tickets.filter(is_newer, stateful=True).to_topic(latest)

    for dimension in DIMENSIONS:
        # Window state is kept per message key, so re-key first: group_by() writes every ticket
        # through a repartition topic, keyed by its category (or priority), so all tickets with the
        # same value meet in one partition.
        (tickets.group_by(lambda ticket, d=dimension: ticket[d], name=dimension)
            .tumbling_window(window_ms, grace_ms=grace_ms, on_late=log_late)
            .count()
            # One message per window, once event time passes end + grace, not an update per ticket.
            # "partition": any newer ticket in the partition closes every key's windows. The default,
            # "key", would leave a category that goes quiet (say "account") unemitted until it returns.
            .final(closing_strategy="partition")
            .apply(lambda window, key, _ts, _headers, d=dimension: stats_row(d, window, key), metadata=True)
            .to_topic(stats, key=stats_key))
    return app


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Count enriched tickets per window into tickets.stats and "
                                                 "keep the latest ticket per customer in customers.latest.")
    parser.add_argument("--group", default="aggregator", help="consumer group id")
    parser.add_argument("--window-seconds", type=int, default=300, help="tumbling window size (event time)")
    parser.add_argument("--grace-seconds", type=int, default=60,
                        help="how long after a window ends late tickets are still counted")
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE_DIR, help="local RocksDB state")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    topics = Topics()
    try:
        require_topics(AdminClient({"bootstrap.servers": config.BOOTSTRAP_SERVERS}), topics.ours())
    except MissingTopics as exc:
        parser.exit(1, f"{exc}\n")
    registry = make_registry(config.SCHEMA_REGISTRY_URL)
    # Fail before consuming anything if the schemas we write aren't registered.
    ensure_registered(registry, topics.stats, config.STATS_SCHEMA_V1)
    ensure_registered(registry, topics.latest, config.ENRICHED_SCHEMA_V1)

    app = build_app(topics, registry, group=args.group, state_dir=args.state_dir,
                    window_ms=args.window_seconds * 1000, grace_ms=args.grace_seconds * 1000,
                    dlq_producer=Producer(producer_config()))
    log.info("windows of %ss with %ss grace; state in %s", args.window_seconds, args.grace_seconds, args.state_dir)
    app.run()  # until Ctrl-C / SIGTERM: Quix finishes the checkpoint, commits and closes


if __name__ == "__main__":
    main()
