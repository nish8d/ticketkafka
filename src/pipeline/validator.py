"""Stage 3: validate raw tickets. Good ones go to tickets.valid, bad ones to tickets.dlq."""
import argparse
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

from confluent_kafka import Consumer, Producer

from pipeline import config
from pipeline.clients import consumer_config, producer_config
from pipeline.models import Ticket
from pipeline.shutdown import install_stop_handler

log = logging.getLogger("validator")

MAX_ERROR_LEN = 1000


@dataclass(frozen=True)
class SourceRef:
    """Where an input message came from — recorded on DLQ messages for debugging and replay."""

    topic: str
    partition: int
    offset: int


@dataclass(frozen=True)
class Output:
    topic: str
    key: bytes | None
    value: bytes | None
    headers: list[tuple[str, bytes]]


@dataclass
class ValidatorStats:
    valid: int = 0
    dead_lettered: int = 0
    batches: int = 0


def parse_ticket(value: bytes | None) -> Ticket:
    if value is None:
        raise ValueError("message has no value (tombstone)")
    # Raises pydantic.ValidationError (a ValueError) on bad JSON, bad UTF-8 or bad fields.
    return Ticket.model_validate_json(value)


def dlq_headers(exc: Exception, source: SourceRef, now: datetime) -> list[tuple[str, bytes]]:
    return [
        ("error.type", type(exc).__name__.encode()),
        ("error.message", str(exc)[:MAX_ERROR_LEN].encode()),
        ("source.topic", source.topic.encode()),
        ("source.partition", str(source.partition).encode()),
        ("source.offset", str(source.offset).encode()),
        ("failed_at", now.isoformat().encode()),
    ]


def route(value: bytes | None, key: bytes | None, source: SourceRef, now: datetime,
          valid_topic: str, dlq_topic: str) -> Output:
    """Decide where one input message goes. Pure: no Kafka involved."""
    try:
        ticket = parse_ticket(value)
    except ValueError as exc:
        # Keep the original bytes untouched so the message can be inspected or replayed.
        return Output(dlq_topic, key, value, dlq_headers(exc, source, now))
    return Output(valid_topic, ticket.customer_id.encode(), ticket.model_dump_json().encode(), [])


def _log_assign(consumer, partitions):
    log.info("assigned partitions: %s", [f"{p.topic}[{p.partition}]" for p in partitions])


def _log_revoke(consumer, partitions):
    log.info("revoked partitions: %s", [f"{p.topic}[{p.partition}]" for p in partitions])


def run_validator(consumer, producer, source_topic: str, valid_topic: str, dlq_topic: str,
                  should_stop: Callable[[], bool], batch_size: int = 100) -> ValidatorStats:
    stats = ValidatorStats()
    delivery_errors: list = []

    def on_delivery(err, msg):
        if err is not None:
            delivery_errors.append(err)

    consumer.subscribe([source_topic], on_assign=_log_assign, on_revoke=_log_revoke)
    try:
        while not should_stop():
            messages = consumer.consume(num_messages=batch_size, timeout=1.0)
            batch = []
            for msg in messages:
                if msg.error():
                    log.warning("consumer error: %s", msg.error())
                else:
                    batch.append(msg)
            if not batch:
                continue

            now = datetime.now(timezone.utc)
            for msg in batch:
                out = route(msg.value(), msg.key(), SourceRef(msg.topic(), msg.partition(), msg.offset()),
                            now, valid_topic, dlq_topic)
                producer.produce(out.topic, key=out.key, value=out.value, headers=out.headers or None,
                                 on_delivery=on_delivery)
                if out.topic == valid_topic:
                    stats.valid += 1
                else:
                    stats.dead_lettered += 1

            # 1) Wait until every output of this batch is acknowledged by the broker...
            remaining = producer.flush(30)
            if remaining or delivery_errors:
                # Crash WITHOUT committing: on restart the whole batch is read again.
                raise RuntimeError(f"outputs not acknowledged ({remaining} pending, "
                                   f"errors: {delivery_errors}); not committing")
            # 2) ...then commit the input offsets. A crash between 1) and 2) means the batch is
            #    processed twice (duplicates) — never lost. That's at-least-once.
            consumer.commit(asynchronous=False)
            stats.batches += 1
            log.info("batch done: %d messages (totals: %s)", len(batch), stats)
    finally:
        # Leave the group cleanly so its partitions are reassigned right away.
        consumer.close()
    return stats


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Validate tickets.raw into tickets.valid / tickets.dlq.")
    parser.add_argument("--group", default="validator", help="consumer group id")
    parser.add_argument("--batch-size", type=int, default=100)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    stats = run_validator(Consumer(consumer_config(args.group)), Producer(producer_config()),
                          config.TOPIC_RAW, config.TOPIC_VALID, config.TOPIC_DLQ,
                          should_stop=install_stop_handler(), batch_size=args.batch_size)
    log.info("stopped: %s", stats)


if __name__ == "__main__":
    main()
