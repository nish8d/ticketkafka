"""Re-publish dead-lettered messages to the topic they came from (after you fix the cause)."""
import argparse
import logging
import time
from dataclasses import dataclass

from confluent_kafka import Consumer, Producer

from pipeline import config
from pipeline.clients import consumer_config, producer_config

log = logging.getLogger("dlq_replay")


@dataclass
class ReplayStats:
    seen: int = 0
    replayed: int = 0
    skipped: int = 0


def header_map(headers: list[tuple[str, bytes]] | None) -> dict[str, str]:
    return {k: v.decode() for k, v in (headers or []) if v is not None}


def replay_target(headers: list[tuple[str, bytes]] | None) -> str | None:
    return header_map(headers).get("source.topic")


def run_replay(consumer, producer, dlq_topic: str, limit: int | None = None, dry_run: bool = False,
               idle_timeout: float = 10.0) -> ReplayStats:
    """Replay DLQ messages until `limit` is reached or nothing arrives for `idle_timeout` seconds."""
    stats = ReplayStats()
    consumer.subscribe([dlq_topic])
    try:
        last_message_at = time.monotonic()
        while limit is None or stats.seen < limit:
            msg = consumer.poll(0.5)
            if msg is None:
                if time.monotonic() - last_message_at > idle_timeout:
                    break
                continue
            if msg.error():
                log.warning("consumer error: %s", msg.error())
                continue
            last_message_at = time.monotonic()
            stats.seen += 1

            target = replay_target(msg.headers())
            if target is None:
                stats.skipped += 1
                log.warning("offset %d has no source.topic header; skipping", msg.offset())
                continue
            if dry_run:
                log.info("would replay offset %d -> %s", msg.offset(), target)
            else:
                origin = f"{msg.topic()}:{msg.partition()}:{msg.offset()}"
                producer.produce(target, key=msg.key(), value=msg.value(),
                                 headers=[("replayed.from", origin.encode())])
            stats.replayed += 1

        # Dry runs commit nothing, so a real run afterwards still sees every message.
        if not dry_run and stats.seen:
            if producer.flush(30):
                raise RuntimeError("replayed messages not acknowledged; offsets not committed")
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()
    return stats


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Replay tickets.dlq messages to their source topic.")
    parser.add_argument("--limit", type=int, default=None, help="replay at most N messages")
    parser.add_argument("--dry-run", action="store_true", help="only log what would be replayed")
    parser.add_argument("--group", default="dlq-replay")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    stats = run_replay(Consumer(consumer_config(args.group)), Producer(producer_config()),
                       config.TOPIC_DLQ, limit=args.limit, dry_run=args.dry_run)
    log.info("done: %s", stats)


if __name__ == "__main__":
    main()
