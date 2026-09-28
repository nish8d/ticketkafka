"""Re-publish dead-lettered messages to the topic they came from (after you fix the cause)."""
import argparse
import logging
import time
from dataclasses import dataclass

from confluent_kafka import OFFSET_INVALID, Consumer, Producer, TopicPartition

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


def reached_snapshot(snapshot_highs: dict[int, int], next_offsets: dict[int, int]) -> bool:
    """True once every partition's next offset to read has reached its snapshot high watermark."""
    return all(next_offsets[p] >= high for p, high in snapshot_highs.items())


def snapshot_dlq(consumer, dlq_topic: str, timeout: float = 10.0) -> tuple[dict[int, int], dict[int, int]]:
    """Return ({partition: high watermark}, {partition: next offset this group will read}).

    The high watermark is "the end of the log right now": the offset the next new message will get.
    Anything at or beyond it arrived after we started (e.g. a replayed message that a live validator
    dead-lettered again), so it is not part of this replay.
    """
    metadata = consumer.list_topics(dlq_topic, timeout=timeout)
    partitions = [TopicPartition(dlq_topic, p) for p in metadata.topics[dlq_topic].partitions]
    highs, next_offsets = {}, {}
    for tp in consumer.committed(partitions, timeout=timeout):
        low, high = consumer.get_watermark_offsets(tp, timeout=timeout)
        highs[tp.partition] = high
        # Without a committed offset, the group starts at the oldest message (auto.offset.reset).
        next_offsets[tp.partition] = low if tp.offset == OFFSET_INVALID else tp.offset
    return highs, next_offsets


def run_replay(consumer, producer, dlq_topic: str, limit: int | None = None, dry_run: bool = False,
               idle_timeout: float = 10.0) -> ReplayStats:
    """Replay the messages that are in the DLQ right now.

    Stops once every partition reaches the end offset captured at start (otherwise a live validator
    re-dead-lettering the replayed messages would feed us forever), when `limit` is reached, or,
    as a safety net, when nothing arrives for `idle_timeout` seconds.
    """
    stats = ReplayStats()
    to_commit: dict[int, int] = {}  # partition -> next offset to read, for partitions we processed
    delivery_errors: list = []

    def on_delivery(err, msg):
        if err is not None:
            delivery_errors.append(err)

    try:
        snapshot_highs, next_offsets = snapshot_dlq(consumer, dlq_topic)
        if not snapshot_highs:
            log.warning("topic %r has no partitions (does it exist? run `uv run python -m "
                       "pipeline.admin` first); nothing to replay", dlq_topic)
        consumer.subscribe([dlq_topic])
        last_message_at = time.monotonic()
        while (limit is None or stats.seen < limit) and not reached_snapshot(snapshot_highs, next_offsets):
            msg = consumer.poll(0.5)
            if msg is None:
                if time.monotonic() - last_message_at > idle_timeout:
                    log.warning("idle timeout (%.1fs) reached before catching up to the snapshot; "
                               "some DLQ messages may remain unreplayed", idle_timeout)
                    break
                continue
            if msg.error():
                log.warning("consumer error: %s", msg.error())
                continue
            if msg.offset() >= snapshot_highs.get(msg.partition(), 0):
                continue  # arrived after we started: not part of this replay
            last_message_at = time.monotonic()
            next_offsets[msg.partition()] = to_commit[msg.partition()] = msg.offset() + 1
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
                                 headers=[("replayed.from", origin.encode())], on_delivery=on_delivery)
            stats.replayed += 1

        # Dry runs commit nothing, so a real run afterwards still sees every message.
        if not dry_run and stats.seen:
            # Wait until every replayed message is acknowledged by the broker...
            remaining = producer.flush(30)
            if remaining or delivery_errors:
                # Crash WITHOUT committing: on restart the whole batch is read again.
                raise RuntimeError(f"replayed messages not acknowledged ({remaining} pending, "
                                   f"errors: {delivery_errors}); offsets not committed")
            # ...then commit the DLQ offsets. A crash between flush and commit means the batch is
            # replayed twice (duplicates) — never lost. That's at-least-once.
            # Commit explicit offsets, not the consumer's position: poll() may already have returned
            # messages past the snapshot that we deliberately did not replay.
            consumer.commit(offsets=[TopicPartition(dlq_topic, p, o) for p, o in to_commit.items()],
                            asynchronous=False)
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
