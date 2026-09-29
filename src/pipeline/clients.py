"""Kafka client settings, in one place so every service behaves the same way."""
import logging

from confluent_kafka import KafkaException

from pipeline import config

log = logging.getLogger("clients")


def producer_config() -> dict:
    return {
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        # acks=all: a write counts only once every in-sync replica has it.
        "acks": "all",
        # Idempotence: the broker de-duplicates producer retries, so a retry can't
        # write a message twice or reorder messages within a partition.
        "enable.idempotence": True,
        # Wait up to 50 ms to fill a batch, then compress the whole batch.
        "linger.ms": 50,
        "compression.type": "lz4",
        # Same key -> partition hashing as the Java client (librdkafka's default differs).
        "partitioner": "murmur2_random",
    }


def consumer_config(group_id: str, **overrides) -> dict:
    conf = {
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        # Consumers with the same group.id share a topic's partitions between them.
        "group.id": group_id,
        # A brand-new group (no committed offsets yet) starts from the oldest message.
        "auto.offset.reset": "earliest",
        # We commit ourselves, only after our outputs are safely written: at-least-once.
        "enable.auto.commit": False,
    }
    conf.update(overrides)
    return conf


def slow_consumer_config(group_id: str, max_poll_interval_ms: int) -> dict:
    """For consumers whose per-message work takes seconds (an LLM call), not microseconds."""
    return consumer_config(
        group_id,
        # Cooperative rebalancing: when an instance joins or leaves, only the partitions that
        # actually move are paused. With the default (eager) strategy every member stops, gives
        # everything back and waits — painful when each message costs seconds of LLM time.
        **{"partition.assignment.strategy": "cooperative-sticky",
           # If we don't call poll/consume within this window the broker assumes we're stuck,
           # evicts us and rebalances. Slow processing needs a longer window than the 5-min default.
           "max.poll.interval.ms": max_poll_interval_ms},
    )


def commit_batch(consumer) -> bool:
    """Commit a finished batch synchronously. False if the group took our partitions meanwhile.

    That happens in any scaled group — a rebalance, or our session timing out while we were busy.
    The batch's outputs are already acknowledged, and the partitions' new owner will redo the batch
    from the last committed offset: duplicates, never loss. So warn and carry on rather than crash.
    """
    try:
        consumer.commit(asynchronous=False)
        return True
    except KafkaException as exc:
        log.warning("commit failed (%s); the partitions were likely reassigned mid-batch, "
                    "so their new owner will redo it", exc.args[0])
        return False
