"""Kafka client settings, in one place so every service behaves the same way."""
from pipeline import config


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


def consumer_config(group_id: str) -> dict:
    return {
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        # Consumers with the same group.id share a topic's partitions between them.
        "group.id": group_id,
        # A brand-new group (no committed offsets yet) starts from the oldest message.
        "auto.offset.reset": "earliest",
        # We commit ourselves, only after our outputs are safely written: at-least-once.
        "enable.auto.commit": False,
    }
