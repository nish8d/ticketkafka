import time
import uuid

import pytest
from confluent_kafka import Consumer, Message
from confluent_kafka.admin import AdminClient

from pipeline.admin import ensure_topics
from pipeline.config import BOOTSTRAP_SERVERS, TopicSpec


@pytest.fixture
def admin() -> AdminClient:
    return AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})


@pytest.fixture
def make_topic(admin):
    """Create uniquely named topics for one test and delete them afterwards."""
    created: list[str] = []

    def _make(prefix: str, partitions: int = 1) -> str:
        name = f"test.{prefix}.{uuid.uuid4().hex[:8]}"
        ensure_topics(admin, [TopicSpec(name, partitions)])
        created.append(name)
        return name

    yield _make
    if created:
        for future in admin.delete_topics(created, operation_timeout=10).values():
            future.result()


def _read_topic(topic: str, expected: int, timeout: float = 30.0) -> list[Message]:
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": f"test-reader-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])
    messages: list[Message] = []
    deadline = time.monotonic() + timeout
    try:
        while len(messages) < expected and time.monotonic() < deadline:
            msg = consumer.poll(0.5)
            if msg is not None and not msg.error():
                messages.append(msg)
    finally:
        consumer.close()
    return messages


@pytest.fixture
def read_topic():
    """Read up to `expected` messages from the start of `topic`."""
    return _read_topic
