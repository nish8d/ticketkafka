import json
import threading
import time
import uuid

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition

from pipeline.clients import consumer_config, producer_config
from pipeline.validator import run_validator

pytestmark = pytest.mark.integration


def _committed(group: str, topic: str, partitions: int) -> list[int]:
    checker = Consumer(consumer_config(group))
    try:
        tps = checker.committed([TopicPartition(topic, p) for p in range(partitions)], timeout=10)
        return [tp.offset for tp in tps]
    finally:
        checker.close()


def test_validator_routes_messages_and_commits_offsets(make_topic, read_topic, ticket_dict):
    raw, valid, dlq = make_topic("raw", 3), make_topic("valid", 3), make_topic("dlq")
    group = f"test-validator-{uuid.uuid4().hex[:8]}"

    producer = Producer(producer_config())
    good = [json.dumps({**ticket_dict, "ticket_id": str(uuid.uuid4())}).encode() for _ in range(3)]
    bad = [b"{not json", json.dumps({**ticket_dict, "body": ""}).encode()]
    for value in good + bad:
        producer.produce(raw, key=b"C-0042", value=value)
    assert producer.flush(10) == 0

    stop = threading.Event()
    worker = threading.Thread(target=run_validator, args=(
        Consumer(consumer_config(group)), Producer(producer_config()), raw, valid, dlq, stop.is_set))
    worker.start()
    try:
        valid_msgs = read_topic(valid, 3)
        dlq_msgs = read_topic(dlq, 2)
    finally:
        stop.set()
        worker.join(15)

    assert len(valid_msgs) == 3
    assert len(dlq_msgs) == 2
    assert all(dict(m.headers())["source.topic"] == raw.encode() for m in dlq_msgs)
    assert sorted(m.value() for m in dlq_msgs) == sorted(bad)
    # Committed offset = "next offset to read"; summed over partitions it equals messages processed.
    assert sum(o for o in _committed(group, raw, 3) if o >= 0) == 5


def test_validator_does_not_commit_when_outputs_are_not_acknowledged(make_topic, fake_producer, ticket_dict):
    raw = make_topic("raw")
    group = f"test-validator-{uuid.uuid4().hex[:8]}"
    producer = Producer(producer_config())
    producer.produce(raw, key=b"C-0042", value=json.dumps(ticket_dict).encode())
    assert producer.flush(10) == 0

    fake_producer.flush_remaining = 1  # pretend the broker never acked our output
    deadline = time.monotonic() + 30
    with pytest.raises(RuntimeError, match="not acknowledged"):
        run_validator(Consumer(consumer_config(group)), fake_producer, raw, "unused.valid", "unused.dlq",
                      should_stop=lambda: time.monotonic() > deadline)

    # Nothing committed (negative = no committed offset), so a restart re-reads the message.
    assert _committed(group, raw, 1)[0] < 0


def test_validator_does_not_commit_when_delivery_fails(make_topic, fake_producer, ticket_dict):
    raw = make_topic("raw")
    group = f"test-validator-{uuid.uuid4().hex[:8]}"
    producer = Producer(producer_config())
    producer.produce(raw, key=b"C-0042", value=json.dumps(ticket_dict).encode())
    assert producer.flush(10) == 0

    fake_producer.delivery_error = Exception("broker rejected the write")  # simulate a failed delivery
    deadline = time.monotonic() + 30
    with pytest.raises(RuntimeError, match="not acknowledged"):
        run_validator(Consumer(consumer_config(group)), fake_producer, raw, "unused.valid", "unused.dlq",
                      should_stop=lambda: time.monotonic() > deadline)

    # Nothing committed (negative = no committed offset), so a restart re-reads the message.
    assert _committed(group, raw, 1)[0] < 0
