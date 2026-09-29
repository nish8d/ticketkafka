import threading
import time
import uuid

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition

from pipeline import config
from pipeline.clients import consumer_config, producer_config
from pipeline.models import Ticket
from pipeline.serde import TicketSerde, load_schema
from pipeline.validator import run_validator

pytestmark = pytest.mark.integration


def _committed(group: str, topic: str, partitions: int) -> list[int]:
    checker = Consumer(consumer_config(group))
    try:
        tps = checker.committed([TopicPartition(topic, p) for p in range(partitions)], timeout=10)
        return [tp.offset for tp in tps]
    finally:
        checker.close()


def test_validator_routes_avro_messages_and_commits_offsets(make_topic, read_topic, ticket_dict,
                                                            registry, register_schema):
    raw, valid, dlq = make_topic("raw", 3), make_topic("valid", 3), make_topic("dlq")
    for topic in (raw, valid):
        register_schema(topic, config.TICKET_SCHEMA_V2)
    serde = TicketSerde(registry, load_schema(config.TICKET_SCHEMA_V2))
    group = f"test-validator-{uuid.uuid4().hex[:8]}"

    producer = Producer(producer_config())
    good = [Ticket.model_validate({**ticket_dict, "ticket_id": str(uuid.uuid4())}) for _ in range(3)]
    good_bytes = [serde.encode(t, raw) for t in good]
    bad = [
        b'{"a stage-3": "JSON ticket"}',                                            # not Avro
        good_bytes[0][:1] + (999_999).to_bytes(4, "big") + good_bytes[0][5:],     # unknown schema id
        serde.encode(good[0].model_copy(update={"body": ""}), raw),                # valid Avro, bad data
    ]
    for value in good_bytes + bad:
        producer.produce(raw, key=b"C-0042", value=value)
    assert producer.flush(10) == 0

    stop = threading.Event()
    worker = threading.Thread(target=run_validator, args=(
        Consumer(consumer_config(group)), Producer(producer_config()), raw, valid, dlq, serde, stop.is_set))
    worker.start()
    try:
        valid_msgs = read_topic(valid, 3)
        dlq_msgs = read_topic(dlq, 3)
    finally:
        stop.set()
        worker.join(15)

    assert sorted(serde.decode(m.value(), valid).ticket_id for m in valid_msgs) == sorted(t.ticket_id for t in good)
    assert sorted(m.value() for m in dlq_msgs) == sorted(bad)
    assert all(dict(m.headers())["source.topic"] == raw.encode() for m in dlq_msgs)
    assert sorted(dict(m.headers())["error.type"] for m in dlq_msgs) == \
        [b"UndecodableMessage", b"UndecodableMessage", b"ValidationError"]
    # Committed offset = "next offset to read"; summed over partitions it equals messages processed.
    assert sum(o for o in _committed(group, raw, 3) if o >= 0) == 6


def test_v2_validator_reads_v1_messages(make_topic, read_topic, ticket_dict, registry, register_schema):
    # Schema evolution end to end: data written with v1, read by a validator already on v2.
    raw, valid, dlq = make_topic("raw"), make_topic("valid"), make_topic("dlq")
    register_schema(raw, config.TICKET_SCHEMA_V1)
    register_schema(raw, config.TICKET_SCHEMA_V2)   # accepted: BACKWARD-compatible
    register_schema(valid, config.TICKET_SCHEMA_V2)
    v1 = TicketSerde(registry, load_schema(config.TICKET_SCHEMA_V1))
    v2 = TicketSerde(registry, load_schema(config.TICKET_SCHEMA_V2))

    producer = Producer(producer_config())
    producer.produce(raw, key=b"C-0042", value=v1.encode(Ticket.model_validate(ticket_dict), raw))
    assert producer.flush(10) == 0

    stop = threading.Event()
    worker = threading.Thread(target=run_validator, args=(
        Consumer(consumer_config(f"test-validator-{uuid.uuid4().hex[:8]}")), Producer(producer_config()),
        raw, valid, dlq, v2, stop.is_set))
    worker.start()
    try:
        [msg] = read_topic(valid, 1)
    finally:
        stop.set()
        worker.join(15)

    assert v2.decode(msg.value(), valid).tier == "free"


def test_validator_does_not_commit_when_outputs_are_not_acknowledged(make_topic, fake_producer, mock_registry):
    raw = make_topic("raw")
    group = f"test-validator-{uuid.uuid4().hex[:8]}"
    producer = Producer(producer_config())
    # Not Avro, so it routes to the DLQ: no schema is needed for an output topic.
    producer.produce(raw, key=b"C-0042", value=b"not avro")
    assert producer.flush(10) == 0

    fake_producer.flush_remaining = 1  # pretend the broker never acked our output
    deadline = time.monotonic() + 30
    with pytest.raises(RuntimeError, match="not acknowledged"):
        run_validator(Consumer(consumer_config(group)), fake_producer, raw, "unused.valid", "unused.dlq",
                      TicketSerde(mock_registry, load_schema(config.TICKET_SCHEMA_V2)), should_stop=lambda: time.monotonic() > deadline)

    # Nothing committed (negative = no committed offset), so a restart re-reads the message.
    assert _committed(group, raw, 1)[0] < 0


def test_validator_does_not_commit_when_delivery_fails(make_topic, fake_producer, mock_registry):
    raw = make_topic("raw")
    group = f"test-validator-{uuid.uuid4().hex[:8]}"
    producer = Producer(producer_config())
    # Not Avro, so it routes to the DLQ: no schema is needed for an output topic.
    producer.produce(raw, key=b"C-0042", value=b"not avro")
    assert producer.flush(10) == 0

    fake_producer.delivery_error = Exception("broker rejected the write")  # simulate a failed delivery
    deadline = time.monotonic() + 30
    with pytest.raises(RuntimeError, match="not acknowledged"):
        run_validator(Consumer(consumer_config(group)), fake_producer, raw, "unused.valid", "unused.dlq",
                      TicketSerde(mock_registry, load_schema(config.TICKET_SCHEMA_V2)), should_stop=lambda: time.monotonic() > deadline)

    # Nothing committed (negative = no committed offset), so a restart re-reads the message.
    assert _committed(group, raw, 1)[0] < 0
