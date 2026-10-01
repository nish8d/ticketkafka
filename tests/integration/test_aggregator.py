import shutil
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from confluent_kafka import Producer

from pipeline import config
from pipeline.aggregator import Topics, build_app
from pipeline.clients import producer_config
from pipeline.models import CATEGORIES, PRIORITIES, EnrichedTicket
from pipeline.serde import EnrichedTicketSerde, TicketStatsSerde, load_schema

pytestmark = pytest.mark.integration

WINDOW_MS, GRACE_MS = 60_000, 10_000
T0 = datetime(2026, 9, 30, 12, 0, tzinfo=UTC)  # a window boundary
IDLE_SECONDS = 15  # app.run() returns once no message has arrived for this long


@pytest.fixture
def group(admin):
    """A fresh consumer group; afterwards delete the changelog/repartition topics Quix made for it."""
    name = f"test-aggregator-{uuid.uuid4().hex[:8]}"
    yield name
    internal = [t for t in admin.list_topics(timeout=10).topics if name in t]
    if internal:
        for future in admin.delete_topics(internal, operation_timeout=10).values():
            future.result()


def _make_topics(make_topic, register_schema, partitions: int) -> Topics:
    topics = Topics(inputs=(make_topic("billing", partitions), make_topic("tech", partitions),
                            make_topic("other", partitions)),
                    stats=make_topic("stats"), latest=make_topic("latest"), dlq=make_topic("dlq"))
    for name in (*topics.inputs, topics.latest):
        register_schema(name, config.ENRICHED_SCHEMA_V1)
    register_schema(topics.stats, config.STATS_SCHEMA_V1)
    return topics


@pytest.fixture
def topics(make_topic, register_schema) -> Topics:
    # One partition per input, so the repartition topics get one partition too, and any later ticket
    # moves event time forward for every key. With several partitions each one closes its own
    # windows, and only when a newer ticket reaches it (see the stage notes).
    return _make_topics(make_topic, register_schema, partitions=1)


@pytest.fixture
def send(topics, registry, ticket_dict):
    """send(input_index, customer, category, priority, seconds_after_T0, subject="s")"""
    serde = EnrichedTicketSerde(registry, load_schema(config.ENRICHED_SCHEMA_V1))
    producer = Producer(producer_config())

    def _send(index, customer, category, priority, seconds, subject="s"):
        at = T0 + timedelta(seconds=seconds)
        ticket = EnrichedTicket.model_validate({
            **ticket_dict, "ticket_id": str(uuid.uuid4()), "customer_id": customer, "created_at": at,
            "subject": subject, "category": category, "priority": priority, "sentiment": 0.0, "summary": "s",
            "enriched_at": at, "model": "stub"})
        topic = topics.inputs[index]
        producer.produce(topic, key=customer.encode(), value=serde.encode(ticket, topic))
        assert producer.flush(10) == 0

    return _send


def _run(topics, registry, group, state_dir):
    app = build_app(topics, registry, group=group, state_dir=state_dir, window_ms=WINDOW_MS, grace_ms=GRACE_MS,
                    dlq_producer=Producer(producer_config()))
    app.run(timeout=IDLE_SECONDS)


def _stats(read_topic, topics, registry, expected):
    serde = TicketStatsSerde(registry, load_schema(config.STATS_SCHEMA_V1))
    # Ask for one more than expected, so an unexpected extra row shows up as a failure.
    messages = read_topic(topics.stats, expected + 1, timeout=5)
    rows = [serde.decode(m.value(), topics.stats) for m in messages]
    for m, row in zip(messages, rows):
        assert m.key() == f"{row.dimension}={row.value}".encode()
    return {(r.dimension, r.value, r.window_start, r.count) for r in rows}, len(rows)


BILLING, TECH, OTHER = 0, 1, 2


def test_counts_per_category_and_priority_for_a_closed_window(topics, registry, group, send, read_topic, tmp_path):
    send(BILLING, "C-0001", "billing", "low", 1)
    send(BILLING, "C-0002", "billing", "high", 2)
    send(TECH, "C-0003", "technical", "urgent", 3)
    send(OTHER, "C-0004", "account", "low", 4)
    send(OTHER, "C-0005", "other", "urgent", 5)
    send(BILLING, "C-0009", "billing", "medium", 130)  # event time passes 60 s + 10 s grace: window 1 closes
    send(BILLING, "C-0008", "billing", "low", 7)       # same partition, after that: late, not counted

    _run(topics, registry, group, tmp_path / "state")

    rows, n = _stats(read_topic, topics, registry, expected=7)
    assert rows == {
        ("category", "billing", T0, 2), ("category", "technical", T0, 1),
        ("category", "account", T0, 1), ("category", "other", T0, 1),
        ("priority", "low", T0, 2), ("priority", "high", T0, 1), ("priority", "urgent", T0, 2),
    }
    assert n == 7  # nothing for the still-open window at 120 s


def test_customers_latest_keeps_the_newest_ticket_per_customer(topics, registry, group, send, read_topic, tmp_path):
    send(BILLING, "C-0001", "billing", "low", 10, subject="newer")
    send(BILLING, "C-0001", "billing", "low", 5, subject="older")  # arrives later, created earlier
    send(TECH, "C-0002", "technical", "low", 3, subject="only")

    _run(topics, registry, group, tmp_path / "state")

    serde = EnrichedTicketSerde(registry, load_schema(config.ENRICHED_SCHEMA_V1))
    messages = read_topic(topics.latest, 3, timeout=5)
    assert sorted((m.key(), serde.decode(m.value(), topics.latest).subject) for m in messages) == [
        (b"C-0001", "newer"), (b"C-0002", "only")]


def test_undecodable_input_is_dead_lettered_and_processing_continues(topics, registry, group, send, read_topic,
                                                                     tmp_path):
    producer = Producer(producer_config())
    producer.produce(topics.inputs[BILLING], key=b"C-0001", value=b"\xff\xfe not avro")
    assert producer.flush(10) == 0
    send(BILLING, "C-0002", "billing", "low", 1, subject="fine")

    _run(topics, registry, group, tmp_path / "state")

    [dlq] = read_topic(topics.dlq, 1)
    headers = dict(dlq.headers())
    assert dlq.value() == b"\xff\xfe not avro"
    assert headers["error.type"] == b"UndecodableMessage"
    assert headers["source.topic"] == topics.inputs[BILLING].encode()
    assert headers["source.offset"] == b"0"
    serde = EnrichedTicketSerde(registry, load_schema(config.ENRICHED_SCHEMA_V1))
    [latest] = read_topic(topics.latest, 1)
    assert serde.decode(latest.value(), topics.latest).subject == "fine"


def test_window_state_is_restored_from_the_changelog(topics, registry, group, send, read_topic, tmp_path):
    state_dir = tmp_path / "state"
    send(BILLING, "C-0001", "billing", "low", 1)
    send(TECH, "C-0002", "technical", "urgent", 2)
    _run(topics, registry, group, state_dir)
    assert _stats(read_topic, topics, registry, expected=0)[1] == 0  # window still open: nothing emitted

    shutil.rmtree(state_dir)  # lose the local RocksDB: only the changelog topics remember the counts
    send(BILLING, "C-0003", "billing", "high", 3)
    send(BILLING, "C-0009", "billing", "medium", 130)  # close window 1
    _run(topics, registry, group, state_dir)

    rows, _ = _stats(read_topic, topics, registry, expected=5)
    assert rows == {
        ("category", "billing", T0, 2), ("category", "technical", T0, 1),
        ("priority", "low", T0, 1), ("priority", "urgent", T0, 1), ("priority", "high", T0, 1),
    }


def test_a_tombstone_on_an_input_is_skipped_not_fatal(topics, registry, group, send, read_topic, tmp_path):
    producer = Producer(producer_config())
    producer.produce(topics.inputs[BILLING], key=b"C-0001", value=None)
    assert producer.flush(10) == 0
    send(BILLING, "C-0002", "billing", "low", 1, subject="fine")

    _run(topics, registry, group, tmp_path / "state")

    serde = EnrichedTicketSerde(registry, load_schema(config.ENRICHED_SCHEMA_V1))
    [latest] = read_topic(topics.latest, 1)
    assert serde.decode(latest.value(), topics.latest).subject == "fine"
    assert read_topic(topics.dlq, 1, timeout=5) == []


def test_three_partition_inputs_go_through_real_repartition_topics(make_topic, register_schema, admin, registry,
                                                                   group, ticket_dict, read_topic, tmp_path):
    topics = _make_topics(make_topic, register_schema, partitions=3)
    serde = EnrichedTicketSerde(registry, load_schema(config.ENRICHED_SCHEMA_V1))
    producer = Producer(producer_config())

    def send(index, customer, category, priority, seconds):
        at = T0 + timedelta(seconds=seconds)
        ticket = EnrichedTicket.model_validate({
            **ticket_dict, "ticket_id": str(uuid.uuid4()), "customer_id": customer, "created_at": at,
            "category": category, "priority": priority, "sentiment": 0.0, "summary": "s",
            "enriched_at": at, "model": "stub"})
        producer.produce(topics.inputs[index], key=customer.encode(), value=serde.encode(ticket, topics.inputs[index]))

    send(BILLING, "C-0001", "billing", "low", 1)
    send(BILLING, "C-0002", "billing", "high", 2)
    send(BILLING, "C-0003", "billing", "low", 3)
    send(TECH, "C-0004", "technical", "urgent", 4)
    send(TECH, "C-0005", "technical", "urgent", 5)
    send(OTHER, "C-0006", "account", "medium", 6)
    send(OTHER, "C-0007", "other", "urgent", 7)
    assert producer.flush(10) == 0
    state_dir = tmp_path / "state"
    # Run 1 sees only window 1. Sending the closing tickets in the same batch would race: the three input
    # partitions are read in no fixed order, so a closing ticket could overtake a window-1 ticket and make
    # it "late". (Between partitions Kafka guarantees no order; grace is what absorbs this in real use.)
    _run(topics, registry, group, state_dir)
    # Closing tickets, one per category and per priority: whichever repartition partition a value hashes
    # to, some closing ticket reaches it with a newer event time, so every window 1 closes. The window at
    # +120 s stays open, so it produces no rows.
    for i, category in enumerate(CATEGORIES):
        send(OTHER, f"C-1{i:03d}", category, PRIORITIES[0], 130)
    for i, priority in enumerate(PRIORITIES):
        send(OTHER, f"C-2{i:03d}", CATEGORIES[0], priority, 130)
    assert producer.flush(10) == 0
    _run(topics, registry, group, state_dir)

    window_1 = {
        ("category", "billing", T0, 3), ("category", "technical", T0, 2),
        ("category", "account", T0, 1), ("category", "other", T0, 1),
        ("priority", "low", T0, 2), ("priority", "high", T0, 1),
        ("priority", "urgent", T0, 3), ("priority", "medium", T0, 1),
    }
    rows, n = _stats(read_topic, topics, registry, expected=len(window_1))
    assert rows == window_1 and n == len(window_1)

    names = admin.list_topics(timeout=10).topics
    repartitions = {t: len(m.partitions) for t, m in names.items() if "repartition__" in t and group in t}
    assert len(repartitions) == 2 and set(repartitions.values()) == {3}, repartitions
