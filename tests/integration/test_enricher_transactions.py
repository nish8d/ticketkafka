import threading
import time
import uuid

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition

from pipeline import config
from pipeline.clients import consumer_config, producer_config, slow_consumer_config, transactional_producer_config
from pipeline.enricher import FatalTransactionError, Routes, Transactional, run_enricher
from pipeline.llm import Classification
from pipeline.models import Ticket
from pipeline.serde import EnrichedTicketSerde, TicketSerde, load_schema

pytestmark = pytest.mark.integration

N = 3  # tickets on tickets.valid


class Crash(Exception):
    """Stands in for os._exit in --crash-before-commit: the producer is left with its transaction open."""


def _classify(ticket: Ticket) -> Classification:
    return Classification(category="billing", priority="low", sentiment=0.0, summary="s")


@pytest.fixture
def setup(make_topic, register_schema, registry, ticket_dict):
    valid = make_topic("valid")
    routes = Routes(billing=make_topic("billing"), tech=make_topic("tech"), other=make_topic("other"),
                    urgent=make_topic("urgent"), dlq=make_topic("dlq"))
    register_schema(valid, config.TICKET_SCHEMA_V2)
    for topic in routes.outputs():
        register_schema(topic, config.ENRICHED_SCHEMA_V1)
    in_serde = TicketSerde(registry, load_schema(config.TICKET_SCHEMA_V2))
    out_serde = EnrichedTicketSerde(registry, load_schema(config.ENRICHED_SCHEMA_V1))
    producer = Producer(producer_config())
    for _ in range(N):
        ticket = Ticket.model_validate({**ticket_dict, "ticket_id": str(uuid.uuid4())})
        producer.produce(valid, key=b"C-0042", value=in_serde.encode(ticket, valid))
    assert producer.flush(10) == 0
    return {"valid": valid, "routes": routes, "in_serde": in_serde, "out_serde": out_serde,
            "group": f"test-enricher-txn-{uuid.uuid4().hex[:8]}",
            "transactional_id": f"test-enricher-{uuid.uuid4().hex[:8]}"}


def _enricher(s, producer, transactional, should_stop, before_commit=lambda batch: None):
    return run_enricher(Consumer(slow_consumer_config(s["group"], 60_000)), producer, s["valid"], s["routes"],
                        in_serde=s["in_serde"], out_serde=s["out_serde"], classify=_classify,
                        should_stop=should_stop, sleep=lambda seconds: True, model="stub", batch_size=N,
                        transactional=transactional, before_commit=before_commit)


def _crash_run(s, producer, transactional) -> None:
    def crash(batch):
        raise Crash

    deadline = time.monotonic() + 60
    with pytest.raises(Crash):
        _enricher(s, producer, transactional, lambda: time.monotonic() > deadline, crash)


def _reader(topic: str, isolation: str) -> Consumer:
    consumer = Consumer({"bootstrap.servers": config.BOOTSTRAP_SERVERS,
                         "group.id": f"test-reader-{uuid.uuid4().hex[:8]}",
                         "enable.auto.commit": False, "isolation.level": isolation})
    consumer.assign([TopicPartition(topic, 0, 0)])
    return consumer


def _count(topic: str, isolation: str, at_least: int = 0, idle: float = 3.0, timeout: float = 60.0) -> int:
    """Messages a reader with this isolation level sees: waits until it has seen `at_least`, then
    until nothing new arrives for `idle` seconds."""
    consumer = _reader(topic, isolation)
    seen, start = 0, time.monotonic()
    last = start
    try:
        while time.monotonic() - start < timeout:
            msg = consumer.poll(0.2)
            if msg is not None and not msg.error():
                seen, last = seen + 1, time.monotonic()
            elif seen >= at_least and time.monotonic() - last > idle:
                break
    finally:
        consumer.close()
    return seen


def _run_until_committed(s, producer, transactional, expected: int) -> None:
    """Run the enricher in the background until a read_committed reader sees `expected` outputs."""
    stop = threading.Event()
    worker = threading.Thread(target=_enricher, args=(s, producer, transactional, stop.is_set))
    worker.start()
    try:
        assert _count(s["routes"].billing, "read_committed", at_least=expected) >= expected
    finally:
        stop.set()
        worker.join(30)


def _committed_offset(s) -> int:
    checker = Consumer(consumer_config(s["group"]))
    try:
        [tp] = checker.committed([TopicPartition(s["valid"], 0)], timeout=10)
    finally:
        checker.close()
    return tp.offset


def test_transactional_batch_commits_outputs_and_offsets_together(setup):
    _run_until_committed(setup, Producer(transactional_producer_config(setup["transactional_id"])), True, N)
    assert _count(setup["routes"].billing, "read_uncommitted") == N  # nothing aborted
    assert _committed_offset(setup) == N


def test_transactional_crash_is_invisible_to_read_committed_readers(setup):
    billing = setup["routes"].billing
    crashed = Producer(transactional_producer_config(setup["transactional_id"]))  # kept alive: "killed", not closed
    _crash_run(setup, crashed, transactional=True)
    written = _count(billing, "read_uncommitted", at_least=1)
    assert 1 <= written <= N                         # the crashed batch's outputs are in the log...
    assert _count(billing, "read_committed") == 0    # ...but not committed, so read_committed skips them

    # Restart as the same instance: init_transactions aborts the leftover transaction, then the batch is redone.
    _run_until_committed(setup, Producer(transactional_producer_config(setup["transactional_id"])), True, N)
    assert _count(billing, "read_committed") == N
    assert _count(billing, "read_uncommitted") == N + written  # aborted copies stay in the log, skipped
    assert _committed_offset(setup) == N


def test_at_least_once_crash_duplicates_for_every_reader(setup):
    billing = setup["routes"].billing
    _crash_run(setup, Producer(producer_config()), transactional=False)
    written = _count(billing, "read_committed", at_least=1)
    assert 1 <= written <= N  # flushed before the crash, so committed as far as Kafka is concerned

    _run_until_committed(setup, Producer(producer_config()), False, N + written)
    assert _count(billing, "read_committed") == N + written  # the crashed batch, written twice
    assert _count(billing, "read_uncommitted") == N + written


def test_a_newer_instance_with_the_same_id_fences_the_older(setup):
    newer = Transactional(Producer(transactional_producer_config(setup["transactional_id"])))
    older = Producer(transactional_producer_config(setup["transactional_id"]))
    deadline = time.monotonic() + 60
    # The newer instance starts while the older one is about to commit its first batch.
    with pytest.raises(FatalTransactionError, match="fenced"):
        _enricher(setup, older, True, lambda: time.monotonic() > deadline, lambda batch: newer.start())
    assert _count(setup["routes"].billing, "read_committed") == 0
