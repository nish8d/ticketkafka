import threading
import time
import uuid

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition

from pipeline import config
from pipeline.clients import consumer_config, producer_config, slow_consumer_config
from pipeline.enricher import Routes, run_enricher
from pipeline.llm import Classification, LLMError
from pipeline.models import Ticket
from pipeline.serde import EnrichedTicketSerde, TicketSerde, load_schema

pytestmark = pytest.mark.integration


def _no_backoff(stop):
    return lambda seconds: not stop.wait(0)  # no real waiting between retries in tests


def _routes(make_topic) -> Routes:
    return Routes(billing=make_topic("billing"), tech=make_topic("tech"), other=make_topic("other"),
                  urgent=make_topic("urgent"), dlq=make_topic("dlq"))


def test_enricher_routes_copies_urgent_and_dead_letters(make_topic, read_topic, ticket_dict,
                                                        registry, register_schema):
    valid = make_topic("valid", 3)
    routes = _routes(make_topic)
    register_schema(valid, config.TICKET_SCHEMA_V2)
    for topic in routes.outputs():
        register_schema(topic, config.ENRICHED_SCHEMA_V1)
    in_serde = TicketSerde(registry, load_schema(config.TICKET_SCHEMA_V2))
    out_serde = EnrichedTicketSerde(registry, load_schema(config.ENRICHED_SCHEMA_V1))

    # The stub "LLM" decides by subject line.
    answers = {"bill": ("billing", "urgent"), "bug": ("technical", "low"), "hi": ("other", "low")}

    def classify(ticket: Ticket) -> Classification:
        if ticket.subject == "down":
            raise LLMError("ollama call failed: connection refused")
        category, priority = answers[ticket.subject]
        return Classification(category=category, priority=priority, sentiment=0.0, summary="s")

    producer = Producer(producer_config())
    for subject in ("bill", "bug", "hi", "down"):
        ticket = Ticket.model_validate({**ticket_dict, "ticket_id": str(uuid.uuid4()), "subject": subject})
        producer.produce(valid, key=b"C-0042", value=in_serde.encode(ticket, valid))
    assert producer.flush(10) == 0

    group = f"test-enricher-{uuid.uuid4().hex[:8]}"
    stop = threading.Event()
    worker = threading.Thread(target=run_enricher, args=(
        Consumer(slow_consumer_config(group, 60_000)), Producer(producer_config()), valid, routes),
        kwargs=dict(in_serde=in_serde, out_serde=out_serde, classify=classify, should_stop=stop.is_set,
                    sleep=_no_backoff(stop), model="stub"))
    worker.start()
    try:
        billing, tech, other = read_topic(routes.billing, 1), read_topic(routes.tech, 1), read_topic(routes.other, 1)
        urgent, dlq = read_topic(routes.urgent, 1), read_topic(routes.dlq, 1)
    finally:
        stop.set()
        worker.join(15)

    assert [out_serde.decode(m.value(), routes.billing).subject for m in billing] == ["bill"]
    assert [out_serde.decode(m.value(), routes.tech).subject for m in tech] == ["bug"]
    assert [out_serde.decode(m.value(), routes.other).subject for m in other] == ["hi"]
    assert [out_serde.decode(m.value(), routes.urgent).subject for m in urgent] == ["bill"]
    assert dict(dlq[0].headers())["error.type"] == b"LLMError"
    assert in_serde.decode(dlq[0].value(), valid).subject == "down"  # original bytes

    checker = Consumer(consumer_config(group))
    try:
        committed = checker.committed([TopicPartition(valid, p) for p in range(3)], timeout=10)
    finally:
        checker.close()
    assert sum(tp.offset for tp in committed if tp.offset >= 0) == 4


def test_two_instances_split_the_partitions_between_them(make_topic, registry):
    # Scaling: same group.id, so the 6 partitions are shared — no partition is read by both.
    valid = make_topic("valid", 6)
    routes = Routes(billing="unused", tech="unused", other="unused", urgent="unused", dlq="unused")
    serde = TicketSerde(registry, load_schema(config.TICKET_SCHEMA_V2))
    group = f"test-enricher-{uuid.uuid4().hex[:8]}"
    stop = threading.Event()
    consumers = [Consumer(slow_consumer_config(group, 60_000)) for _ in range(2)]
    workers = [threading.Thread(target=run_enricher, args=(c, Producer(producer_config()), valid, routes),
                                kwargs=dict(in_serde=serde, out_serde=None, classify=None,
                                            should_stop=stop.is_set, sleep=_no_backoff(stop)))
               for c in consumers]
    for w in workers:
        w.start()
    assigned: list[set[int]] = []
    try:
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            assigned = [{tp.partition for tp in c.assignment()} for c in consumers]
            if all(len(a) == 3 for a in assigned):
                break
            time.sleep(0.5)
    finally:
        stop.set()
        for w in workers:
            w.join(15)

    assert all(len(a) == 3 for a in assigned), assigned
    assert assigned[0].isdisjoint(assigned[1])
    assert assigned[0] | assigned[1] == set(range(6))
