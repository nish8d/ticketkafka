import random
from collections import defaultdict

import pytest
from confluent_kafka import Producer

from pipeline import config
from pipeline.clients import producer_config
from pipeline.generator import run_generator
from pipeline.llm import TicketText
from pipeline.serde import TicketSerde, load_schema

pytestmark = pytest.mark.integration


def test_same_customer_always_lands_on_same_partition(make_topic, read_topic, registry, register_schema):
    topic = make_topic("raw", partitions=6)
    register_schema(topic, config.TICKET_SCHEMA_V2)
    serde = TicketSerde(registry, load_schema(config.TICKET_SCHEMA_V2))
    stats = run_generator(Producer(producer_config()), lambda seed: TicketText(subject="s", body="b"),
                          topic, count=60, rate=1000, bad_ratio=0.0, rng=random.Random(7), serde=serde)
    assert stats.delivered == 60

    messages = read_topic(topic, 60)
    assert len(messages) == 60
    partitions_by_key = defaultdict(set)
    for msg in messages:
        partitions_by_key[msg.key()].add(msg.partition())
    assert all(len(parts) == 1 for parts in partitions_by_key.values())
    assert len({msg.partition() for msg in messages}) > 1
    # Values are Avro now: every one decodes back into a ticket whose customer matches the key.
    assert all(serde.decode(msg.value(), topic).customer_id.encode() == msg.key() for msg in messages)
