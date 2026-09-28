import uuid

import pytest
from confluent_kafka import Consumer, Producer

from pipeline.clients import consumer_config, producer_config
from pipeline.dlq_replay import run_replay

pytestmark = pytest.mark.integration


def test_replay_sends_messages_back_and_commits(make_topic, read_topic, fake_producer):
    dlq, target = make_topic("dlq"), make_topic("raw")
    producer = Producer(producer_config())
    producer.produce(dlq, key=b"C-0001", value=b"one", headers=[("source.topic", target.encode())])
    producer.produce(dlq, key=b"C-0002", value=b"two", headers=[("source.topic", target.encode())])
    producer.produce(dlq, value=b"orphan")
    assert producer.flush(10) == 0
    group = f"test-replay-{uuid.uuid4().hex[:8]}"

    dry = run_replay(Consumer(consumer_config(group)), fake_producer, dlq, dry_run=True)
    assert (dry.seen, dry.replayed, dry.skipped) == (3, 2, 1)
    assert fake_producer.messages == []

    real = run_replay(Consumer(consumer_config(group)), Producer(producer_config()), dlq)
    assert (real.seen, real.replayed, real.skipped) == (3, 2, 1)
    replayed = read_topic(target, 2)
    assert sorted(m.value() for m in replayed) == [b"one", b"two"]
    assert all(dict(m.headers())["replayed.from"].startswith(dlq.encode()) for m in replayed)

    again = run_replay(Consumer(consumer_config(group)), Producer(producer_config()), dlq)
    assert again.seen == 0
