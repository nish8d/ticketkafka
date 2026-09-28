import time
import uuid

import pytest
from confluent_kafka import Consumer, Producer

from pipeline.clients import consumer_config, producer_config
from pipeline.dlq_replay import run_replay

pytestmark = pytest.mark.integration


class ProducerWithFlushRemaining:
    """Producer stub that simulates unacknowledged messages on flush."""

    def __init__(self, flush_remaining):
        self.flush_remaining = flush_remaining

    def produce(self, topic, key=None, value=None, headers=None, on_delivery=None):
        if on_delivery is not None:
            on_delivery(None, None)

    def flush(self, timeout=None):
        return self.flush_remaining


class ProducerWithDeliveryError:
    """Producer stub that simulates delivery errors."""

    def __init__(self, error_obj):
        self.error_obj = error_obj

    def produce(self, topic, key=None, value=None, headers=None, on_delivery=None):
        if on_delivery is not None:
            on_delivery(self.error_obj, None)

    def flush(self, timeout=None):
        return 0


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


def test_replay_no_commit_when_flush_has_remaining(make_topic):
    dlq, target = make_topic("dlq"), make_topic("target")
    producer = Producer(producer_config())
    producer.produce(dlq, key=b"C-0001", value=b"msg", headers=[("source.topic", target.encode())])
    assert producer.flush(10) == 0
    group = f"test-replay-flush-{uuid.uuid4().hex[:8]}"

    # Simulate unacknowledged messages by returning flush_remaining > 0
    failing_producer = ProducerWithFlushRemaining(flush_remaining=1)
    with pytest.raises(RuntimeError, match="not acknowledged"):
        run_replay(Consumer(consumer_config(group)), failing_producer, dlq)

    # Verify the message was not consumed (offset not committed)
    retry = run_replay(Consumer(consumer_config(group)), Producer(producer_config()), dlq, dry_run=True)
    assert retry.seen == 1


def test_replay_no_commit_when_delivery_fails(make_topic):
    dlq, target = make_topic("dlq"), make_topic("target")
    producer = Producer(producer_config())
    producer.produce(dlq, key=b"C-0001", value=b"msg", headers=[("source.topic", target.encode())])
    assert producer.flush(10) == 0
    group = f"test-replay-delivery-{uuid.uuid4().hex[:8]}"

    # Simulate delivery error
    failing_producer = ProducerWithDeliveryError(error_obj=Exception("broker error"))
    with pytest.raises(RuntimeError, match="not acknowledged"):
        run_replay(Consumer(consumer_config(group)), failing_producer, dlq)

    # Verify the message was not consumed (offset not committed)
    retry = run_replay(Consumer(consumer_config(group)), Producer(producer_config()), dlq, dry_run=True)
    assert retry.seen == 1


def test_replay_stops_at_dlq_end_offsets_captured_at_start(make_topic, read_topic):
    # The message's source.topic is the DLQ itself, so every replay lands back in the DLQ: a
    # deterministic version of "a live validator dead-letters the replayed message again".
    dlq = make_topic("dlq")
    producer = Producer(producer_config())
    producer.produce(dlq, key=b"C-0001", value=b"loop", headers=[("source.topic", dlq.encode())])
    assert producer.flush(10) == 0
    group = f"test-replay-loop-{uuid.uuid4().hex[:8]}"

    started = time.monotonic()
    stats = run_replay(Consumer(consumer_config(group)), Producer(producer_config()), dlq,
                       idle_timeout=3.0)
    assert time.monotonic() - started < 3.0  # ended by the snapshot check, not the idle timeout
    assert (stats.seen, stats.replayed) == (1, 1)
    assert len(read_topic(dlq, 3, timeout=5.0)) == 2
