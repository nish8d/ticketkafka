import logging

from confluent_kafka import TopicPartition

from pipeline import dlq_replay
from pipeline.dlq_replay import header_map, reached_snapshot, replay_target, run_replay


def test_replay_target_reads_source_topic_header():
    headers = [("error.type", b"ValidationError"), ("source.topic", b"tickets.raw")]
    assert replay_target(headers) == "tickets.raw"


def test_replay_target_is_none_without_header():
    assert replay_target([("error.type", b"X")]) is None
    assert replay_target(None) is None


def test_header_map_skips_null_header_values():
    assert header_map([("a", b"1"), ("b", None)]) == {"a": "1"}


def test_reached_snapshot_when_every_partition_is_caught_up():
    assert reached_snapshot({0: 5, 1: 3}, {0: 5, 1: 3})
    assert reached_snapshot({0: 5}, {0: 7})  # already past the snapshot


def test_reached_snapshot_false_while_a_partition_is_behind():
    assert not reached_snapshot({0: 5, 1: 3}, {0: 5, 1: 2})


def test_reached_snapshot_with_no_partitions_is_done():
    assert reached_snapshot({}, {})


class _FakeIdleConsumer:
    """poll() always returns nothing, so the loop can only exit via the idle timeout."""

    def subscribe(self, topics):
        pass

    def poll(self, timeout):
        return None

    def assignment(self):
        return [TopicPartition("tickets.dlq", 0)]

    def position(self, partitions):
        return [TopicPartition("tickets.dlq", 0, 0)]  # stuck before the snapshot's offset 5

    def close(self):
        pass


class _FakeIdleProducer:
    def produce(self, *args, **kwargs):
        raise AssertionError("should never be reached: no message ever arrives")

    def flush(self, timeout=None):
        return 0


def test_run_replay_warns_when_idle_timeout_exits_before_reaching_snapshot(monkeypatch, caplog):
    # Snapshot says partition 0 has messages up to offset 5, but our fake consumer never
    # delivers any: the loop can only leave via the idle-timeout safety net, with messages
    # still unread.
    monkeypatch.setattr(dlq_replay, "snapshot_dlq", lambda consumer, topic, timeout=10.0: ({0: 5}, {0: 0}))

    with caplog.at_level(logging.WARNING, logger="dlq_replay"):
        stats = run_replay(_FakeIdleConsumer(), _FakeIdleProducer(), "tickets.dlq", idle_timeout=0.1)

    assert stats.seen == 0
    assert any("idle timeout" in record.message for record in caplog.records)
