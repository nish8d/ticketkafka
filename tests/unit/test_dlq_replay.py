from pipeline.dlq_replay import header_map, reached_snapshot, replay_target


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
