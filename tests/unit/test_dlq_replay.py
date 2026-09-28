from pipeline.dlq_replay import header_map, replay_target


def test_replay_target_reads_source_topic_header():
    headers = [("error.type", b"ValidationError"), ("source.topic", b"tickets.raw")]
    assert replay_target(headers) == "tickets.raw"


def test_replay_target_is_none_without_header():
    assert replay_target([("error.type", b"X")]) is None
    assert replay_target(None) is None


def test_header_map_skips_null_header_values():
    assert header_map([("a", b"1"), ("b", None)]) == {"a": "1"}
