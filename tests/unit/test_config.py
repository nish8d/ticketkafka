from pipeline import config


def test_stage_1_to_3_topics_are_defined():
    specs = {s.name: s for s in config.TOPIC_SPECS}
    assert specs["tickets.raw"].partitions == 6
    assert specs["tickets.valid"].partitions == 6
    assert specs["tickets.dlq"].partitions == 1


def test_topic_names_are_unique():
    names = [s.name for s in config.TOPIC_SPECS]
    assert len(names) == len(set(names))
