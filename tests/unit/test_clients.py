from pipeline.clients import consumer_config, producer_config, slow_consumer_config


def test_producer_is_durable_and_idempotent():
    conf = producer_config()
    assert conf["acks"] == "all"
    assert conf["enable.idempotence"] is True
    assert conf["partitioner"] == "murmur2_random"


def test_consumer_commits_manually_from_earliest():
    conf = consumer_config("my-group")
    assert conf["group.id"] == "my-group"
    assert conf["enable.auto.commit"] is False
    assert conf["auto.offset.reset"] == "earliest"


def test_consumer_config_accepts_overrides():
    conf = consumer_config("g", **{"max.poll.interval.ms": 123})
    assert conf["max.poll.interval.ms"] == 123
    assert conf["enable.auto.commit"] is False


def test_slow_consumers_rebalance_cooperatively_with_a_long_poll_interval():
    conf = slow_consumer_config("enricher", max_poll_interval_ms=600_000)
    assert conf["partition.assignment.strategy"] == "cooperative-sticky"
    assert conf["max.poll.interval.ms"] == 600_000
    assert conf["group.id"] == "enricher"
