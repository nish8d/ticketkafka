from pipeline.clients import consumer_config, producer_config


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
