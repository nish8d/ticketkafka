from pipeline.clients import producer_config


def test_producer_is_durable_and_idempotent():
    conf = producer_config()
    assert conf["acks"] == "all"
    assert conf["enable.idempotence"] is True
    assert conf["partitioner"] == "murmur2_random"
