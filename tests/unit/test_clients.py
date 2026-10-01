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


class _Consumer:
    def __init__(self, exc=None):
        self.exc, self.commits = exc, 0

    def commit(self, asynchronous=True):
        self.commits += 1
        if self.exc is not None:
            raise self.exc


def test_commit_batch_commits_synchronously():
    from pipeline.clients import commit_batch

    consumer = _Consumer()
    assert commit_batch(consumer) is True
    assert consumer.commits == 1


def test_commit_batch_survives_losing_the_partitions_mid_batch(caplog):
    # e.g. our session timed out and the group handed our partitions to another instance: their new
    # owner redoes the batch (duplicates, never loss), so this instance should carry on, not crash.
    from confluent_kafka import KafkaError, KafkaException

    from pipeline.clients import commit_batch

    lost = KafkaException(KafkaError(KafkaError._WAIT_COORD, "Commit failed: Local: Waiting for coordinator"))
    assert commit_batch(_Consumer(lost)) is False
    assert "redo" in caplog.text


def test_consumers_read_committed_data_only():
    # Aborted (or still open) transactions from the transactional enricher are never delivered.
    assert consumer_config("g")["isolation.level"] == "read_committed"
    assert slow_consumer_config("g", 600_000)["isolation.level"] == "read_committed"
