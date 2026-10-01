import pytest
from confluent_kafka import KafkaError, KafkaException, TopicPartition

from pipeline.clients import producer_config, transactional_producer_config
from pipeline.enricher import (
    TXN_ATTEMPTS,
    AtLeastOnce,
    FatalTransactionError,
    Routes,
    Transactional,
    next_offsets,
    rewind_positions,
    run_enricher,
)
from pipeline.llm import Classification
from pipeline.messages import Output


class _Msg:
    def __init__(self, topic, partition, offset, value=b"v"):
        self._topic, self._partition, self._offset, self._value = topic, partition, offset, value

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def value(self):
        return self._value

    def key(self):
        return b"C-0042"

    def error(self):
        return None


def _error(retriable=False, abort=False, fatal=False) -> KafkaException:
    return KafkaException(KafkaError(KafkaError._FAIL, "boom", fatal=fatal, retriable=retriable,
                                     txn_requires_abort=abort))


class FakeTxnProducer:
    """Records the transactional calls; `fail` queues exceptions per method name."""

    def __init__(self, events=None, fail=None):
        self.events = events if events is not None else []
        self.fail = fail or {}

    def _call(self, name, entry=None):
        self.events.append(entry or name)
        queue = self.fail.get(name)
        if queue:
            raise queue.pop(0)

    def init_transactions(self, timeout=None):
        self._call("init_transactions", "init")

    def begin_transaction(self):
        self._call("begin_transaction", "begin")

    def produce(self, topic, key=None, value=None, headers=None, on_delivery=None):
        self.events.append(("produce", topic))

    def poll(self, timeout=0):
        return 0

    def flush(self, timeout=None):
        self.events.append("flush")
        return 0

    def send_offsets_to_transaction(self, offsets, group_metadata, timeout=None):
        self._call("send_offsets_to_transaction",
                   ("send_offsets", [(tp.topic, tp.partition, tp.offset) for tp in offsets], group_metadata))

    def commit_transaction(self, timeout=None):
        self._call("commit_transaction", "commit")

    def abort_transaction(self, timeout=None):
        self._call("abort_transaction", "abort")


class FakeTxnConsumer:
    def __init__(self, owned=(), batches=(), events=None):
        self.owned = [TopicPartition(t, p) for t, p in owned]
        self.batches, self.seeks, self.closed = list(batches), [], False
        self.events = events if events is not None else []

    def assignment(self):
        return self.owned

    def seek(self, tp):
        self.seeks.append((tp.topic, tp.partition, tp.offset))

    def consumer_group_metadata(self):
        return "group-metadata"

    def subscribe(self, topics, on_assign=None, on_revoke=None):
        self.events.append("subscribe")

    def consume(self, num_messages=1, timeout=1.0):
        return self.batches.pop(0) if self.batches else []

    def commit(self, asynchronous=True):
        self.events.append("consumer.commit")

    def close(self):
        self.closed = True


OUTPUTS = [Output("tickets.billing", b"C-0042", b"x", []), Output("tickets.urgent", b"C-0042", b"x", [])]
BATCH = [_Msg("tickets.valid", 0, 5), _Msg("tickets.valid", 1, 9), _Msg("tickets.valid", 0, 7)]


# --- offsets ---------------------------------------------------------------------------------------

def test_next_offsets_is_one_past_the_highest_offset_per_partition():
    batch = [*BATCH, _Msg("other", 0, 1)]
    assert [(tp.topic, tp.partition, tp.offset) for tp in next_offsets(batch)] == [
        ("other", 0, 2), ("tickets.valid", 0, 8), ("tickets.valid", 1, 10)]


def test_rewind_positions_is_the_lowest_offset_per_partition():
    assert rewind_positions(BATCH) == {("tickets.valid", 0): 5, ("tickets.valid", 1): 9}


def test_transactional_producer_keeps_the_durable_settings():
    conf = transactional_producer_config("enricher-1")
    assert conf["transactional.id"] == "enricher-1"
    assert conf["transaction.timeout.ms"] == 60_000
    assert {key: conf[key] for key in producer_config()} == producer_config()


# --- the transactional writer ----------------------------------------------------------------------

def test_transactional_write_puts_outputs_and_offsets_in_one_transaction():
    producer = FakeTxnProducer()
    writer = Transactional(producer)
    assert writer.write(FakeTxnConsumer(), OUTPUTS, BATCH, lambda: producer.events.append("hook")) is True
    assert producer.events == [
        "begin", ("produce", "tickets.billing"), ("produce", "tickets.urgent"),
        ("send_offsets", [("tickets.valid", 0, 8), ("tickets.valid", 1, 10)], "group-metadata"),
        # Flushed before the commit point, so a crash there (--crash-before-commit, kill -9) leaves the
        # outputs on the broker as an open transaction rather than lost in the client's buffer.
        "flush", "hook", "commit"]
    assert writer.mode == "transactional"


def test_start_initialises_transactions():
    producer = FakeTxnProducer()
    Transactional(producer).start()
    assert producer.events == ["init"]


def test_abortable_error_aborts_and_rewinds_only_owned_partitions():
    producer = FakeTxnProducer(fail={"commit_transaction": [_error(abort=True)]})
    # A rebalance gave partition 1 to another member; we still own partition 0.
    consumer = FakeTxnConsumer(owned=[("tickets.valid", 0)])
    assert Transactional(producer).write(consumer, OUTPUTS, BATCH, lambda: None) is False
    assert producer.events[-2:] == ["commit", "abort"]
    assert consumer.seeks == [("tickets.valid", 0, 5)]


def test_retriable_error_is_retried():
    producer = FakeTxnProducer(fail={"commit_transaction": [_error(retriable=True)]})
    assert Transactional(producer).write(FakeTxnConsumer(), OUTPUTS, BATCH, lambda: None) is True
    assert producer.events.count("commit") == 2


def test_retriable_error_gives_up_after_the_last_attempt():
    producer = FakeTxnProducer(fail={"commit_transaction": [_error(retriable=True)] * TXN_ATTEMPTS})
    with pytest.raises(KafkaException):
        Transactional(producer).write(FakeTxnConsumer(), OUTPUTS, BATCH, lambda: None)
    assert producer.events.count("commit") == TXN_ATTEMPTS
    assert "abort" not in producer.events


def test_fatal_error_stops_without_aborting_or_rewinding():
    producer = FakeTxnProducer(fail={"commit_transaction": [_error(fatal=True)]})
    consumer = FakeTxnConsumer(owned=[("tickets.valid", 0)])
    with pytest.raises(FatalTransactionError, match="boom"):
        Transactional(producer).write(consumer, OUTPUTS, BATCH, lambda: None)
    assert "abort" not in producer.events and consumer.seeks == []


def test_other_errors_propagate():
    producer = FakeTxnProducer(fail={"send_offsets_to_transaction": [_error()]})
    with pytest.raises(KafkaException):
        Transactional(producer).write(FakeTxnConsumer(), OUTPUTS, BATCH, lambda: None)
    assert "commit" not in producer.events


# --- the at-least-once writer ----------------------------------------------------------------------

class _Crash(Exception):
    pass


def test_at_least_once_crash_hook_runs_after_flush_before_commit(fake_producer):
    consumer = FakeTxnConsumer()

    def crash():
        raise _Crash

    with pytest.raises(_Crash):
        AtLeastOnce(fake_producer).write(consumer, OUTPUTS, BATCH, crash)
    assert [m.topic() for m in fake_producer.messages] == ["tickets.billing", "tickets.urgent"]  # written...
    assert "consumer.commit" not in consumer.events                                             # ...not committed
    assert AtLeastOnce(fake_producer).mode == "at-least-once"


# --- run_enricher in transactional mode ------------------------------------------------------------

ROUTES = Routes.default()


def _run(consumer, producer, serde_v2, enriched_serde, rounds, **kwargs):
    return run_enricher(consumer, producer, "tickets.valid", ROUTES, in_serde=serde_v2, out_serde=enriched_serde,
                        classify=lambda t: Classification(category="billing", priority="low", sentiment=0.0,
                                                          summary="s"),
                        should_stop=lambda: next(rounds), sleep=lambda s: True, model="stub", **kwargs)


def test_transactions_are_initialised_before_subscribing(serde_v2, enriched_serde, ticket_dict):
    # Until a crashed predecessor's transaction is aborted (by our init_transactions), a new consumer
    # in the group receives nothing — so init must come first.
    from pipeline.models import Ticket

    events: list = []
    message = _Msg("tickets.valid", 0, 0, serde_v2.encode(Ticket.model_validate(ticket_dict), "tickets.valid"))
    consumer = FakeTxnConsumer(batches=[[message]], events=events)
    producer = FakeTxnProducer(events=events)
    batches_seen = []
    stats = _run(consumer, producer, serde_v2, enriched_serde, iter([False, False, True]), transactional=True,
                 before_commit=batches_seen.append)
    assert events[:2] == ["init", "subscribe"]
    assert "consumer.commit" not in events  # offsets go through the transaction only
    assert events[-1] == "commit"
    assert batches_seen == [1]
    assert (stats.batches, stats.enriched, stats.routed) == (1, 1, 1)
    assert consumer.closed


def test_run_enricher_keeps_going_after_an_aborted_batch(serde_v2, enriched_serde, ticket_dict):
    from pipeline.models import Ticket

    value = serde_v2.encode(Ticket.model_validate(ticket_dict), "tickets.valid")
    consumer = FakeTxnConsumer(owned=[("tickets.valid", 0)],
                               batches=[[_Msg("tickets.valid", 0, 0, value)], [_Msg("tickets.valid", 0, 0, value)]])
    producer = FakeTxnProducer(fail={"commit_transaction": [_error(abort=True)]})
    stats = _run(consumer, producer, serde_v2, enriched_serde, iter([False, False, False, True]), transactional=True)
    assert (stats.commit_failures, stats.batches) == (1, 1)  # aborted once, redone and committed
    assert consumer.seeks == [("tickets.valid", 0, 0)]
