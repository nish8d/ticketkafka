from datetime import datetime, timezone

import pytest

from pipeline import config
from pipeline.enricher import Routes, enrich, main, output_topics, process, run_enricher
from pipeline.llm import Classification, LLMError
from pipeline.messages import SourceRef
from pipeline.models import EnrichedTicket, Ticket
from pipeline.retry import RetryPolicy, Stopping

NOW = datetime(2026, 9, 29, 12, 0, 5, tzinfo=timezone.utc)
SOURCE = SourceRef("tickets.valid", 2, 77)
ROUTES = Routes.default()


def _cls(category="billing", priority="high") -> Classification:
    return Classification(category=category, priority=priority, sentiment=-0.5, summary="Charged twice.")


@pytest.fixture
def ticket(ticket_dict) -> Ticket:
    return Ticket.model_validate({**ticket_dict, "tier": "pro"})


def _no_wait(seconds):
    return True


def _process(value, serde_v2, enriched_serde, classify, sleep=_no_wait):
    return process(value, b"C-0042", SOURCE, NOW, in_serde=serde_v2, out_serde=enriched_serde,
                   classify=classify, policy=RetryPolicy(), sleep=sleep, routes=ROUTES, model="stub-model")


def test_routes_default_to_the_configured_topics():
    assert ROUTES == Routes(billing="tickets.billing", tech="tickets.tech", other="tickets.enriched.other",
                            urgent="tickets.urgent", dlq="tickets.dlq")


@pytest.mark.parametrize("category, priority, expected", [
    ("billing", "high", ["tickets.billing"]),
    ("technical", "low", ["tickets.tech"]),
    ("account", "medium", ["tickets.enriched.other"]),
    ("other", "low", ["tickets.enriched.other"]),
    ("billing", "urgent", ["tickets.billing", "tickets.urgent"]),
    ("other", "urgent", ["tickets.enriched.other", "tickets.urgent"]),
])
def test_output_topics(ticket, category, priority, expected):
    enriched = enrich(ticket, _cls(category, priority), NOW, "m")
    assert output_topics(enriched, ROUTES) == expected


def test_enrich_keeps_the_ticket_and_adds_the_classification(ticket):
    enriched = enrich(ticket, _cls(), NOW, "qwen3.5:4b")
    assert isinstance(enriched, EnrichedTicket)
    assert enriched.ticket_id == ticket.ticket_id and enriched.tier == "pro"
    assert (enriched.category, enriched.priority, enriched.model, enriched.enriched_at) == \
        ("billing", "high", "qwen3.5:4b", NOW)


def test_process_routes_an_enriched_ticket_keyed_by_customer(serde_v2, enriched_serde, ticket):
    outputs = _process(serde_v2.encode(ticket, "tickets.valid"), serde_v2, enriched_serde,
                       lambda t: _cls("billing", "urgent"))
    assert [o.topic for o in outputs] == ["tickets.billing", "tickets.urgent"]
    assert all(o.key == b"C-0042" and o.headers == [] for o in outputs)
    decoded = enriched_serde.decode(outputs[0].value, "tickets.billing")
    assert (decoded.ticket_id, decoded.priority, decoded.model) == (ticket.ticket_id, "urgent", "stub-model")


def test_process_retries_transient_llm_failures(serde_v2, enriched_serde, ticket):
    answers = [LLMError("ollama call failed: timeout"), _cls("technical", "low")]
    waits = []

    def classify(t):
        answer = answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer

    outputs = _process(serde_v2.encode(ticket, "tickets.valid"), serde_v2, enriched_serde, classify,
                       sleep=lambda s: waits.append(s) or True)
    assert [o.topic for o in outputs] == ["tickets.tech"]
    assert waits == [1.0]


def test_exhausted_retries_dead_letter_the_original_bytes(serde_v2, enriched_serde, ticket):
    value = serde_v2.encode(ticket, "tickets.valid")

    def always_down(t):
        raise LLMError("ollama call failed: connection refused")

    [out] = _process(value, serde_v2, enriched_serde, always_down)
    headers = dict(out.headers)
    assert out.topic == "tickets.dlq" and out.value == value and out.key == b"C-0042"
    assert headers["error.type"] == b"LLMError"
    assert b"gave up after 4 attempts" in headers["error.message"]
    assert headers["source.topic"] == b"tickets.valid"
    assert headers["source.offset"] == b"77"


def test_undecodable_input_is_dead_lettered_without_calling_the_llm(serde_v2, enriched_serde):
    calls = []
    [out] = _process(b"not avro", serde_v2, enriched_serde, lambda t: calls.append(t))
    assert out.topic == "tickets.dlq"
    assert dict(out.headers)["error.type"] == b"UndecodableMessage"
    assert calls == []


def test_tombstone_is_dead_lettered(serde_v2, enriched_serde):
    [out] = _process(None, serde_v2, enriched_serde, lambda t: pytest.fail("classified a tombstone"))
    assert out.topic == "tickets.dlq" and b"tombstone" in dict(out.headers)["error.message"]


def test_stopping_propagates_instead_of_dead_lettering(serde_v2, enriched_serde, ticket):
    def down(t):
        raise LLMError("ollama call failed")

    with pytest.raises(Stopping):
        _process(serde_v2.encode(ticket, "tickets.valid"), serde_v2, enriched_serde, down,
                 sleep=lambda s: False)


class FakeConsumer:
    """Hands out prepared messages once, records commits."""

    def __init__(self, messages):
        self.batches, self.commits, self.closed = [messages], 0, False

    def subscribe(self, topics, on_assign=None, on_revoke=None):
        self.topics = topics

    def consume(self, num_messages=1, timeout=1.0):
        return self.batches.pop(0) if self.batches else []

    commit_error = None

    def commit(self, asynchronous=True):
        self.commits += 1
        if self.commit_error is not None:
            raise self.commit_error

    def close(self):
        self.closed = True


class RawMessage:
    def __init__(self, value, offset):
        self._value, self._offset = value, offset

    def error(self):
        return None

    def value(self):
        return self._value

    def key(self):
        return b"C-0042"

    def topic(self):
        return "tickets.valid"

    def partition(self):
        return 0

    def offset(self):
        return self._offset


def test_run_enricher_commits_after_the_batch_is_acknowledged(serde_v2, enriched_serde, ticket, fake_producer):
    consumer = FakeConsumer([RawMessage(serde_v2.encode(ticket, "tickets.valid"), 0), RawMessage(b"junk", 1)])
    rounds = iter([False, False, True])
    stats = run_enricher(consumer, fake_producer, "tickets.valid", ROUTES, in_serde=serde_v2,
                         out_serde=enriched_serde, classify=lambda t: _cls("technical", "urgent"),
                         should_stop=lambda: next(rounds), sleep=_no_wait, model="stub")
    assert (stats.enriched, stats.routed, stats.dead_lettered, stats.batches) == (1, 2, 1, 1)
    assert sorted(m.topic() for m in fake_producer.messages) == ["tickets.dlq", "tickets.tech", "tickets.urgent"]
    assert consumer.commits == 1 and consumer.closed


def test_stopping_mid_batch_commits_nothing_and_dlqs_nothing(serde_v2, enriched_serde, ticket, fake_producer):
    consumer = FakeConsumer([RawMessage(serde_v2.encode(ticket, "tickets.valid"), 0)])

    def down(t):
        raise LLMError("ollama call failed")

    stats = run_enricher(consumer, fake_producer, "tickets.valid", ROUTES, in_serde=serde_v2,
                         out_serde=enriched_serde, classify=down, should_stop=lambda: False,
                         sleep=lambda s: False, model="stub")
    assert consumer.commits == 0 and consumer.closed
    assert fake_producer.messages == []
    assert stats.dead_lettered == 0


def test_run_enricher_does_not_commit_unacknowledged_outputs(serde_v2, enriched_serde, ticket, fake_producer):
    consumer = FakeConsumer([RawMessage(serde_v2.encode(ticket, "tickets.valid"), 0)])
    fake_producer.flush_remaining = 1
    with pytest.raises(RuntimeError, match="not acknowledged"):
        run_enricher(consumer, fake_producer, "tickets.valid", ROUTES, in_serde=serde_v2, out_serde=enriched_serde,
                     classify=lambda t: _cls(), should_stop=lambda: False, sleep=_no_wait, model="stub")
    assert consumer.commits == 0 and consumer.closed


def test_main_refuses_a_batch_that_does_not_fit_the_poll_interval(monkeypatch):
    from pipeline import enricher

    monkeypatch.setattr(enricher, "run_enricher", lambda *a, **kw: pytest.fail("started anyway"))
    with pytest.raises(ValueError, match="max.poll.interval.ms"):
        main(["--batch-size", "100"])


def test_main_refuses_to_start_when_the_output_schema_is_not_registered(monkeypatch):
    from pipeline import enricher
    from pipeline.serde import SchemaNotRegistered

    monkeypatch.setattr(config, "SCHEMA_REGISTRY_URL", "mock://nothing-registered")
    monkeypatch.setattr(enricher, "run_enricher", lambda *a, **kw: pytest.fail("started anyway"))
    with pytest.raises(SchemaNotRegistered, match="enriched_ticket.v1.avsc"):
        main([])


def test_run_enricher_keeps_going_when_a_commit_fails_after_a_rebalance(serde_v2, enriched_serde, ticket,
                                                                        fake_producer):
    from confluent_kafka import KafkaError, KafkaException

    consumer = FakeConsumer([RawMessage(serde_v2.encode(ticket, "tickets.valid"), 0)])
    consumer.commit_error = KafkaException(KafkaError(KafkaError._WAIT_COORD, "Commit failed"))
    rounds = iter([False, False, True])
    stats = run_enricher(consumer, fake_producer, "tickets.valid", ROUTES, in_serde=serde_v2,
                         out_serde=enriched_serde, classify=lambda t: _cls(), should_stop=lambda: next(rounds),
                         sleep=_no_wait, model="stub")
    assert (stats.batches, stats.commit_failures) == (0, 1)
    assert consumer.closed
