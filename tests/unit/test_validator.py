from datetime import datetime, timezone

import httpx
import pytest

from pipeline import config
from pipeline.models import Ticket
from pipeline.serde import TicketSerde, load_schema
from pipeline.validator import MAX_ERROR_LEN, SourceRef, dlq_headers, route

NOW = datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)
SOURCE = SourceRef("tickets.raw", 4, 1234)


@pytest.fixture
def ticket(ticket_dict) -> Ticket:
    return Ticket.model_validate(ticket_dict)


def _route(value, serde, key=b"C-0042"):
    return route(value, key, SOURCE, NOW, "tickets.valid", "tickets.dlq", serde)


def test_valid_ticket_goes_to_valid_topic_keyed_by_customer(serde_v2, ticket):
    out = _route(serde_v2.encode(ticket, "tickets.raw"), serde_v2, key=None)
    assert out.topic == "tickets.valid"
    assert out.key == b"C-0042"
    assert serde_v2.decode(out.value, "tickets.valid") == ticket
    assert out.headers == []


def test_v1_input_is_upgraded_to_the_validators_schema(serde_v1, serde_v2, ticket):
    # The validator (v2) reads a v1 message and writes v2: tier is filled with its default.
    out = _route(serde_v1.encode(ticket, "tickets.raw"), serde_v2)
    assert out.topic == "tickets.valid"
    assert serde_v2.decode(out.value, "tickets.valid").tier == "free"


@pytest.mark.parametrize("value", [
    b'{"ticket_id": "0b6a4a3e-9f5e-4f2e-8a7e-1f2d3c4b5a69", "body": "a stage-3 JSON ticket"}',
    b"\x00\x00\x0f\x42\x3f" + b"\x00" * 20,
    b"\xff\xfe\x00garbage",
    b"",
])
def test_undecodable_messages_go_to_dlq_with_original_bytes(serde_v2, value):
    out = _route(value, serde_v2)
    assert out.topic == "tickets.dlq"
    assert out.value == value
    assert out.key == b"C-0042"
    assert dict(out.headers)["error.type"] == b"UndecodableMessage"


def test_dlq_headers_describe_the_failure(serde_v2, ticket):
    value = serde_v2.encode(ticket.model_copy(update={"body": ""}), "tickets.raw")
    out = _route(value, serde_v2)
    assert out.topic == "tickets.dlq"
    assert out.value == value
    headers = dict(out.headers)
    assert headers["error.type"] == b"ValidationError"
    assert b"body" in headers["error.message"]
    assert headers["source.topic"] == b"tickets.raw"
    assert headers["source.partition"] == b"4"
    assert headers["source.offset"] == b"1234"
    assert headers["failed_at"] == NOW.isoformat().encode()


def test_route_propagates_registry_outage(mock_registry, ticket, monkeypatch):
    value = TicketSerde(mock_registry, load_schema(config.TICKET_SCHEMA_V2)).encode(ticket, "tickets.raw")
    fresh = TicketSerde(mock_registry, load_schema(config.TICKET_SCHEMA_V2))

    def down(*args, **kwargs):
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr(mock_registry, "get_schema", down)
    # Not a DLQ case: the run loop crashes before committing, and the batch is retried on restart.
    with pytest.raises(httpx.ConnectError):
        _route(value, fresh)


def test_tombstone_is_reported_as_such(serde_v2):
    out = _route(None, serde_v2)
    assert out.topic == "tickets.dlq"
    headers = dict(out.headers)
    assert headers["error.type"] == b"ValueError"
    assert b"tombstone" in headers["error.message"]


def test_long_error_messages_are_truncated():
    headers = dict(dlq_headers(ValueError("x" * 5000), SOURCE, NOW))
    assert len(headers["error.message"]) == MAX_ERROR_LEN


def test_main_refuses_to_start_when_the_schema_is_not_registered(monkeypatch):
    from pipeline import validator
    from pipeline.serde import SchemaNotRegistered

    monkeypatch.setattr(config, "SCHEMA_REGISTRY_URL", "mock://nothing-registered")
    monkeypatch.setattr(validator, "run_validator", lambda *a, **kw: pytest.fail("consumed without a schema"))
    with pytest.raises(SchemaNotRegistered, match="pipeline.schemas register"):
        validator.main([])


def test_run_validator_keeps_going_when_a_commit_fails_after_a_rebalance(serde_v2, ticket, fake_producer):
    from confluent_kafka import KafkaError, KafkaException

    from pipeline.validator import run_validator

    class Msg:
        def __init__(self, value):
            self._value = value

        def error(self):
            return None

        def value(self):
            return self._value

        def key(self):
            return b"C-0042"

        def topic(self):
            return "tickets.raw"

        def partition(self):
            return 0

        def offset(self):
            return 0

    class Consumer:
        batches, commits, closed = None, 0, False

        def subscribe(self, topics, on_assign=None, on_revoke=None):
            pass

        def consume(self, num_messages=1, timeout=1.0):
            return self.batches.pop(0) if self.batches else []

        def commit(self, asynchronous=True):
            self.commits += 1
            raise KafkaException(KafkaError(KafkaError._WAIT_COORD, "Commit failed"))

        def close(self):
            self.closed = True

    consumer = Consumer()
    consumer.batches = [[Msg(serde_v2.encode(ticket, "tickets.raw"))]]
    rounds = iter([False, False, True])
    stats = run_validator(consumer, fake_producer, "tickets.raw", "tickets.valid", "tickets.dlq", serde_v2,
                          should_stop=lambda: next(rounds))
    assert (stats.valid, stats.batches, stats.commit_failures) == (1, 0, 1)
    assert consumer.closed
