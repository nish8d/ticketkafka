import json
from datetime import datetime, timezone

import pytest

from pipeline.models import Ticket
from pipeline.validator import MAX_ERROR_LEN, SourceRef, dlq_headers, route

NOW = datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)
SOURCE = SourceRef("tickets.raw", 4, 1234)


def _route(value, key=b"C-0042"):
    return route(value, key, SOURCE, NOW, "tickets.valid", "tickets.dlq")


def test_valid_ticket_goes_to_valid_topic_keyed_by_customer(ticket_dict):
    out = _route(json.dumps(ticket_dict).encode(), key=None)
    assert out.topic == "tickets.valid"
    assert out.key == b"C-0042"
    assert Ticket.model_validate_json(out.value) == Ticket.model_validate(ticket_dict)
    assert out.headers == []


def test_valid_output_drops_unknown_fields(ticket_dict):
    out = _route(json.dumps({**ticket_dict, "extra": 1}).encode())
    assert "extra" not in json.loads(out.value)


@pytest.mark.parametrize("value", [
    b"{not json",
    b"\xff\xfe\x00garbage",
    b"",
    None,
    b'{"ticket_id": "0b6a4a3e-9f5e-4f2e-8a7e-1f2d3c4b5a69"}',
])
def test_bad_messages_go_to_dlq_with_original_bytes(value):
    out = _route(value)
    assert out.topic == "tickets.dlq"
    assert out.value == value
    assert out.key == b"C-0042"


def test_dlq_headers_describe_the_failure(ticket_dict):
    out = _route(json.dumps({**ticket_dict, "body": ""}).encode())
    headers = dict(out.headers)
    assert headers["error.type"] == b"ValidationError"
    assert b"body" in headers["error.message"]
    assert headers["source.topic"] == b"tickets.raw"
    assert headers["source.partition"] == b"4"
    assert headers["source.offset"] == b"1234"
    assert headers["failed_at"] == NOW.isoformat().encode()


def test_tombstone_is_reported_as_such():
    headers = dict(_route(None).headers)
    assert headers["error.type"] == b"ValueError"
    assert b"tombstone" in headers["error.message"]


def test_long_error_messages_are_truncated():
    headers = dict(dlq_headers(ValueError("x" * 5000), SOURCE, NOW))
    assert len(headers["error.message"]) == MAX_ERROR_LEN
