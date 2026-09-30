import logging
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from pydantic import ValidationError

from pipeline.aggregator import (
    event_time_ms,
    has_value,
    is_newer,
    log_late,
    make_consumer_error_handler,
    stats_key,
    stats_row,
)
from pipeline.models import EnrichedTicket
from pipeline.serde import UndecodableMessage

T0 = datetime(2026, 9, 30, 12, 0, tzinfo=UTC)
T0_MS = 1_790_769_600_000  # T0 in epoch milliseconds
NOW = datetime(2026, 9, 30, 13, 0, tzinfo=UTC)


def test_t0_constant_is_right():
    assert T0_MS == int(T0.timestamp()) * 1000


def test_event_time_is_created_at_in_epoch_ms():
    assert event_time_ms({"created_at": "2026-09-30T12:00:00.123Z"}, None, 0, None) == T0_MS + 123


def test_event_time_ignores_the_kafka_timestamp_and_the_utc_offset():
    value = {"created_at": "2026-09-30T14:00:00+02:00"}  # same instant as T0
    assert event_time_ms(value, None, 999, None) == T0_MS


def test_stats_row_turns_a_closed_window_into_a_ticket_stats_record():
    window = {"start": T0_MS, "end": T0_MS + 300_000, "value": 4}
    assert stats_row("category", window, "billing") == {
        "dimension": "category", "value": "billing", "window_start": T0,
        "window_end": T0 + timedelta(minutes=5), "count": 4}


def test_stats_row_refuses_a_key_from_the_wrong_dimension():
    with pytest.raises(ValidationError):
        stats_row("category", {"start": T0_MS, "end": T0_MS + 300_000, "value": 1}, "urgent")


def test_stats_key():
    assert stats_key({"dimension": "priority", "value": "urgent"}) == "priority=urgent"


class FakeState:
    """Quix's State scoped to one message key: get/set by name."""

    def __init__(self):
        self.data = {}

    def get(self, key, default=None):
        return self.data.get(key, default)

    def set(self, key, value):
        self.data[key] = value


def _at(seconds: int) -> dict:
    return {"created_at": (T0 + timedelta(seconds=seconds)).isoformat()}


def test_is_newer_forwards_the_first_ticket_and_remembers_it():
    state = FakeState()
    assert is_newer(_at(10), state) is True
    assert list(state.data.values()) == [T0_MS + 10_000]


def test_is_newer_drops_an_older_ticket_and_keeps_the_newer_timestamp():
    state = FakeState()
    is_newer(_at(10), state)
    assert is_newer(_at(5), state) is False
    assert list(state.data.values()) == [T0_MS + 10_000]
    assert is_newer(_at(20), state) is True


def test_is_newer_ignores_an_equal_timestamp():
    # At-least-once: after a crash the same ticket can be processed again; don't re-send it.
    state = FakeState()
    is_newer(_at(10), state)
    assert is_newer(_at(10), state) is False


def test_tombstones_are_skipped_with_a_warning(caplog):
    with caplog.at_level(logging.WARNING, logger="aggregator"):
        assert has_value(None) is False
    assert "tombstone" in caplog.text
    assert has_value({"ticket_id": "x"}) is True


def test_log_late_logs_and_suppresses_quix_default_warning(caplog):
    with caplog.at_level(logging.INFO, logger="aggregator"):
        suppress_default = log_late({"ticket_id": "abc"}, "billing", T0_MS + 1_000, 90_000, T0_MS, T0_MS + 300_000,
                                    "store", "tickets.billing", 0, 42)
    assert suppress_default is False
    assert "abc" in caplog.text and "billing" in caplog.text and "90.0s" in caplog.text


# --- on_consumer_error: bad data -> DLQ and skip; anything else -> crash (return False) ---

@pytest.fixture
def raw(raw_message):
    return raw_message("tickets.billing", b"C-0042", b"\xff\xfe junk")


def _handler(producer):
    return make_consumer_error_handler(producer, "tickets.dlq", now=lambda: NOW)


def test_undecodable_message_is_dead_lettered_with_original_bytes(fake_producer, raw):
    suppressed = _handler(fake_producer)(UndecodableMessage("unsupported framing"), raw, logging.getLogger("t"))
    assert suppressed is True
    [dlq] = fake_producer.messages
    assert (dlq.topic(), dlq.key(), dlq.value()) == ("tickets.dlq", b"C-0042", b"\xff\xfe junk")
    headers = dict(dlq.headers())
    assert headers["error.type"] == b"UndecodableMessage"
    assert headers["source.topic"] == b"tickets.billing"
    assert headers["failed_at"] == NOW.isoformat().encode()


def test_invalid_record_is_dead_lettered(fake_producer, raw):
    try:
        EnrichedTicket.model_validate({})
    except ValidationError as exc:
        error = exc
    assert _handler(fake_producer)(error, raw, logging.getLogger("t")) is True
    assert dict(fake_producer.messages[0].headers())["error.type"] == b"ValidationError"


def test_registry_outage_is_not_dead_lettered(fake_producer, raw):
    handled = _handler(fake_producer)(httpx.ConnectError("connection refused"), raw, logging.getLogger("t"))
    assert handled is False
    assert fake_producer.messages == []


def test_errors_without_a_message_are_not_suppressed(fake_producer):
    # Poll-level errors (broker down, ...) arrive with message=None: nothing to dead-letter.
    assert _handler(fake_producer)(UndecodableMessage("x"), None, logging.getLogger("t")) is False
    assert fake_producer.messages == []


@pytest.mark.parametrize("failure", ["not_flushed", "delivery_error"])
def test_unacknowledged_dlq_write_is_not_skipped(fake_producer, raw, failure):
    # Skipping a message whose DLQ copy may not exist would lose it. Crash instead: it's redone.
    if failure == "not_flushed":
        fake_producer.flush_remaining = 1
    else:
        fake_producer.delivery_error = "broker said no"
    assert _handler(fake_producer)(UndecodableMessage("x"), raw, logging.getLogger("t")) is False
