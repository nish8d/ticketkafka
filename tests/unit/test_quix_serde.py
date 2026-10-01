import json
from datetime import UTC, datetime

import pytest
from quixstreams.models.serializers import SerializationContext

from pipeline.models import EnrichedTicket, TicketStats
from pipeline.quix_serde import QuixAvroDeserializer, QuixAvroSerializer
from pipeline.serde import UndecodableMessage

TOPIC = "tickets.billing"


@pytest.fixture
def enriched(ticket_dict) -> EnrichedTicket:
    return EnrichedTicket.model_validate({
        **ticket_dict, "category": "billing", "priority": "high", "sentiment": -0.5, "summary": "Double charge.",
        "enriched_at": "2026-09-28T12:00:05+00:00", "model": "stub"})


def _ctx(topic: str = TOPIC) -> SerializationContext:
    return SerializationContext(topic=topic, field="value")


def test_deserializer_returns_a_json_safe_dict(enriched_serde, enriched):
    # group_by() writes rows through a repartition topic as JSON, so UUIDs and datetimes
    # must already be strings.
    row = QuixAvroDeserializer(enriched_serde)(enriched_serde.encode(enriched, TOPIC), _ctx())
    assert row == enriched.model_dump(mode="json")
    json.dumps(row)


def test_serializer_round_trips_through_our_serde(enriched_serde, enriched):
    value = QuixAvroSerializer(enriched_serde)(enriched.model_dump(mode="json"), _ctx("customers.latest"))
    assert enriched_serde.decode(value, "customers.latest") == enriched


def test_serializer_accepts_python_objects_too(stats_serde):
    row = {"dimension": "category", "value": "billing", "window_start": datetime(2026, 9, 30, 12, 0, tzinfo=UTC),
           "window_end": datetime(2026, 9, 30, 12, 5, tzinfo=UTC), "count": 2}
    value = QuixAvroSerializer(stats_serde)(row, _ctx("tickets.stats"))
    assert stats_serde.decode(value, "tickets.stats") == TicketStats.model_validate(row)


def test_bad_bytes_raise_our_own_error_unwrapped(enriched_serde):
    # Quix hands whatever the deserializer raises straight to on_consumer_error, so the
    # error handler can tell bad data from a registry outage by type.
    with pytest.raises(UndecodableMessage):
        QuixAvroDeserializer(enriched_serde)(b"\xff\xfe not avro", _ctx())
