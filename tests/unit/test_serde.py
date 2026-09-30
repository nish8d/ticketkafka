from datetime import UTC, datetime

import httpx
import pytest
from confluent_kafka.schema_registry.error import SchemaRegistryError
from pydantic import ValidationError

from pipeline import config
from pipeline.models import EnrichedTicket, Ticket, TicketStats
from pipeline.serde import (
    EnrichedTicketSerde,
    SchemaNotRegistered,
    TicketSerde,
    UndecodableMessage,
    ensure_registered,
    load_schema,
    make_registry,
    subject_for,
)


@pytest.fixture
def ticket(ticket_dict) -> Ticket:
    # created_at has whole milliseconds, so it survives timestamp-millis unchanged.
    return Ticket.model_validate({**ticket_dict, "tier": "pro"})


def test_subject_follows_topic_name_strategy():
    assert subject_for("tickets.raw") == "tickets.raw-value"


def test_round_trip(serde_v2, avro_topic, ticket):
    assert serde_v2.decode(serde_v2.encode(ticket, avro_topic), avro_topic) == ticket


def test_wire_format_is_magic_byte_then_schema_id(serde_v2, avro_topic, ticket, mock_registry):
    value = serde_v2.encode(ticket, avro_topic)
    registered = ensure_registered(mock_registry, avro_topic, config.TICKET_SCHEMA_V2)
    assert value[0] == 0
    assert int.from_bytes(value[1:5], "big") == registered


def test_avro_is_smaller_than_json(serde_v2, avro_topic, ticket):
    assert len(serde_v2.encode(ticket, avro_topic)) < len(ticket.model_dump_json())


def test_timestamps_keep_only_milliseconds(serde_v2, avro_topic, ticket):
    precise = ticket.model_copy(update={"created_at": ticket.created_at.replace(microsecond=123456)})
    decoded = serde_v2.decode(serde_v2.encode(precise, avro_topic), avro_topic)
    assert decoded.created_at.microsecond == 123000


def test_new_reader_fills_the_default_for_old_data(serde_v1, serde_v2, avro_topic, ticket):
    # BACKWARD compatibility: a v2 consumer reads a v1 message; tier gets its schema default.
    v1_bytes = serde_v1.encode(ticket, avro_topic)
    assert serde_v2.decode(v1_bytes, avro_topic).tier == "free"


def test_old_reader_ignores_the_new_field(serde_v1, serde_v2, avro_topic, ticket):
    # FORWARD direction: a v1 consumer reads a v2 message; it never sees tier ("pro" is lost to it).
    v2_bytes = serde_v2.encode(ticket, avro_topic)
    decoded = serde_v1.decode(v2_bytes, avro_topic)
    assert decoded.body == ticket.body
    assert decoded.tier == "free"


def test_avro_refuses_to_write_a_record_missing_a_field(serde_v2, avro_topic, ticket):
    # With JSON a producer could send anything; Avro stops a missing field before it reaches Kafka.
    # model_construct skips validation, so we can build a Ticket that has no body at all.
    incomplete = Ticket.model_construct(**{k: getattr(ticket, k) for k in Ticket.model_fields if k != "body"})
    with pytest.raises(ValueError, match="body"):
        serde_v2.encode(incomplete, avro_topic)


@pytest.mark.parametrize("value", [
    b'{"ticket_id": "0b6a4a3e", "body": "a stage-3 JSON ticket"}',
    b"\x00\x00",
    b"",
])
def test_non_avro_bytes_are_undecodable(serde_v2, avro_topic, value):
    with pytest.raises(UndecodableMessage):
        serde_v2.decode(value, avro_topic)


def test_unknown_schema_id_is_undecodable(serde_v2, avro_topic, ticket):
    value = serde_v2.encode(ticket, avro_topic)
    forged = value[:1] + (999_999).to_bytes(4, "big") + value[5:]
    with pytest.raises(UndecodableMessage, match="999999"):
        serde_v2.decode(forged, avro_topic)


def test_truncated_payload_is_undecodable(serde_v2, avro_topic, ticket):
    value = serde_v2.encode(ticket, avro_topic)
    with pytest.raises(UndecodableMessage):
        serde_v2.decode(value[:12], avro_topic)


def test_decodable_but_invalid_ticket_raises_validation_error(serde_v2, avro_topic, ticket):
    # Avro checks shape (fields, types, enum symbols); business rules are still Pydantic's job.
    empty_body = serde_v2.encode(ticket.model_copy(update={"body": ""}), avro_topic)
    with pytest.raises(ValidationError):
        serde_v2.decode(empty_body, avro_topic)


def test_decode_propagates_registry_outage(mock_registry, avro_topic, ticket, monkeypatch):
    value = TicketSerde(mock_registry, load_schema(config.TICKET_SCHEMA_V2)).encode(ticket, avro_topic)
    fresh = TicketSerde(mock_registry, load_schema(config.TICKET_SCHEMA_V2))  # empty schema cache

    def down(*args, **kwargs):
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr(mock_registry, "get_schema", down)
    with pytest.raises(httpx.ConnectError):
        fresh.decode(value, avro_topic)


def test_decode_propagates_registry_server_errors(mock_registry, avro_topic, ticket, monkeypatch):
    value = TicketSerde(mock_registry, load_schema(config.TICKET_SCHEMA_V2)).encode(ticket, avro_topic)
    fresh = TicketSerde(mock_registry, load_schema(config.TICKET_SCHEMA_V2))

    def broken(*args, **kwargs):
        raise SchemaRegistryError(500, 50001, "store error")

    monkeypatch.setattr(mock_registry, "get_schema", broken)
    with pytest.raises(SchemaRegistryError):
        fresh.decode(value, avro_topic)


def test_ensure_registered_returns_the_schema_id(mock_registry, avro_topic):
    assert ensure_registered(mock_registry, avro_topic, config.TICKET_SCHEMA_V1) > 0


def test_ensure_registered_explains_how_to_fix_a_missing_schema(avro_topic):
    empty = make_registry("mock://empty")
    with pytest.raises(SchemaNotRegistered) as excinfo:
        ensure_registered(empty, avro_topic, config.TICKET_SCHEMA_V2)
    message = str(excinfo.value)
    assert "tickets.raw-value" in message
    assert f"uv run python -m pipeline.schemas register {config.TICKET_SCHEMA_V2}" in message


def test_clis_print_no_third_party_warnings():
    # authlib (a Schema Registry client dependency) warns on import; the CLIs shouldn't show that.
    import subprocess
    import sys

    result = subprocess.run([sys.executable, "-m", "pipeline.schemas", "--help"], capture_output=True, text=True)
    assert result.returncode == 0
    assert result.stderr == ""


# --- Bad data that decodes far enough to trip over itself must still be DLQ-able, not crash. ---

def _frame(schema_id: int, record: dict, schema: dict) -> bytes:
    """Confluent framing around a record written with an arbitrary schema (bypassing our serde)."""
    import io

    import fastavro

    out = io.BytesIO()
    fastavro.schemaless_writer(out, fastavro.parse_schema(schema), record)
    return b"\x00" + schema_id.to_bytes(4, "big") + out.getvalue()


def _v2_dict() -> dict:
    import json

    return json.loads(load_schema(config.TICKET_SCHEMA_V2))


def test_out_of_range_timestamp_is_undecodable(mock_registry, serde_v2, avro_topic, ticket):
    # A producer bug writes microseconds into a timestamp-millis field: fastavro raises OverflowError.
    v2_id = ensure_registered(mock_registry, avro_topic, config.TICKET_SCHEMA_V2)
    plain = _v2_dict()
    for field in plain["fields"]:
        if field["name"] == "created_at":
            field["type"] = "long"
    record = {**ticket.model_dump(mode="json"), "created_at": 1_790_000_000_000_000_000}
    with pytest.raises(UndecodableMessage):
        serde_v2.decode(_frame(v2_id, record, plain), avro_topic)


def test_schema_id_of_a_different_record_is_undecodable(mock_registry, serde_v2, avro_topic):
    from confluent_kafka.schema_registry import Schema

    customer = {"type": "record", "name": "Customer", "fields": [{"name": "id", "type": "int"}]}
    import json

    customer_id = mock_registry.register_schema("customers.latest-value", Schema(json.dumps(customer), "AVRO"))
    with pytest.raises(UndecodableMessage):
        serde_v2.decode(_frame(customer_id, {"id": 7}, customer), avro_topic)


def test_enum_symbol_the_reader_does_not_know_is_undecodable(mock_registry, serde_v2, avro_topic, ticket):
    # BACKWARD accepts adding an enum symbol; a v2 reader then meets a symbol it has never heard of.
    from confluent_kafka.schema_registry import Schema
    import json

    newer = _v2_dict()
    for field in newer["fields"]:
        if field["name"] == "tier":
            field["type"]["symbols"].append("trial")
    newer_id = mock_registry.register_schema(subject_for(avro_topic), Schema(json.dumps(newer), "AVRO"))
    record = {**ticket.model_dump(), "tier": "trial"}
    with pytest.raises(UndecodableMessage):
        serde_v2.decode(_frame(newer_id, record, newer), avro_topic)


def test_guid_framing_is_undecodable_with_a_clear_reason(serde_v2, avro_topic, ticket):
    value = b"\x01" + b"\x12" * 16 + serde_v2.encode(ticket, avro_topic)[5:]
    with pytest.raises(UndecodableMessage, match="magic byte 1"):
        serde_v2.decode(value, avro_topic)


@pytest.mark.parametrize("error", [
    SchemaRegistryError(404, -1, "404 page not found"),   # a proxy or wrong URL, not the registry
    ValueError("Expecting value: line 1 column 1"),       # a 2xx with a non-JSON body
])
def test_registry_answers_that_are_not_about_the_message_propagate(mock_registry, avro_topic, ticket,
                                                                    monkeypatch, error):
    value = TicketSerde(mock_registry, load_schema(config.TICKET_SCHEMA_V2)).encode(ticket, avro_topic)
    fresh = TicketSerde(mock_registry, load_schema(config.TICKET_SCHEMA_V2))

    def broken(*args, **kwargs):
        raise error

    monkeypatch.setattr(mock_registry, "get_schema", broken)
    with pytest.raises(type(error)) as excinfo:
        fresh.decode(value, avro_topic)
    assert not isinstance(excinfo.value, UndecodableMessage)  # infra trouble must not be dead-lettered


def test_ensure_registered_propagates_a_404_that_is_not_from_the_registry(mock_registry, avro_topic, monkeypatch):
    def not_the_registry(*args, **kwargs):
        raise SchemaRegistryError(404, -1, "404 page not found")

    monkeypatch.setattr(mock_registry, "lookup_schema", not_the_registry)
    with pytest.raises(SchemaRegistryError):
        ensure_registered(mock_registry, avro_topic, config.TICKET_SCHEMA_V2)


def test_decode_only_asks_the_registry_for_the_writer_schema(mock_registry, avro_topic, ticket, monkeypatch):
    # Newer clients' default ("associated") subject strategy makes its own registry call while
    # decoding, inside the phase where every error counts as bad bytes. We pin the plain topic
    # strategy instead. raising=False: that registry method only exists in clients >= 2.12.
    value = TicketSerde(mock_registry, load_schema(config.TICKET_SCHEMA_V2)).encode(ticket, avro_topic)
    fresh = TicketSerde(mock_registry, load_schema(config.TICKET_SCHEMA_V2))

    def down(*args, **kwargs):
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr(mock_registry, "get_associations_by_resource_name", down, raising=False)
    assert fresh.decode(value, avro_topic) == ticket


def _enriched(ticket: Ticket) -> EnrichedTicket:
    return EnrichedTicket.model_validate({
        **ticket.model_dump(), "category": "technical", "priority": "urgent", "sentiment": -0.25,
        "summary": "App crashes on login.", "enriched_at": "2026-09-29T12:00:05+00:00", "model": "stub"})


def test_enriched_round_trip(enriched_serde, ticket):
    enriched = _enriched(ticket)
    value = enriched_serde.encode(enriched, "tickets.tech")
    assert enriched_serde.decode(value, "tickets.tech") == enriched


def test_enriched_serde_rejects_a_plain_ticket_message(enriched_serde, serde_v2, ticket):
    # A Ticket record is not an EnrichedTicket (different name, missing fields): bad data, DLQ-able.
    with pytest.raises(UndecodableMessage):
        enriched_serde.decode(serde_v2.encode(ticket, "tickets.raw"), "tickets.tech")


def test_stats_round_trip(stats_serde):
    stats = TicketStats(dimension="priority", value="urgent", window_start=datetime(2026, 9, 30, 12, 0, tzinfo=UTC),
                        window_end=datetime(2026, 9, 30, 12, 5, tzinfo=UTC), count=7)
    assert stats_serde.decode(stats_serde.encode(stats, "tickets.stats"), "tickets.stats") == stats
