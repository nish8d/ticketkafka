import json

import fastavro
import pytest

from pipeline import config
from pipeline.models import CATEGORIES, CHANNELS, PRIORITIES, TIERS, DIMENSIONS, EnrichedTicket, Ticket, TicketStats


def _load(path) -> dict:
    return json.loads(path.read_text())


def _fields(schema: dict) -> dict[str, dict]:
    return {f["name"]: f for f in schema["fields"]}


SCHEMA_FILES = ["ticket.v1.avsc", "ticket.v2.avsc", "ticket.v3-breaking.avsc", "enriched_ticket.v1.avsc",
                "ticket_stats.v1.avsc"]


def test_all_schema_files_exist():
    assert {p.name for p in config.SCHEMA_DIR.glob("*.avsc")} == set(SCHEMA_FILES)


@pytest.mark.parametrize("name", SCHEMA_FILES)
def test_every_schema_file_is_valid_avro(name):
    fastavro.parse_schema(_load(config.SCHEMA_DIR / name))


def test_v2_has_exactly_the_ticket_model_fields():
    # The Pydantic model and the Avro schema describe the same record — keep them in lockstep.
    assert set(_fields(_load(config.TICKET_SCHEMA_V2))) == set(Ticket.model_fields)


def test_v2_is_v1_plus_tier_with_a_default():
    v1, v2 = _fields(_load(config.TICKET_SCHEMA_V1)), _fields(_load(config.TICKET_SCHEMA_V2))
    assert set(v2) - set(v1) == {"tier"}
    assert v2["tier"]["default"] == "free"
    assert {k: v for k, v in v2.items() if k != "tier"} == v1


def test_enum_symbols_match_the_model():
    fields = _fields(_load(config.TICKET_SCHEMA_V2))
    assert tuple(fields["channel"]["type"]["symbols"]) == CHANNELS
    assert tuple(fields["tier"]["type"]["symbols"]) == TIERS


def test_breaking_schema_renames_body_without_a_default():
    fields = _fields(_load(config.SCHEMA_DIR / "ticket.v3-breaking.avsc"))
    assert "body" not in fields
    assert "message" in fields and "default" not in fields["message"]


def test_enriched_schema_matches_the_model():
    fields = _fields(_load(config.ENRICHED_SCHEMA_V1))
    assert set(fields) == set(EnrichedTicket.model_fields)
    assert tuple(fields["category"]["type"]["symbols"]) == CATEGORIES
    assert tuple(fields["priority"]["type"]["symbols"]) == PRIORITIES


def test_enriched_schema_reuses_the_ticket_v2_fields():
    ticket_fields = _fields(_load(config.TICKET_SCHEMA_V2))
    enriched_fields = _fields(_load(config.ENRICHED_SCHEMA_V1))
    assert {k: enriched_fields[k] for k in ticket_fields} == ticket_fields


def test_stats_schema_has_exactly_the_model_fields():
    assert set(_fields(_load(config.STATS_SCHEMA_V1))) == set(TicketStats.model_fields)


def test_stats_schema_enum_and_timestamps_match_the_model():
    fields = _fields(_load(config.STATS_SCHEMA_V1))
    assert tuple(fields["dimension"]["type"]["symbols"]) == DIMENSIONS
    for name in ("window_start", "window_end"):
        assert fields[name]["type"] == {"type": "long", "logicalType": "timestamp-millis"}
    assert fields["count"]["type"] == "long"
