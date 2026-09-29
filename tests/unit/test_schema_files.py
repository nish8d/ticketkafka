import json

import fastavro
import pytest

from pipeline import config
from pipeline.models import CHANNELS, TIERS, Ticket


def _load(path) -> dict:
    return json.loads(path.read_text())


def _fields(schema: dict) -> dict[str, dict]:
    return {f["name"]: f for f in schema["fields"]}


def test_all_three_schema_files_exist():
    assert {p.name for p in config.SCHEMA_DIR.glob("*.avsc")} == {
        "ticket.v1.avsc", "ticket.v2.avsc", "ticket.v3-breaking.avsc"}


@pytest.mark.parametrize("name", ["ticket.v1.avsc", "ticket.v2.avsc", "ticket.v3-breaking.avsc"])
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
