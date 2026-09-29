import uuid

import pytest
from confluent_kafka.schema_registry.error import SchemaRegistryError

from pipeline import config
from pipeline.schemas import IncompatibleSchema, check, register
from pipeline.serde import load_schema, subject_for

pytestmark = pytest.mark.integration

BREAKING = config.SCHEMA_DIR / "ticket.v3-breaking.avsc"


@pytest.fixture
def topic(registry):
    name = f"test.schemas.{uuid.uuid4().hex[:8]}"
    yield name
    for permanent in (False, True):
        try:
            registry.delete_subject(subject_for(name), permanent=permanent)
        except SchemaRegistryError:
            pass  # the test may not have registered anything


def test_v2_is_accepted_after_v1(registry, topic):
    [v1] = register(registry, [topic], load_schema(config.TICKET_SCHEMA_V1))
    [v2] = register(registry, [topic], load_schema(config.TICKET_SCHEMA_V2))
    assert (v1.version, v2.version) == (1, 2)
    assert registry.get_compatibility(subject_for(topic)).upper() == "BACKWARD"


def test_breaking_change_is_rejected_with_reasons(registry, topic):
    register(registry, [topic], load_schema(config.TICKET_SCHEMA_V2))

    [result] = check(config.SCHEMA_REGISTRY_URL, [topic], load_schema(BREAKING))
    assert result.compatible is False
    assert any("message" in m for m in result.messages)

    with pytest.raises(IncompatibleSchema):
        register(registry, [topic], load_schema(BREAKING))
    assert registry.get_versions(subject_for(topic)) == [1]


def test_check_against_an_empty_subject_is_compatible(topic):
    [result] = check(config.SCHEMA_REGISTRY_URL, [topic], load_schema(BREAKING))
    assert result.compatible is True
