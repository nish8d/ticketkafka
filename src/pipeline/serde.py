"""Avro through Schema Registry: the wire format for ticket topics from stage 4 on.

On the wire each value is: 1 magic byte (0) + 4-byte schema id + Avro binary. The id tells a
consumer which schema *wrote* the message; the consumer's own schema is the *reader* schema, and
Avro resolves one into the other (filling defaults for fields the writer didn't know about).
"""
from pathlib import Path

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import MessageField, SerializationContext
from pydantic import BaseModel

from pipeline import config
from pipeline.models import EnrichedTicket, Ticket

# The registry's own "no such schema/subject" codes (40400 is what the in-memory mock uses). Any other
# 404 — a proxy, a wrong URL — says nothing about the message, so it's treated as infrastructure.
_NOT_FOUND_CODES = {40400, 40401, 40403}


class UndecodableMessage(ValueError):
    """The bytes aren't an Avro message of a schema the registry knows. Bad data: DLQ it."""


class SchemaNotRegistered(RuntimeError):
    pass


def load_schema(path: Path) -> str:
    # Stripped because the serializer strips too: registry lookups then match the string exactly.
    return Path(path).read_text().strip()


def subject_for(topic: str) -> str:
    # TopicNameStrategy: one subject (and so one evolving schema) per topic's values.
    return f"{topic}-value"


def make_registry(url: str = config.SCHEMA_REGISTRY_URL) -> SchemaRegistryClient:
    # new_client() returns an in-memory mock for "mock://..." URLs — used by the unit tests.
    return SchemaRegistryClient.new_client({"url": url})


def ensure_registered(registry: SchemaRegistryClient, topic: str, schema_path: Path) -> int:
    """Fail fast, with the fix in the message, if `schema_path` isn't registered for `topic`."""
    subject = subject_for(topic)
    try:
        return registry.lookup_schema(subject, Schema(load_schema(schema_path), "AVRO")).schema_id
    except SchemaRegistryError as exc:
        if exc.error_code not in _NOT_FOUND_CODES:
            raise
        raise SchemaNotRegistered(
            f"{schema_path} is not registered under subject {subject!r}. Register it first:\n"
            f"  uv run python -m pipeline.schemas register {schema_path}"
        ) from exc


class AvroSerde:
    """Encodes models with one schema and decodes any registered writer version into that same schema."""

    model: type[BaseModel] = Ticket

    def __init__(self, registry: SchemaRegistryClient, schema_str: str):
        self._registry = registry
        # auto.register.schemas=False: producers may only use schemas someone registered on purpose,
        # so the registry's compatibility check can't be bypassed by just deploying new code.
        # TOPIC = subject_for(): "<topic>-value". Set explicitly because the client's default strategy
        # asks the registry for the subject, and decode() relies on phase 2 making no network calls.
        self._serializer = AvroSerializer(registry, schema_str, conf={
            "auto.register.schemas": False, "subject.name.strategy.type": "TOPIC"})
        # Passing schema_str makes it the reader schema: every writer version is resolved into it.
        self._deserializer = AvroDeserializer(registry, schema_str, conf={"subject.name.strategy.type": "TOPIC"})

    def encode(self, obj: BaseModel, topic: str) -> bytes:
        # model_dump() keeps UUID/datetime objects, which Avro's uuid/timestamp-millis types expect.
        # Fields the schema doesn't have (tier, when writing v1) are simply not written.
        return self._serializer(obj.model_dump(), SerializationContext(topic, MessageField.VALUE))

    def decode(self, value: bytes, topic: str) -> BaseModel:
        """Two phases, so every failure lands in the right bucket:

        1. Fetch the writer schema. Only the registry's "not found" is the message's fault; anything
           else (unreachable, 5xx, garbage answers) propagates, so the validator crashes uncommitted.
        2. Decode. The writer schema is now cached, so no network is involved: any error at all is
           bad bytes — out-of-range values, a foreign record's id, an enum symbol we don't know yet.
        """
        if len(value) <= 5:
            raise UndecodableMessage(f"{len(value)} bytes is too short for Schema Registry framing")
        if value[0] != 0:
            # Our producers write magic byte 0 (+ 4-byte id). JSON starts with "{", i.e. magic byte 123.
            raise UndecodableMessage(f"unsupported framing: magic byte {value[0]} (expected 0)")
        schema_id = int.from_bytes(value[1:5], "big")
        try:
            # Same subject the deserializer uses, so this fills the exact cache entry it will read.
            self._registry.get_schema(schema_id, subject_for(topic))
        except SchemaRegistryError as exc:
            if exc.error_code in _NOT_FOUND_CODES:
                raise UndecodableMessage(f"unknown schema id {schema_id}: {exc}") from exc
            raise
        try:
            record = self._deserializer(value, SerializationContext(topic, MessageField.VALUE))
        except Exception as exc:
            raise UndecodableMessage(f"{type(exc).__name__}: {exc}") from exc
        # Outside the try: a ValidationError here is a readable record that breaks our rules.
        return self.model.model_validate(record)


class TicketSerde(AvroSerde):
    model = Ticket


class EnrichedTicketSerde(AvroSerde):
    model = EnrichedTicket
