"""Avro through Schema Registry: the wire format for ticket topics from stage 4 on.

On the wire each value is: 1 magic byte (0) + 4-byte schema id + Avro binary. The id tells a
consumer which schema *wrote* the message; the consumer's own schema is the *reader* schema, and
Avro resolves one into the other (filling defaults for fields the writer didn't know about).
"""
from pathlib import Path

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import MessageField, SerializationContext, SerializationError

from pipeline import config
from pipeline.models import Ticket

# What fastavro raises on corrupt Avro bytes, beyond the client's own SerializationError.
_CORRUPT_AVRO = (SerializationError, EOFError, ValueError, IndexError)


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
        if exc.http_status_code != 404:
            raise
        raise SchemaNotRegistered(
            f"{schema_path} is not registered under subject {subject!r}. Register it first:\n"
            f"  uv run python -m pipeline.schemas register {schema_path}"
        ) from exc


class TicketSerde:
    """Encodes Tickets with one schema and decodes any registered version into that same schema."""

    def __init__(self, registry: SchemaRegistryClient, schema_str: str):
        # auto.register.schemas=False: producers may only use schemas someone registered on purpose,
        # so the registry's compatibility check can't be bypassed by just deploying new code.
        self._serializer = AvroSerializer(registry, schema_str, conf={"auto.register.schemas": False})
        # Passing schema_str makes it the reader schema: every writer version is resolved into it.
        self._deserializer = AvroDeserializer(registry, schema_str)

    def encode(self, ticket: Ticket, topic: str) -> bytes:
        # model_dump() keeps UUID/datetime objects, which Avro's uuid/timestamp-millis types expect.
        # Fields the schema doesn't have (tier, when writing v1) are simply not written.
        return self._serializer(ticket.model_dump(), SerializationContext(topic, MessageField.VALUE))

    def decode(self, value: bytes, topic: str) -> Ticket:
        try:
            record = self._deserializer(value, SerializationContext(topic, MessageField.VALUE))
        except SchemaRegistryError as exc:
            if exc.http_status_code == 404:
                schema_id = int.from_bytes(value[1:5], "big")
                raise UndecodableMessage(f"unknown schema id {schema_id}: {exc}") from exc
            raise  # the registry is broken, not the message: crash, don't dead-letter
        except _CORRUPT_AVRO as exc:
            raise UndecodableMessage(f"{type(exc).__name__}: {exc}") from exc
        # Outside the try: a ValidationError here is a readable ticket that breaks our rules.
        return Ticket.model_validate(record)
