"""Adapters so Quix Streams reads and writes through our own Avro serdes (serde.py).

Why not Quix's built-in Avro serializers: ours already sort every failure into bad data
(UndecodableMessage, or a ValidationError from the model: DLQ it) and infrastructure (anything
else: crash), and they validate records with the same Pydantic models as every other service.
Quix passes whatever a deserializer raises, unwrapped, to Application(on_consumer_error=...).
"""
from quixstreams.models.serializers import Deserializer, SerializationContext, Serializer

from pipeline.serde import AvroSerde


class QuixAvroDeserializer(Deserializer):
    def __init__(self, serde: AvroSerde):
        super().__init__()
        self._serde = serde

    def __call__(self, value: bytes, ctx: SerializationContext) -> dict:
        # mode="json": UUIDs and datetimes become strings. Rows must be JSON-safe because
        # group_by() writes them through a repartition topic as JSON.
        return self._serde.decode(value, ctx.topic).model_dump(mode="json")


class QuixAvroSerializer(Serializer):
    def __init__(self, serde: AvroSerde):
        super().__init__()
        self._serde = serde

    def __call__(self, value: dict, ctx: SerializationContext) -> bytes:
        # model_validate parses strings back into UUIDs/datetimes, and refuses bad records before
        # they reach the topic.
        return self._serde.encode(self._serde.model.model_validate(value), ctx.topic)
