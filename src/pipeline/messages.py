"""Message shapes shared by every service that consumes and produces: outputs and DLQ context."""
from dataclasses import dataclass
from datetime import datetime

MAX_ERROR_LEN = 1000


@dataclass(frozen=True)
class SourceRef:
    """Where an input message came from — recorded on DLQ messages for debugging and replay."""

    topic: str
    partition: int
    offset: int


@dataclass(frozen=True)
class Output:
    topic: str
    key: bytes | None
    value: bytes | None
    headers: list[tuple[str, bytes]]


def dlq_headers(exc: Exception, source: SourceRef, now: datetime) -> list[tuple[str, bytes]]:
    return [
        ("error.type", type(exc).__name__.encode()),
        ("error.message", str(exc)[:MAX_ERROR_LEN].encode()),
        ("source.topic", source.topic.encode()),
        ("source.partition", str(source.partition).encode()),
        ("source.offset", str(source.offset).encode()),
        ("failed_at", now.isoformat().encode()),
    ]
