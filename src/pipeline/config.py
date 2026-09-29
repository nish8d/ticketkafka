"""Central settings. Each value can be overridden with an environment variable."""
import os
from dataclasses import dataclass, field
from pathlib import Path

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
DEFAULT_MODEL = os.environ.get("OLLAMA_MODEL", "qwen3.5:4b")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")

# Avro schemas are shared contracts (Kafka Connect reads them too), so they live at the repo root.
SCHEMA_DIR = Path(__file__).resolve().parents[2] / "schemas"
TICKET_SCHEMA_V1 = SCHEMA_DIR / "ticket.v1.avsc"
TICKET_SCHEMA_V2 = SCHEMA_DIR / "ticket.v2.avsc"
DEFAULT_TICKET_SCHEMA = TICKET_SCHEMA_V2

TOPIC_RAW = "tickets.raw"
TOPIC_VALID = "tickets.valid"
TOPIC_DLQ = "tickets.dlq"


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    config: dict[str, str] = field(default_factory=dict)


TOPIC_SPECS = [
    # 6 partitions = up to 6 consumers in one group can share the work.
    TopicSpec(TOPIC_RAW, 6),
    TopicSpec(TOPIC_VALID, 6),
    # One partition is plenty for the DLQ; keep failures for 30 days.
    TopicSpec(TOPIC_DLQ, 1, {"retention.ms": str(30 * 24 * 3600 * 1000)}),
]
