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
ENRICHED_SCHEMA_V1 = SCHEMA_DIR / "enriched_ticket.v1.avsc"

TOPIC_RAW = "tickets.raw"
TOPIC_VALID = "tickets.valid"
TOPIC_DLQ = "tickets.dlq"
TOPIC_BILLING = "tickets.billing"
TOPIC_TECH = "tickets.tech"
TOPIC_URGENT = "tickets.urgent"
TOPIC_ENRICHED_OTHER = "tickets.enriched.other"
ENRICHED_TOPICS: tuple[str, ...] = (TOPIC_BILLING, TOPIC_TECH, TOPIC_URGENT, TOPIC_ENRICHED_OTHER)


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
    # Routed by the enricher. 3 partitions each: smaller, downstream topics.
    *(TopicSpec(name, 3) for name in ENRICHED_TOPICS),
]
