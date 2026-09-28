"""Central settings. Each value can be overridden with an environment variable."""
import os
from dataclasses import dataclass, field

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
DEFAULT_MODEL = os.environ.get("OLLAMA_MODEL", "qwen3.5:4b")

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
