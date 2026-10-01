"""Central settings. Each value can be overridden with an environment variable."""
import os
from dataclasses import dataclass, field
from pathlib import Path

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
DEFAULT_MODEL = os.environ.get("OLLAMA_MODEL", "qwen3.5:4b")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
# Kafka Connect's REST API and the Postgres its sinks write to (stage 7). Postgres is published on
# 5433 because the host may already run its own Postgres on 5432.
CONNECT_URL = os.environ.get("CONNECT_URL", "http://localhost:8083")
POSTGRES_DSN = os.environ.get("POSTGRES_DSN", "postgresql://tickets:tickets@localhost:5433/tickets")

# Avro schemas are shared contracts (Kafka Connect reads them too), so they live at the repo root.
SCHEMA_DIR = Path(__file__).resolve().parents[2] / "schemas"
TICKET_SCHEMA_V1 = SCHEMA_DIR / "ticket.v1.avsc"
TICKET_SCHEMA_V2 = SCHEMA_DIR / "ticket.v2.avsc"
DEFAULT_TICKET_SCHEMA = TICKET_SCHEMA_V2
ENRICHED_SCHEMA_V1 = SCHEMA_DIR / "enriched_ticket.v1.avsc"
STATS_SCHEMA_V1 = SCHEMA_DIR / "ticket_stats.v1.avsc"

# Connector configs (JSON) and the SQL that creates the tables they write to.
CONNECT_DIR = Path(__file__).resolve().parents[2] / "connect"
SINK_TABLES_SQL = CONNECT_DIR / "sql" / "init.sql"

TOPIC_RAW = "tickets.raw"
TOPIC_VALID = "tickets.valid"
TOPIC_DLQ = "tickets.dlq"
TOPIC_BILLING = "tickets.billing"
TOPIC_TECH = "tickets.tech"
TOPIC_URGENT = "tickets.urgent"
TOPIC_ENRICHED_OTHER = "tickets.enriched.other"
ENRICHED_TOPICS: tuple[str, ...] = (TOPIC_BILLING, TOPIC_TECH, TOPIC_URGENT, TOPIC_ENRICHED_OTHER)

# Written by the aggregator (stage 6).
TOPIC_STATS = "tickets.stats"
TOPIC_CUSTOMERS_LATEST = "customers.latest"
# Every enriched ticket exactly once: tickets.urgent only holds copies, so reading it would count
# urgent tickets twice.
AGGREGATOR_INPUTS: tuple[str, ...] = (TOPIC_BILLING, TOPIC_TECH, TOPIC_ENRICHED_OTHER)

# Written by Kafka Connect when a sink can't convert or write a record (stage 7). Separate from
# tickets.dlq because Connect uses its own header names (__connect.errors.*).
TOPIC_SINK_DLQ = "tickets.sink.dlq"


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    config: dict[str, str] = field(default_factory=dict)


DLQ_RETENTION = {"retention.ms": str(30 * 24 * 3600 * 1000)}

TOPIC_SPECS = [
    # 6 partitions = up to 6 consumers in one group can share the work.
    TopicSpec(TOPIC_RAW, 6),
    TopicSpec(TOPIC_VALID, 6),
    # One partition is plenty for the DLQ; keep failures for 30 days.
    TopicSpec(TOPIC_DLQ, 1, DLQ_RETENTION),
    # Routed by the enricher. 3 partitions each: smaller, downstream topics.
    *(TopicSpec(name, 3) for name in ENRICHED_TOPICS),
    TopicSpec(TOPIC_STATS, 3),
    # Compacted: Kafka eventually keeps only the newest message per key, so the topic behaves like
    # a table of customers. Tiny segments and a low dirty ratio make the cleaner run within minutes,
    # for the demo. Production would keep the defaults (7-day segments, dirty ratio 0.5).
    TopicSpec(TOPIC_CUSTOMERS_LATEST, 3,
              {"cleanup.policy": "compact", "segment.ms": "60000", "min.cleanable.dirty.ratio": "0.01"}),
    # Kafka Connect's DLQ for the Postgres sinks; same sizing as ours.
    TopicSpec(TOPIC_SINK_DLQ, 1, DLQ_RETENTION),
]
