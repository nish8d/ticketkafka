# ticketkafka

A Kafka data pipeline for customer support tickets, built in stages to learn Kafka. A local LLM
(via [Ollama](https://ollama.com)) writes realistic dummy tickets, and the pipeline then validates,
classifies, aggregates and stores them.

```
                    ┌──────────────┐
 Ollama ──────────► │  generator   │  Avro tickets, keyed by customer_id
                    └──────┬───────┘
                           ▼
                      tickets.raw ─────► validator ─────► tickets.valid
                                            │                  │
                                            ▼                  ▼
                                       tickets.dlq ◄──── enricher ◄──── Ollama
                                            │          (category, priority, sentiment, summary)
                                       dlq_replay              │
                                                               ▼
                         tickets.billing · tickets.tech · tickets.enriched.other · tickets.urgent
                                    │                                     │
                                    ▼                                     ▼
                         aggregator (Quix Streams)              Kafka Connect JDBC sink
                     5-min windows → tickets.stats ───────────►  Postgres: tickets,
                   latest per customer → customers.latest          ticket_stats
                                                                       │
                                                                       ▼
                                                              Streamlit dashboard
```

## Stages

| # | Built | Kafka concepts |
|---|---|---|
| 1 | Docker Compose: single KRaft broker and Kafka UI | Topics, partitions, offsets, retention |
| 2 | **Generator**: Ollama → `tickets.raw`, keyed by `customer_id` | Keys and partitioning, `acks=all`, idempotence, batching |
| 3 | **Validator**: `tickets.raw` → `tickets.valid` / `tickets.dlq`; DLQ replay | Consumer groups, manual commits, at-least-once, dead-letter queues |
| 4 | Schema Registry and Avro, then a schema change | Serialization, compatibility modes, schema evolution |
| 5 | **Enricher**: LLM classification → routed topics; run several | Scaling, rebalancing, consumer lag, retries, backpressure |
| 6 | **Aggregator** (Quix Streams): tumbling-window counts, latest ticket per customer | Event time, windows and grace, state stores, changelog topics, compaction |
| 7 | **Kafka Connect** JDBC sinks → Postgres; Streamlit dashboard | Connectors and tasks, converters, SMTs, idempotent upserts, Connect's DLQ |
| 8 | **Transactional enricher**: each batch's outputs and offsets in one Kafka transaction | Exactly-once, transactional producers, fencing, `read_committed`, abort markers |

Stages 1–5 and 8 use the plain `confluent-kafka` client with hand-written poll loops and commits, so
every mechanism is visible. Stage 6 uses a stream-processing library and stage 7 uses Kafka Connect.

## Requirements

- Docker with Compose
- [uv](https://docs.astral.sh/uv/) (installs Python 3.12+ and the dependencies)
- [Ollama](https://ollama.com) running on the host, with a model pulled:
  `ollama pull qwen3.5:4b` (the default) or `ollama pull qwen2.5:0.5b` (small and fast; pass
  `--model qwen2.5:0.5b`)
- About 4 GB of free RAM for the stack (Kafka Connect alone uses ~1 GB), plus the model

## Quick start

```bash
# 1. Start Kafka, Schema Registry, Kafka UI, Postgres and Kafka Connect
docker compose up -d --build
uv sync

# 2. Create the topics and register the Avro schemas (both safe to re-run)
uv run python -m pipeline.admin
uv run python -m pipeline.schemas register schemas/ticket.v2.avsc
uv run python -m pipeline.schemas register schemas/enriched_ticket.v1.avsc
uv run python -m pipeline.schemas register schemas/ticket_stats.v1.avsc

# 3. Start the Postgres sinks
uv run python -m pipeline.connectors apply

# 4. Run the services, each in its own terminal
uv run python -m pipeline.validator
uv run python -m pipeline.enricher --model qwen2.5:0.5b
uv run python -m pipeline.aggregator

# 5. Produce some tickets, then watch them arrive
uv run python -m pipeline.generator --count 20 --model qwen2.5:0.5b
uv run streamlit run dashboard/app.py
```

| Service | Address |
|---|---|
| Kafka | `localhost:9092` |
| Kafka UI | http://localhost:8080 |
| Schema Registry | http://localhost:8081 |
| Kafka Connect REST API | http://localhost:8083 |
| Postgres | `localhost:5433` (user, password and database `tickets`) |
| Dashboard | http://localhost:8501 |

Postgres is published on **5433**, so it doesn't clash with a Postgres already running on the host's 5432.

## Commands

| Command | What it does |
|---|---|
| `uv run python -m pipeline.admin` | Create the pipeline topics |
| `uv run python -m pipeline.schemas register\|check FILE` / `list` | Register a schema, check its compatibility first, or list subjects |
| `uv run python -m pipeline.generator [--count N] [--rate R] [--model M] [--bad-ratio 0.05] [--seed S]` | Produce LLM-written tickets, with a share deliberately broken to exercise the DLQ |
| `uv run python -m pipeline.validator` | `tickets.raw` → `tickets.valid`, or `tickets.dlq` for bad tickets |
| `uv run python -m pipeline.enricher [--model M] [--batch-size 4] [--instance N] [--at-least-once]` | Classify tickets and route them by category, in one transaction per batch; start several (each with its own `--instance`) to share the work |
| `uv run python -m pipeline.aggregator [--window-seconds 300] [--grace-seconds 60]` | Windowed counts → `tickets.stats`, latest ticket per customer → `customers.latest` |
| `uv run python -m pipeline.dlq_replay [--dry-run] [--limit N]` | Send dead-lettered messages back to their source topic |
| `uv run python -m pipeline.connectors apply\|status\|restart\|delete [NAME]` | Manage the Kafka Connect sinks defined in `connect/*.json` |
| `uv run streamlit run dashboard/app.py` | Read-only dashboard over Postgres |

## Topics

| Topic | Partitions | Contents |
|---|---|---|
| `tickets.raw` | 6 | Generated tickets (Avro `Ticket`), key `customer_id` |
| `tickets.valid` | 6 | Tickets that passed validation |
| `tickets.dlq` | 1 | Failed messages: original bytes plus `error.*` / `source.*` headers |
| `tickets.billing`, `tickets.tech`, `tickets.enriched.other` | 3 each | Enriched tickets (Avro `EnrichedTicket`), routed by category |
| `tickets.urgent` | 3 | A copy of every `priority=urgent` ticket |
| `tickets.stats` | 3 | Window counts per category and per priority (Avro `TicketStats`) |
| `customers.latest` | 3 | Newest ticket per customer, compacted |
| `tickets.sink.dlq` | 1 | Kafka Connect's DLQ, with `__connect.errors.*` headers |

## Layout

```
docker-compose.yml     Kafka (KRaft), Schema Registry, Kafka UI, Postgres, Kafka Connect
schemas/               Avro schemas (ticket v1/v2, a deliberately breaking v3, enriched ticket, stats)
src/pipeline/          the services and CLIs; topic names and settings in config.py
connect/               Connect image (JDBC plugin), sink connector configs, Postgres tables
dashboard/app.py       Streamlit page
tests/unit/            no Kafka, no LLM
tests/integration/     against the Compose stack, with throwaway topics, tables and connectors
```

## Tests

```bash
uv run pytest                  # unit tests: no Docker, and the LLM is always stubbed
uv run pytest -m integration   # needs `docker compose up -d --build`
```

## Design notes

- **At-least-once almost everywhere, exactly-once in the enricher.** Offsets are committed only after
  the output is acknowledged, so a crash means some messages are processed again; `ticket_id` is the
  idempotency key and the Postgres sinks upsert on it, so duplicates collapse. The enricher goes
  further: each batch's outputs and offsets commit in one Kafka transaction, and every reader uses
  `isolation.level=read_committed`, so a crashed batch leaves no duplicates at all
  (`--at-least-once` switches back, for comparison).
- **Dead-letter queues keep the original bytes.** The reason goes in headers, so a fixed message
  can be replayed unchanged.
- **Logic lives in pure functions.** Validation, routing, prompt parsing and windowing are tested
  without Kafka or the LLM.
- **The Postgres tables are hand-written** (`connect/sql/init.sql`), and the sinks never create or
  alter them. A unit test keeps the Avro schemas and the table definitions in step.
