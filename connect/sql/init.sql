-- Tables the Kafka Connect JDBC sinks write into (connect/*-sink.json).
-- Postgres runs this file only when it initialises an EMPTY data volume. After changing it:
--   docker compose down -v && docker compose up -d --build     (drops all Postgres data)
-- The sinks run with auto.create=false and auto.evolve=false: these tables are the contract.
-- Column names must equal the Avro field names; the sink maps fields to columns by name.

CREATE TABLE tickets (
    ticket_id    TEXT PRIMARY KEY,                    -- the upsert key; Avro's uuid arrives as a string
    customer_id  TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL,                -- Avro timestamp-millis
    channel      TEXT NOT NULL,                       -- Avro enums arrive as strings
    product      TEXT NOT NULL,
    subject      TEXT NOT NULL,
    body         TEXT NOT NULL,
    tier         TEXT NOT NULL,
    category     TEXT NOT NULL,
    priority     TEXT NOT NULL,
    sentiment    DOUBLE PRECISION NOT NULL,
    summary      TEXT NOT NULL,
    enriched_at  TIMESTAMPTZ NOT NULL,
    model        TEXT NOT NULL,
    kafka_topic  TEXT NOT NULL,                       -- added by the InsertField SMT: the copy written last wins
    loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now()   -- first insert only; an upsert never sends it
);
CREATE INDEX tickets_created_at ON tickets (created_at);
CREATE INDEX tickets_customer ON tickets (customer_id, created_at DESC);

CREATE TABLE ticket_stats (
    dimension     TEXT NOT NULL,
    value         TEXT NOT NULL,
    window_start  TIMESTAMPTZ NOT NULL,
    window_end    TIMESTAMPTZ NOT NULL,
    count         BIGINT NOT NULL,
    loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (dimension, value, window_start)      -- a window emitted twice upserts to one row
);
