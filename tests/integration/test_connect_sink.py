import time
import uuid
from datetime import UTC, datetime, timedelta

import httpx
import psycopg
import pytest
from confluent_kafka import KafkaError, KafkaException, Producer
from psycopg import sql
from psycopg.rows import dict_row

from pipeline import config
from pipeline.clients import producer_config
from pipeline.connectors import ConnectClient, Connector, load_connectors
from pipeline.models import EnrichedTicket, TicketStats
from pipeline.serde import EnrichedTicketSerde, TicketStatsSerde, load_schema

pytestmark = pytest.mark.integration

TIMEOUT = 60.0  # a new connector needs a few seconds to start its tasks and join its consumer group


@pytest.fixture
def connect() -> ConnectClient:
    client = ConnectClient(config.CONNECT_URL)
    try:
        client.names()
    except httpx.TransportError:
        pytest.skip(f"Kafka Connect is not running at {config.CONNECT_URL} (docker compose up -d --build)")
    return client


@pytest.fixture
def pg():
    try:
        conn = psycopg.connect(config.POSTGRES_DSN, autocommit=True, connect_timeout=5)
    except psycopg.OperationalError:
        pytest.skip(f"Postgres is not reachable at {config.POSTGRES_DSN} (docker compose up -d --build)")
    with conn:
        yield conn


@pytest.fixture
def make_table(pg):
    """A throwaway copy of a sink table (same columns, defaults and primary key), dropped afterwards."""
    made: list[str] = []

    def _make(like: str) -> str:
        name = f"test_{like}_{uuid.uuid4().hex[:8]}"
        pg.execute(sql.SQL("CREATE TABLE {} (LIKE {} INCLUDING ALL)").format(sql.Identifier(name), sql.Identifier(like)))
        made.append(name)
        return name

    yield _make
    for name in made:
        pg.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(name)))


def _delete_group(admin, group: str) -> None:
    # The group can only be deleted once the deleted connector's tasks have left it.
    deadline = time.monotonic() + 30
    while True:
        try:
            admin.delete_consumer_groups([group], request_timeout=10)[group].result()
            return
        except KafkaException as exc:
            if exc.args[0].code() == KafkaError.GROUP_ID_NOT_FOUND:
                return
            if time.monotonic() > deadline:
                raise
            time.sleep(1)


@pytest.fixture
def run_sink(connect, admin):
    """Start a copy of a committed connector with test overrides; delete it and its consumer group afterwards.

    Request this fixture after make_topic, so the connector is torn down before its topics are."""
    started: list[str] = []

    def _run(base: str, overrides: dict[str, str]) -> str:
        [connector] = [c for c in load_connectors() if c.name == base]
        name = f"test-{base}-{uuid.uuid4().hex[:8]}"
        # max.retries=0: a record Postgres rejects goes to the DLQ at once, not after the 5-minute budget.
        connect.apply(Connector(name, {**connector.config, "max.retries": "0", **overrides}))
        started.append(name)
        return name

    yield _run
    for name in started:
        connect.delete(name)
        _delete_group(admin, f"connect-{name}")


def _wait_for(fetch, done, timeout: float = TIMEOUT):
    """Poll fetch() until done(result) or the timeout; return the last result for the test to assert on."""
    deadline = time.monotonic() + timeout
    while True:
        result = fetch()
        if done(result) or time.monotonic() > deadline:
            return result
        time.sleep(0.5)


def _rows(pg, table: str) -> list[dict]:
    with pg.cursor(row_factory=dict_row) as cur:
        return cur.execute(sql.SQL("SELECT * FROM {}").format(sql.Identifier(table))).fetchall()


def _headers(message) -> dict[str, str]:
    return {key: value.decode() for key, value in (message.headers() or [])}


def _all_running(connect, name: str) -> bool:
    status = connect.status(name)
    states = [status["connector"]["state"], *(t["state"] for t in status["tasks"])]
    return bool(status["tasks"]) and set(states) == {"RUNNING"}


@pytest.fixture
def producer() -> Producer:
    return Producer(producer_config())


@pytest.fixture
def produce_ticket(registry, ticket_dict, producer):
    serde = EnrichedTicketSerde(registry, load_schema(config.ENRICHED_SCHEMA_V1))

    def _produce(topic: str, ticket_id: str | None = None, **fields) -> EnrichedTicket:
        ticket = EnrichedTicket.model_validate({
            **ticket_dict, "ticket_id": ticket_id or str(uuid.uuid4()), "tier": "pro", "category": "billing",
            "priority": "urgent", "sentiment": -0.5, "summary": "Charged twice", "enriched_at": datetime.now(UTC),
            "model": "stub", **fields})
        producer.produce(topic, key=ticket.customer_id.encode(), value=serde.encode(ticket, topic))
        assert producer.flush(10) == 0
        return ticket

    return _produce


@pytest.fixture
def ticket_topics(make_topic, register_schema):
    """(billing, urgent, dlq) test topics, with the enriched schema registered for the first two."""
    billing, urgent, dlq = make_topic("sink.billing"), make_topic("sink.urgent"), make_topic("sink.dlq")
    for topic in (billing, urgent):
        register_schema(topic, config.ENRICHED_SCHEMA_V1)
    return billing, urgent, dlq


def test_a_copy_on_another_topic_updates_the_same_row_and_keeps_loaded_at(
        ticket_topics, make_table, run_sink, produce_ticket, pg):
    billing, urgent, dlq = ticket_topics
    table = make_table("tickets")
    run_sink("tickets-sink", {"topics": f"{billing},{urgent}", "table.name.format": table,
                              "errors.deadletterqueue.topic.name": dlq})

    ticket = produce_ticket(billing)
    [first] = _wait_for(lambda: _rows(pg, table), lambda rows: len(rows) == 1)
    assert first["ticket_id"] == str(ticket.ticket_id)
    assert first["kafka_topic"] == billing           # filled in by the InsertField SMT
    assert first["created_at"] == ticket.created_at  # timestamp-millis → TIMESTAMPTZ, same instant
    assert first["channel"] == ticket.channel        # Avro enum → TEXT

    # The same ticket again, as the enricher's copy on tickets.urgent. With insert.mode=insert this would
    # violate the primary key; with upsert it overwrites the row.
    produce_ticket(urgent, ticket_id=str(ticket.ticket_id), summary="copy from urgent")
    rows = _wait_for(lambda: _rows(pg, table), lambda rows: rows[0]["kafka_topic"] == urgent)
    assert len(rows) == 1
    [row] = rows
    assert (row["kafka_topic"], row["summary"]) == (urgent, "copy from urgent")
    assert row["loaded_at"] == first["loaded_at"]  # the upsert never sends loaded_at


def test_bytes_that_are_not_avro_go_to_the_dlq_and_the_sink_keeps_running(
        ticket_topics, make_table, run_sink, produce_ticket, producer, read_topic, pg, connect):
    billing, urgent, dlq = ticket_topics
    table = make_table("tickets")
    producer.produce(billing, key=b"C-0001", value=b"not avro at all")
    assert producer.flush(10) == 0
    good = produce_ticket(billing)
    name = run_sink("tickets-sink", {"topics": f"{billing},{urgent}", "table.name.format": table,
                                     "errors.deadletterqueue.topic.name": dlq})

    rows = _wait_for(lambda: _rows(pg, table), lambda rows: len(rows) == 1)
    assert [r["ticket_id"] for r in rows] == [str(good.ticket_id)]
    [message] = read_topic(dlq, 1, timeout=TIMEOUT)
    assert message.value() == b"not avro at all"  # the original bytes
    headers = _headers(message)
    assert headers["__connect.errors.stage"] == "VALUE_CONVERTER"
    assert headers["__connect.errors.topic"] == billing
    assert headers["__connect.errors.offset"] == "0"
    assert _wait_for(lambda: _all_running(connect, name), bool)


def test_a_record_postgres_rejects_goes_to_the_dlq_and_the_rest_are_written(
        ticket_topics, make_table, run_sink, produce_ticket, read_topic, pg, registry):
    billing, urgent, dlq = ticket_topics
    table = make_table("tickets")
    # Only for this test's copy: a column too short for one of the records.
    pg.execute(sql.SQL("ALTER TABLE {} ALTER COLUMN model TYPE varchar(8)").format(sql.Identifier(table)))
    before = produce_ticket(billing)
    rejected = produce_ticket(billing, model="much-too-long-model")
    after = produce_ticket(billing)
    run_sink("tickets-sink", {"topics": f"{billing},{urgent}", "table.name.format": table,
                              "errors.deadletterqueue.topic.name": dlq})

    rows = _wait_for(lambda: _rows(pg, table), lambda rows: len(rows) == 2)
    assert {r["ticket_id"] for r in rows} == {str(before.ticket_id), str(after.ticket_id)}
    [message] = read_topic(dlq, 1, timeout=TIMEOUT)
    headers = _headers(message)
    assert headers["__connect.errors.stage"] == "TASK_PUT"
    assert "value too long" in headers["__connect.errors.exception.message"]
    serde = EnrichedTicketSerde(registry, load_schema(config.ENRICHED_SCHEMA_V1))
    assert serde.decode(message.value(), billing).ticket_id == rejected.ticket_id


def test_a_window_emitted_twice_is_one_row_with_the_last_count(
        make_topic, register_schema, make_table, run_sink, producer, registry, pg):
    stats_topic, dlq = make_topic("sink.stats"), make_topic("sink.dlq")
    register_schema(stats_topic, config.STATS_SCHEMA_V1)
    table = make_table("ticket_stats")
    serde = TicketStatsSerde(registry, load_schema(config.STATS_SCHEMA_V1))
    start = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)

    def emit(count: int) -> None:
        row = TicketStats(dimension="category", value="billing", window_start=start,
                          window_end=start + timedelta(minutes=5), count=count)
        producer.produce(stats_topic, key=b"category=billing", value=serde.encode(row, stats_topic))
        assert producer.flush(10) == 0

    run_sink("stats-sink", {"topics": stats_topic, "table.name.format": table,
                            "errors.deadletterqueue.topic.name": dlq})
    emit(3)
    [first] = _wait_for(lambda: _rows(pg, table), lambda rows: len(rows) == 1)
    assert (first["dimension"], first["value"], first["window_start"], first["count"]) == ("category", "billing", start, 3)

    # An aggregator restart can emit the same closed window again (at-least-once).
    emit(5)
    rows = _wait_for(lambda: _rows(pg, table), lambda rows: rows[0]["count"] == 5)
    assert len(rows) == 1
    assert rows[0]["count"] == 5
    assert rows[0]["loaded_at"] == first["loaded_at"]
