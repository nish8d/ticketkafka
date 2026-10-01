import json

from pipeline import config


def _avro_fields(path) -> list[str]:
    return [field["name"] for field in json.loads(path.read_text())["fields"]]


def test_init_sql_defines_the_two_sink_tables(sink_tables):
    assert set(sink_tables) == {"tickets", "ticket_stats"}


def test_every_enriched_ticket_field_has_a_column(sink_tables):
    # The sink maps Avro fields to columns by name; a field with no column fails every write.
    columns, _ = sink_tables["tickets"]
    assert set(_avro_fields(config.ENRICHED_SCHEMA_V1)) <= set(columns)


def test_every_stats_field_has_a_column(sink_tables):
    columns, _ = sink_tables["ticket_stats"]
    assert set(_avro_fields(config.STATS_SCHEMA_V1)) <= set(columns)


def test_extra_columns_are_only_ones_the_smt_or_postgres_fills(sink_tables):
    # Any other extra NOT NULL column would make Postgres reject every row the sink writes.
    tickets, _ = sink_tables["tickets"]
    stats, _ = sink_tables["ticket_stats"]
    assert set(tickets) - set(_avro_fields(config.ENRICHED_SCHEMA_V1)) == {"kafka_topic", "loaded_at"}
    assert set(stats) - set(_avro_fields(config.STATS_SCHEMA_V1)) == {"loaded_at"}


def test_primary_keys_are_the_upsert_keys(sink_tables):
    assert sink_tables["tickets"][1] == ("ticket_id",)
    assert sink_tables["ticket_stats"][1] == ("dimension", "value", "window_start")
