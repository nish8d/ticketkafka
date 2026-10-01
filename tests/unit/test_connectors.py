import json

import httpx
import psycopg
import pytest

from pipeline import config
from pipeline.connectors import (
    REQUIRED_KEYS,
    ConnectClient,
    Connector,
    ConnectorError,
    format_status,
    has_failed,
    load_connector,
    load_connectors,
    main,
    missing_tables,
)

SCHEMA_FOR = {"tickets-sink": config.ENRICHED_SCHEMA_V1, "stats-sink": config.STATS_SCHEMA_V1}


@pytest.fixture(scope="module")
def connectors() -> dict[str, Connector]:
    return {c.name: c for c in load_connectors()}


# --- the committed connector files ---------------------------------------------------------------

def test_the_committed_connectors(connectors):
    assert set(connectors) == {"tickets-sink", "stats-sink"}
    for path in config.CONNECT_DIR.glob("*.json"):
        assert load_connector(path).name == path.stem  # file name = connector name


def test_tickets_sink_reads_every_enriched_topic_including_urgent(connectors):
    # tickets.urgent only holds copies; the upsert on ticket_id makes them land on the same row.
    assert set(connectors["tickets-sink"].config["topics"].split(",")) == set(config.ENRICHED_TOPICS)


def test_stats_sink_reads_tickets_stats(connectors):
    assert connectors["stats-sink"].config["topics"] == config.TOPIC_STATS


def test_each_sink_writes_into_an_init_sql_table_keyed_by_its_upsert_key(connectors, sink_tables):
    for connector in connectors.values():
        assert connector.table in sink_tables
        _, primary_key = sink_tables[connector.table]
        assert tuple(connector.config["pk.fields"].split(",")) == primary_key


def test_pk_fields_exist_in_the_avro_schema(connectors):
    for name, connector in connectors.items():
        fields = {f["name"] for f in json.loads(SCHEMA_FOR[name].read_text())["fields"]}
        assert set(connector.config["pk.fields"].split(",")) <= fields


def test_both_sinks_share_the_write_and_error_policy(connectors):
    expected = {
        "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
        "insert.mode": "upsert",
        "pk.mode": "record_value",
        "auto.create": "false",
        "auto.evolve": "false",
        # 60 x 5 s: Postgres may be down for 5 minutes before records spill into the DLQ.
        "max.retries": "60",
        "retry.backoff.ms": "5000",
        "errors.tolerance": "all",
        "errors.deadletterqueue.topic.name": config.TOPIC_SINK_DLQ,
        "errors.deadletterqueue.topic.replication.factor": "1",
        "errors.deadletterqueue.context.headers.enable": "true",
        # Connect runs in Docker, so it reaches Postgres by its service name, not localhost:5433.
        "connection.url": "jdbc:postgresql://postgres:5432/tickets",
    }
    for connector in connectors.values():
        assert {k: connector.config.get(k) for k in expected} == expected


def test_task_counts(connectors):
    assert connectors["tickets-sink"].config["tasks.max"] == "2"
    assert connectors["stats-sink"].config["tasks.max"] == "1"


def test_tickets_sink_records_the_source_topic(connectors):
    cfg = connectors["tickets-sink"].config
    assert cfg["transforms"] == "addTopic"
    assert cfg["transforms.addTopic.type"] == "org.apache.kafka.connect.transforms.InsertField$Value"
    assert cfg["transforms.addTopic.topic.field"] == "kafka_topic"
    assert "transforms" not in connectors["stats-sink"].config


# --- loading -------------------------------------------------------------------------------------

def _write(tmp_path, name, data) -> object:
    path = tmp_path / name
    path.write_text(json.dumps(data))
    return path


def test_load_connector_names_missing_keys(tmp_path):
    cfg = {k: "x" for k in REQUIRED_KEYS if k not in ("pk.fields", "topics")}
    with pytest.raises(ValueError, match=r"bad\.json: missing topics, pk\.fields"):
        load_connector(_write(tmp_path, "bad.json", {"name": "bad", "config": cfg}))


def test_load_connector_wants_name_and_config(tmp_path):
    with pytest.raises(ValueError, match=r"odd\.json: expected"):
        load_connector(_write(tmp_path, "odd.json", {"connector.class": "x"}))


def test_missing_tables():
    a = Connector("a", {"table.name.format": "tickets"})
    b = Connector("b", {"table.name.format": "ticket_stats"})
    assert missing_tables([a, b], {"tickets", "ticket_stats", "other"}) == []
    assert missing_tables([a, b], {"tickets"}) == ["ticket_stats"]


# --- status formatting ---------------------------------------------------------------------------

RUNNING = {"name": "tickets-sink", "type": "sink",
           "connector": {"state": "RUNNING", "worker_id": "connect:8083"},
           "tasks": [{"id": 1, "state": "RUNNING", "worker_id": "connect:8083"},
                     {"id": 0, "state": "RUNNING", "worker_id": "connect:8083"}]}


def test_format_status_running():
    assert format_status(RUNNING) == [
        "tickets-sink (sink): RUNNING on connect:8083",
        "  task 0: RUNNING on connect:8083",
        "  task 1: RUNNING on connect:8083",
    ]
    assert not has_failed(RUNNING)


def test_format_status_shows_the_start_of_a_failed_tasks_trace():
    trace = "\n".join(f"line {i}" for i in range(1, 9))
    status = {**RUNNING, "tasks": [{"id": 0, "state": "FAILED", "worker_id": "connect:8083", "trace": trace}]}
    assert format_status(status) == [
        "tickets-sink (sink): RUNNING on connect:8083",
        "  task 0: FAILED on connect:8083",
        "      line 1", "      line 2", "      line 3", "      line 4", "      line 5",
        "      … 3 more lines (docker compose logs connect)",
    ]
    assert has_failed(status)


def test_status_without_tasks_is_not_a_failure():
    status = {**RUNNING, "tasks": []}
    assert format_status(status)[1] == "  no tasks yet (Connect assigns them a moment after the connector starts)"
    assert not has_failed(status)
    assert main(["status", "tickets-sink"], client=ConnectClient("http://c:8083", http=_FakeHttp(_Response(200, status)))) == 0


# --- the REST client -----------------------------------------------------------------------------

class _Response:
    def __init__(self, status_code, payload=None, text=""):
        self.status_code, self._payload, self.text = status_code, payload, text

    def json(self):
        if self._payload is None:
            raise ValueError("not JSON")
        return self._payload


class _FakeHttp:
    """Stands in for the httpx module: returns the queued responses in order and records the calls."""

    def __init__(self, *responses):
        self.responses, self.calls = list(responses), []

    def _call(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    def get(self, url, **kwargs):
        return self._call("GET", url, **kwargs)

    def put(self, url, **kwargs):
        return self._call("PUT", url, **kwargs)

    def post(self, url, **kwargs):
        return self._call("POST", url, **kwargs)

    def delete(self, url, **kwargs):
        return self._call("DELETE", url, **kwargs)


SINK = Connector("tickets-sink", {"table.name.format": "tickets", "topics": "t"})


def test_apply_puts_the_config_and_says_whether_it_was_new():
    http = _FakeHttp(_Response(201, {}), _Response(200, {}))
    client = ConnectClient("http://c:8083/", http=http)
    assert client.apply(SINK) == "created"
    assert client.apply(SINK) == "updated"
    method, url, kwargs = http.calls[0]
    assert (method, url, kwargs["json"]) == ("PUT", "http://c:8083/connectors/tickets-sink/config", SINK.config)


def test_restart_asks_for_failed_tasks_only():
    http = _FakeHttp(_Response(204))
    ConnectClient("http://c:8083", http=http).restart("tickets-sink")
    method, url, kwargs = http.calls[0]
    assert (method, url) == ("POST", "http://c:8083/connectors/tickets-sink/restart")
    assert kwargs["params"] == {"includeTasks": "true", "onlyFailed": "true"}


def test_an_http_error_carries_connects_message():
    http = _FakeHttp(_Response(404, {"error_code": 404, "message": "Connector nope not found"}))
    with pytest.raises(ConnectorError, match="nope: Connect answered 404: Connector nope not found"):
        ConnectClient("http://c:8083", http=http).status("nope")


# --- the CLI -------------------------------------------------------------------------------------

def _all_tables(_dsn):
    return {"tickets", "ticket_stats"}


def test_apply_submits_every_committed_connector(capsys):
    http = _FakeHttp(_Response(201, {}), _Response(200, {}))
    assert main(["apply"], client=ConnectClient("http://c:8083", http=http), tables=_all_tables) == 0
    assert [url for _, url, _ in http.calls] == ["http://c:8083/connectors/stats-sink/config",
                                                 "http://c:8083/connectors/tickets-sink/config"]
    assert capsys.readouterr().out.splitlines() == ["stats-sink: created", "tickets-sink: updated"]


def test_apply_refuses_when_a_table_is_missing(capsys):
    http = _FakeHttp()
    code = main(["apply"], client=ConnectClient("http://c:8083", http=http), tables=lambda _dsn: {"tickets"})
    assert code == 1
    assert http.calls == []  # nothing submitted
    err = capsys.readouterr().err
    assert "missing table(s) in Postgres: ticket_stats" in err
    assert "docker compose down -v" in err


def test_apply_exits_1_when_postgres_is_unreachable(capsys):
    def unreachable(_dsn):
        raise psycopg.OperationalError("connection refused")

    assert main(["apply"], client=ConnectClient("http://c:8083", http=_FakeHttp()), tables=unreachable) == 1
    assert "cannot reach Postgres" in capsys.readouterr().err


def test_apply_prints_connects_rejection_and_exits_1(capsys):
    rejected = _Response(400, {"error_code": 400, "message": "Connector configuration is invalid"})
    http = _FakeHttp(rejected)
    assert main(["apply"], client=ConnectClient("http://c:8083", http=http), tables=_all_tables) == 1
    assert "Connector configuration is invalid" in capsys.readouterr().err


def test_unreachable_connect_exits_1_with_a_hint(capsys):
    http = _FakeHttp(httpx.ConnectError("connection refused"))
    assert main(["status"], client=ConnectClient("http://c:8083", http=http)) == 1
    err = capsys.readouterr().err
    assert "cannot reach Kafka Connect at http://c:8083" in err
    assert "docker compose up -d --build" in err


def test_status_of_all_connectors_exits_1_if_any_failed(capsys):
    failed = {**RUNNING, "name": "stats-sink", "connector": {"state": "FAILED", "worker_id": "connect:8083"}}
    http = _FakeHttp(_Response(200, ["tickets-sink", "stats-sink"]), _Response(200, failed), _Response(200, RUNNING))
    assert main(["status"], client=ConnectClient("http://c:8083", http=http)) == 1
    out = capsys.readouterr().out
    assert "stats-sink (sink): FAILED" in out and "tickets-sink (sink): RUNNING" in out


def test_status_with_no_connectors_says_how_to_create_them(capsys):
    assert main(["status"], client=ConnectClient("http://c:8083", http=_FakeHttp(_Response(200, [])))) == 0
    assert "pipeline.connectors apply" in capsys.readouterr().out


def test_delete_mentions_that_the_offsets_stay(capsys):
    http = _FakeHttp(_Response(204))
    assert main(["delete", "tickets-sink"], client=ConnectClient("http://c:8083", http=http)) == 0
    assert http.calls[0][:2] == ("DELETE", "http://c:8083/connectors/tickets-sink")
    assert "connect-tickets-sink" in capsys.readouterr().out
