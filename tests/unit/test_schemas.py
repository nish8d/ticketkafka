import pytest

from pipeline import config
from pipeline.schemas import (
    TICKET_TOPICS,
    Registered,
    check,
    list_subjects,
    main,
    register,
)
from pipeline.serde import load_schema, make_registry


@pytest.fixture
def registry(monkeypatch):
    """Mock registry. The mock doesn't implement set_compatibility (it would make an HTTP call),
    so record the calls instead."""
    registry = make_registry("mock://schemas-cli")
    registry.compat_calls = []
    monkeypatch.setattr(registry, "set_compatibility",
                        lambda subject_name=None, level=None: registry.compat_calls.append((subject_name, level)))
    return registry


def test_schemas_cover_both_ticket_topics():
    assert TICKET_TOPICS == ("tickets.raw", "tickets.valid")


def test_register_puts_the_schema_under_every_topic_subject(registry):
    results = register(registry, TICKET_TOPICS, load_schema(config.TICKET_SCHEMA_V1))
    assert [r.subject for r in results] == ["tickets.raw-value", "tickets.valid-value"]
    assert all(isinstance(r, Registered) and r.version == 1 for r in results)
    assert list_subjects(registry) == {"tickets.raw-value": [1], "tickets.valid-value": [1]}
    assert registry.compat_calls == [("tickets.raw-value", "BACKWARD"), ("tickets.valid-value", "BACKWARD")]


def test_list_subjects_sorts_versions(registry):
    register(registry, ["tickets.raw"], load_schema(config.TICKET_SCHEMA_V1))
    register(registry, ["tickets.raw"], load_schema(config.TICKET_SCHEMA_V2))
    assert list_subjects(registry) == {"tickets.raw-value": [1, 2]}


def test_registering_the_same_schema_twice_is_a_no_op(registry):
    first = register(registry, TICKET_TOPICS, load_schema(config.TICKET_SCHEMA_V1))
    again = register(registry, TICKET_TOPICS, load_schema(config.TICKET_SCHEMA_V1))
    assert first == again


class _FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code, self._payload = status_code, payload

    def json(self):
        return self._payload


class _FakeHttp:
    def __init__(self, response):
        self.response, self.calls = response, []

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.response


def test_check_reports_incompatibility_reasons():
    http = _FakeHttp(_FakeResponse(200, {"is_compatible": False, "messages": ["READER_FIELD_MISSING_DEFAULT_VALUE"]}))
    [result] = check("http://sr:8081", ["tickets.raw"], "{}", http=http)
    assert result.compatible is False
    assert result.messages == ["READER_FIELD_MISSING_DEFAULT_VALUE"]
    url, kwargs = http.calls[0]
    assert url == "http://sr:8081/compatibility/subjects/tickets.raw-value/versions/latest"
    assert kwargs["params"] == {"verbose": "true"}
    assert kwargs["json"] == {"schema": "{}", "schemaType": "AVRO"}


def test_check_treats_an_empty_subject_as_compatible():
    http = _FakeHttp(_FakeResponse(404, {"error_code": 40401, "message": "Subject not found"}))
    [result] = check("http://sr:8081", ["tickets.raw"], "{}", http=http)
    assert result.compatible is True
    assert result.messages == ["no versions registered yet"]


def test_cli_requires_a_subcommand():
    with pytest.raises(SystemExit):
        main([])
