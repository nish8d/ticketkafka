from pipeline import config


def test_stage_1_to_3_topics_are_defined():
    specs = {s.name: s for s in config.TOPIC_SPECS}
    assert specs["tickets.raw"].partitions == 6
    assert specs["tickets.valid"].partitions == 6
    assert specs["tickets.dlq"].partitions == 1


def test_topic_names_are_unique():
    names = [s.name for s in config.TOPIC_SPECS]
    assert len(names) == len(set(names))


def test_schema_registry_url_defaults_to_localhost():
    assert config.SCHEMA_REGISTRY_URL == "http://localhost:8081"


def test_ticket_schema_paths_point_into_the_schemas_dir():
    assert config.TICKET_SCHEMA_V1 == config.SCHEMA_DIR / "ticket.v1.avsc"
    assert config.TICKET_SCHEMA_V2 == config.SCHEMA_DIR / "ticket.v2.avsc"
    assert config.DEFAULT_TICKET_SCHEMA == config.TICKET_SCHEMA_V2
    assert config.SCHEMA_DIR.name == "schemas"


def test_stage_5_routing_topics_are_defined():
    specs = {s.name: s for s in config.TOPIC_SPECS}
    for name in ("tickets.billing", "tickets.tech", "tickets.urgent", "tickets.enriched.other"):
        assert specs[name].partitions == 3
    assert config.ENRICHED_TOPICS == ("tickets.billing", "tickets.tech", "tickets.urgent", "tickets.enriched.other")
    assert config.ENRICHED_SCHEMA_V1 == config.SCHEMA_DIR / "enriched_ticket.v1.avsc"
