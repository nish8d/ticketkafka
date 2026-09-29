import pytest
from confluent_kafka.admin import AdminClient

from pipeline.config import BOOTSTRAP_SERVERS

pytestmark = pytest.mark.integration


def test_broker_is_reachable():
    metadata = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS}).list_topics(timeout=10)
    assert len(metadata.brokers) == 1


def test_schema_registry_is_reachable():
    import httpx

    from pipeline.config import SCHEMA_REGISTRY_URL

    response = httpx.get(f"{SCHEMA_REGISTRY_URL}/subjects", timeout=5)
    assert response.status_code == 200
    assert isinstance(response.json(), list)
