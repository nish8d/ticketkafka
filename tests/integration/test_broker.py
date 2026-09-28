import pytest
from confluent_kafka.admin import AdminClient

from pipeline.config import BOOTSTRAP_SERVERS

pytestmark = pytest.mark.integration


def test_broker_is_reachable():
    metadata = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS}).list_topics(timeout=10)
    assert len(metadata.brokers) == 1
