import pytest
from confluent_kafka.admin import AdminClient

from pipeline.admin import TopicSetupError, ensure_topics
from pipeline.config import TopicSpec


def test_unreachable_broker_fails_fast_with_clear_error():
    admin = AdminClient({"bootstrap.servers": "localhost:1"})
    with pytest.raises(TopicSetupError, match="cannot reach Kafka"):
        ensure_topics(admin, [TopicSpec("x", 1)], timeout=2)
