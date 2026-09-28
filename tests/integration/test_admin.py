import uuid

import pytest

from pipeline.admin import ensure_topics
from pipeline.config import TopicSpec

pytestmark = pytest.mark.integration


def test_ensure_topics_creates_once_and_is_idempotent(admin):
    name = f"test.admin.{uuid.uuid4().hex[:8]}"
    spec = TopicSpec(name, 2, {"retention.ms": "60000"})
    try:
        assert ensure_topics(admin, [spec]) == [name]
        assert ensure_topics(admin, [spec]) == []
        assert len(admin.list_topics(name, timeout=10).topics[name].partitions) == 2
    finally:
        admin.delete_topics([name], operation_timeout=10)[name].result()
