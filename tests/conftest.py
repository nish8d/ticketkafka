import pytest


@pytest.fixture
def ticket_dict() -> dict:
    return {
        "ticket_id": "0b6a4a3e-9f5e-4f2e-8a7e-1f2d3c4b5a69",
        "customer_id": "C-0042",
        "created_at": "2026-09-28T12:00:00+00:00",
        "channel": "email",
        "product": "CloudDrive Pro",
        "subject": "Charged twice",
        "body": "I was billed twice this month. Please refund one of the charges.",
    }


class FakeMessage:
    def __init__(self, topic, key, value, headers):
        self._topic, self._key, self._value, self._headers = topic, key, value, headers

    def topic(self):
        return self._topic

    def key(self):
        return self._key

    def value(self):
        return self._value

    def headers(self):
        return self._headers

    def partition(self):
        return 0

    def offset(self):
        return 0


class FakeProducer:
    """Records produced messages. Set flush_remaining > 0 to simulate unacknowledged writes.

    Set delivery_error to a non-None object to simulate a delivery callback reporting failure
    (e.g. the broker rejected the write) while still calling back with a message.
    """

    def __init__(self):
        self.messages: list[FakeMessage] = []
        self.flush_remaining = 0
        self.delivery_error = None

    def produce(self, topic, key=None, value=None, headers=None, on_delivery=None):
        msg = FakeMessage(topic, key, value, headers)
        self.messages.append(msg)
        if on_delivery is not None:
            on_delivery(self.delivery_error, msg)

    def poll(self, timeout=0):
        return 0

    def flush(self, timeout=None):
        return self.flush_remaining


@pytest.fixture
def fake_producer() -> FakeProducer:
    return FakeProducer()
