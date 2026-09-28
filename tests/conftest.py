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
