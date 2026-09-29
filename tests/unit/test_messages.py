from datetime import datetime, timezone

from pipeline import messages, validator


def test_validator_reexports_the_shared_helpers():
    # Moved, not copied: both services must build identical DLQ messages.
    assert validator.dlq_headers is messages.dlq_headers
    assert validator.SourceRef is messages.SourceRef
    assert validator.Output is messages.Output


def test_dlq_headers_are_the_fixed_six():
    now = datetime(2026, 9, 29, tzinfo=timezone.utc)
    headers = messages.dlq_headers(ValueError("x"), messages.SourceRef("t", 1, 2), now)
    assert [k for k, _ in headers] == ["error.type", "error.message", "source.topic",
                                       "source.partition", "source.offset", "failed_at"]
