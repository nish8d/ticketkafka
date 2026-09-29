import threading

import pytest

from pipeline.llm import LLMError
from pipeline.retry import (
    RetryPolicy,
    Stopping,
    call_with_retries,
    check_poll_budget,
    interruptible_sleep,
    max_seconds_per_message,
)


class Flaky:
    def __init__(self, failures: int, result="ok"):
        self.failures, self.result, self.calls = failures, result, 0

    def __call__(self):
        self.calls += 1
        if self.calls <= self.failures:
            raise LLMError(f"ollama call failed: attempt {self.calls}")
        return self.result


def _recording_sleep(log):
    def sleep(seconds):
        log.append(seconds)
        return True
    return sleep


def test_default_policy_is_three_retries_with_doubling_backoff():
    policy = RetryPolicy()
    assert policy.attempts == 4
    assert policy.delays() == [1.0, 2.0, 4.0]


def test_succeeds_after_transient_failures_with_backoff():
    waits: list[float] = []
    flaky = Flaky(failures=2)
    assert call_with_retries(flaky, RetryPolicy(), _recording_sleep(waits)) == "ok"
    assert flaky.calls == 3
    assert waits == [1.0, 2.0]


def test_gives_up_after_the_last_retry_and_says_so():
    waits: list[float] = []
    flaky = Flaky(failures=10)
    with pytest.raises(LLMError, match="gave up after 4 attempts: ollama call failed: attempt 4"):
        call_with_retries(flaky, RetryPolicy(), _recording_sleep(waits))
    assert flaky.calls == 4
    assert waits == [1.0, 2.0, 4.0]


def test_other_errors_are_not_retried():
    def boom():
        raise KeyError("a bug, not a transient failure")

    with pytest.raises(KeyError):
        call_with_retries(boom, RetryPolicy(), _recording_sleep([]))


def test_stop_during_backoff_raises_stopping_not_llm_error():
    flaky = Flaky(failures=10)
    with pytest.raises(Stopping) as excinfo:
        call_with_retries(flaky, RetryPolicy(), lambda seconds: False)  # interrupted at the first wait
    assert not isinstance(excinfo.value, LLMError)  # must not be dead-lettered
    assert flaky.calls == 1


def test_on_retry_is_told_about_each_retry():
    seen = []
    call_with_retries(Flaky(failures=1), RetryPolicy(), _recording_sleep([]),
                      on_retry=lambda attempt, exc, delay: seen.append((attempt, delay)))
    assert seen == [(1, 1.0)]


def test_interruptible_sleep_returns_false_once_stopped():
    stop = threading.Event()
    sleep = interruptible_sleep(stop)
    assert sleep(0.01) is True
    stop.set()
    assert sleep(10) is False  # returns immediately


def test_worst_case_time_per_message():
    # 4 attempts x 30 s timeout + 1 + 2 + 4 s of backoff
    assert max_seconds_per_message(RetryPolicy(), call_timeout=30) == 127


def test_poll_budget_accepts_a_batch_that_fits():
    assert check_poll_budget(4, RetryPolicy(), 30, max_poll_interval_ms=600_000) == 508


def test_poll_budget_rejects_a_batch_that_could_get_us_kicked_out():
    with pytest.raises(ValueError, match="--batch-size") as excinfo:
        check_poll_budget(100, RetryPolicy(), 30, max_poll_interval_ms=600_000)
    assert "max.poll.interval.ms" in str(excinfo.value)
