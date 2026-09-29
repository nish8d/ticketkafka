"""Retrying slow, flaky calls (the LLM) without stalling shutdown or overrunning max.poll.interval.ms."""
import threading
from dataclasses import dataclass
from typing import Callable, TypeVar

from pipeline.llm import LLMError

T = TypeVar("T")


@dataclass(frozen=True)
class RetryPolicy:
    retries: int = 3
    base_delay: float = 1.0

    @property
    def attempts(self) -> int:
        return self.retries + 1

    def delays(self) -> list[float]:
        # Exponential backoff: give a struggling service more room each time (1 s, 2 s, 4 s).
        return [self.base_delay * 2 ** n for n in range(self.retries)]


class Stopping(Exception):
    """Shutdown interrupted a retry wait. Not the message's fault, so never dead-letter it."""


def interruptible_sleep(stop: threading.Event) -> Callable[[float], bool]:
    """A sleep that Ctrl-C cuts short. Returns True if it slept the whole time."""
    return lambda seconds: not stop.wait(seconds)


def call_with_retries(fn: Callable[[], T], policy: RetryPolicy, sleep: Callable[[float], bool],
                      retry_on: tuple[type[Exception], ...] = (LLMError,),
                      on_retry: Callable[[int, Exception, float], None] | None = None) -> T:
    delays = policy.delays()
    for attempt in range(1, policy.attempts + 1):
        try:
            return fn()
        except retry_on as exc:
            if attempt == policy.attempts:
                raise type(exc)(f"gave up after {policy.attempts} attempts: {exc}") from exc
            delay = delays[attempt - 1]
            if on_retry is not None:
                on_retry(attempt, exc, delay)
            if not sleep(delay):
                raise Stopping(f"shutdown during retry wait (attempt {attempt})") from exc
    raise AssertionError("unreachable")


def max_seconds_per_message(policy: RetryPolicy, call_timeout: float) -> float:
    return policy.attempts * call_timeout + sum(policy.delays())


def check_poll_budget(batch_size: int, policy: RetryPolicy, call_timeout: float, max_poll_interval_ms: int) -> float:
    """Worst-case seconds between two consume() calls. It must fit inside max.poll.interval.ms,
    or the group evicts us mid-batch, hands our partitions to someone else, and the batch is redone."""
    worst = batch_size * max_seconds_per_message(policy, call_timeout)
    if worst * 1000 >= max_poll_interval_ms:
        raise ValueError(
            f"a batch could take {worst:.0f}s (--batch-size {batch_size} x {policy.attempts} attempts x "
            f"{call_timeout:.0f}s LLM timeout + backoff) but max.poll.interval.ms is "
            f"{max_poll_interval_ms / 1000:.0f}s: lower --batch-size or the LLM timeout, or raise the interval"
        )
    return worst
