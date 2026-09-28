import json
import random
import re
from datetime import datetime, timezone

import pytest

from pipeline.generator import (
    CORRUPTIONS, TicketSeed, build_ticket, corrupt, encode, parse_args, pick_seed, run_generator,
)
from pipeline.llm import LLMError, TicketText
from pipeline.models import CHANNELS, PRODUCTS, Ticket

NOW = datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)
SEED = TicketSeed(customer_id="C-0007", channel="chat", product="StreamBox TV", persona="angry")
TEXT = TicketText(subject="No picture", body="The screen stays black.")


def _no_sleep(_seconds):
    pass


def test_pick_seed_draws_from_the_pools():
    rng = random.Random(1)
    for _ in range(200):
        seed = pick_seed(rng)
        assert re.fullmatch(r"C-\d{4}", seed.customer_id)
        assert 1 <= int(seed.customer_id[2:]) <= 200
        assert seed.channel in CHANNELS and seed.product in PRODUCTS


def test_pick_seed_is_deterministic_for_a_given_rng_seed():
    assert pick_seed(random.Random(5)) == pick_seed(random.Random(5))


def test_build_ticket_combines_seed_and_text():
    ticket = build_ticket(SEED, TEXT, NOW)
    assert (ticket.customer_id, ticket.channel, ticket.product) == ("C-0007", "chat", "StreamBox TV")
    assert (ticket.subject, ticket.body, ticket.created_at) == (TEXT.subject, TEXT.body, NOW)


@pytest.mark.parametrize("kind", CORRUPTIONS)
def test_every_corruption_fails_validation(kind):
    raw = corrupt(build_ticket(SEED, TEXT, NOW), kind)
    with pytest.raises(ValueError):
        Ticket.model_validate_json(raw)


def test_encode_with_zero_bad_ratio_is_always_valid():
    rng = random.Random(3)
    ticket = build_ticket(SEED, TEXT, NOW)
    for _ in range(50):
        assert Ticket.model_validate_json(encode(ticket, rng, 0.0)) == ticket


def test_encode_with_full_bad_ratio_is_never_valid():
    rng = random.Random(3)
    ticket = build_ticket(SEED, TEXT, NOW)
    for _ in range(50):
        with pytest.raises(ValueError):
            Ticket.model_validate_json(encode(ticket, rng, 1.0))


def test_run_generator_produces_keyed_tickets(fake_producer):
    stats = run_generator(fake_producer, lambda seed: TEXT, "t.raw", count=5, rate=1000,
                          bad_ratio=0.0, rng=random.Random(9), sleep=_no_sleep)
    assert (stats.produced, stats.delivered) == (5, 5)
    for msg in fake_producer.messages:
        ticket = Ticket.model_validate_json(msg.value())
        assert msg.topic() == "t.raw"
        assert msg.key() == ticket.customer_id.encode()


def test_run_generator_survives_llm_failures_and_backs_off(fake_producer):
    outcomes = [LLMError("ollama call failed: down"), LLMError("model returned unusable output"), TEXT]
    sleeps: list[float] = []

    def flaky(seed):
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    stats = run_generator(fake_producer, flaky, "t.raw", count=1, rate=1000, bad_ratio=0.0,
                          rng=random.Random(9), llm_backoff=2.5, sleep=sleeps.append)
    assert (stats.produced, stats.llm_failed) == (1, 2)
    assert sleeps[:2] == [2.5, 2.5]


def test_run_generator_respects_rate(fake_producer):
    sleeps: list[float] = []
    run_generator(fake_producer, lambda seed: TEXT, "t.raw", count=3, rate=2.0, bad_ratio=0.0,
                  rng=random.Random(9), sleep=sleeps.append)
    assert len(sleeps) == 3
    assert all(0.4 < s <= 0.5 for s in sleeps)


def test_run_generator_stops_when_asked(fake_producer):
    stats = run_generator(fake_producer, lambda seed: TEXT, "t.raw", count=None, rate=1000,
                          bad_ratio=0.0, rng=random.Random(9),
                          should_stop=lambda: len(fake_producer.messages) >= 3, sleep=_no_sleep)
    assert stats.produced == 3


def test_parse_args_defaults():
    args = parse_args([])
    assert (args.rate, args.count, args.bad_ratio, args.topic) == (1.0, None, 0.05, "tickets.raw")


@pytest.mark.parametrize("argv", [
    ["--rate", "0"], ["--rate", "-1"], ["--count", "0"], ["--bad-ratio", "1.5"], ["--bad-ratio", "-0.1"],
])
def test_parse_args_rejects_bad_values(argv):
    with pytest.raises(SystemExit):
        parse_args(argv)
