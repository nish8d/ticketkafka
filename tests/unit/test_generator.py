import random
import re
from datetime import datetime, timezone

import pytest

from pipeline import config
from pipeline.generator import (
    CORRUPTIONS, TicketSeed, build_ticket, corrupt, encode, parse_args, pick_seed, run_generator, tier_for,
)
from pipeline.llm import LLMError, TicketText
from pipeline.models import CHANNELS, PRODUCTS, TIERS
from pipeline.serde import UndecodableMessage

NOW = datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)
SEED = TicketSeed(customer_id="C-0007", channel="chat", product="StreamBox TV", persona="angry", tier="pro")
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


def test_tier_is_stable_per_customer_and_covers_every_tier():
    assert tier_for("C-0010") == "enterprise"
    assert tier_for("C-0042") == "pro"
    assert tier_for("C-0047") == "free"
    assert {tier_for(f"C-{n:04d}") for n in range(1, 201)} == set(TIERS)


def test_pick_seed_derives_tier_from_customer():
    rng = random.Random(1)
    for _ in range(50):
        seed = pick_seed(rng)
        assert seed.tier == tier_for(seed.customer_id)


def test_build_ticket_carries_the_seed_tier():
    assert build_ticket(SEED, TEXT, NOW).tier == "pro"


@pytest.mark.parametrize("kind", CORRUPTIONS)
def test_every_corruption_fails_to_decode_or_validate(kind, serde_v2, avro_topic):
    # UndecodableMessage and pydantic's ValidationError are both ValueErrors: both go to the DLQ.
    with pytest.raises(ValueError):
        serde_v2.decode(corrupt(build_ticket(SEED, TEXT, NOW), kind, serde_v2, avro_topic), avro_topic)


def test_not_avro_is_a_legacy_json_ticket(serde_v2, avro_topic):
    with pytest.raises(UndecodableMessage):
        serde_v2.decode(corrupt(build_ticket(SEED, TEXT, NOW), "not_avro", serde_v2, avro_topic), avro_topic)


def test_unknown_corruption_kind_is_rejected(serde_v2, avro_topic):
    with pytest.raises(ValueError, match="unknown corruption"):
        corrupt(build_ticket(SEED, TEXT, NOW), "missing_field", serde_v2, avro_topic)


def test_encode_with_zero_bad_ratio_is_always_valid(serde_v2, avro_topic):
    rng = random.Random(3)
    ticket = build_ticket(SEED, TEXT, NOW)
    for _ in range(50):
        assert serde_v2.decode(encode(ticket, rng, 0.0, serde_v2, avro_topic), avro_topic) == ticket


def test_encode_with_full_bad_ratio_is_never_valid(serde_v2, avro_topic):
    rng = random.Random(3)
    ticket = build_ticket(SEED, TEXT, NOW)
    for _ in range(50):
        with pytest.raises(ValueError):
            serde_v2.decode(encode(ticket, rng, 1.0, serde_v2, avro_topic), avro_topic)


def test_v1_writer_drops_tier(serde_v1, serde_v2, avro_topic):
    value = encode(build_ticket(SEED, TEXT, NOW), random.Random(1), 0.0, serde_v1, avro_topic)
    assert serde_v2.decode(value, avro_topic).tier == "free"


def test_run_generator_produces_keyed_tickets(fake_producer, serde_v2, avro_topic):
    stats = run_generator(fake_producer, lambda seed: TEXT, avro_topic, count=5, rate=1000,
                          bad_ratio=0.0, rng=random.Random(9), serde=serde_v2, sleep=_no_sleep)
    assert (stats.produced, stats.delivered) == (5, 5)
    for msg in fake_producer.messages:
        ticket = serde_v2.decode(msg.value(), avro_topic)
        assert msg.topic() == avro_topic
        assert msg.key() == ticket.customer_id.encode()


def test_run_generator_survives_llm_failures_and_backs_off(fake_producer, serde_v2, avro_topic):
    outcomes = [LLMError("ollama call failed: down"), LLMError("model returned unusable output"), TEXT]
    sleeps: list[float] = []

    def flaky(seed):
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    stats = run_generator(fake_producer, flaky, avro_topic, count=1, rate=1000, bad_ratio=0.0,
                          rng=random.Random(9), serde=serde_v2, llm_backoff=2.5, sleep=sleeps.append)
    assert (stats.produced, stats.llm_failed) == (1, 2)
    assert sleeps[:2] == [2.5, 2.5]


def test_run_generator_respects_rate(fake_producer, serde_v2, avro_topic):
    sleeps: list[float] = []
    run_generator(fake_producer, lambda seed: TEXT, avro_topic, count=3, rate=2.0, bad_ratio=0.0,
                  rng=random.Random(9), serde=serde_v2, sleep=sleeps.append)
    assert len(sleeps) == 3
    assert all(0.4 < s <= 0.5 for s in sleeps)


def test_run_generator_stops_when_asked(fake_producer, serde_v2, avro_topic):
    stats = run_generator(fake_producer, lambda seed: TEXT, avro_topic, count=None, rate=1000,
                          bad_ratio=0.0, rng=random.Random(9), serde=serde_v2,
                          should_stop=lambda: len(fake_producer.messages) >= 3, sleep=_no_sleep)
    assert stats.produced == 3


def test_parse_args_defaults():
    args = parse_args([])
    assert (args.rate, args.count, args.bad_ratio, args.topic) == (1.0, None, 0.05, "tickets.raw")
    assert args.schema == config.DEFAULT_TICKET_SCHEMA


@pytest.mark.parametrize("argv", [
    ["--rate", "0"], ["--rate", "-1"], ["--count", "0"], ["--bad-ratio", "1.5"], ["--bad-ratio", "-0.1"],
])
def test_parse_args_rejects_bad_values(argv):
    with pytest.raises(SystemExit):
        parse_args(argv)


def test_main_refuses_to_start_when_the_schema_is_not_registered(monkeypatch):
    from pipeline import generator
    from pipeline.serde import SchemaNotRegistered

    monkeypatch.setattr(config, "SCHEMA_REGISTRY_URL", "mock://nothing-registered")
    monkeypatch.setattr(generator, "run_generator", lambda *a, **kw: pytest.fail("generated without a schema"))
    with pytest.raises(SchemaNotRegistered, match="pipeline.schemas register"):
        generator.main(["--count", "1"])


def test_run_generator_gives_up_after_too_many_llm_failures_in_a_row(fake_producer, serde_v2, avro_topic):
    # With Ollama down (or the model not pulled) every call fails: without a limit, --count N would
    # never finish.
    calls = []

    def down(seed):
        calls.append(seed)
        raise LLMError("ollama call failed: connection refused")

    stats = run_generator(fake_producer, down, avro_topic, count=5, rate=1000, bad_ratio=0.0,
                          rng=random.Random(1), serde=serde_v2, sleep=lambda s: None, max_llm_failures=3)
    assert len(calls) == 3
    assert (stats.produced, stats.llm_failed, stats.gave_up) == (0, 3, True)


def test_a_success_resets_the_llm_failure_count(fake_producer, serde_v2, avro_topic):
    down = LLMError("ollama call failed: timeout")
    outcomes = [down, down, TEXT, down, down, TEXT]  # never 3 in a row

    def flaky(seed):
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    stats = run_generator(fake_producer, flaky, avro_topic, count=2, rate=1000, bad_ratio=0.0,
                          rng=random.Random(1), serde=serde_v2, sleep=lambda s: None, max_llm_failures=3)
    assert (stats.produced, stats.llm_failed, stats.gave_up) == (2, 4, False)


def test_main_exits_1_when_the_generator_gives_up(monkeypatch, mock_registry):
    from pipeline import generator

    monkeypatch.setattr(generator, "make_registry", lambda url: mock_registry)
    monkeypatch.setattr(generator, "install_stop_handler", lambda: (lambda: False))
    monkeypatch.setattr(generator, "Producer", lambda conf: None)
    monkeypatch.setattr(generator, "run_generator",
                        lambda *a, **kw: generator.GeneratorStats(llm_failed=10, gave_up=True))
    assert generator.main(["--count", "1"]) == 1
    monkeypatch.setattr(generator, "run_generator", lambda *a, **kw: generator.GeneratorStats(produced=1))
    assert generator.main(["--count", "1"]) == 0
