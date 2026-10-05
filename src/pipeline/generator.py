"""Stage 2 (Avro since stage 4): an LLM writes support tickets; we produce them to tickets.raw keyed by customer."""
import argparse
import logging
import random
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from confluent_kafka import Producer

from pipeline import config
from pipeline.clients import producer_config
from pipeline.llm import PERSONAS, LLMError, TicketText, generate_ticket_text, make_client
from pipeline.models import CHANNELS, PRODUCTS, Ticket, Tier
from pipeline.serde import TicketSerde, ensure_registered, load_schema, make_registry
from pipeline.shutdown import install_stop_handler

log = logging.getLogger("generator")

CUSTOMER_POOL_SIZE = 200
# With Avro a producer can't leave out a field (the serializer refuses), so the bad messages are:
# bytes that skip the serializer, an Avro header naming a schema that doesn't exist, and valid Avro
# that breaks a business rule.
CORRUPTIONS = ("not_avro", "unknown_schema_id", "empty_body")
UNKNOWN_SCHEMA_ID = 999_999


@dataclass(frozen=True)
class TicketSeed:
    """The structured, cheap-to-generate part of a ticket."""

    customer_id: str
    channel: str
    product: str
    persona: str
    tier: str


@dataclass
class GeneratorStats:
    produced: int = 0
    delivered: int = 0
    delivery_failed: int = 0
    llm_failed: int = 0
    gave_up: bool = False  # stopped early: the LLM failed max_llm_failures times in a row


def tier_for(customer_id: str) -> Tier:
    """A customer's plan, derived from their id so it never changes between tickets."""
    last_digit = int(customer_id[-1])
    if last_digit == 0:
        return "enterprise"
    if last_digit <= 3:
        return "pro"
    return "free"


def pick_seed(rng: random.Random) -> TicketSeed:
    customer_id = f"C-{rng.randint(1, CUSTOMER_POOL_SIZE):04d}"
    return TicketSeed(
        customer_id=customer_id,
        channel=rng.choice(CHANNELS),
        product=rng.choice(PRODUCTS),
        persona=rng.choice(PERSONAS),
        tier=tier_for(customer_id),
    )


def build_ticket(seed: TicketSeed, text: TicketText, now: datetime) -> Ticket:
    return Ticket(
        ticket_id=uuid.uuid4(),
        customer_id=seed.customer_id,
        created_at=now,
        channel=seed.channel,
        product=seed.product,
        subject=text.subject,
        body=text.body,
        tier=seed.tier,
    )


def corrupt(ticket: Ticket, kind: str, serde: TicketSerde, topic: str) -> bytes:
    """A deliberately broken value for `ticket`, so the DLQ has something to catch."""
    if kind == "not_avro":
        return ticket.model_dump_json().encode()  # what a stage-2 JSON producer would still send
    if kind == "unknown_schema_id":
        good = serde.encode(ticket, topic)
        return good[:1] + UNKNOWN_SCHEMA_ID.to_bytes(4, "big") + good[5:]
    if kind == "empty_body":
        # model_copy skips validation, so Pydantic lets us build the invalid ticket; Avro accepts "".
        return serde.encode(ticket.model_copy(update={"body": ""}), topic)
    raise ValueError(f"unknown corruption kind: {kind}")


def encode(ticket: Ticket, rng: random.Random, bad_ratio: float, serde: TicketSerde, topic: str) -> bytes:
    if rng.random() < bad_ratio:
        return corrupt(ticket, rng.choice(CORRUPTIONS), serde, topic)
    return serde.encode(ticket, topic)


def run_generator(
    producer,
    text_source: Callable[[TicketSeed], TicketText],
    topic: str,
    count: int | None,
    rate: float,
    bad_ratio: float,
    rng: random.Random,
    serde: TicketSerde,
    should_stop: Callable[[], bool] = lambda: False,
    llm_backoff: float = 1.0,
    sleep: Callable[[float], None] = time.sleep,
    max_llm_failures: int = 10,
) -> GeneratorStats:
    stats = GeneratorStats()
    failures_in_a_row = 0

    # Called from producer.poll()/flush() once the broker has acked (or rejected) a message.
    def on_delivery(err, msg):
        if err is not None:
            stats.delivery_failed += 1
            log.error("delivery failed: %s", err)
        else:
            stats.delivered += 1
            log.info("delivered key=%s -> %s[%d] @ offset %d",
                     msg.key(), msg.topic(), msg.partition(), msg.offset())

    interval = 1.0 / rate
    while (count is None or stats.produced < count) and not should_stop():
        started = time.monotonic()
        seed = pick_seed(rng)
        try:
            text = text_source(seed)
        except LLMError as exc:
            stats.llm_failed += 1
            failures_in_a_row += 1
            if failures_in_a_row >= max_llm_failures:
                # Ollama down, or the model not pulled: every call will fail, so --count would never
                # be reached. Stop instead of retrying for ever.
                log.error("LLM failed %d times in a row (last: %s); giving up. Is Ollama running, and is "
                          "the model pulled?", failures_in_a_row, exc)
                stats.gave_up = True
                break
            log.warning("LLM failed, skipping this ticket: %s", exc)
            sleep(llm_backoff)
            continue
        failures_in_a_row = 0

        ticket = build_ticket(seed, text, datetime.now(timezone.utc))
        # produce() only queues the message; it's sent in the background in batches.
        producer.produce(topic, key=seed.customer_id.encode(), value=encode(ticket, rng, bad_ratio, serde, topic),
                         on_delivery=on_delivery)
        stats.produced += 1
        producer.poll(0)  # serve delivery callbacks for anything already acked
        sleep(max(0.0, interval - (time.monotonic() - started)))

    remaining = producer.flush(30)  # block until everything queued is acked
    if remaining:
        log.error("%d messages were never acknowledged", remaining)
    return stats


def _positive_float(value: str) -> float:
    number = float(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be > 0")
    return number


def _positive_int(value: str) -> int:
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be > 0")
    return number


def _ratio(value: str) -> float:
    number = float(value)
    if not 0.0 <= number <= 1.0:
        raise argparse.ArgumentTypeError("must be between 0 and 1")
    return number


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate LLM-written support tickets into Kafka.")
    parser.add_argument("--rate", type=_positive_float, default=1.0, help="max tickets per second")
    parser.add_argument("--count", type=_positive_int, default=None, help="stop after N tickets (default: forever)")
    parser.add_argument("--model", default=config.DEFAULT_MODEL, help="Ollama model name")
    parser.add_argument("--bad-ratio", type=_ratio, default=0.05, help="fraction of deliberately broken tickets")
    parser.add_argument("--seed", type=int, default=None, help="random seed for reproducible runs")
    parser.add_argument("--topic", default=config.TOPIC_RAW)
    parser.add_argument("--schema", type=Path, default=config.DEFAULT_TICKET_SCHEMA,
                        help="Avro schema file to write with (must be registered)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    should_stop = install_stop_handler()
    client = make_client(config.OLLAMA_HOST)
    registry = make_registry(config.SCHEMA_REGISTRY_URL)
    # Fail before generating anything if nobody registered this schema for the topic.
    ensure_registered(registry, args.topic, args.schema)
    serde = TicketSerde(registry, load_schema(args.schema))

    def text_source(seed: TicketSeed) -> TicketText:
        return generate_ticket_text(client, args.model, seed.persona, seed.product, seed.channel)

    stats = run_generator(Producer(producer_config()), text_source, args.topic, args.count, args.rate,
                          args.bad_ratio, random.Random(args.seed), serde, should_stop=should_stop)
    log.info("done: %s", stats)
    return 1 if stats.gave_up else 0


if __name__ == "__main__":
    sys.exit(main())
