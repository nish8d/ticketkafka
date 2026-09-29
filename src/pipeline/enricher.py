"""Stage 5: classify valid tickets with the LLM and route them by category (urgent ones copied too).

Run several instances with the same --group to share tickets.valid's partitions between them.
"""
import argparse
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from confluent_kafka import Consumer, Producer

from pipeline import config
from pipeline.clients import producer_config, slow_consumer_config
from pipeline.llm import Classification, LLMError, classify_ticket, make_client
from pipeline.messages import Output, SourceRef, dlq_headers
from pipeline.models import EnrichedTicket, Ticket
from pipeline.retry import RetryPolicy, Stopping, call_with_retries, check_poll_budget, interruptible_sleep
from pipeline.serde import EnrichedTicketSerde, TicketSerde, ensure_registered, load_schema, make_registry
from pipeline.shutdown import install_stop_event

log = logging.getLogger("enricher")


@dataclass(frozen=True)
class Routes:
    billing: str
    tech: str
    other: str
    urgent: str
    dlq: str

    @classmethod
    def default(cls) -> "Routes":
        return cls(config.TOPIC_BILLING, config.TOPIC_TECH, config.TOPIC_ENRICHED_OTHER,
                   config.TOPIC_URGENT, config.TOPIC_DLQ)

    def outputs(self) -> tuple[str, ...]:
        return (self.billing, self.tech, self.other, self.urgent)


@dataclass
class EnricherStats:
    enriched: int = 0
    routed: int = 0
    dead_lettered: int = 0
    batches: int = 0


def output_topics(enriched: EnrichedTicket, routes: Routes) -> list[str]:
    by_category = {"billing": routes.billing, "technical": routes.tech,
                   "account": routes.other, "other": routes.other}
    topics = [by_category[enriched.category]]
    if enriched.priority == "urgent":
        topics.append(routes.urgent)  # a copy, so an on-call view needs only one topic
    return topics


def enrich(ticket: Ticket, c: Classification, now: datetime, model: str) -> EnrichedTicket:
    return EnrichedTicket(**ticket.model_dump(), **c.model_dump(), enriched_at=now, model=model)


def process(value: bytes | None, key: bytes | None, source: SourceRef, now: datetime, *,
            in_serde: TicketSerde, out_serde: EnrichedTicketSerde,
            classify: Callable[[Ticket], Classification], policy: RetryPolicy,
            sleep: Callable[[float], bool], routes: Routes, model: str) -> list[Output]:
    """Where one input message goes. Stopping (shutdown) and infrastructure errors propagate."""
    try:
        if value is None:
            raise ValueError("message has no value (tombstone)")
        ticket = in_serde.decode(value, source.topic)
        classification = call_with_retries(
            lambda: classify(ticket), policy, sleep,
            on_retry=lambda attempt, exc, delay: log.warning(
                "%s[%d]@%d: LLM attempt %d failed (%s); retrying in %.0fs",
                source.topic, source.partition, source.offset, attempt, exc, delay))
    except (ValueError, LLMError) as exc:
        # Bad data, or an LLM that stayed broken through every retry: park it, keep the partition moving.
        return [Output(routes.dlq, key, value, dlq_headers(exc, source, now))]
    enriched = enrich(ticket, classification, now, model)
    customer = enriched.customer_id.encode()
    return [Output(topic, customer, out_serde.encode(enriched, topic), []) for topic in output_topics(enriched, routes)]


def _log_assign(consumer, partitions):
    log.info("assigned partitions: %s", [f"{p.topic}[{p.partition}]" for p in partitions])


def _log_revoke(consumer, partitions):
    log.info("revoked partitions: %s", [f"{p.topic}[{p.partition}]" for p in partitions])


def run_enricher(consumer, producer, source_topic: str, routes: Routes, *,
                 in_serde: TicketSerde, out_serde: EnrichedTicketSerde,
                 classify: Callable[[Ticket], Classification], should_stop: Callable[[], bool],
                 sleep: Callable[[float], bool], policy: RetryPolicy = RetryPolicy(),
                 model: str = "unknown", batch_size: int = 4) -> EnricherStats:
    stats = EnricherStats()
    delivery_errors: list = []

    def on_delivery(err, msg):
        if err is not None:
            delivery_errors.append(err)

    # With cooperative-sticky these callbacks receive only the partitions that moved.
    consumer.subscribe([source_topic], on_assign=_log_assign, on_revoke=_log_revoke)
    try:
        while not should_stop():
            # Small batches: every message may cost seconds of LLM time, and we must be back in
            # consume() before max.poll.interval.ms runs out (see check_poll_budget).
            messages = []
            for msg in consumer.consume(num_messages=batch_size, timeout=1.0):
                if msg.error():
                    log.warning("consumer error: %s", msg.error())
                else:
                    messages.append(msg)
            if not messages:
                continue
            for msg in messages:
                # Stopping (Ctrl-C during a retry wait) propagates from here: nothing of this batch
                # is committed, so it's all redone on restart — duplicates possible, loss impossible.
                outputs = process(msg.value(), msg.key(), SourceRef(msg.topic(), msg.partition(), msg.offset()),
                                  datetime.now(timezone.utc), in_serde=in_serde, out_serde=out_serde,
                                  classify=classify, policy=policy, sleep=sleep, routes=routes, model=model)
                for out in outputs:
                    producer.produce(out.topic, key=out.key, value=out.value, headers=out.headers or None,
                                     on_delivery=on_delivery)
                    producer.poll(0)
                if outputs[0].topic == routes.dlq:
                    stats.dead_lettered += 1
                else:
                    stats.enriched += 1
                    stats.routed += len(outputs)

            # 1) Wait until every output of this batch is acknowledged, 2) then commit the inputs.
            remaining = producer.flush(30)
            if remaining or delivery_errors:
                raise RuntimeError(f"outputs not acknowledged ({remaining} pending, "
                                   f"errors: {delivery_errors}); not committing")
            consumer.commit(asynchronous=False)
            stats.batches += 1
            log.info("batch done: %d messages (totals: %s)", len(messages), stats)
    except Stopping:
        log.info("stopped mid-batch; its offsets were not committed and it will be redone")
    finally:
        # Leave the group cleanly so the other instances take over our partitions right away.
        consumer.close()
    return stats


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Classify tickets.valid with the LLM and route them.")
    parser.add_argument("--group", default="enricher", help="consumer group id (same group = share the work)")
    parser.add_argument("--model", default=config.DEFAULT_MODEL, help="Ollama model name")
    parser.add_argument("--batch-size", type=int, default=4)
    parser.add_argument("--llm-timeout", type=float, default=30.0, help="seconds per LLM call")
    parser.add_argument("--max-poll-interval-ms", type=int, default=600_000)
    parser.add_argument("--schema-in", type=Path, default=config.DEFAULT_TICKET_SCHEMA)
    parser.add_argument("--schema-out", type=Path, default=config.ENRICHED_SCHEMA_V1)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO,
                        format=f"%(asctime)s %(name)s[{os.getpid()}] %(levelname)s %(message)s")

    policy = RetryPolicy()
    # Refuse a configuration that could get us evicted from the group mid-batch.
    worst = check_poll_budget(args.batch_size, policy, args.llm_timeout, args.max_poll_interval_ms)
    routes = Routes.default()
    registry = make_registry(config.SCHEMA_REGISTRY_URL)
    for topic in routes.outputs():
        ensure_registered(registry, topic, args.schema_out)
    in_serde = TicketSerde(registry, load_schema(args.schema_in))
    out_serde = EnrichedTicketSerde(registry, load_schema(args.schema_out))
    client = make_client(config.OLLAMA_HOST, timeout=args.llm_timeout)
    stop = install_stop_event()
    log.info("worst-case batch time %.0fs (max.poll.interval.ms = %ds)", worst, args.max_poll_interval_ms // 1000)

    consumer_conf = slow_consumer_config(args.group, args.max_poll_interval_ms)
    # client.id shows up in Kafka UI's consumer view, so you can tell the instances apart.
    consumer_conf["client.id"] = f"enricher-{os.getpid()}"
    stats = run_enricher(Consumer(consumer_conf), Producer(producer_config()), config.TOPIC_VALID, routes,
                         in_serde=in_serde, out_serde=out_serde,
                         classify=lambda ticket: classify_ticket(client, args.model, ticket),
                         should_stop=stop.is_set, sleep=interruptible_sleep(stop), policy=policy,
                         model=args.model, batch_size=args.batch_size)
    log.info("stopped: %s", stats)


if __name__ == "__main__":
    main()
