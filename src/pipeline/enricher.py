"""Stage 5: classify valid tickets with the LLM and route them by category (urgent ones copied too).

Run several instances with the same --group to share tickets.valid's partitions between them.
"""
import argparse
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition

from pipeline import config
from pipeline.clients import commit_batch, producer_config, slow_consumer_config, transactional_producer_config
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
    commit_failures: int = 0


class FatalTransactionError(RuntimeError):
    """The transactional producer can't go on — usually fenced by a newer instance with our transactional.id."""


def next_offsets(messages) -> list[TopicPartition]:
    """The offsets a finished batch commits: per partition, one past the highest offset consumed."""
    highest: dict[tuple[str, int], int] = {}
    for msg in messages:
        key = (msg.topic(), msg.partition())
        highest[key] = max(highest.get(key, -1), msg.offset())
    return [TopicPartition(topic, partition, offset + 1) for (topic, partition), offset in sorted(highest.items())]


def rewind_positions(messages) -> dict[tuple[str, int], int]:
    """Where to seek after an aborted batch: per partition, the lowest offset in it."""
    lowest: dict[tuple[str, int], int] = {}
    for msg in messages:
        key = (msg.topic(), msg.partition())
        lowest[key] = min(lowest.get(key, msg.offset()), msg.offset())
    return lowest


def rewind(consumer, messages) -> None:
    """Seek back to the start of an aborted batch, on the partitions we still own. Partitions a
    rebalance gave away are left to their new owner, which resumes from the committed offset."""
    owned = {(tp.topic, tp.partition) for tp in consumer.assignment()}
    for (topic, partition), offset in rewind_positions(messages).items():
        if (topic, partition) in owned:
            consumer.seek(TopicPartition(topic, partition, offset))


class AtLeastOnce:
    """Stage 5: produce the batch, wait until every output is acknowledged, then commit the inputs.
    A crash between the two redoes the batch on restart: duplicates possible, loss impossible."""
    mode = "at-least-once"

    def __init__(self, producer):
        self.producer = producer

    def start(self) -> None:
        pass

    def write(self, consumer, outputs: list[Output], messages, before_commit: Callable[[], None]) -> bool:
        errors: list = []

        def on_delivery(err, msg):
            if err is not None:
                errors.append(err)

        for out in outputs:
            self.producer.produce(out.topic, key=out.key, value=out.value, headers=out.headers or None,
                                  on_delivery=on_delivery)
            self.producer.poll(0)
        remaining = self.producer.flush(30)
        if remaining or errors:
            raise RuntimeError(f"outputs not acknowledged ({remaining} pending, errors: {errors}); not committing")
        before_commit()
        return commit_batch(consumer)


TXN_ATTEMPTS = 3  # for errors the client marks retriable, such as a coordinator timeout


class Transactional:
    """Stage 8: a batch's outputs and its input offsets in one transaction. Readers with
    isolation.level=read_committed see all of it or none of it, so a crash can't duplicate outputs.
    The transaction spans only these writes (milliseconds), never the LLM calls: while it is open,
    read_committed readers can't read past it."""
    mode = "transactional"

    def __init__(self, producer, timeout: float = 30.0):
        self.producer, self.timeout = producer, timeout

    def start(self) -> None:
        # Fences any older producer with our transactional.id and aborts its unfinished transaction.
        # Must run before the consumer subscribes: until that transaction ends, the group's committed
        # offsets count as unstable and a new consumer in the group receives nothing.
        self._call(lambda: self.producer.init_transactions(self.timeout))

    def write(self, consumer, outputs: list[Output], messages, before_commit: Callable[[], None]) -> bool:
        try:
            self.producer.begin_transaction()
            for out in outputs:
                self.producer.produce(out.topic, key=out.key, value=out.value, headers=out.headers or None)
                self.producer.poll(0)
            # The offsets join the transaction, tagged with our group generation: if a rebalance gave
            # these partitions to another member meanwhile, they are refused and we must abort.
            self._call(lambda: self.producer.send_offsets_to_transaction(
                next_offsets(messages), consumer.consumer_group_metadata(), self.timeout))
            # commit_transaction() flushes anyway; flushing first puts the outputs on the broker (as an
            # open transaction) before the commit point, so a crash right there leaves them in the log
            # to be aborted, instead of silently dropped from the client's buffer.
            self.producer.flush(self.timeout)
            before_commit()
            self._call(lambda: self.producer.commit_transaction(self.timeout))
            return True
        except KafkaException as exc:
            error = exc.args[0]
            if error.fatal():
                raise FatalTransactionError(error.str()) from exc
            if not error.txn_requires_abort():
                raise
            log.warning("transaction aborted (%s); rewinding to redo the batch", error.str())
            self._call(lambda: self.producer.abort_transaction(self.timeout))
            rewind(consumer, messages)
            return False

    def _call(self, fn):
        for attempt in range(1, TXN_ATTEMPTS + 1):
            try:
                return fn()
            except KafkaException as exc:
                if not exc.args[0].retriable() or attempt == TXN_ATTEMPTS:
                    raise
                log.warning("retriable transaction error (%s); attempt %d of %d", exc.args[0].str(),
                            attempt, TXN_ATTEMPTS)


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
                 model: str = "unknown", batch_size: int = 4, transactional: bool = False,
                 before_commit: Callable[[int], None] = lambda batch: None) -> EnricherStats:
    stats = EnricherStats()
    writer = Transactional(producer) if transactional else AtLeastOnce(producer)
    writer.start()  # before subscribing: see Transactional.start
    # With cooperative-sticky these callbacks receive only the partitions that moved.
    consumer.subscribe([source_topic], on_assign=_log_assign, on_revoke=_log_revoke)
    attempt = 0
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
            # Classify the whole batch first, with nothing written yet. Stopping (Ctrl-C during a
            # retry wait) propagates from here: nothing of this batch is written or committed.
            outputs: list[Output] = []
            for msg in messages:
                out = process(msg.value(), msg.key(), SourceRef(msg.topic(), msg.partition(), msg.offset()),
                              datetime.now(timezone.utc), in_serde=in_serde, out_serde=out_serde,
                              classify=classify, policy=policy, sleep=sleep, routes=routes, model=model)
                outputs.extend(out)
                if out[0].topic == routes.dlq:
                    stats.dead_lettered += 1
                else:
                    stats.enriched += 1
                    stats.routed += len(out)
            attempt += 1
            if not writer.write(consumer, outputs, messages, lambda: before_commit(attempt)):
                stats.commit_failures += 1
                continue
            stats.batches += 1
            log.info("batch done (%s): %d messages (totals: %s)", writer.mode, len(messages), stats)
    except Stopping:
        log.info("stopped mid-batch; nothing of it was committed and it will be redone")
    finally:
        # Leave the group cleanly so the other instances take over our partitions right away.
        consumer.close()
    return stats


def _positive_int(text: str) -> int:
    value = int(text)
    if value < 1:
        raise argparse.ArgumentTypeError("must be 1 or more")
    return value


def crash_hook(crash_at: int | None, exit=os._exit) -> Callable[[int], None]:
    """DEMO ONLY (--crash-before-commit): end the process abruptly on batch `crash_at`, after its
    outputs are written and before they are committed — the one moment where at-least-once and
    transactional runs differ. os._exit skips every cleanup, like a kill -9."""
    def before_commit(batch: int) -> None:
        if batch == crash_at:
            log.warning("--crash-before-commit %d: exiting now; outputs written, nothing committed", batch)
            logging.shutdown()
            exit(1)
    return before_commit


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Classify tickets.valid with the LLM and route them.")
    parser.add_argument("--group", default="enricher", help="consumer group id (same group = share the work)")
    parser.add_argument("--model", default=config.DEFAULT_MODEL, help="Ollama model name")
    parser.add_argument("--batch-size", type=int, default=4)
    parser.add_argument("--llm-timeout", type=float, default=30.0, help="seconds per LLM call")
    parser.add_argument("--max-poll-interval-ms", type=int, default=600_000)
    parser.add_argument("--schema-in", type=Path, default=config.DEFAULT_TICKET_SCHEMA)
    parser.add_argument("--schema-out", type=Path, default=config.ENRICHED_SCHEMA_V1)
    parser.add_argument("--at-least-once", action="store_true",
                        help="stage 5 mode: commit the offsets after the outputs, instead of in one transaction")
    parser.add_argument("--instance", type=_positive_int, default=1,
                        help="this enricher's number; transactional.id = enricher-N (one number per running enricher)")
    parser.add_argument("--crash-before-commit", type=_positive_int, default=None, metavar="N",
                        help="DEMO ONLY: exit abruptly on the Nth batch, after writing its outputs, before committing")
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

    transactional_id = f"enricher-{args.instance}"
    if args.at_least_once:
        producer_conf = producer_config()
        log.info("mode: at-least-once (stage 5)")
    else:
        producer_conf = transactional_producer_config(transactional_id)
        log.info("mode: transactional, transactional.id=%s", transactional_id)
    consumer_conf = slow_consumer_config(args.group, args.max_poll_interval_ms)
    # client.id shows up in Kafka UI's consumer view, so you can tell the instances apart.
    consumer_conf["client.id"] = f"enricher-{os.getpid()}"
    try:
        stats = run_enricher(Consumer(consumer_conf), Producer(producer_conf), config.TOPIC_VALID, routes,
                             in_serde=in_serde, out_serde=out_serde,
                             classify=lambda ticket: classify_ticket(client, args.model, ticket),
                             should_stop=stop.is_set, sleep=interruptible_sleep(stop), policy=policy,
                             model=args.model, batch_size=args.batch_size, transactional=not args.at_least_once,
                             before_commit=crash_hook(args.crash_before_commit))
    except FatalTransactionError as exc:
        log.error("fenced or otherwise unable to continue as %s (%s): is another enricher running with "
                  "--instance %d? Nothing of the current batch was committed.", transactional_id, exc, args.instance)
        return 1
    log.info("stopped: %s", stats)
    return 0


if __name__ == "__main__":
    sys.exit(main())
