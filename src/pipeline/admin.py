"""Create the pipeline's topics. Safe to run any number of times."""
import logging

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from pipeline import config
from pipeline.config import TopicSpec

log = logging.getLogger("admin")

REPLICATION_FACTOR = 1  # single-broker cluster


class TopicSetupError(RuntimeError):
    pass


def ensure_topics(admin: AdminClient, specs: list[TopicSpec], timeout: float = 10.0) -> list[str]:
    """Create any topics in `specs` that don't exist yet. Returns the names it created."""
    try:
        existing = set(admin.list_topics(timeout=timeout).topics)
    except KafkaException as exc:
        raise TopicSetupError(f"cannot reach Kafka: {exc}") from exc

    missing = [s for s in specs if s.name not in existing]
    if not missing:
        return []

    futures = admin.create_topics(
        [NewTopic(s.name, num_partitions=s.partitions, replication_factor=REPLICATION_FACTOR, config=s.config)
         for s in missing],
        operation_timeout=timeout,
    )
    created = []
    for name, future in futures.items():
        try:
            future.result()
            created.append(name)
        except KafkaException as exc:
            if exc.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                continue  # someone else created it in the meantime — fine
            raise TopicSetupError(f"failed to create {name}: {exc}") from exc
    return created


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    admin = AdminClient({"bootstrap.servers": config.BOOTSTRAP_SERVERS})
    created = ensure_topics(admin, config.TOPIC_SPECS)
    log.info("created: %s", created or "nothing (all topics already exist)")


if __name__ == "__main__":
    main()
