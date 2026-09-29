"""Manage ticket schemas in Schema Registry: register a version, check one first, list what's there.

  uv run python -m pipeline.schemas register schemas/ticket.v1.avsc
  uv run python -m pipeline.schemas check schemas/ticket.v2.avsc
  uv run python -m pipeline.schemas list
"""
import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

import httpx
from confluent_kafka.schema_registry import Schema
from confluent_kafka.schema_registry.error import SchemaRegistryError

from pipeline import config
from pipeline.serde import load_schema, make_registry, subject_for

TICKET_TOPICS: tuple[str, ...] = (config.TOPIC_RAW, config.TOPIC_VALID)
# BACKWARD: a new schema must be able to read data written with the previous one.
# So consumers upgrade first, producers second.
COMPATIBILITY = "BACKWARD"


class IncompatibleSchema(RuntimeError):
    pass


@dataclass(frozen=True)
class Registered:
    subject: str
    schema_id: int
    version: int


@dataclass(frozen=True)
class Compatibility:
    subject: str
    compatible: bool
    messages: list[str]


def register(registry, topics, schema_str: str) -> list[Registered]:
    results = []
    schema = Schema(schema_str, "AVRO")
    for topic in topics:
        subject = subject_for(topic)
        registry.set_compatibility(subject_name=subject, level=COMPATIBILITY)
        try:
            registry.register_schema(subject, schema)
        except SchemaRegistryError as exc:
            if exc.http_status_code == 409:
                raise IncompatibleSchema(f"{subject}: rejected as incompatible ({COMPATIBILITY}): {exc}") from exc
            raise
        # Registering an identical schema again is a no-op that returns the existing version.
        found = registry.lookup_schema(subject, schema)
        results.append(Registered(subject, found.schema_id, found.version))
    return results


def check(url: str, topics, schema_str: str, http=httpx) -> list[Compatibility]:
    # Straight to the REST API: the Python client's test_compatibility() drops the reasons.
    results = []
    for topic in topics:
        subject = subject_for(topic)
        response = http.post(f"{url}/compatibility/subjects/{subject}/versions/latest",
                             params={"verbose": "true"},
                             json={"schema": schema_str, "schemaType": "AVRO"}, timeout=10)
        body = response.json()
        if response.status_code == 404:
            results.append(Compatibility(subject, True, ["no versions registered yet"]))
            continue
        if response.status_code != 200:
            raise RuntimeError(f"{subject}: registry answered {response.status_code}: {body}")
        results.append(Compatibility(subject, body["is_compatible"], body.get("messages", [])))
    return results


def list_subjects(registry) -> dict[str, list[int]]:
    return {subject: sorted(registry.get_versions(subject)) for subject in sorted(registry.get_subjects())}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Register, check and list ticket schemas.")
    parser.add_argument("--topic", action="append", dest="topics",
                        help=f"topic whose value subject to use (repeatable; default: {', '.join(TICKET_TOPICS)})")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("register", help="register a schema file (rejected if incompatible)").add_argument("path", type=Path)
    sub.add_parser("check", help="test a schema file against the latest version").add_argument("path", type=Path)
    sub.add_parser("list", help="list subjects and their versions")
    args = parser.parse_args(argv)
    topics = args.topics or TICKET_TOPICS
    registry = make_registry(config.SCHEMA_REGISTRY_URL)

    if args.command == "list":
        for subject, versions in list_subjects(registry).items():
            print(f"{subject}: versions {versions}")
        return 0
    schema_str = load_schema(args.path)
    if args.command == "check":
        results = check(config.SCHEMA_REGISTRY_URL, topics, schema_str)
        for r in results:
            print(f"{r.subject}: {'compatible' if r.compatible else 'INCOMPATIBLE'}")
            for message in r.messages:
                print(f"    {message}")
        return 0 if all(r.compatible for r in results) else 1
    try:
        for r in register(registry, topics, schema_str):
            print(f"{r.subject}: version {r.version} (schema id {r.schema_id})")
    except IncompatibleSchema as exc:
        print(exc, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
