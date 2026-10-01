"""Manage the Kafka Connect connectors defined in connect/*.json, through Connect's REST API.

  uv run python -m pipeline.connectors apply            # create or update every connector
  uv run python -m pipeline.connectors status [NAME]    # connector and task states
  uv run python -m pipeline.connectors restart [NAME]   # restart failed tasks
  uv run python -m pipeline.connectors delete NAME      # remove one; its consumer group keeps its offsets
"""
import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path

import httpx
import psycopg

from pipeline import config

# Every sink file must set these; anything else falls back to Connect's or the plugin's defaults.
REQUIRED_KEYS: tuple[str, ...] = ("connector.class", "topics", "connection.url", "table.name.format",
                                  "insert.mode", "pk.mode", "pk.fields")
# A failed task's trace is a Java stack trace; its first lines say what went wrong.
TRACE_LINES = 5


class ConnectorError(RuntimeError):
    pass


@dataclass(frozen=True)
class Connector:
    name: str
    config: dict[str, str]

    @property
    def table(self) -> str:
        return self.config["table.name.format"]


def load_connector(path: Path) -> Connector:
    path = Path(path)
    data = json.loads(path.read_text())
    name, cfg = data.get("name"), data.get("config")
    if not isinstance(name, str) or not isinstance(cfg, dict):
        raise ValueError(f'{path.name}: expected {{"name": ..., "config": {{...}}}}')
    missing = [key for key in REQUIRED_KEYS if key not in cfg]
    if missing:
        raise ValueError(f"{path.name}: missing {', '.join(missing)}")
    return Connector(name, cfg)


def load_connectors(directory: Path = config.CONNECT_DIR) -> list[Connector]:
    return [load_connector(path) for path in sorted(Path(directory).glob("*.json"))]


def missing_tables(connectors: list[Connector], existing: set[str]) -> list[str]:
    """Tables the connectors write to that don't exist. With auto.create=false a missing table sends
    every record straight to the DLQ (no retries), so `apply` checks first."""
    return sorted({c.table for c in connectors} - existing)


def existing_tables(dsn: str) -> set[str]:
    with psycopg.connect(dsn, connect_timeout=5) as conn:
        rows = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'").fetchall()
    return {row[0] for row in rows}


def _trace(state: dict) -> list[str]:
    lines = (state.get("trace") or "").splitlines()
    shown = [f"      {line}" for line in lines[:TRACE_LINES]]
    if len(lines) > TRACE_LINES:
        shown.append(f"      … {len(lines) - TRACE_LINES} more lines (docker compose logs connect)")
    return shown


def format_status(status: dict) -> list[str]:
    connector = status["connector"]
    lines = [f"{status['name']} ({status.get('type', '?')}): {connector['state']} on {connector['worker_id']}",
             *_trace(connector)]
    tasks = sorted(status.get("tasks", []), key=lambda task: task["id"])
    if not tasks:
        lines.append("  no tasks yet (Connect assigns them a moment after the connector starts)")
    for task in tasks:
        lines.append(f"  task {task['id']}: {task['state']} on {task['worker_id']}")
        lines += _trace(task)
    return lines


def has_failed(status: dict) -> bool:
    states = [status["connector"]["state"], *(task["state"] for task in status.get("tasks", []))]
    return "FAILED" in states


class ConnectClient:
    """A thin wrapper over Connect's REST API. `http` is the httpx module, or a fake in tests."""

    def __init__(self, url: str = config.CONNECT_URL, http=httpx):
        self.url, self.http = url.rstrip("/"), http

    def names(self) -> list[str]:
        return sorted(self._check(self.http.get(f"{self.url}/connectors", timeout=10), "list connectors").json())

    def apply(self, connector: Connector) -> str:
        # PUT .../config creates the connector or replaces its config, so it is safe to repeat.
        response = self.http.put(f"{self.url}/connectors/{connector.name}/config", json=connector.config, timeout=30)
        self._check(response, connector.name)
        return "created" if response.status_code == 201 else "updated"

    def status(self, name: str) -> dict:
        return self._check(self.http.get(f"{self.url}/connectors/{name}/status", timeout=10), name).json()

    def restart(self, name: str) -> None:
        self._check(self.http.post(f"{self.url}/connectors/{name}/restart",
                                   params={"includeTasks": "true", "onlyFailed": "true"}, timeout=30), name)

    def delete(self, name: str) -> None:
        self._check(self.http.delete(f"{self.url}/connectors/{name}", timeout=30), name)

    @staticmethod
    def _check(response, what: str):
        if response.status_code >= 400:
            try:
                message = response.json().get("message", response.text)
            except ValueError:
                message = response.text
            raise ConnectorError(f"{what}: Connect answered {response.status_code}: {message}")
        return response


def _apply(client: ConnectClient, tables) -> int:
    connectors = load_connectors()
    try:
        missing = missing_tables(connectors, tables(config.POSTGRES_DSN))
    except psycopg.OperationalError as exc:
        print(f"cannot reach Postgres to check the sink tables: {exc}".strip(), file=sys.stderr)
        return 1
    if missing:
        print(f"missing table(s) in Postgres: {', '.join(missing)}. connect/sql/init.sql creates them, but only "
              "on a fresh volume: run `docker compose down -v && docker compose up -d --build` (drops Postgres "
              "data), or `docker compose exec -T postgres psql -U tickets -d tickets < connect/sql/init.sql`",
              file=sys.stderr)
        return 1
    for connector in connectors:
        print(f"{connector.name}: {client.apply(connector)}")
    return 0


def _run(args, client: ConnectClient, tables) -> int:
    if args.command == "apply":
        return _apply(client, tables)
    if args.command == "delete":
        client.delete(args.name)
        print(f"{args.name}: deleted (its consumer group connect-{args.name} keeps its offsets)")
        return 0
    names = [args.name] if args.name else client.names()
    if not names:
        print("no connectors; create them with `uv run python -m pipeline.connectors apply`")
        return 0
    if args.command == "restart":
        for name in names:
            client.restart(name)
            print(f"{name}: restart requested for failed tasks")
        return 0
    failed = False
    for name in names:
        status = client.status(name)
        print("\n".join(format_status(status)))
        failed = failed or has_failed(status)
    return 1 if failed else 0


def main(argv: list[str] | None = None, client: ConnectClient | None = None, tables=existing_tables) -> int:
    parser = argparse.ArgumentParser(description="Create, inspect, restart and delete Kafka Connect connectors.")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("apply", help="create or update every connector in connect/*.json")
    sub.add_parser("status", help="connector and task states").add_argument("name", nargs="?")
    sub.add_parser("restart", help="restart failed tasks").add_argument("name", nargs="?")
    sub.add_parser("delete", help="delete one connector").add_argument("name")
    args = parser.parse_args(argv)
    client = client or ConnectClient()
    try:
        return _run(args, client, tables)
    except httpx.TransportError:
        print(f"cannot reach Kafka Connect at {client.url}; start it with `docker compose up -d --build`",
              file=sys.stderr)
        return 1
    except ConnectorError as exc:
        print(exc, file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
