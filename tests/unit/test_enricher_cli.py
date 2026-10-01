import logging
import threading

import pytest

from pipeline import enricher
from pipeline.enricher import EnricherStats, FatalTransactionError, crash_hook, main


@pytest.fixture
def started(monkeypatch, mock_registry):
    """Run main() up to run_enricher, capturing what it would start with."""
    captured: dict = {}
    monkeypatch.setattr(enricher, "make_registry", lambda url: mock_registry)
    monkeypatch.setattr(enricher, "install_stop_event", threading.Event)
    monkeypatch.setattr(enricher, "Consumer", lambda conf: ("consumer", conf))
    monkeypatch.setattr(enricher, "Producer", lambda conf: ("producer", conf))

    def fake_run(consumer, producer, *args, **kwargs):
        captured.update(consumer=consumer[1], producer=producer[1], **kwargs)
        return EnricherStats()

    monkeypatch.setattr(enricher, "run_enricher", fake_run)
    return captured


def test_transactional_by_default_as_instance_1(started):
    assert main([]) == 0
    assert started["transactional"] is True
    assert started["producer"]["transactional.id"] == "enricher-1"
    assert started["consumer"]["isolation.level"] == "read_committed"


def test_instance_number_names_the_transactional_id(started):
    assert main(["--instance", "3"]) == 0
    assert started["producer"]["transactional.id"] == "enricher-3"


def test_at_least_once_mode_uses_a_plain_producer(started):
    assert main(["--at-least-once"]) == 0
    assert started["transactional"] is False
    assert "transactional.id" not in started["producer"]


def test_main_exits_1_when_fenced(started, monkeypatch, caplog):
    def fenced(*args, **kwargs):
        raise FatalTransactionError("Local: This instance has been fenced by a newer instance")

    monkeypatch.setattr(enricher, "run_enricher", fenced)
    with caplog.at_level(logging.ERROR, logger="enricher"):
        assert main(["--instance", "2"]) == 1
    assert "enricher-2" in caplog.text and "fenced" in caplog.text


def test_crash_before_commit_is_wired_to_the_hook(started, monkeypatch):
    seen = []
    monkeypatch.setattr(enricher, "crash_hook", lambda crash_at: seen.append(crash_at) or (lambda batch: None))
    main(["--crash-before-commit", "2"])
    main([])
    assert seen == [2, None]


def test_crash_hook_exits_only_on_the_chosen_batch():
    exits = []
    hook = crash_hook(2, exit=exits.append)
    hook(1)
    assert exits == []
    hook(2)
    assert exits == [1]


def test_no_crash_hook_by_default():
    exits = []
    hook = crash_hook(None, exit=exits.append)
    for batch in range(1, 5):
        hook(batch)
    assert exits == []


@pytest.mark.parametrize("flag", ["--instance", "--crash-before-commit"])
def test_numbers_must_be_positive(flag):
    with pytest.raises(SystemExit):
        main([flag, "0"])
