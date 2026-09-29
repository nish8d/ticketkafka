import os
import signal

from pipeline.shutdown import install_stop_event, install_stop_handler


def test_sigint_sets_stop_flag():
    old_int, old_term = signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM)
    try:
        should_stop = install_stop_handler()
        assert should_stop() is False
        os.kill(os.getpid(), signal.SIGINT)
        assert should_stop() is True
    finally:
        signal.signal(signal.SIGINT, old_int)
        signal.signal(signal.SIGTERM, old_term)


def test_stop_event_is_set_by_sigterm():
    old_int, old_term = signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM)
    try:
        stop = install_stop_event()
        assert not stop.is_set()
        os.kill(os.getpid(), signal.SIGTERM)
        assert stop.wait(1)
    finally:
        signal.signal(signal.SIGINT, old_int)
        signal.signal(signal.SIGTERM, old_term)
