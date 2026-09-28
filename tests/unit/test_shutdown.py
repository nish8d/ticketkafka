import os
import signal

from pipeline.shutdown import install_stop_handler


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
