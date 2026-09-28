"""Graceful shutdown: turn Ctrl-C / SIGTERM into a flag the run-loops check."""
import signal
import threading
from typing import Callable


def install_stop_handler() -> Callable[[], bool]:
    stop = threading.Event()

    def _handler(signum, frame):
        stop.set()

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)
    return stop.is_set
