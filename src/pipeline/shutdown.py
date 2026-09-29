"""Graceful shutdown: turn Ctrl-C / SIGTERM into a flag the run-loops check."""
import signal
import threading
from typing import Callable


def install_stop_event() -> threading.Event:
    """Set on the first Ctrl-C / SIGTERM. Wait on it to sleep in a way shutdown can interrupt."""
    stop = threading.Event()

    def _handler(signum, frame):
        stop.set()

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)
    return stop


def install_stop_handler() -> Callable[[], bool]:
    return install_stop_event().is_set
