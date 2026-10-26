"""The gunicorn worker class the image runs."""

from __future__ import annotations

from typing import Any
from typing import ClassVar

from uvicorn_worker import UvicornWorker

# Seconds uvicorn waits for open requests at shutdown before it cancels
# them and runs the application's shutdown. gunicorn kills the worker 30 s
# after SIGTERM (its --graceful-timeout), so this leaves the application
# about 10 s to close its consumers and its producer.
GRACEFUL_SHUTDOWN_S = 20


class Worker(UvicornWorker):
    """UvicornWorker with a limit on how long shutdown waits for requests.

    UvicornWorker sets no limit, so a request that stays open would hold
    the worker until gunicorn kills it, before the application's shutdown
    runs.
    """

    CONFIG_KWARGS: ClassVar[dict[str, Any]] = {
        **UvicornWorker.CONFIG_KWARGS,
        'timeout_graceful_shutdown': GRACEFUL_SHUTDOWN_S,
    }
