"""Unit tests for the gunicorn worker class the image runs."""

from __future__ import annotations

import json
import pathlib

import uvicorn
from uvicorn_worker import UvicornWorker

from web_service.worker import GRACEFUL_SHUTDOWN_S
from web_service.worker import Worker

DOCKERFILE = pathlib.Path(__file__).parents[1] / 'web_service' / 'Dockerfile'


def _command() -> list[str]:
    for line in DOCKERFILE.read_text().splitlines():
        if line.startswith('CMD '):
            return json.loads(line.removeprefix('CMD '))
    raise AssertionError('The Dockerfile has no CMD')


def test_worker_limits_how_long_shutdown_waits_for_requests() -> None:
    assert issubclass(Worker, UvicornWorker)
    assert Worker.CONFIG_KWARGS == {
        **UvicornWorker.CONFIG_KWARGS,
        'timeout_graceful_shutdown': 20,
    }
    # uvicorn takes the setting under that name
    config = uvicorn.Config(app=None, **Worker.CONFIG_KWARGS)
    assert config.timeout_graceful_shutdown == GRACEFUL_SHUTDOWN_S


def test_image_runs_the_worker_and_leaves_time_for_shutdown() -> None:
    command = _command()
    assert command[command.index('-k') + 1] == 'web_service.worker.Worker'
    graceful_timeout = int(command[command.index('--graceful-timeout') + 1])
    # gunicorn kills the worker after graceful_timeout; the service's own
    # shutdown runs after the wait for requests
    assert graceful_timeout - GRACEFUL_SHUTDOWN_S >= 10
