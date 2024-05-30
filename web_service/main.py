"""Diaspora Web Service."""

from __future__ import annotations

import os

from fastapi import FastAPI

from common.utils import EnvironmentChecker
from web_service import __version__

app = FastAPI()


@app.get('/')
async def root() -> dict[str, str]:
    """One and only entry point."""
    EnvironmentChecker.check_env_variables(
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY',
        'SERVER_CLIENT_ID',
        'SERVER_SECRET',
    )

    version = __version__
    client_id = os.environ['SERVER_CLIENT_ID']
    return {
        'message': (
            'Hello World from Web Service '
            f'version {version} with CLIENT_ID={client_id}'
        ),
    }
