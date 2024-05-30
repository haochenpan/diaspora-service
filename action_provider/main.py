"""Diaspora Action Provider."""

from __future__ import annotations

import os

from flask import Flask

from action_provider import __version__
from common.utils import EnvironmentChecker

app = Flask(__name__)


@app.route('/')
def hello_world() -> str:
    """One and only entry point."""
    version = __version__
    client_id = os.environ['CLIENT_ID']
    return (
        'Hello World! from Action Provider '
        f'version {version} with CLIENT_ID={client_id}'
    )


if __name__ == '__main__':
    EnvironmentChecker.check_env_variables(
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY',
        'CLIENT_ID',
        'CLIENT_SECRET',
        'CLIENT_SCOPE',
        'DEFAULT_SERVERS',
    )
    app.run(host='0.0.0.0', port=8000)
