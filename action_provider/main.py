"""Diaspora Action Provider."""

from __future__ import annotations

from flask import Flask

from action_provider import __version__

app = Flask(__name__)


@app.route('/')
def hello_world() -> str:
    """One and only entry point."""
    return f'Hello World from Action Provider version {__version__}'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
