"""Diaspora Web Service."""

from __future__ import annotations

from fastapi import FastAPI

from web_service import __version__

app = FastAPI()


@app.get('/')
async def root() -> dict[str, str]:
    """One and only entry point."""
    return {'message': f'Hello World from Web Service version {__version__}'}
