from __future__ import annotations

import importlib.metadata as importlib_metadata

from fastapi.testclient import TestClient

from web_service.main import app

SUCCESS_STATUS_CODE = 200

client = TestClient(app)
__version__ = importlib_metadata.version('diaspora_service')


def test_root():
    """Test the root endpoint."""
    response = client.get('/')
    assert response.status_code == SUCCESS_STATUS_CODE
    assert response.json()['message'].startswith(
        f'Hello World! from Web Service version {__version__}',
    )
