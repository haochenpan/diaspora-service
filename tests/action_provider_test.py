from __future__ import annotations

import importlib.metadata as importlib_metadata
import logging

import pytest

from action_provider.main import app

SUCCESS_STATUS_CODE = 200

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

__version__ = importlib_metadata.version('diaspora_service')


@pytest.fixture()
def client():
    """Test the Flask service."""
    with app.test_client() as client:
        yield client


def test_hello_world(client):
    """Test the hello_world endpoint."""
    response = client.get('/')
    logger.info(f'Response data: {response.data.decode("utf-8")}')

    assert response.status_code == SUCCESS_STATUS_CODE
    assert (
        response.data.decode('utf-8')
        == f'Hello World from Action Provider version {__version__}'
    )
