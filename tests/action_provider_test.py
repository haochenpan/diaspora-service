from __future__ import annotations

import importlib.metadata as importlib_metadata
import json
import logging
import os

import pytest

from action_provider.main import create_app
from action_provider.utils import load_schema
from testing.globus import get_access_token

SUCCESS_STATUS_CODE = 200
SUCCESS_STATUS_STRING = 'SUCCEEDED'


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

__version__ = importlib_metadata.version('diaspora_service')


@pytest.fixture(scope='module')
def client():
    """Create the Flask service."""
    app = create_app()
    with app.test_client() as client:
        yield client


@pytest.fixture(scope='module')
def access_token():
    """Retrieve the access token."""
    return get_access_token()


def test_load_schema():
    """Test the load_schema function."""
    schema = load_schema()
    assert isinstance(schema, dict)
    assert 'type' in schema
    assert schema['type'] == 'object'


def test_root_endpoint(client):
    """Test the root endpoint."""
    response = client.get('/')
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response data: {response_data}')

    assert response.status_code == SUCCESS_STATUS_CODE
    assert response_data['title'] == 'Diaspora Action Provider'
    assert response_data['api_version'] == __version__


def test_status_endpoint(client, access_token):
    """Test the status endpoint."""
    action_id = 'my_action_id'
    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    response = client.get(f'/{action_id}/status', headers=headers)
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response data: {response_data}')

    assert response.status_code == SUCCESS_STATUS_CODE
    assert response_data['status'] == SUCCESS_STATUS_STRING
