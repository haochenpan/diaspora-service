import importlib.metadata as importlib_metadata
import json
import logging
from unittest.mock import patch

import pytest
from flask import Flask
from globus_action_provider_tools import ActionRequest
from globus_action_provider_tools.flask.types import ActionStatus

from action_provider.main import create_app
from action_provider.utils import load_schema

SUCCESS_STATUS_CODE = 200

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

__version__ = importlib_metadata.version('diaspora_service')


@pytest.fixture(scope='module')
def client():
    """Create the Flask service."""
    app = create_app()
    with app.test_client() as client:
        yield client


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


# @patch('globus_action_provider_tools.flask.helpers.check_token')
# @patch('action_provider.main.action_status')
# def test_action_status(mock_action_status, mock_check_token, client):
#     """Test the action_status endpoint with mocked authentication."""
#     action_id = "test_action_id"  # Use a valid test action_id

#     # Mock the authentication check to return a valid AuthState
#     mock_auth_state = mock_check_token.return_value
#     mock_auth_state.check_authorization.return_value = True

#     # Mock the action status to return a predefined status
#     mock_action_status.return_value = (
#         ActionStatus(
#             action_id=action_id,
#             status='ACTIVE',
#             creator_id='urn:globus:auth:identity:00000000-0000-0000-0000-000000000000',  # Valid creator_id
#             details={'description': 'test details'}
#         ),
#         SUCCESS_STATUS_CODE
#     )

#     headers = {
#         'Authorization': 'Bearer valid_token'  # This token value doesn't matter due to mocking
#     }
#     response = client.get(f'/actions/{action_id}/status', headers=headers)
#     response_data = json.loads(response.data.decode('utf-8'))
#     logger.info(f'Response data: {response_data}')

#     assert response.status_code == SUCCESS_STATUS_CODE
#     assert 'status' in response_data
#     assert 'action_id' in response_data
#     assert response_data['action_id'] == action_id
#     assert response_data['status'] == 'ACTIVE'
#     assert response_data['creator_id'] == 'urn:globus:auth:identity:00000000-0000-0000-0000-000000000000'
#     assert 'details' in response_data
#     assert response_data['details']['description'] == 'test details'
