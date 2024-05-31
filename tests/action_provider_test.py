from __future__ import annotations

import importlib.metadata as importlib_metadata
import json
import logging

from action_provider.utils import load_schema
from testing.fixtures import access_token  # noqa: F401
from testing.fixtures import client  # noqa: F401

SUCCESS_STATUS_CODE = 200
ACCEPTED_STATUS_CODE = 202
UNPROCESSABLE_STATUS_CODE = 422
SUCCESS_STATUS_STRING = 'SUCCEEDED'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

__version__ = importlib_metadata.version('diaspora_service')


def test_load_schema():
    """Test the load_schema function."""
    schema = load_schema()
    assert isinstance(schema, dict)
    assert 'type' in schema
    assert schema['type'] == 'object'


def test_root_endpoint(client):  # noqa: F811
    """Test the root endpoint."""
    response = client.get('/')
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response data: {response_data}')

    assert response.status_code == SUCCESS_STATUS_CODE
    assert response_data['title'] == 'Diaspora Action Provider'
    assert response_data['api_version'] == __version__


def test_status_endpoint(client, access_token):  # noqa: F811
    """Test the status endpoint."""
    action_id = 'my_action_id'
    headers = {
        'Authorization': f'Bearer {access_token}',
    }

    response = client.get(f'/{action_id}/status', headers=headers)
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response data: {response_data}')

    assert response.status_code == SUCCESS_STATUS_CODE
    assert response_data['status'] == SUCCESS_STATUS_STRING


def test_cancel_endpoint(client, access_token):  # noqa: F811
    """Test the cancel endpoint."""
    action_id = 'my_action_id'
    headers = {
        'Authorization': f'Bearer {access_token}',
    }

    response = client.post(f'/{action_id}/cancel', headers=headers)
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response data: {response_data}')

    assert response.status_code == SUCCESS_STATUS_CODE
    assert response_data['status'] == SUCCESS_STATUS_STRING


def test_release_endpoint(client, access_token):  # noqa: F811
    """Test the release endpoint."""
    action_id = 'my_action_id'
    headers = {
        'Authorization': f'Bearer {access_token}',
    }

    response = client.post(f'/{action_id}/release', headers=headers)
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response data: {response_data}')

    assert response.status_code == SUCCESS_STATUS_CODE
    assert response_data['status'] == SUCCESS_STATUS_STRING


def test_run_endpoint_no_msg(client, access_token):  # noqa: F811
    """Test the run endpoint (no msg field)."""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    }

    data = {
        'request_id': '100',
        'body': {
            'action': 'produce',
            'topic': 'a_topic',
        },
    }
    response = client.post('/run', json=data, headers=headers)
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response code: {response.status_code}')
    logger.info(f'Response data: {response_data}')

    assert response.status_code == ACCEPTED_STATUS_CODE
    assert response_data['details']['error'] == "'msgs' does not exist."


def test_run_endpoint_no_msgs(client, access_token):  # noqa: F811
    """Test the run endpoint (no msgs field)."""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    }

    data = {
        'request_id': '100',
        'body': {
            'action': 'produce',
            'topic': 'a_topic',
        },
    }
    response = client.post('/run', json=data, headers=headers)
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response code: {response.status_code}')
    logger.info(f'Response data: {response_data}')

    assert response.status_code == ACCEPTED_STATUS_CODE
    assert response_data['details']['error'] == "'msgs' does not exist."


def test_run_endpoint_empty_msgs(client, access_token):  # noqa: F811
    """Test the run endpoint (empty msgs field)."""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    }

    data = {
        'request_id': '100',
        'body': {
            'action': 'produce',
            'topic': 'a_topic',
            'msgs': [],
        },
    }
    response = client.post('/run', json=data, headers=headers)
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response code: {response.status_code}')
    logger.info(f'Response data: {response_data}')

    assert response.status_code == ACCEPTED_STATUS_CODE
    assert response_data['details']['error'] == "'msgs' is empty."


def test_run_endpoint_bad_msgs(client, access_token):  # noqa: F811
    """Test the run endpoint (bad msgs)."""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    }

    data = {
        'request_id': '100',
        'body': {
            'action': 'produce',
            'topic': 'a_topic',
            'msgs': ['msg1', 'msg2', 'msg3'],
        },
    }
    response = client.post('/run', json=data, headers=headers)
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response code: {response.status_code}')
    logger.info(f'Response data: {response_data}')

    assert response.status_code == UNPROCESSABLE_STATUS_CODE
    assert response_data['code'] == 'RequestValidationError'


def test_run_endpoint_lens_mismatch_1(client, access_token):  # noqa: F811
    """Test the run endpoint (lens mismatch case 1)."""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    }

    data = {
        'request_id': '100',
        'body': {
            'action': 'produce',
            'topic': 'a_topic',
            'msgs': [
                {'content': 'hello world1'},
                {'content': 'hello world2'},
            ],
            'keys': [
                'key1',
            ],
        },
    }
    response = client.post('/run', json=data, headers=headers)
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response code: {response.status_code}')
    logger.info(f'Response data: {response_data}')

    assert response.status_code == ACCEPTED_STATUS_CODE
    assert (
        response_data['details']['error']
        == "The len of 'keys' (1) must match that of 'msgs' (2)."
    )


def test_run_endpoint_lens_mismatch_2(client, access_token):  # noqa: F811
    """Test the run endpoint (lens mismatch case 2)."""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    }

    data = {
        'request_id': '100',
        'body': {
            'action': 'produce',
            'topic': 'a_topic',
            'msgs': [
                {'content': 'hello world1'},
                {'content': 'hello world2'},
                {'content': 'hello world3'},
            ],
            'keys': [
                'key1',
                'key2',
            ],
        },
    }
    response = client.post('/run', json=data, headers=headers)
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response code: {response.status_code}')
    logger.info(f'Response data: {response_data}')

    assert response.status_code == ACCEPTED_STATUS_CODE
    assert (
        response_data['details']['error']
        == "The len of 'keys' (2) must match that of 'msgs' (3)."
    )


def test_run_endpoint_send_no_keys(client, access_token):  # noqa: F811
    """Test the run endpoint (send without keys)."""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    }

    data = {
        'request_id': '100',
        'body': {
            'action': 'produce',
            'topic': 'diaspora-cicd',
            'msgs': [
                {'content': 'hello world1'},
                {'content': 'hello world2'},
                {'content': 'hello world3'},
            ],
        },
    }
    response = client.post('/run', json=data, headers=headers)
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response code: {response.status_code}')
    logger.info(f'Response data: {response_data}')

    assert response.status_code == ACCEPTED_STATUS_CODE
    assert response_data['status'] == SUCCESS_STATUS_STRING


def test_run_endpoint_send_one_key(client, access_token):  # noqa: F811
    """Test the run endpoint (send under a single key)."""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    }

    data = {
        'request_id': '100',
        'body': {
            'action': 'produce',
            'topic': 'diaspora-cicd',
            'msgs': [
                {'content': 'hello world1'},
                {'content': 'hello world2'},
                {'content': 'hello world3'},
            ],
            'keys': 'single-key',
        },
    }
    response = client.post('/run', json=data, headers=headers)
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response code: {response.status_code}')
    logger.info(f'Response data: {response_data}')

    assert response.status_code == ACCEPTED_STATUS_CODE
    assert response_data['status'] == SUCCESS_STATUS_STRING


def test_run_endpoint_send_multiple_keys(client, access_token):  # noqa: F811
    """Test the run endpoint (send under a single key)."""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    }

    data = {
        'request_id': '100',
        'body': {
            'action': 'produce',
            'topic': 'diaspora-cicd',
            'msgs': [
                {'content': 'hello world1'},
                {'content': 'hello world2'},
                {'content': 'hello world3'},
            ],
            'keys': ['key1', 'key2', 'key3'],
        },
    }
    response = client.post('/run', json=data, headers=headers)
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response code: {response.status_code}')
    logger.info(f'Response data: {response_data}')

    assert response.status_code == ACCEPTED_STATUS_CODE
    assert response_data['status'] == SUCCESS_STATUS_STRING
