from __future__ import annotations

import importlib.metadata as importlib_metadata
import json
import logging

from action_provider.utils import load_schema
from action_provider.utils import random_request_id
from testing.fixtures import access_token  # noqa: F401
from testing.fixtures import client  # noqa: F401

SUCCESS_STATUS_CODE = 200
NOT_FOUND_STATUS_CODE = 404
UNPROCESSABLE_STATUS_CODE = 422
SUCCESS_STATUS_STRING = 'SUCCEEDED'
FAILED_STATUS_STRING = 'FAILED'

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

    assert response.status_code == NOT_FOUND_STATUS_CODE
    # assert response_data['status'] == SUCCESS_STATUS_STRING


def test_endpoints_complex1(client, access_token):  # noqa: F811
    """Test multiple endpoints with an action id."""
    # First, send an request with error.
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    }

    req_id = random_request_id()
    logger.info(f'req_id: {req_id}')

    data = {
        'request_id': req_id,
        'body': {
            'action': 'consume',
            'topic': 'diaspora-cicd',
            'filters': [
                {
                    'Pattern': {
                        'value': {'content': [{'prefix': req_id}]},
                    },
                },
            ],
        },
    }
    response = client.post('/run', json=data, headers=headers)
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response code: {response.status_code}')
    logger.info(f'Response data: {response_data}')

    # status endpoint (should show active)
    action_id = response_data['action_id']
    headers = {
        'Authorization': f'Bearer {access_token}',
    }

    response = client.get(f'/{action_id}/status', headers=headers)
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response code: {response.status_code}')
    logger.info(f'Response data: {response_data}')

    # release endpoint (should fail because it does not complete)
    response = client.post(f'/{action_id}/release', headers=headers)
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response code: {response.status_code}')
    logger.info(f'Response data: {response_data}')

    # cancel endpoint
    response = client.post(f'/{action_id}/cancel', headers=headers)
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response code: {response.status_code}')
    logger.info(f'Response data: {response_data}')

    # cancel endpoint (should return FAILED)
    response = client.post(f'/{action_id}/cancel', headers=headers)
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response code: {response.status_code}')
    logger.info(f'Response data: {response_data}')

    # get status (should return FAILED)
    response = client.get(f'/{action_id}/status', headers=headers)
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response code123: {response.status_code}')
    logger.info(f'Response data123: {response_data}')

    # release endpoint
    response = client.post(f'/{action_id}/release', headers=headers)
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response code: {response.status_code}')
    logger.info(f'Response data: {response_data}')


def test_endpoints_complex2(client, access_token):  # noqa: F811
    """Test multiple endpoints with an action id."""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    }

    req_id = random_request_id()
    logger.info(f'req_id: {req_id}')

    data = {
        'request_id': req_id,
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

    # same request, already succeed, do nothing
    data = {
        'request_id': req_id,
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


def test_endpoints_complex3(client, access_token):  # noqa: F811
    """Test multiple endpoints with an action id."""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    }

    req_id = random_request_id()
    logger.info(f'req_id: {req_id}')

    data = {
        'request_id': req_id,
        'body': {
            'action': 'consume',
            'topic': 'diaspora-cicd',
            'filters': [  # a new param
                {
                    'Pattern': {
                        'value': {'content': [{'prefix': req_id}]},
                    },
                },
            ],
        },
    }
    response = client.post('/run', json=data, headers=headers)
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response code: {response.status_code}')
    logger.info(f'Response data: {response_data}')

    # same request, will return ACTIVE as well.
    data = {
        'request_id': req_id,
        'body': {
            'action': 'consume',
            'topic': 'diaspora-cicd',
            'filters': [  # a new param
                {
                    'Pattern': {
                        'value': {'content': [{'prefix': req_id}]},
                    },
                },
            ],
        },
    }
    response = client.post('/run', json=data, headers=headers)
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response code: {response.status_code}')
    logger.info(f'Response data: {response_data}')

    # a different request, raise ActionConflict
    data = {
        'request_id': req_id,
        'body': {
            'action': 'consume',
            'topic': 'diaspora-cicd',
        },
    }
    response = client.post('/run', json=data, headers=headers)
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response code: {response.status_code}')
    logger.info(f'Response data: {response_data}')


def test_cancel_endpoint(client, access_token):  # noqa: F811
    """Test the cancel endpoint."""
    action_id = 'my_action_id'
    headers = {
        'Authorization': f'Bearer {access_token}',
    }

    response = client.post(f'/{action_id}/cancel', headers=headers)
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response data: {response_data}')

    assert response.status_code == NOT_FOUND_STATUS_CODE
    # assert response_data['status'] == SUCCESS_STATUS_STRING


def test_release_endpoint(client, access_token):  # noqa: F811
    """Test the release endpoint."""
    action_id = 'my_action_id'
    headers = {
        'Authorization': f'Bearer {access_token}',
    }

    response = client.post(f'/{action_id}/release', headers=headers)
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response data: {response_data}')

    assert response.status_code == NOT_FOUND_STATUS_CODE
    # assert response_data['status'] == SUCCESS_STATUS_STRING


def test_run_endpoint_bad_action(client, access_token):  # noqa: F811
    """Test the run endpoint (bad action)."""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    }

    data = {
        'request_id': random_request_id(),
        'body': {
            'action': 'bad_action',
            'topic': 'a_topic',
        },
    }
    response = client.post('/run', json=data, headers=headers)
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response code: {response.status_code}')
    logger.info(f'Response data: {response_data}')

    assert response.status_code == UNPROCESSABLE_STATUS_CODE
    assert response_data['code'] == 'RequestValidationError'


def test_run_endpoint_bad_msgs(client, access_token):  # noqa: F811
    """Test the run endpoint (bad msgs)."""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    }

    data = {
        'request_id': random_request_id(),
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
