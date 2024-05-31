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
FAILED_STATUS_STRING = 'FAILED'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

__version__ = importlib_metadata.version('diaspora_service')


def test_run_endpoint_bad_topic(client, access_token):  # noqa: F811
    """Test the run endpoint (bad topic)."""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    }

    data = {
        'request_id': '100',
        'body': {
            'action': 'consume',
            'topic': '__bad_topic',
        },
    }
    response = client.post('/run', json=data, headers=headers)
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response code: {response.status_code}')
    logger.info(f'Response data: {response_data}')

    assert response.status_code == ACCEPTED_STATUS_CODE
    assert response_data['status'] == FAILED_STATUS_STRING


def test_run_endpoint_from_curr_ts(client, access_token):  # noqa: F811
    """Test the run endpoint (from current ts)."""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    }

    data = {
        'request_id': '100',
        'body': {
            'action': 'consume',
            'topic': 'diaspora-cicd',
        },
    }
    response = client.post('/run', json=data, headers=headers)
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response code: {response.status_code}')
    logger.info(f'Response data: {response_data}')

    assert response.status_code == ACCEPTED_STATUS_CODE
    assert response_data['status'] == SUCCESS_STATUS_STRING

def test_run_endpoint_from_past_ts(client, access_token):  # noqa: F811
    """Test the run endpoint (from a past ts)."""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    }

    data = {
        'request_id': '100',
        'body': {
            'action': 'consume',
            'topic': 'diaspora-cicd',
            'ts': 1715930522000,
        },
    }
    response = client.post('/run', json=data, headers=headers)
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response code: {response.status_code}')
    logger.info(f'Response data: {response_data}')

    assert response.status_code == ACCEPTED_STATUS_CODE
    assert response_data['status'] == SUCCESS_STATUS_STRING
