from __future__ import annotations

import json
import logging
import time

from testing.fixtures import access_token  # noqa: F401
from testing.fixtures import client  # noqa: F401

ACCEPTED_STATUS_CODE = 202
SUCCESS_STATUS_STRING = 'SUCCEEDED'
FAILED_STATUS_STRING = 'FAILED'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_current_time_ms_minus_one_hour():
    # Get the current time in milliseconds
    current_time_ms = int(time.time() * 1000)
    # Subtract one day in milliseconds
    one_day_ms = 1 * 60 * 60 * 1000
    return current_time_ms - one_day_ms


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


def test_run_endpoint_consume_with_non_filter(
    client,
    access_token,
):  # noqa: F811
    """Test the run endpoint (consume with prefix and hellow world 1)."""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    }

    data = {
        'request_id': '101',
        'body': {
            'action': 'consume',
            'topic': 'diaspora-cicd',
            'ts': get_current_time_ms_minus_one_hour(),
            'filters': [  # bad pattern does not have effect
                {
                    'BadPattern': {
                        'value': {'content': [{'prefix': 'hello world1'}]},
                    },
                },
            ],
        },
    }
    response = client.post('/run', json=data, headers=headers)
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response code: {response.status_code}')
    logger.info(f'Response data: {response_data}')

    assert response.status_code == ACCEPTED_STATUS_CODE
    assert response_data['status'] == SUCCESS_STATUS_STRING


def test_run_endpoint_consume_with_suffix_and_hello_world1(
    client,
    access_token,
):  # noqa: F811
    """Test the run endpoint (consume with prefix and hellow world 1)."""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    }

    data = {
        'request_id': '101',
        'body': {
            'action': 'consume',
            'topic': 'diaspora-cicd',
            'ts': get_current_time_ms_minus_one_hour(),
            'filters': [
                {
                    'Pattern': {
                        'value': {'content': [{'prefix': 'hello world1'}]},
                    },
                },
            ],
        },
    }
    response = client.post('/run', json=data, headers=headers)
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response code: {response.status_code}')
    logger.info(f'Response data: {response_data}')

    assert response.status_code == ACCEPTED_STATUS_CODE
    assert response_data['status'] == SUCCESS_STATUS_STRING

    details = response_data.get('details', {})
    print(details, len(details))
    logger.info(f'Response data: {details}')
    logger.info(f'Response data: {len(details)}')

    for messages in details.values():
        assert all(
            message['value']['content'] != 'hello world2'
            and message['value']['content'] != 'hello world3'
            for message in messages
        )


def test_run_endpoint_consume_with_suffix_and_hello_world2(
    client,
    access_token,
):  # noqa: F811
    """Test the run endpoint (consume with suffix and hello world2)."""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    }

    data = {
        'request_id': '104',
        'body': {
            'action': 'consume',
            'topic': 'diaspora-cicd',
            'ts': get_current_time_ms_minus_one_hour(),
            'filters': [
                {
                    'Pattern': {
                        'value': {'content': [{'suffix': 'world2'}]},
                    },
                },
            ],
        },
    }
    response = client.post('/run', json=data, headers=headers)
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response code: {response.status_code}')
    logger.info(f'Response data: {response_data}')

    assert response.status_code == ACCEPTED_STATUS_CODE
    assert response_data['status'] == SUCCESS_STATUS_STRING

    details = response_data.get('details', {})
    print(details, len(details))
    logger.info(f'Response data: {details}')
    logger.info(f'Response data: {len(details)}')

    for messages in details.values():
        assert all(
            message['value']['content'] != 'hello world1'
            and message['value']['content'] != 'hello world3'
            for message in messages
        )


def test_run_endpoint_consume_with_suffix_and_hello_world3(
    client,
    access_token,
):  # noqa: F811
    """Test the run endpoint (consume with suffix and hello world3)."""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    }

    data = {
        'request_id': '104',
        'body': {
            'action': 'consume',
            'topic': 'diaspora-cicd',
            'ts': get_current_time_ms_minus_one_hour(),
            'filters': [
                {
                    'Pattern': {
                        'value': {'content': [{'suffix': 'world3'}]},
                    },
                },
                {
                    'Pattern': {
                        'value': {'content': [{'suffix': 'world3'}]},
                    },
                },
            ],
        },
    }
    response = client.post('/run', json=data, headers=headers)
    response_data = json.loads(response.data.decode('utf-8'))
    logger.info(f'Response code: {response.status_code}')
    logger.info(f'Response data: {response_data}')

    assert response.status_code == ACCEPTED_STATUS_CODE
    assert response_data['status'] == SUCCESS_STATUS_STRING

    details = response_data.get('details', {})
    print(details, len(details))
    logger.info(f'Response data: {details}')
    logger.info(f'Response data: {len(details)}')

    for messages in details.values():
        assert all(
            message['value']['content'] != 'hello world1'
            and message['value']['content'] != 'hello world2'
            for message in messages
        )
