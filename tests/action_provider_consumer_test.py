from __future__ import annotations

import importlib.metadata as importlib_metadata
import json
import logging

from kafka import TopicPartition

from testing.fixtures import access_token  # noqa: F401
from testing.fixtures import client  # noqa: F401

ACCEPTED_STATUS_CODE = 202
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


def test_run_endpoint_no_records(client, access_token, mocker):  # noqa: F811
    """Test the run endpoint when no records are returned."""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    }

    # Mock the KafkaConsumer poll method to return no records
    mocker.patch('kafka.KafkaConsumer.poll', return_value={})
    # Mock the KafkaConsumer offsets_for_times method to return offsets with None
    mocker.patch('kafka.KafkaConsumer.offsets_for_times', return_value={})

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
    assert response_data['details'] == {}  # Ensure no messages are present

    # Additional test for when offsets return None
    mocker.patch(
        'kafka.KafkaConsumer.offsets_for_times',
        return_value={TopicPartition('diaspora-cicd', 0): None},
    )

    response = client.post('/run', json=data, headers=headers)
    response_data = json.loads(response.data.decode('utf-8'))

    logger.info(f'Additional Response code: {response.status_code}')
    logger.info(f'Additional Response data: {response_data}')

    assert response.status_code == ACCEPTED_STATUS_CODE
    assert response_data['status'] == SUCCESS_STATUS_STRING
    assert response_data['details'] == {}  # Ensure no messages are present


def test_run_endpoint_empty_topic(client, access_token, mocker):  # noqa: F811
    """Test the run endpoint with an empty topic."""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    }

    # Mock the KafkaConsumer to have an empty topic
    mocker.patch('kafka.KafkaConsumer.partitions_for_topic', return_value=[])

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
    assert response_data['status'] == FAILED_STATUS_STRING
