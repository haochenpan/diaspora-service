"""Diaspora Action Provider utilities.

This module contains utility functions and classes for handling AWS MSK tokens,
Kafka operations, and building action statuses.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from datetime import timedelta
from random import choice
from string import ascii_uppercase
from typing import Any

import boto3
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from botocore.exceptions import ClientError
from globus_action_provider_tools import ActionRequest
from globus_action_provider_tools import ActionStatus
from globus_action_provider_tools import ActionStatusValue
from globus_action_provider_tools import AuthState
from globus_action_provider_tools.flask.exceptions import ActionNotFound

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
request_table = dynamodb.Table('diaspora-request')
action_table = dynamodb.Table('diaspora-action')

DELETE_SUCCEED = 200


class MSKTokenProviderFromRole:
    """MSKTokenProviderFromRole."""

    def __init__(self, open_id: str) -> None:  # pragma: no cover
        """MSKTokenProviderFromRole init."""
        if open_id == '2b9d2f5c-fa32-45b5-875b-b24cd343b917':
            open_id = 'diaspora-cicd'
        self.open_id = open_id

    def token(self) -> str:
        """MSKTokenProviderFromRole token."""
        token, _ = MSKAuthTokenProvider.generate_auth_token_from_role_arn(
            'us-east-1',
            f'arn:aws:iam::{os.getenv("AWS_ACCOUNT_ID")}:role/ap/{self.open_id}-role',
        )
        return token


def random_request_id() -> str:
    """Get a random request ID."""
    return str(''.join(choice(ascii_uppercase) for i in range(12)))


def load_schema() -> dict[str, Any]:
    """Load Event Schema."""
    with open(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'schema.json',
        ),
    ) as f:
        schema = json.load(f)
    return schema


def build_action_status(
    auth: AuthState,
    status_value: ActionStatusValue,
    request: ActionRequest,
    result: dict[str, Any] | None = None,
) -> ActionStatus:
    """Build an ActionStatus object."""
    return ActionStatus(
        status=status_value,
        creator_id=auth.effective_identity,
        monitor_by=request.monitor_by,
        manage_by=request.manage_by,
        start_time=str(datetime.now().isoformat()),
        completion_time=str(datetime.now().isoformat()),
        release_after=request.release_after or 'P30D',
        display_status=status_value,
        details=result,
    )


def _get_ttl_for_one_year() -> int:
    one_year_from_now = datetime.now() + timedelta(days=365)
    return int(one_year_from_now.timestamp())


def _insert_into_request_table(
    full_request_id: str,
    request: ActionRequest,
    action_id: str,
) -> None:
    try:
        request_table.put_item(
            Item={
                'request_id': full_request_id,
                'request': request.json(),
                'action_id': action_id,
                'ttl': _get_ttl_for_one_year(),
            },
        )
    except ClientError as e:  # pragma: no cover
        print(f'Failed to insert: {e.response["Error"]["Message"]}')


def _insert_into_action_table(
    action_status: ActionStatus,
    request: ActionRequest,
) -> None:
    try:
        action_table.put_item(
            Item={
                'action_id': action_status.action_id,
                'action_status': action_status.json(),
                'request': request.json(),  # to rerun
                'ttl': _get_ttl_for_one_year(),
            },
        )
    except ClientError as e:  # pragma: no cover
        print(f'Failed to insert: {e.response["Error"]["Message"]}')


def _get_request_from_dynamo(full_request_id: str) -> dict[str, str] | None:
    try:
        response = request_table.get_item(Key={'request_id': full_request_id})
        if 'Item' in response:
            item = response['Item']
            return item
        else:  # pragma: no cover
            return None
    except ClientError as e:  # pragma: no cover
        print(
            f'Failed to retrieve: {e.response["Error"]["Message"]}',
        )
        return None


def _get_action_from_dynamo(action_id: str) -> dict[str, str] | None:
    try:
        response = action_table.get_item(Key={'action_id': action_id})
        if 'Item' in response:
            item = response['Item']
            return item
        else:  # pragma: no cover
            return None
    except ClientError as e:  # pragma: no cover
        print(
            f'Failed to retrieve: {e.response["Error"]["Message"]}',
        )
        return None


def _get_status_request(action_id: str) -> tuple[ActionStatus, ActionRequest]:
    prev_action = _get_action_from_dynamo(action_id)
    if not prev_action:
        raise ActionNotFound(
            f'No Action with id {action_id}',
        )
    print('prev_action_status', prev_action)
    prev_status_dict = json.loads(prev_action['action_status'])
    prev_request_dict = json.loads(prev_action['request'])
    return ActionStatus(**prev_status_dict), ActionRequest(**prev_request_dict)


def _delete_request(full_request_id: str) -> None:
    try:
        response = request_table.delete_item(
            Key={'request_id': full_request_id},
        )
        code = response.get('ResponseMetadata', {}).get('HTTPStatusCode')
        if code == DELETE_SUCCEED:
            pass
        else:  # pragma: no cover
            print(f'Failed to delete request {full_request_id}: {response}')
    except ClientError as e:  # pragma: no cover
        print(f'Failed to delete request: {e.response["Error"]["Message"]}')


def _delete_action(action_id: str) -> None:
    try:
        response = action_table.delete_item(
            Key={'action_id': action_id},
        )
        code = response.get('ResponseMetadata', {}).get('HTTPStatusCode')
        if code == DELETE_SUCCEED:
            pass
        else:  # pragma: no cover
            print(f'Failed to delete action {action_id}: {response}')
    except ClientError as e:  # pragma: no cover
        print(f'Failed to delete action: {e.response["Error"]["Message"]}')


def setup_logging(
    logger_name: str = __name__,
    log_file: str = 'log_output.log',
    level: int = logging.DEBUG,
) -> logging.Logger:
    """Set up logging for the application."""
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)

    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(level)

    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    )
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Add handlers
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    # Set levels for external libraries
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)
    logging.getLogger('fsevents').setLevel(logging.WARNING)
    logging.getLogger('globus_action_provider_tools').setLevel(logging.WARNING)
    logging.getLogger('globus_sdk').setLevel(logging.WARNING)
    logging.getLogger('kafka').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('werkzeug').setLevel(logging.WARNING)

    return logger
