"""Diaspora Action Provider utilities.

This module contains utility functions and classes for handling AWS MSK tokens,
Kafka operations, and building action statuses.
"""

from __future__ import annotations

import datetime
import json
import os
from random import choice
from string import ascii_uppercase
from typing import Any

from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from globus_action_provider_tools import ActionRequest
from globus_action_provider_tools import ActionStatus
from globus_action_provider_tools import ActionStatusValue
from globus_action_provider_tools import AuthState


class MSKTokenProviderFromRole:
    """MSKTokenProviderFromRole."""

    def __init__(self, open_id: str) -> None:
        """MSKTokenProviderFromRole init."""
        self.open_id = open_id

    def token(self) -> str:
        """MSKTokenProviderFromRole token."""
        token, _ = MSKAuthTokenProvider.generate_auth_token_from_role_arn(
            'us-east-1',
            f'arn:aws:iam::845889416464:role/ap/{self.open_id}-role',
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
    status_value: ActionStatusValue | None = None,
    request: ActionRequest | None = None,
    result: dict[str, Any] | None = None,
) -> ActionStatus:
    """Build an ActionStatus object depending on whetherrequest is None."""
    if request is None:
        return ActionStatus(
            status=ActionStatusValue.SUCCEEDED,
            creator_id=auth.effective_identity,
            start_time=str(datetime.datetime.now().isoformat()),
            completion_time=str(datetime.datetime.now().isoformat()),
            release_after='P30D',
            display_status=ActionStatusValue.SUCCEEDED,
            details={'result': None},
        )
    else:
        return ActionStatus(
            status=status_value,
            creator_id=auth.effective_identity,
            monitor_by=request.monitor_by,
            manage_by=request.manage_by,
            start_time=str(datetime.datetime.now().isoformat()),
            completion_time=str(datetime.datetime.now().isoformat()),
            release_after=request.release_after or 'P30D',
            display_status=status_value,
            details=result,
        )
