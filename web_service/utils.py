"""Utility classes for the web service."""

from __future__ import annotations

import logging
import os
import re
import uuid
from typing import Any

from globus_sdk import ConfidentialAppAuthClient

logger = logging.getLogger(__name__)


class EnvironmentChecker:
    """Check if environment variables are set."""

    @staticmethod
    def check_env_variables(*variables: str) -> None:
        """Check if each environment variable in vars is set."""
        for var in variables:
            assert os.getenv(var), f'{var} environment variable is not set.'


class AuthManager:
    """Manage authentication and validation of access tokens."""

    TOKEN_MIN = 32
    TOKEN_MAX = 255

    def __init__(
        self,
        server_client_id: str | None,
        server_secret: str | None,
        client_id: str,
    ) -> None:
        """Initialize the AuthManager with client credentials."""
        if server_client_id is None or server_secret is None:
            raise ValueError(
                'SERVER_CLIENT_ID and SERVER_SECRET must be set.',
            )
        self.client = ConfidentialAppAuthClient(
            server_client_id,
            server_secret,
        )
        self.action_scope = (
            f'https://auth.globus.org/scopes/{server_client_id}/action_all'
        )
        self.scopes = f'openid email profile {self.action_scope}'
        self.client_id = client_id  # SDK's internal_auth_client
        self.server_client_id = server_client_id

    def validate_access_token(  # noqa: PLR0911
        self,
        user_id: str,
        token: str,
    ) -> dict[str, Any] | None:
        """Validate the access token for the given user ID."""
        if not self.is_uuid(user_id):
            return self.error_response('Invalid user ID; expected UUID.')

        if not token.startswith('Bearer '):
            return self.error_response(
                'Auth token must start with "Bearer ".',
            )

        token = token.split(' ')[1]  # Extract the token part
        if not self.is_valid_token(token):
            return self.error_response('Invalid token format.')

        introspection_response = self.client.oauth2_token_introspect(token)
        if not introspection_response.get('active'):
            return self.error_response('Token is inactive.')

        if introspection_response.get('sub') != user_id:
            return self.error_response(
                'Token does not belong to the user.',
            )

        if self.server_client_id not in introspection_response.get('aud'):
            logger.info(
                'Token audience %s lacks the service',
                introspection_response.get('aud'),
            )
            return self.error_response('Not in the audience set.')

        if self.action_scope not in introspection_response.get(
            'scope',
            '',
        ):
            return self.error_response('Token lacks the scope for action.')

        # If all checks pass, return None to indicate success
        return None

    @staticmethod
    def error_response(reason: Any) -> dict[str, Any]:
        """Return an error response with the given reason."""
        return {'status': 'error', 'reason': reason}

    @staticmethod
    def is_uuid(value: str) -> bool:
        """Check if the given value is a valid UUID."""
        try:
            uuid.UUID(value)
        except ValueError:
            return False
        else:
            return True

    @staticmethod
    def is_valid_token(token: str) -> bool:
        """Check if the given token is valid."""
        return (
            len(token) > AuthManager.TOKEN_MIN
            and len(token) < AuthManager.TOKEN_MAX
            and re.match(r'^[\w]+$', token) is not None
        )
