"""Globus related testing code for Diaspora Service."""

from __future__ import annotations

import os

from globus_sdk import ConfidentialAppAuthClient


def get_access_token() -> str:
    """Get an access token to SERVER_CLIENT_ID."""
    client_id = os.getenv('SERVER_CLIENT_ID')
    client_secret = os.getenv('SERVER_SECRET')
    requested_scopes = os.getenv('CLIENT_SCOPE')

    if not all([client_id, client_secret, requested_scopes]):
        raise ValueError(
            'Missing one or more environment variables: SERVER_CLIENT_ID, SERVER_SECRET, CLIENT_SCOPE'
        )

    ca = ConfidentialAppAuthClient(
        client_id=client_id,
        client_secret=client_secret,
    )
    token_response = ca.oauth2_client_credentials_tokens(
        requested_scopes=requested_scopes,
    )
    access_token = token_response.by_resource_server[client_id]['access_token']
    return access_token
