"""Fixtures for Diaspora Service."""

from __future__ import annotations

import pytest

from action_provider.main import create_app
from testing.globus import get_access_token


@pytest.fixture(scope='module')
def client():
    """Create the Flask service."""
    app = create_app()
    with app.test_client() as client:
        yield client


@pytest.fixture(scope='module')
def access_token():
    """Retrieve the access token."""
    return get_access_token()
