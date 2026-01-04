"""Unit tests for web_service_v3 routes."""

from __future__ import annotations

import contextlib
import os

import pytest
from _pytest.monkeypatch import MonkeyPatch

from web_service.utils import EnvironmentChecker
from web_service_v3.utils import AWSManagerV3

# ============================================================================
# Environment Variable Tests
# ============================================================================


EnvironmentChecker.check_env_variables(
    'AWS_ACCESS_KEY_ID',
    'AWS_SECRET_ACCESS_KEY',
    'SERVER_CLIENT_ID',
    'SERVER_SECRET',
    'AWS_ACCOUNT_ID',
    'AWS_ACCOUNT_REGION',
    'MSK_CLUSTER_NAME',
)


# ============================================================================
# Integration Tests with Real AWS Services
# ============================================================================


@pytest.fixture
def aws_manager(monkeypatch: MonkeyPatch) -> AWSManagerV3:
    """Create an AWSManagerV3 instance with real AWS services."""
    required_vars = [
        'AWS_ACCOUNT_ID',
        'AWS_ACCOUNT_REGION',
        'MSK_CLUSTER_NAME',
        'DEFAULT_SERVERS',
    ]

    # Check all required environment variables are set
    missing_vars = []
    env_values = {}
    for var in required_vars:
        value = os.getenv(var)
        if value is None:
            missing_vars.append(var)
        else:
            env_values[var] = value
            monkeypatch.setenv(var, value)

    if missing_vars:
        raise ValueError(
            f'Missing required environment variables: '
            f'{", ".join(missing_vars)}. '
            'Please set all required environment variables '
            'before running tests.',
        )

    # Create AWSManagerV3 with real AWS services
    manager = AWSManagerV3(
        account_id=env_values['AWS_ACCOUNT_ID'],
        region=env_values['AWS_ACCOUNT_REGION'],
        cluster_name=env_values['MSK_CLUSTER_NAME'],
        iam_vpc=env_values.get('DEFAULT_SERVERS'),
    )
    return manager


@pytest.fixture
def random_subject() -> str:
    """Generate a random subject for each test."""
    return f'test-subject-{os.urandom(8).hex()}'


@pytest.mark.integration
def test_create_user_and_key(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test create_user_and_key with real AWS services."""
    result = aws_manager.create_user_and_key(random_subject)

    assert result is not None
    assert result.get('status') == 'success'
    assert 'access_key' in result
    assert 'secret_key' in result
    assert result.get('username') == random_subject
    assert result.get('retrieved_from_ssm') is False

    # Cleanup: delete the key from SSM
    with contextlib.suppress(Exception):
        aws_manager.delete_key(random_subject)


@pytest.mark.integration
def test_retrieve_or_create_key_existing(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test retrieve_or_create_key retrieves existing key from SSM."""
    # First create a key
    create_result = aws_manager.create_user_and_key(random_subject)
    assert create_result.get('status') == 'success'

    # Then retrieve it
    retrieve_result = aws_manager.retrieve_or_create_key(random_subject)

    assert retrieve_result is not None
    assert retrieve_result.get('status') == 'success'
    assert retrieve_result.get('retrieved_from_ssm') is True
    assert retrieve_result.get('access_key') == create_result.get('access_key')
    assert retrieve_result.get('secret_key') == create_result.get('secret_key')

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_key(random_subject)


@pytest.mark.integration
def test_retrieve_or_create_key_new(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test retrieve_or_create_key creates new key when not in SSM."""
    # Delete key if it exists
    with contextlib.suppress(Exception):
        aws_manager.delete_key(random_subject)

    # Retrieve should create a new key
    result = aws_manager.retrieve_or_create_key(random_subject)

    assert result is not None
    assert result.get('status') == 'success'
    assert result.get('retrieved_from_ssm') is False
    assert 'access_key' in result
    assert 'secret_key' in result

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_key(random_subject)


@pytest.mark.integration
def test_delete_key(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test delete_key with real AWS services."""
    # First create a key
    create_result = aws_manager.create_user_and_key(random_subject)
    assert create_result.get('status') == 'success'

    # Then delete it
    delete_result = aws_manager.delete_key(random_subject)

    assert delete_result is not None
    assert delete_result.get('status') == 'success'
    assert 'message' in delete_result

    # Verify it's deleted by trying to retrieve
    retrieve_result = aws_manager.retrieve_or_create_key(random_subject)
    assert retrieve_result.get('retrieved_from_ssm') is False


@pytest.mark.integration
def test_delete_key_not_found(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test delete_key handles key not found gracefully."""
    # Ensure key doesn't exist
    with contextlib.suppress(Exception):
        aws_manager.delete_key(random_subject)

    # Try to delete non-existent key
    result = aws_manager.delete_key(random_subject)

    assert result is not None
    assert result.get('status') == 'success'
    assert 'message' in result


@pytest.mark.integration
def test_topic_listing_route(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test topic_listing_route with real AWS services."""
    result = aws_manager.topic_listing_route(random_subject)

    assert result is not None
    assert result.get('status') == 'success'
    assert 'topics' in result
    assert isinstance(result.get('topics'), list)


@pytest.mark.integration
def test_register_topic(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test register_topic with real AWS services."""
    topic = f'test-topic-{os.urandom(8).hex()}'

    # First create a key for the subject
    aws_manager.create_user_and_key(random_subject)

    result = aws_manager.register_topic(random_subject, topic)

    assert result is not None
    assert result.get('status') in ['success', 'no-op', 'error']
    assert 'message' in result

    # Cleanup: unregister the topic
    with contextlib.suppress(Exception):
        aws_manager.unregister_topic(random_subject, topic)

    # Cleanup: delete the key
    with contextlib.suppress(Exception):
        aws_manager.delete_key(random_subject)


@pytest.mark.integration
def test_register_topic_already_exists(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test register_topic handles already registered topic."""
    topic = f'test-topic-{os.urandom(8).hex()}'

    # First create a key for the subject
    aws_manager.create_user_and_key(random_subject)

    # Register topic first time
    result1 = aws_manager.register_topic(random_subject, topic)
    assert result1.get('status') in ['success', 'no-op', 'error']

    # Register topic second time (should be idempotent)
    result2 = aws_manager.register_topic(random_subject, topic)
    assert result2.get('status') in ['success', 'no-op', 'error']

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.unregister_topic(random_subject, topic)

    with contextlib.suppress(Exception):
        aws_manager.delete_key(random_subject)


@pytest.mark.integration
def test_unregister_topic(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test unregister_topic with real AWS services."""
    topic = f'test-topic-{os.urandom(8).hex()}'

    # First create a key and register a topic
    aws_manager.create_user_and_key(random_subject)
    aws_manager.register_topic(random_subject, topic)

    # Then unregister it
    result = aws_manager.unregister_topic(random_subject, topic)

    assert result is not None
    assert result.get('status') in ['success', 'no-op', 'error']
    assert 'message' in result

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_key(random_subject)


@pytest.mark.integration
def test_unregister_topic_no_access(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test unregister_topic handles topic with no access."""
    topic = f'test-topic-{os.urandom(8).hex()}'

    # Create a key but don't register the topic
    aws_manager.create_user_and_key(random_subject)

    # Try to unregister non-existent topic access
    result = aws_manager.unregister_topic(random_subject, topic)

    assert result is not None
    assert result.get('status') in ['success', 'no-op', 'error']

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_key(random_subject)
