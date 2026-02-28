"""Unit and integration tests for web_service FastAPI routes (main.py).

Unit tests use mocks to validate exact response formats.
Integration tests use real AWS services to verify
error conditions, input validation, and edge cases.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from web_service.main import DiasporaService
from web_service.main import extract_val

# ============================================================================
# Test Fixtures
# ============================================================================


@pytest.fixture
def mock_auth_manager() -> MagicMock:
    """Create a mock AuthManager for unit tests."""
    auth = MagicMock()
    auth.validate_access_token.return_value = None  # Success by default
    return auth


@pytest.fixture
def mock_web_service() -> MagicMock:
    """Create a mock WebService for unit tests."""
    web_service = MagicMock()
    return web_service


@pytest.fixture
def mock_diaspora_service(
    mock_auth_manager: MagicMock,
    mock_web_service: MagicMock,
) -> DiasporaService:
    """Create DiasporaService with mocked deps for unit tests."""
    with (
        patch(
            'web_service.main.AuthManager',
            return_value=mock_auth_manager,
        ),
        patch('web_service.main.IAMService'),
        patch('web_service.main.KafkaService'),
        patch('web_service.main.DynamoDBService'),
        patch('web_service.main.NamespaceService'),
        patch(
            'web_service.main.WebService',
            return_value=mock_web_service,
        ),
    ):
        service = DiasporaService()
        service.auth = mock_auth_manager
        service.web_service = mock_web_service
        return service


@pytest.fixture
def diaspora_service() -> DiasporaService:
    """Create a DiasporaService instance with real services."""
    # Ensure required environment variables are set
    required_vars = [
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY',
        'SERVER_CLIENT_ID',
        'SERVER_SECRET',
        'AWS_ACCOUNT_ID',
        'AWS_ACCOUNT_REGION',
        'MSK_CLUSTER_NAME',
    ]
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        pytest.skip(
            f'Missing required environment variables: {", ".join(missing)}',
        )

    # Override table names for testing
    os.environ['KEYS_TABLE_NAME'] = 'test-diaspora-keys-table'
    os.environ['USERS_TABLE_NAME'] = 'test-diaspora-users-table'
    os.environ['NAMESPACE_TABLE_NAME'] = 'test-diaspora-namespace-table'

    return DiasporaService()


@pytest.fixture
def valid_subject() -> str:
    """Generate a valid UUID subject."""
    return '123e4567-e89b-12d3-a456-426614174000'


@pytest.fixture
def valid_token() -> str:
    """Generate valid token format (requires real token)."""
    # Note: For real authentication, this would need a valid Globus token
    # For testing, we'll test both authenticated and unauthenticated paths
    return 'Bearer ' + 'a' * 50


# ============================================================================
# Tests for extract_val()
# ============================================================================


@pytest.mark.asyncio
async def test_extract_val_from_header(
    valid_subject: str,
) -> None:
    """Test extract_val extracts value from header."""
    extract_func = extract_val('subject')

    # Simulate header value
    result = await extract_func(
        header=valid_subject,
        body=None,
    )

    assert result == valid_subject


@pytest.mark.asyncio
async def test_extract_val_from_body(
    valid_subject: str,
) -> None:
    """Test extract_val extracts value from body."""
    extract_func = extract_val('subject')

    # Simulate body value (no header)
    result = await extract_func(
        header=None,
        body=valid_subject,
    )

    assert result == valid_subject


@pytest.mark.asyncio
async def test_extract_val_missing_value() -> None:
    """Test extract_val raises error when value is missing."""
    extract_func = extract_val('subject')

    # Both header and body are None
    # HTTPException is from fastapi, avoid importing it to prevent
    # Pydantic dependency issues. Catch Exception and check attributes.
    with pytest.raises(Exception) as exc_info:  # noqa: PT011
        await extract_func(
            header=None,
            body=None,
        )

    # Check it's an HTTPException-like exception with status_code 400
    # The exception should have status_code and detail attributes
    assert hasattr(exc_info.value, 'status_code')
    http_400_bad_request = 400
    assert exc_info.value.status_code == http_400_bad_request
    assert hasattr(exc_info.value, 'detail')
    assert 'subject' in str(exc_info.value.detail).lower()


@pytest.mark.asyncio
async def test_extract_val_prefers_header_over_body(
    valid_subject: str,
) -> None:
    """Test extract_val prefers header over body."""
    extract_func = extract_val('subject')

    header_subject = valid_subject
    body_subject = 'different-subject'

    # Should use header value
    result = await extract_func(
        header=header_subject,
        body=body_subject,
    )

    assert result == header_subject


# ============================================================================
# Unit Tests - Response Format Validation (Using Mocks)
# ============================================================================
# These tests use mocks to validate exact response formats without AWS services


@pytest.mark.asyncio
async def test_create_user_success_exact_format(
    mock_diaspora_service: DiasporaService,
    valid_subject: str,
    valid_token: str,
    mock_web_service: MagicMock,
) -> None:
    """Test create_user returns exact success response format."""
    expected_namespace = 'ns-123e4567e89b'
    mock_web_service.create_user.return_value = {
        'status': 'success',
        'message': (
            f'User {valid_subject} created with namespace {expected_namespace}'
        ),
        'subject': valid_subject,
        'namespace': expected_namespace,
    }

    result = await mock_diaspora_service.create_user(
        subject=valid_subject,
        token=valid_token,
    )

    # Validate exact response structure
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert result['subject'] == valid_subject
    assert result['namespace'] == expected_namespace
    assert 'message' in result
    assert valid_subject in result['message']
    assert expected_namespace in result['message']
    # Ensure no extra fields
    assert set(result.keys()) == {'status', 'message', 'subject', 'namespace'}


@pytest.mark.asyncio
async def test_create_user_authentication_failure_exact_format(
    mock_diaspora_service: DiasporaService,
    valid_subject: str,
    mock_auth_manager: MagicMock,
) -> None:
    """Test create_user returns exact authentication error format."""
    mock_auth_manager.validate_access_token.return_value = {
        'status': 'error',
        'reason': 'Invalid token: token expired',
    }

    result = await mock_diaspora_service.create_user(
        subject=valid_subject,
        token='Bearer invalid-token',
    )

    # Validate exact error response structure
    assert isinstance(result, dict)
    assert result['status'] == 'error'
    assert 'reason' in result
    assert isinstance(result['reason'], str)
    assert 'token' in result['reason'].lower()
    # Ensure web_service was not called
    web_svc = mock_diaspora_service.web_service
    if isinstance(web_svc, MagicMock):
        web_svc.create_user.assert_not_called()


@pytest.mark.asyncio
async def test_create_user_iam_failure_exact_format(
    mock_diaspora_service: DiasporaService,
    valid_subject: str,
    valid_token: str,
    mock_web_service: MagicMock,
) -> None:
    """Test create_user returns exact IAM failure format."""
    mock_web_service.create_user.return_value = {
        'status': 'failure',
        'message': 'Failed to create IAM user: User already exists',
    }

    result = await mock_diaspora_service.create_user(
        subject=valid_subject,
        token=valid_token,
    )

    # Validate exact failure response structure
    assert isinstance(result, dict)
    assert result['status'] == 'failure'
    assert 'message' in result
    assert 'IAM user' in result['message']
    assert 'Failed to create' in result['message']


@pytest.mark.asyncio
async def test_delete_user_success_exact_format(
    mock_diaspora_service: DiasporaService,
    valid_subject: str,
    valid_token: str,
    mock_web_service: MagicMock,
) -> None:
    """Test delete_user returns exact success response format."""
    mock_web_service.delete_user.return_value = {
        'status': 'success',
        'message': f'User {valid_subject} deleted',
    }

    result = await mock_diaspora_service.delete_user(
        subject=valid_subject,
        token=valid_token,
    )

    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'message' in result
    assert valid_subject in result['message']
    assert 'deleted' in result['message'].lower()
    assert set(result.keys()) == {'status', 'message'}


@pytest.mark.asyncio
async def test_create_key_success_exact_format(
    mock_diaspora_service: DiasporaService,
    valid_subject: str,
    valid_token: str,
    mock_web_service: MagicMock,
) -> None:
    """Test create_key returns exact success response format."""
    expected_access_key = 'AKIAIOSFODNN7EXAMPLE'  # pragma: allowlist secret
    expected_secret_key = (  # pragma: allowlist secret
        'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'  # pragma: allowlist secret  # noqa: E501
    )
    expected_create_date = '2024-01-01T00:00:00'
    expected_endpoint = 'bootstrap.kafka:9092'

    mock_web_service.create_key.return_value = {
        'status': 'success',
        'message': f'Access key created for {valid_subject}',
        'access_key': expected_access_key,
        'secret_key': expected_secret_key,
        'create_date': expected_create_date,
        'endpoint': expected_endpoint,
        'fresh': True,
    }

    result = await mock_diaspora_service.create_key(
        subject=valid_subject,
        token=valid_token,
    )

    # Validate exact response structure
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert result['access_key'] == expected_access_key
    assert result['secret_key'] == expected_secret_key
    assert result['create_date'] == expected_create_date
    assert result['endpoint'] == expected_endpoint
    assert result['fresh'] is True
    assert 'message' in result
    # Validate all required fields present
    required_fields = {
        'status',
        'message',
        'access_key',
        'secret_key',
        'create_date',
        'endpoint',
        'fresh',
    }
    assert set(result.keys()) == required_fields


@pytest.mark.asyncio
async def test_delete_key_success_exact_format(
    mock_diaspora_service: DiasporaService,
    valid_subject: str,
    valid_token: str,
    mock_web_service: MagicMock,
) -> None:
    """Test delete_key returns exact success response format."""
    mock_web_service.delete_key.return_value = {
        'status': 'success',
        'message': f'Access key deleted for {valid_subject}',
    }

    result = await mock_diaspora_service.delete_key(
        subject=valid_subject,
        token=valid_token,
    )

    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'message' in result
    assert 'deleted' in result['message'].lower()
    assert set(result.keys()) == {'status', 'message'}


@pytest.mark.asyncio
async def test_create_topic_success_exact_format(
    mock_diaspora_service: DiasporaService,
    valid_subject: str,
    valid_token: str,
    mock_web_service: MagicMock,
) -> None:
    """Test create_topic returns exact success response format."""
    namespace = 'ns-test123'
    topic = 'test-topic'
    mock_web_service.create_topic.return_value = {
        'status': 'success',
        'message': f'Topic {topic} created in {namespace}',
        'topics': [topic, 'other-topic'],
    }

    result = await mock_diaspora_service.create_topic(
        namespace=namespace,
        topic=topic,
        subject=valid_subject,
        token=valid_token,
    )

    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'message' in result
    assert topic in result['message']
    assert namespace in result['message']
    assert 'topics' in result
    assert isinstance(result['topics'], list)
    assert topic in result['topics']
    assert set(result.keys()) == {'status', 'message', 'topics'}


@pytest.mark.asyncio
async def test_delete_topic_success_exact_format(
    mock_diaspora_service: DiasporaService,
    valid_subject: str,
    valid_token: str,
    mock_web_service: MagicMock,
) -> None:
    """Test delete_topic returns exact success response format."""
    namespace = 'ns-test123'
    topic = 'test-topic'
    mock_web_service.delete_topic.return_value = {
        'status': 'success',
        'message': f'Topic {topic} deleted from {namespace}',
        'topics': ['remaining-topic'],
    }

    result = await mock_diaspora_service.delete_topic(
        namespace=namespace,
        topic=topic,
        subject=valid_subject,
        token=valid_token,
    )

    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'message' in result
    assert topic in result['message']
    assert namespace in result['message']
    assert 'topics' in result
    assert isinstance(result['topics'], list)
    assert set(result.keys()) == {'status', 'message', 'topics'}


@pytest.mark.asyncio
async def test_recreate_topic_success_exact_format(
    mock_diaspora_service: DiasporaService,
    valid_subject: str,
    valid_token: str,
    mock_web_service: MagicMock,
) -> None:
    """Test recreate_topic returns exact success response format."""
    namespace = 'ns-test123'
    topic = 'test-topic'
    mock_web_service.recreate_topic.return_value = {
        'status': 'success',
        'message': 'Topic recreated',
    }

    result = await mock_diaspora_service.recreate_topic(
        namespace=namespace,
        topic=topic,
        subject=valid_subject,
        token=valid_token,
    )

    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'message' in result
    assert 'recreated' in result['message'].lower()
    assert set(result.keys()) == {'status', 'message'}


@pytest.mark.asyncio
async def test_list_namespace_and_topics_success_exact_format(
    mock_diaspora_service: DiasporaService,
    valid_subject: str,
    valid_token: str,
    mock_web_service: MagicMock,
) -> None:
    """Test list_namespace_and_topics returns exact success response format."""
    namespace_service = mock_web_service.namespace_service
    namespace_service.list_namespace_and_topics.return_value = {
        'status': 'success',
        'message': f'Found 2 namespaces for {valid_subject}',
        'namespaces': {
            'ns-test1': ['topic1', 'topic2'],
            'ns-test2': ['topic3'],
        },
    }

    result = await mock_diaspora_service.list_namespace_and_topics(
        subject=valid_subject,
        token=valid_token,
    )

    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'message' in result
    assert 'namespaces' in result['message'].lower()
    assert 'namespaces' in result
    assert isinstance(result['namespaces'], dict)
    assert 'ns-test1' in result['namespaces']
    assert 'ns-test2' in result['namespaces']
    assert isinstance(result['namespaces']['ns-test1'], list)
    assert 'topic1' in result['namespaces']['ns-test1']
    assert set(result.keys()) == {'status', 'message', 'namespaces'}


# ============================================================================
# Integration Tests - Authentication Failures
# ============================================================================
# These tests use real services and verify authentication error handling


@pytest.mark.integration
@pytest.mark.asyncio
async def test_create_user_authentication_failure(
    diaspora_service: DiasporaService,
    valid_subject: str,
) -> None:
    """Test create_user with invalid token."""
    result = await diaspora_service.create_user(
        subject=valid_subject,
        token='Bearer invalid-token',
    )

    assert result['status'] == 'error'
    assert 'reason' in result


@pytest.mark.integration
@pytest.mark.asyncio
async def test_delete_user_authentication_failure(
    diaspora_service: DiasporaService,
    valid_subject: str,
) -> None:
    """Test delete_user with invalid token."""
    result = await diaspora_service.delete_user(
        subject=valid_subject,
        token='Bearer invalid-token',
    )

    assert result['status'] == 'error'
    assert 'reason' in result


@pytest.mark.integration
@pytest.mark.asyncio
async def test_create_key_authentication_failure(
    diaspora_service: DiasporaService,
    valid_subject: str,
) -> None:
    """Test create_key with invalid token."""
    result = await diaspora_service.create_key(
        subject=valid_subject,
        token='Bearer invalid-token',
    )

    assert result['status'] == 'error'


@pytest.mark.integration
@pytest.mark.integration
@pytest.mark.asyncio
async def test_delete_key_authentication_failure(
    diaspora_service: DiasporaService,
    valid_subject: str,
) -> None:
    """Test delete_key with invalid token."""
    result = await diaspora_service.delete_key(
        subject=valid_subject,
        token='Bearer invalid-token',
    )

    assert result['status'] == 'error'


@pytest.mark.integration
@pytest.mark.asyncio
async def test_create_topic_authentication_failure(
    diaspora_service: DiasporaService,
    valid_subject: str,
) -> None:
    """Test create_topic with invalid token."""
    result = await diaspora_service.create_topic(
        namespace='ns-test123',
        topic='test-topic',
        subject=valid_subject,
        token='Bearer invalid-token',
    )

    assert result['status'] == 'error'


@pytest.mark.integration
@pytest.mark.asyncio
async def test_delete_topic_authentication_failure(
    diaspora_service: DiasporaService,
    valid_subject: str,
) -> None:
    """Test delete_topic with invalid token."""
    result = await diaspora_service.delete_topic(
        namespace='ns-test123',
        topic='test-topic',
        subject=valid_subject,
        token='Bearer invalid-token',
    )

    assert result['status'] == 'error'


@pytest.mark.integration
@pytest.mark.asyncio
async def test_recreate_topic_authentication_failure(
    diaspora_service: DiasporaService,
    valid_subject: str,
) -> None:
    """Test recreate_topic with invalid token."""
    result = await diaspora_service.recreate_topic(
        namespace='ns-test123',
        topic='test-topic',
        subject=valid_subject,
        token='Bearer invalid-token',
    )

    assert result['status'] == 'error'


@pytest.mark.integration
@pytest.mark.asyncio
async def test_list_namespace_and_topics_authentication_failure(
    diaspora_service: DiasporaService,
    valid_subject: str,
) -> None:
    """Test list_namespace_and_topics with invalid token."""
    result = await diaspora_service.list_namespace_and_topics(
        subject=valid_subject,
        token='Bearer invalid-token',
    )

    assert result['status'] == 'error'
