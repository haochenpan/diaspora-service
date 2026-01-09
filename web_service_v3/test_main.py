"""Comprehensive unit tests for web_service_v3 FastAPI routes (main.py).

Tests validate exact response formats, error conditions, input validation,
and edge cases.
"""

from __future__ import annotations

from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from web_service_v3.main import DiasporaService
from web_service_v3.main import extract_val

# ============================================================================
# Test Fixtures
# ============================================================================


@pytest.fixture
def mock_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set up mock environment variables."""
    env_vars = {
        'AWS_ACCESS_KEY_ID': 'test-access-key',  # pragma: allowlist secret
        'AWS_SECRET_ACCESS_KEY': 'test-secret-key',  # pragma: allowlist secret
        'SERVER_CLIENT_ID': 'test-client-id',  # pragma: allowlist secret
        'SERVER_SECRET': 'test-server-secret',  # pragma: allowlist secret
        'AWS_ACCOUNT_ID': '123456789012',
        'AWS_ACCOUNT_REGION': 'us-east-1',
        'MSK_CLUSTER_NAME': 'test-cluster',
        'DEFAULT_SERVERS': 'test-servers',
        'KEYS_TABLE_NAME': 'test-diaspora-keys-table',
        'USERS_TABLE_NAME': 'test-diaspora-users-table',
        'NAMESPACE_TABLE_NAME': 'test-diaspora-namespace-table',
    }
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)


@pytest.fixture
def mock_auth_manager() -> MagicMock:
    """Create a mock AuthManager."""
    auth = MagicMock()
    auth.validate_access_token.return_value = None  # Success by default
    return auth


@pytest.fixture
def mock_web_service() -> MagicMock:
    """Create a mock WebService."""
    # Use MagicMock without spec to avoid import issues
    web_service = MagicMock()
    return web_service


@pytest.fixture
def diaspora_service(
    mock_env_vars: None,
    mock_auth_manager: MagicMock,
    mock_web_service: MagicMock,
) -> DiasporaService:
    """Create a DiasporaService instance with mocked dependencies."""
    with (
        patch(
            'web_service_v3.main.AuthManager',
            return_value=mock_auth_manager,
        ),
        patch('web_service_v3.main.IAMService'),
        patch('web_service_v3.main.KafkaService'),
        patch('web_service_v3.main.DynamoDBService'),
        patch('web_service_v3.main.NamespaceService'),
        patch(
            'web_service_v3.main.WebService',
            return_value=mock_web_service,
        ),
    ):
        service = DiasporaService()
        service.auth = mock_auth_manager
        service.web_service = mock_web_service
        return service


@pytest.fixture
def valid_subject() -> str:
    """Generate a valid UUID subject."""
    return '123e4567-e89b-12d3-a456-426614174000'


@pytest.fixture
def valid_token() -> str:
    """Generate a valid token."""
    return 'Bearer ' + 'a' * 50  # Valid token format


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
# Tests for create_user()
# ============================================================================


@pytest.mark.asyncio
async def test_create_user_success(
    diaspora_service: DiasporaService,
    valid_subject: str,
    valid_token: str,
    mock_auth_manager: MagicMock,
    mock_web_service: MagicMock,
) -> None:
    """Test create_user success path."""
    mock_web_service.create_user.return_value = {
        'status': 'success',
        'message': 'User created',
        'subject': valid_subject,
        'namespace': 'ns-test123',
    }

    result = await diaspora_service.create_user(
        subject=valid_subject,
        token=valid_token,
    )

    assert result['status'] == 'success'
    assert result['subject'] == valid_subject
    assert 'namespace' in result
    mock_auth_manager.validate_access_token.assert_called_once_with(
        valid_subject,
        valid_token,
    )
    mock_web_service.create_user.assert_called_once_with(valid_subject)


@pytest.mark.asyncio
async def test_create_user_authentication_failure(
    diaspora_service: DiasporaService,
    valid_subject: str,
    mock_auth_manager: MagicMock,
) -> None:
    """Test create_user with authentication failure."""
    mock_auth_manager.validate_access_token.return_value = {
        'status': 'error',
        'reason': 'Invalid token',
    }

    result = await diaspora_service.create_user(
        subject=valid_subject,
        token='Bearer invalid-token',
    )

    assert result['status'] == 'error'
    assert 'reason' in result
    # Verify web_service.create_user was not called due to auth failure
    web_svc = diaspora_service.web_service
    if isinstance(web_svc, MagicMock):
        web_svc.create_user.assert_not_called()


@pytest.mark.asyncio
async def test_create_user_service_failure(
    diaspora_service: DiasporaService,
    valid_subject: str,
    valid_token: str,
    mock_auth_manager: MagicMock,
    mock_web_service: MagicMock,
) -> None:
    """Test create_user with service failure."""
    mock_web_service.create_user.return_value = {
        'status': 'failure',
        'message': 'Failed to create IAM user',
    }

    result = await diaspora_service.create_user(
        subject=valid_subject,
        token=valid_token,
    )

    assert result['status'] == 'failure'
    assert 'message' in result


# ============================================================================
# Tests for delete_user()
# ============================================================================


@pytest.mark.asyncio
async def test_delete_user_success(
    diaspora_service: DiasporaService,
    valid_subject: str,
    valid_token: str,
    mock_auth_manager: MagicMock,
    mock_web_service: MagicMock,
) -> None:
    """Test delete_user success path."""
    mock_web_service.delete_user.return_value = {
        'status': 'success',
        'message': f'User {valid_subject} deleted',
    }

    result = await diaspora_service.delete_user(
        subject=valid_subject,
        token=valid_token,
    )

    assert result['status'] == 'success'
    mock_web_service.delete_user.assert_called_once_with(valid_subject)


@pytest.mark.asyncio
async def test_delete_user_authentication_failure(
    diaspora_service: DiasporaService,
    valid_subject: str,
    mock_auth_manager: MagicMock,
) -> None:
    """Test delete_user with authentication failure."""
    mock_auth_manager.validate_access_token.return_value = {
        'status': 'error',
        'reason': 'Token expired',
    }

    result = await diaspora_service.delete_user(
        subject=valid_subject,
        token='Bearer expired-token',
    )

    assert result['status'] == 'error'
    # Verify web_service.delete_user was not called due to auth failure
    web_svc = diaspora_service.web_service
    if isinstance(web_svc, MagicMock):
        web_svc.delete_user.assert_not_called()


# ============================================================================
# Tests for create_key()
# ============================================================================


@pytest.mark.asyncio
async def test_create_key_success(
    diaspora_service: DiasporaService,
    valid_subject: str,
    valid_token: str,
    mock_auth_manager: MagicMock,
    mock_web_service: MagicMock,
) -> None:
    """Test create_key success path."""
    mock_web_service.create_key.return_value = {
        'status': 'success',
        'message': 'Access key created',
        'access_key': 'AKIAIOSFODNN7EXAMPLE',  # pragma: allowlist secret
        'secret_key': (  # pragma: allowlist secret
            'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'  # pragma: allowlist secret  # noqa: E501
        ),
        'create_date': '2024-01-01T00:00:00',
    }

    result = await diaspora_service.create_key(
        subject=valid_subject,
        token=valid_token,
    )

    assert result['status'] == 'success'
    assert 'access_key' in result
    assert 'secret_key' in result
    mock_web_service.create_key.assert_called_once_with(valid_subject)


@pytest.mark.asyncio
async def test_create_key_authentication_failure(
    diaspora_service: DiasporaService,
    valid_subject: str,
    mock_auth_manager: MagicMock,
) -> None:
    """Test create_key with authentication failure."""
    mock_auth_manager.validate_access_token.return_value = {
        'status': 'error',
        'reason': 'Invalid token',
    }

    result = await diaspora_service.create_key(
        subject=valid_subject,
        token='Bearer invalid-token',
    )

    assert result['status'] == 'error'


# ============================================================================
# Tests for get_key()
# ============================================================================


@pytest.mark.asyncio
async def test_get_key_success(
    diaspora_service: DiasporaService,
    valid_subject: str,
    valid_token: str,
    mock_auth_manager: MagicMock,
    mock_web_service: MagicMock,
) -> None:
    """Test get_key success path."""
    mock_web_service.get_key.return_value = {
        'status': 'success',
        'message': 'Access key retrieved',
        'access_key': 'AKIAIOSFODNN7EXAMPLE',  # pragma: allowlist secret
        'secret_key': (  # pragma: allowlist secret
            'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'  # pragma: allowlist secret  # noqa: E501
        ),
        'create_date': '2024-01-01T00:00:00',
        'fresh': False,
    }

    result = await diaspora_service.get_key(
        subject=valid_subject,
        token=valid_token,
    )

    assert result['status'] == 'success'
    assert 'access_key' in result
    mock_web_service.get_key.assert_called_once_with(valid_subject)


# ============================================================================
# Tests for delete_key()
# ============================================================================


@pytest.mark.asyncio
async def test_delete_key_success(
    diaspora_service: DiasporaService,
    valid_subject: str,
    valid_token: str,
    mock_auth_manager: MagicMock,
    mock_web_service: MagicMock,
) -> None:
    """Test delete_key success path."""
    mock_web_service.delete_key.return_value = {
        'status': 'success',
        'message': 'Access key deleted',
    }

    result = await diaspora_service.delete_key(
        subject=valid_subject,
        token=valid_token,
    )

    assert result['status'] == 'success'
    mock_web_service.delete_key.assert_called_once_with(valid_subject)


# ============================================================================
# Tests for create_topic()
# ============================================================================


@pytest.mark.asyncio
async def test_create_topic_success(
    diaspora_service: DiasporaService,
    valid_subject: str,
    valid_token: str,
    mock_auth_manager: MagicMock,
    mock_web_service: MagicMock,
) -> None:
    """Test create_topic success path."""
    namespace = 'ns-test123'
    topic = 'test-topic'
    mock_web_service.create_topic.return_value = {
        'status': 'success',
        'message': f'Topic {topic} created in {namespace}',
        'topics': [topic],
    }

    result = await diaspora_service.create_topic(
        namespace=namespace,
        topic=topic,
        subject=valid_subject,
        token=valid_token,
    )

    assert result['status'] == 'success'
    mock_web_service.create_topic.assert_called_once_with(
        valid_subject,
        namespace,
        topic,
    )


@pytest.mark.asyncio
async def test_create_topic_authentication_failure(
    diaspora_service: DiasporaService,
    valid_subject: str,
    mock_auth_manager: MagicMock,
) -> None:
    """Test create_topic with authentication failure."""
    mock_auth_manager.validate_access_token.return_value = {
        'status': 'error',
        'reason': 'Invalid token',
    }

    result = await diaspora_service.create_topic(
        namespace='ns-test123',
        topic='test-topic',
        subject=valid_subject,
        token='Bearer invalid-token',
    )

    assert result['status'] == 'error'


# ============================================================================
# Tests for delete_topic()
# ============================================================================


@pytest.mark.asyncio
async def test_delete_topic_success(
    diaspora_service: DiasporaService,
    valid_subject: str,
    valid_token: str,
    mock_auth_manager: MagicMock,
    mock_web_service: MagicMock,
) -> None:
    """Test delete_topic success path."""
    namespace = 'ns-test123'
    topic = 'test-topic'
    mock_web_service.delete_topic.return_value = {
        'status': 'success',
        'message': f'Topic {topic} deleted from {namespace}',
        'topics': [],
    }

    result = await diaspora_service.delete_topic(
        namespace=namespace,
        topic=topic,
        subject=valid_subject,
        token=valid_token,
    )

    assert result['status'] == 'success'
    mock_web_service.delete_topic.assert_called_once_with(
        valid_subject,
        namespace,
        topic,
    )


# ============================================================================
# Tests for recreate_topic()
# ============================================================================


@pytest.mark.asyncio
async def test_recreate_topic_success(
    diaspora_service: DiasporaService,
    valid_subject: str,
    valid_token: str,
    mock_auth_manager: MagicMock,
    mock_web_service: MagicMock,
) -> None:
    """Test recreate_topic success path."""
    namespace = 'ns-test123'
    topic = 'test-topic'
    mock_web_service.recreate_topic.return_value = {
        'status': 'success',
        'message': 'Topic recreated',
    }

    result = await diaspora_service.recreate_topic(
        namespace=namespace,
        topic=topic,
        subject=valid_subject,
        token=valid_token,
    )

    assert result['status'] == 'success'
    mock_web_service.recreate_topic.assert_called_once_with(
        valid_subject,
        namespace,
        topic,
    )


@pytest.mark.asyncio
async def test_recreate_topic_failure(
    diaspora_service: DiasporaService,
    valid_subject: str,
    valid_token: str,
    mock_auth_manager: MagicMock,
    mock_web_service: MagicMock,
) -> None:
    """Test recreate_topic failure path."""
    namespace = 'ns-test123'
    topic = 'test-topic'
    mock_web_service.recreate_topic.return_value = {
        'status': 'failure',
        'message': 'Topic not found',
    }

    result = await diaspora_service.recreate_topic(
        namespace=namespace,
        topic=topic,
        subject=valid_subject,
        token=valid_token,
    )

    assert result['status'] == 'failure'


# ============================================================================
# Tests for list_namespace_and_topics()
# ============================================================================


@pytest.mark.asyncio
async def test_list_namespace_and_topics_success(
    diaspora_service: DiasporaService,
    valid_subject: str,
    valid_token: str,
    mock_auth_manager: MagicMock,
    mock_web_service: MagicMock,
) -> None:
    """Test list_namespace_and_topics success path."""
    namespace_service = mock_web_service.namespace_service
    namespace_service.list_namespace_and_topics.return_value = {
        'status': 'success',
        'message': 'Found 1 namespaces',
        'namespaces': {
            'ns-test123': ['topic1', 'topic2'],
        },
    }

    result = await diaspora_service.list_namespace_and_topics(
        subject=valid_subject,
        token=valid_token,
    )

    assert result['status'] == 'success'
    assert 'namespaces' in result
    namespace_service = mock_web_service.namespace_service
    namespace_service.list_namespace_and_topics.assert_called_once_with(
        valid_subject,
    )


@pytest.mark.asyncio
async def test_list_namespace_and_topics_empty(
    diaspora_service: DiasporaService,
    valid_subject: str,
    valid_token: str,
    mock_auth_manager: MagicMock,
    mock_web_service: MagicMock,
) -> None:
    """Test list_namespace_and_topics with no namespaces."""
    namespace_service = mock_web_service.namespace_service
    namespace_service.list_namespace_and_topics.return_value = {
        'status': 'success',
        'message': 'Found 0 namespaces',
        'namespaces': {},
    }

    result = await diaspora_service.list_namespace_and_topics(
        subject=valid_subject,
        token=valid_token,
    )

    assert result['status'] == 'success'
    assert result['namespaces'] == {}


@pytest.mark.asyncio
async def test_list_namespace_and_topics_authentication_failure(
    diaspora_service: DiasporaService,
    valid_subject: str,
    mock_auth_manager: MagicMock,
) -> None:
    """Test list_namespace_and_topics with authentication failure."""
    mock_auth_manager.validate_access_token.return_value = {
        'status': 'error',
        'reason': 'Invalid token',
    }

    result = await diaspora_service.list_namespace_and_topics(
        subject=valid_subject,
        token='Bearer invalid-token',
    )

    assert result['status'] == 'error'
