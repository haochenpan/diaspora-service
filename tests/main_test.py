"""Unit and integration tests for web_service FastAPI routes (main.py).

Unit tests use mocks to validate exact response formats.
Integration tests use real AWS services to verify
error conditions, input validation, and edge cases.
"""

from __future__ import annotations

import json
import os
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from web_service.consumer_models import AssignmentRequest
from web_service.consumer_models import CreateConsumerRequest
from web_service.consumer_models import OffsetsCommitRequest
from web_service.consumer_models import PartitionInfo
from web_service.consumer_models import PartitionOffset
from web_service.consumer_models import SeekPartitionsRequest
from web_service.consumer_models import SeekRequest
from web_service.consumer_models import SubscriptionRequest
from web_service.main import DiasporaService

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
def mock_consumer_service() -> MagicMock:
    """Create a mock ConsumerService for unit tests."""
    return MagicMock()


@pytest.fixture
def mock_diaspora_service(
    mock_auth_manager: MagicMock,
    mock_web_service: MagicMock,
    mock_consumer_service: MagicMock,
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
        patch(
            'web_service.main.ConsumerService',
            return_value=mock_consumer_service,
        ),
    ):
        service = DiasporaService()
        service.auth = mock_auth_manager
        service.web_service = mock_web_service
        service.consumer_service = mock_consumer_service
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


# ============================================================================
# Consumer REST API - Constants
# ============================================================================

CONSUMER_NS = 'ns-test123'
CONSUMER_GROUP = 'my-group'
CONSUMER_INST = 'my-instance'


# ============================================================================
# Consumer REST API - Helper to parse Response body
# ============================================================================


def _response_json(response: object) -> Any:
    """Extract JSON body from a Response object."""
    body = getattr(response, 'body', b'')
    if not body:
        return None
    return json.loads(body)


# ============================================================================
# Consumer REST API - _consumer_response helper tests
# ============================================================================


def test_consumer_response_204_returns_empty_response() -> None:
    """Test _consumer_response returns empty Response for 204."""
    from fastapi.responses import JSONResponse

    resp = DiasporaService._consumer_response(None, 204)
    assert resp.status_code == 204
    assert not isinstance(resp, JSONResponse)
    assert resp.body == b''


def test_consumer_response_200_returns_json() -> None:
    """Test _consumer_response returns JSONResponse for 200."""
    from fastapi.responses import JSONResponse

    resp = DiasporaService._consumer_response({'key': 'val'}, 200)
    assert isinstance(resp, JSONResponse)
    assert resp.status_code == 200


# ============================================================================
# Consumer REST API - Auth Failure Tests (401)
# ============================================================================


def _setup_auth_failure(mock_auth_manager: MagicMock) -> None:
    """Configure mock auth manager to return auth error."""
    mock_auth_manager.validate_access_token.return_value = {
        'status': 'error',
        'reason': 'Invalid token',
    }


@pytest.mark.asyncio
async def test_create_consumer_instance_auth_failure(
    mock_diaspora_service: DiasporaService,
    mock_auth_manager: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
) -> None:
    """Test create_consumer_instance returns 401 on auth failure."""
    _setup_auth_failure(mock_auth_manager)
    body = CreateConsumerRequest()
    response = await mock_diaspora_service.create_consumer_instance(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        body=body,
        subject=valid_subject,
        token='Bearer invalid',
    )
    assert response.status_code == 401
    mock_consumer_service.create_consumer.assert_not_called()


@pytest.mark.asyncio
async def test_delete_consumer_instance_auth_failure(
    mock_diaspora_service: DiasporaService,
    mock_auth_manager: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
) -> None:
    """Test delete_consumer_instance returns 401 on auth failure."""
    _setup_auth_failure(mock_auth_manager)
    response = await mock_diaspora_service.delete_consumer_instance(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        subject=valid_subject,
        token='Bearer invalid',
    )
    assert response.status_code == 401
    mock_consumer_service.delete_consumer.assert_not_called()


@pytest.mark.asyncio
async def test_subscribe_consumer_auth_failure(
    mock_diaspora_service: DiasporaService,
    mock_auth_manager: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
) -> None:
    """Test subscribe_consumer returns 401 on auth failure."""
    _setup_auth_failure(mock_auth_manager)
    body = SubscriptionRequest(topics=['t1'])
    response = await mock_diaspora_service.subscribe_consumer(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        body=body,
        subject=valid_subject,
        token='Bearer invalid',
    )
    assert response.status_code == 401
    mock_consumer_service.subscribe.assert_not_called()


@pytest.mark.asyncio
async def test_get_consumer_subscription_auth_failure(
    mock_diaspora_service: DiasporaService,
    mock_auth_manager: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
) -> None:
    """Test get_consumer_subscription returns 401 on auth failure."""
    _setup_auth_failure(mock_auth_manager)
    response = await mock_diaspora_service.get_consumer_subscription(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        subject=valid_subject,
        token='Bearer invalid',
    )
    assert response.status_code == 401
    mock_consumer_service.get_subscription.assert_not_called()


@pytest.mark.asyncio
async def test_unsubscribe_consumer_auth_failure(
    mock_diaspora_service: DiasporaService,
    mock_auth_manager: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
) -> None:
    """Test unsubscribe_consumer returns 401 on auth failure."""
    _setup_auth_failure(mock_auth_manager)
    response = await mock_diaspora_service.unsubscribe_consumer(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        subject=valid_subject,
        token='Bearer invalid',
    )
    assert response.status_code == 401
    mock_consumer_service.unsubscribe.assert_not_called()


def test_fetch_consumer_records_auth_failure(
    mock_diaspora_service: DiasporaService,
    mock_auth_manager: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
) -> None:
    """Test fetch_consumer_records returns 401 on auth failure (sync)."""
    _setup_auth_failure(mock_auth_manager)
    response = mock_diaspora_service.fetch_consumer_records(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        subject=valid_subject,
        token='Bearer invalid',
    )
    assert response.status_code == 401
    mock_consumer_service.poll_records.assert_not_called()


@pytest.mark.asyncio
async def test_commit_consumer_offsets_auth_failure(
    mock_diaspora_service: DiasporaService,
    mock_auth_manager: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
) -> None:
    """Test commit_consumer_offsets returns 401 on auth failure."""
    _setup_auth_failure(mock_auth_manager)
    response = await mock_diaspora_service.commit_consumer_offsets(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        subject=valid_subject,
        token='Bearer invalid',
    )
    assert response.status_code == 401
    mock_consumer_service.commit_offsets.assert_not_called()


@pytest.mark.asyncio
async def test_get_committed_offsets_auth_failure(
    mock_diaspora_service: DiasporaService,
    mock_auth_manager: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
) -> None:
    """Test get_committed_offsets returns 401 on auth failure."""
    _setup_auth_failure(mock_auth_manager)
    mock_request = AsyncMock()
    mock_request.json.return_value = {
        'partitions': [{'topic': 't1', 'partition': 0}],
    }
    response = await mock_diaspora_service.get_committed_offsets(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        request=mock_request,
        subject=valid_subject,
        token='Bearer invalid',
    )
    assert response.status_code == 401
    mock_consumer_service.get_committed.assert_not_called()


@pytest.mark.asyncio
async def test_assign_consumer_partitions_auth_failure(
    mock_diaspora_service: DiasporaService,
    mock_auth_manager: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
) -> None:
    """Test assign_consumer_partitions returns 401 on auth failure."""
    _setup_auth_failure(mock_auth_manager)
    body = AssignmentRequest(
        partitions=[PartitionInfo(topic='t1', partition=0)],
    )
    response = await mock_diaspora_service.assign_consumer_partitions(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        body=body,
        subject=valid_subject,
        token='Bearer invalid',
    )
    assert response.status_code == 401
    mock_consumer_service.assign_partitions.assert_not_called()


@pytest.mark.asyncio
async def test_get_consumer_assignments_auth_failure(
    mock_diaspora_service: DiasporaService,
    mock_auth_manager: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
) -> None:
    """Test get_consumer_assignments returns 401 on auth failure."""
    _setup_auth_failure(mock_auth_manager)
    response = await mock_diaspora_service.get_consumer_assignments(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        subject=valid_subject,
        token='Bearer invalid',
    )
    assert response.status_code == 401
    mock_consumer_service.get_assignments.assert_not_called()


@pytest.mark.asyncio
async def test_seek_consumer_positions_auth_failure(
    mock_diaspora_service: DiasporaService,
    mock_auth_manager: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
) -> None:
    """Test seek_consumer_positions returns 401 on auth failure."""
    _setup_auth_failure(mock_auth_manager)
    body = SeekRequest(
        offsets=[PartitionOffset(topic='t1', partition=0, offset=0)],
    )
    response = await mock_diaspora_service.seek_consumer_positions(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        body=body,
        subject=valid_subject,
        token='Bearer invalid',
    )
    assert response.status_code == 401
    mock_consumer_service.seek.assert_not_called()


@pytest.mark.asyncio
async def test_seek_consumer_beginning_auth_failure(
    mock_diaspora_service: DiasporaService,
    mock_auth_manager: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
) -> None:
    """Test seek_consumer_beginning returns 401 on auth failure."""
    _setup_auth_failure(mock_auth_manager)
    body = SeekPartitionsRequest(
        partitions=[PartitionInfo(topic='t1', partition=0)],
    )
    response = await mock_diaspora_service.seek_consumer_beginning(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        body=body,
        subject=valid_subject,
        token='Bearer invalid',
    )
    assert response.status_code == 401
    mock_consumer_service.seek_to_beginning.assert_not_called()


@pytest.mark.asyncio
async def test_seek_consumer_end_auth_failure(
    mock_diaspora_service: DiasporaService,
    mock_auth_manager: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
) -> None:
    """Test seek_consumer_end returns 401 on auth failure."""
    _setup_auth_failure(mock_auth_manager)
    body = SeekPartitionsRequest(
        partitions=[PartitionInfo(topic='t1', partition=0)],
    )
    response = await mock_diaspora_service.seek_consumer_end(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        body=body,
        subject=valid_subject,
        token='Bearer invalid',
    )
    assert response.status_code == 401
    mock_consumer_service.seek_to_end.assert_not_called()


# ============================================================================
# Consumer REST API - Namespace Validation Failure Tests (404)
# ============================================================================


def _setup_namespace_failure(mock_web_service: MagicMock) -> None:
    """Configure mock so namespace validation fails."""
    ns_db = mock_web_service.namespace_service.dynamodb
    ns_db.get_user_namespaces.return_value = []


def _setup_namespace_success(
    mock_web_service: MagicMock,
    namespace: str = CONSUMER_NS,
) -> None:
    """Configure mock so namespace validation passes."""
    ns_db = mock_web_service.namespace_service.dynamodb
    ns_db.get_user_namespaces.return_value = [namespace]


@pytest.mark.asyncio
async def test_create_consumer_instance_namespace_failure(
    mock_diaspora_service: DiasporaService,
    mock_web_service: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
    valid_token: str,
) -> None:
    """Test create_consumer_instance returns 404 on namespace failure."""
    _setup_namespace_failure(mock_web_service)
    body = CreateConsumerRequest()
    response = await mock_diaspora_service.create_consumer_instance(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        body=body,
        subject=valid_subject,
        token=valid_token,
    )
    assert response.status_code == 404
    mock_consumer_service.create_consumer.assert_not_called()


@pytest.mark.asyncio
async def test_delete_consumer_instance_namespace_failure(
    mock_diaspora_service: DiasporaService,
    mock_web_service: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
    valid_token: str,
) -> None:
    """Test delete_consumer_instance returns 404 on namespace failure."""
    _setup_namespace_failure(mock_web_service)
    response = await mock_diaspora_service.delete_consumer_instance(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        subject=valid_subject,
        token=valid_token,
    )
    assert response.status_code == 404
    mock_consumer_service.delete_consumer.assert_not_called()


@pytest.mark.asyncio
async def test_subscribe_consumer_namespace_failure(
    mock_diaspora_service: DiasporaService,
    mock_web_service: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
    valid_token: str,
) -> None:
    """Test subscribe_consumer returns 404 on namespace failure."""
    _setup_namespace_failure(mock_web_service)
    body = SubscriptionRequest(topics=['t1'])
    response = await mock_diaspora_service.subscribe_consumer(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        body=body,
        subject=valid_subject,
        token=valid_token,
    )
    assert response.status_code == 404
    mock_consumer_service.subscribe.assert_not_called()


@pytest.mark.asyncio
async def test_get_consumer_subscription_namespace_failure(
    mock_diaspora_service: DiasporaService,
    mock_web_service: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
    valid_token: str,
) -> None:
    """Test get_consumer_subscription returns 404 on namespace failure."""
    _setup_namespace_failure(mock_web_service)
    response = await mock_diaspora_service.get_consumer_subscription(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        subject=valid_subject,
        token=valid_token,
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_unsubscribe_consumer_namespace_failure(
    mock_diaspora_service: DiasporaService,
    mock_web_service: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
    valid_token: str,
) -> None:
    """Test unsubscribe_consumer returns 404 on namespace failure."""
    _setup_namespace_failure(mock_web_service)
    response = await mock_diaspora_service.unsubscribe_consumer(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        subject=valid_subject,
        token=valid_token,
    )
    assert response.status_code == 404


def test_fetch_consumer_records_namespace_failure(
    mock_diaspora_service: DiasporaService,
    mock_web_service: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
    valid_token: str,
) -> None:
    """Test fetch_consumer_records returns 404 on namespace failure."""
    _setup_namespace_failure(mock_web_service)
    response = mock_diaspora_service.fetch_consumer_records(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        subject=valid_subject,
        token=valid_token,
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_commit_consumer_offsets_namespace_failure(
    mock_diaspora_service: DiasporaService,
    mock_web_service: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
    valid_token: str,
) -> None:
    """Test commit_consumer_offsets returns 404 on namespace failure."""
    _setup_namespace_failure(mock_web_service)
    response = await mock_diaspora_service.commit_consumer_offsets(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        subject=valid_subject,
        token=valid_token,
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_committed_offsets_namespace_failure(
    mock_diaspora_service: DiasporaService,
    mock_web_service: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
    valid_token: str,
) -> None:
    """Test get_committed_offsets returns 404 on namespace failure."""
    _setup_namespace_failure(mock_web_service)
    mock_request = AsyncMock()
    response = await mock_diaspora_service.get_committed_offsets(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        request=mock_request,
        subject=valid_subject,
        token=valid_token,
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_assign_consumer_partitions_namespace_failure(
    mock_diaspora_service: DiasporaService,
    mock_web_service: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
    valid_token: str,
) -> None:
    """Test assign_consumer_partitions returns 404 on namespace failure."""
    _setup_namespace_failure(mock_web_service)
    body = AssignmentRequest(
        partitions=[PartitionInfo(topic='t1', partition=0)],
    )
    response = await mock_diaspora_service.assign_consumer_partitions(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        body=body,
        subject=valid_subject,
        token=valid_token,
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_consumer_assignments_namespace_failure(
    mock_diaspora_service: DiasporaService,
    mock_web_service: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
    valid_token: str,
) -> None:
    """Test get_consumer_assignments returns 404 on namespace failure."""
    _setup_namespace_failure(mock_web_service)
    response = await mock_diaspora_service.get_consumer_assignments(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        subject=valid_subject,
        token=valid_token,
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_seek_consumer_positions_namespace_failure(
    mock_diaspora_service: DiasporaService,
    mock_web_service: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
    valid_token: str,
) -> None:
    """Test seek_consumer_positions returns 404 on namespace failure."""
    _setup_namespace_failure(mock_web_service)
    body = SeekRequest(
        offsets=[PartitionOffset(topic='t1', partition=0, offset=0)],
    )
    response = await mock_diaspora_service.seek_consumer_positions(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        body=body,
        subject=valid_subject,
        token=valid_token,
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_seek_consumer_beginning_namespace_failure(
    mock_diaspora_service: DiasporaService,
    mock_web_service: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
    valid_token: str,
) -> None:
    """Test seek_consumer_beginning returns 404 on namespace failure."""
    _setup_namespace_failure(mock_web_service)
    body = SeekPartitionsRequest(
        partitions=[PartitionInfo(topic='t1', partition=0)],
    )
    response = await mock_diaspora_service.seek_consumer_beginning(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        body=body,
        subject=valid_subject,
        token=valid_token,
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_seek_consumer_end_namespace_failure(
    mock_diaspora_service: DiasporaService,
    mock_web_service: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
    valid_token: str,
) -> None:
    """Test seek_consumer_end returns 404 on namespace failure."""
    _setup_namespace_failure(mock_web_service)
    body = SeekPartitionsRequest(
        partitions=[PartitionInfo(topic='t1', partition=0)],
    )
    response = await mock_diaspora_service.seek_consumer_end(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        body=body,
        subject=valid_subject,
        token=valid_token,
    )
    assert response.status_code == 404


# ============================================================================
# Consumer REST API - Subscribe-Specific Validation Tests
# ============================================================================


@pytest.mark.asyncio
async def test_subscribe_consumer_mutual_exclusivity(
    mock_diaspora_service: DiasporaService,
    mock_web_service: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
    valid_token: str,
) -> None:
    """Test subscribe returns 409 when both topics and pattern given."""
    _setup_namespace_success(mock_web_service)
    body = SubscriptionRequest(topics=['t1'], topic_pattern='.*')
    response = await mock_diaspora_service.subscribe_consumer(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        body=body,
        subject=valid_subject,
        token=valid_token,
    )
    assert response.status_code == 409
    resp_body = _response_json(response)
    assert resp_body['error_code'] == 40903
    mock_consumer_service.subscribe.assert_not_called()


@pytest.mark.asyncio
async def test_subscribe_consumer_topic_validation_failure(
    mock_diaspora_service: DiasporaService,
    mock_web_service: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
    valid_token: str,
) -> None:
    """Test subscribe returns 404 when topic not found in namespace."""
    _setup_namespace_success(mock_web_service)
    ns_db = mock_web_service.namespace_service.dynamodb
    ns_db.get_namespace_topics.return_value = [
        'other-topic',
    ]
    body = SubscriptionRequest(topics=['nonexistent-topic'])
    response = await mock_diaspora_service.subscribe_consumer(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        body=body,
        subject=valid_subject,
        token=valid_token,
    )
    assert response.status_code == 404
    mock_consumer_service.subscribe.assert_not_called()


# ============================================================================
# Consumer REST API - Assign-Specific Validation Tests
# ============================================================================


@pytest.mark.asyncio
async def test_assign_consumer_partitions_topic_validation_failure(
    mock_diaspora_service: DiasporaService,
    mock_web_service: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
    valid_token: str,
) -> None:
    """Test assign returns 404 when topic not found in namespace."""
    _setup_namespace_success(mock_web_service)
    ns_db = mock_web_service.namespace_service.dynamodb
    ns_db.get_namespace_topics.return_value = []
    body = AssignmentRequest(
        partitions=[PartitionInfo(topic='bad-topic', partition=0)],
    )
    response = await mock_diaspora_service.assign_consumer_partitions(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        body=body,
        subject=valid_subject,
        token=valid_token,
    )
    assert response.status_code == 404
    mock_consumer_service.assign_partitions.assert_not_called()


# ============================================================================
# Consumer REST API - Happy Path Tests
# ============================================================================


@pytest.mark.asyncio
async def test_create_consumer_instance_success(
    mock_diaspora_service: DiasporaService,
    mock_web_service: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
    valid_token: str,
) -> None:
    """Test create_consumer_instance success path."""
    _setup_namespace_success(mock_web_service)
    mock_consumer_service.create_consumer.return_value = (
        {
            'instance_id': 'abc',
            'base_uri': '/api/v3/ns/consumers/g/instances/abc',
        },
        200,
    )
    body = CreateConsumerRequest(name='abc')
    response = await mock_diaspora_service.create_consumer_instance(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        body=body,
        subject=valid_subject,
        token=valid_token,
    )
    assert response.status_code == 200
    resp_body = _response_json(response)
    assert resp_body['instance_id'] == 'abc'
    assert 'base_uri' in resp_body


@pytest.mark.asyncio
async def test_delete_consumer_instance_success(
    mock_diaspora_service: DiasporaService,
    mock_web_service: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
    valid_token: str,
) -> None:
    """Test delete_consumer_instance success path (204 empty body)."""
    _setup_namespace_success(mock_web_service)
    mock_consumer_service.delete_consumer.return_value = (None, 204)
    response = await mock_diaspora_service.delete_consumer_instance(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        subject=valid_subject,
        token=valid_token,
    )
    assert response.status_code == 204
    assert response.body == b''


@pytest.mark.asyncio
async def test_subscribe_consumer_success(
    mock_diaspora_service: DiasporaService,
    mock_web_service: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
    valid_token: str,
) -> None:
    """Test subscribe_consumer success path."""
    _setup_namespace_success(mock_web_service)
    ns_db = mock_web_service.namespace_service.dynamodb
    ns_db.get_namespace_topics.return_value = [
        't1',
    ]
    mock_consumer_service.subscribe.return_value = (None, 204)
    body = SubscriptionRequest(topics=['t1'])
    response = await mock_diaspora_service.subscribe_consumer(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        body=body,
        subject=valid_subject,
        token=valid_token,
    )
    assert response.status_code == 204


@pytest.mark.asyncio
async def test_get_consumer_subscription_success(
    mock_diaspora_service: DiasporaService,
    mock_web_service: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
    valid_token: str,
) -> None:
    """Test get_consumer_subscription success path."""
    _setup_namespace_success(mock_web_service)
    mock_consumer_service.get_subscription.return_value = (
        {'topics': ['t1']},
        200,
    )
    response = await mock_diaspora_service.get_consumer_subscription(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        subject=valid_subject,
        token=valid_token,
    )
    assert response.status_code == 200
    resp_body = _response_json(response)
    assert resp_body['topics'] == ['t1']


@pytest.mark.asyncio
async def test_unsubscribe_consumer_success(
    mock_diaspora_service: DiasporaService,
    mock_web_service: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
    valid_token: str,
) -> None:
    """Test unsubscribe_consumer success path."""
    _setup_namespace_success(mock_web_service)
    mock_consumer_service.unsubscribe.return_value = (None, 204)
    response = await mock_diaspora_service.unsubscribe_consumer(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        subject=valid_subject,
        token=valid_token,
    )
    assert response.status_code == 204


def test_fetch_consumer_records_success(
    mock_diaspora_service: DiasporaService,
    mock_web_service: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
    valid_token: str,
) -> None:
    """Test fetch_consumer_records success path (sync)."""
    _setup_namespace_success(mock_web_service)
    mock_consumer_service.poll_records.return_value = (
        [
            {
                'topic': 't1',
                'key': None,
                'value': 'aGVsbG8=',
                'partition': 0,
                'offset': 0,
            },
        ],
        200,
    )
    response = mock_diaspora_service.fetch_consumer_records(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        subject=valid_subject,
        token=valid_token,
    )
    assert response.status_code == 200
    resp_body = _response_json(response)
    assert isinstance(resp_body, list)
    assert len(resp_body) == 1


def test_fetch_consumer_records_default_timeout(
    mock_diaspora_service: DiasporaService,
    mock_web_service: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
    valid_token: str,
) -> None:
    """Test fetch_consumer_records uses default timeout=5000."""
    _setup_namespace_success(mock_web_service)
    mock_consumer_service.poll_records.return_value = ([], 200)
    mock_diaspora_service.fetch_consumer_records(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        subject=valid_subject,
        token=valid_token,
        timeout=None,
        max_bytes=None,
    )
    mock_consumer_service.poll_records.assert_called_once_with(
        subject=valid_subject,
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        timeout_ms=5000,
        max_bytes=None,
    )


def test_fetch_consumer_records_custom_timeout(
    mock_diaspora_service: DiasporaService,
    mock_web_service: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
    valid_token: str,
) -> None:
    """Test fetch_consumer_records with custom timeout and max_bytes."""
    _setup_namespace_success(mock_web_service)
    mock_consumer_service.poll_records.return_value = ([], 200)
    mock_diaspora_service.fetch_consumer_records(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        subject=valid_subject,
        token=valid_token,
        timeout=10000,
        max_bytes=1048576,
    )
    mock_consumer_service.poll_records.assert_called_once_with(
        subject=valid_subject,
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        timeout_ms=10000,
        max_bytes=1048576,
    )


@pytest.mark.asyncio
async def test_commit_consumer_offsets_success(
    mock_diaspora_service: DiasporaService,
    mock_web_service: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
    valid_token: str,
) -> None:
    """Test commit_consumer_offsets success path."""
    _setup_namespace_success(mock_web_service)
    mock_consumer_service.commit_offsets.return_value = (None, 200)
    response = await mock_diaspora_service.commit_consumer_offsets(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        subject=valid_subject,
        token=valid_token,
        body=None,
    )
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_commit_consumer_offsets_with_explicit_offsets(
    mock_diaspora_service: DiasporaService,
    mock_web_service: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
    valid_token: str,
) -> None:
    """Test commit passes offsets through to service via model_dump."""
    _setup_namespace_success(mock_web_service)
    mock_consumer_service.commit_offsets.return_value = (None, 200)
    body = OffsetsCommitRequest(
        offsets=[PartitionOffset(topic='t1', partition=0, offset=5)],
    )
    await mock_diaspora_service.commit_consumer_offsets(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        subject=valid_subject,
        token=valid_token,
        body=body,
    )
    call_kwargs = mock_consumer_service.commit_offsets.call_args[1]
    assert call_kwargs['offsets'] == [
        {'topic': 't1', 'partition': 0, 'offset': 5},
    ]


@pytest.mark.asyncio
async def test_get_committed_offsets_success(
    mock_diaspora_service: DiasporaService,
    mock_web_service: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
    valid_token: str,
) -> None:
    """Test get_committed_offsets success path."""
    _setup_namespace_success(mock_web_service)
    mock_consumer_service.get_committed.return_value = (
        {
            'offsets': [
                {'topic': 't1', 'partition': 0, 'offset': 42, 'metadata': ''},
            ],
        },
        200,
    )
    mock_request = AsyncMock()
    mock_request.json.return_value = {
        'partitions': [{'topic': 't1', 'partition': 0}],
    }
    response = await mock_diaspora_service.get_committed_offsets(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        request=mock_request,
        subject=valid_subject,
        token=valid_token,
    )
    assert response.status_code == 200
    resp_body = _response_json(response)
    assert 'offsets' in resp_body


@pytest.mark.asyncio
async def test_assign_consumer_partitions_success(
    mock_diaspora_service: DiasporaService,
    mock_web_service: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
    valid_token: str,
) -> None:
    """Test assign_consumer_partitions success path."""
    _setup_namespace_success(mock_web_service)
    ns_db = mock_web_service.namespace_service.dynamodb
    ns_db.get_namespace_topics.return_value = [
        't1',
    ]
    mock_consumer_service.assign_partitions.return_value = (None, 204)
    body = AssignmentRequest(
        partitions=[PartitionInfo(topic='t1', partition=0)],
    )
    response = await mock_diaspora_service.assign_consumer_partitions(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        body=body,
        subject=valid_subject,
        token=valid_token,
    )
    assert response.status_code == 204


@pytest.mark.asyncio
async def test_get_consumer_assignments_success(
    mock_diaspora_service: DiasporaService,
    mock_web_service: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
    valid_token: str,
) -> None:
    """Test get_consumer_assignments success path."""
    _setup_namespace_success(mock_web_service)
    mock_consumer_service.get_assignments.return_value = (
        {'partitions': [{'topic': 't1', 'partition': 0}]},
        200,
    )
    response = await mock_diaspora_service.get_consumer_assignments(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        subject=valid_subject,
        token=valid_token,
    )
    assert response.status_code == 200
    resp_body = _response_json(response)
    assert 'partitions' in resp_body


@pytest.mark.asyncio
async def test_seek_consumer_positions_success(
    mock_diaspora_service: DiasporaService,
    mock_web_service: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
    valid_token: str,
) -> None:
    """Test seek_consumer_positions success path."""
    _setup_namespace_success(mock_web_service)
    mock_consumer_service.seek.return_value = (None, 204)
    body = SeekRequest(
        offsets=[PartitionOffset(topic='t1', partition=0, offset=10)],
    )
    response = await mock_diaspora_service.seek_consumer_positions(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        body=body,
        subject=valid_subject,
        token=valid_token,
    )
    assert response.status_code == 204


@pytest.mark.asyncio
async def test_seek_consumer_beginning_success(
    mock_diaspora_service: DiasporaService,
    mock_web_service: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
    valid_token: str,
) -> None:
    """Test seek_consumer_beginning success path."""
    _setup_namespace_success(mock_web_service)
    mock_consumer_service.seek_to_beginning.return_value = (None, 204)
    body = SeekPartitionsRequest(
        partitions=[PartitionInfo(topic='t1', partition=0)],
    )
    response = await mock_diaspora_service.seek_consumer_beginning(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        body=body,
        subject=valid_subject,
        token=valid_token,
    )
    assert response.status_code == 204


@pytest.mark.asyncio
async def test_seek_consumer_end_success(
    mock_diaspora_service: DiasporaService,
    mock_web_service: MagicMock,
    mock_consumer_service: MagicMock,
    valid_subject: str,
    valid_token: str,
) -> None:
    """Test seek_consumer_end success path."""
    _setup_namespace_success(mock_web_service)
    mock_consumer_service.seek_to_end.return_value = (None, 204)
    body = SeekPartitionsRequest(
        partitions=[PartitionInfo(topic='t1', partition=0)],
    )
    response = await mock_diaspora_service.seek_consumer_end(
        namespace=CONSUMER_NS,
        group_name=CONSUMER_GROUP,
        instance_id=CONSUMER_INST,
        body=body,
        subject=valid_subject,
        token=valid_token,
    )
    assert response.status_code == 204
