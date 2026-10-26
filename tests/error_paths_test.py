"""Error path tests for web_service services.

Most tests use mocks to simulate specific error conditions that are difficult
to reproduce with real AWS services (e.g., IAM exceptions, network failures).
Some tests have integration variants that use real AWS services where the
error conditions can be naturally tested.

To run only unit tests (mocked):
    pytest web_service/test_error_paths.py
To run only integration tests:
    pytest web_service/test_error_paths.py -m integration
"""

from __future__ import annotations

import logging
import threading
from unittest.mock import call
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError
from botocore.stub import Stubber
from kafka.errors import KafkaError
from kafka.errors import KafkaTimeoutError
from kafka.errors import TopicAlreadyExistsError

from web_service.services import ADMIN_OPERATION_TIMEOUT_MS
from web_service.services import ADMIN_REQUEST_TIMEOUT_MS
from web_service.services import DynamoDBService
from web_service.services import IAMService
from web_service.services import KafkaService
from web_service.services import NamespaceService
from web_service.services import WebService

# Test constants
EXPECTED_RETRY_COUNT = 2

# ============================================================================
# IAMService Error Path Tests
# ============================================================================


def test_create_user_and_policy_iam_exception() -> None:
    """Test create_user_and_policy handles IAM API exceptions."""
    iam_service = IAMService(
        account_id='123456789012',
        region='us-east-1',
        cluster_name='test-cluster',
    )

    # Mock IAM client to raise exception
    with patch.object(iam_service.iam, 'create_user') as mock_create_user:
        mock_create_user.side_effect = Exception('IAM service unavailable')

        result = iam_service.create_user_and_policy('test-subject')

        assert result['status'] == 'failure'
        assert 'message' in result
        assert 'Failed to create user and policy' in result['message']


def test_iam_failure_message_leaves_out_iams_text(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """IAM's text, which names the account, goes to the log only."""
    iam_service = IAMService(
        account_id='123456789012',
        region='us-east-1',
        cluster_name='test-cluster',
    )
    text = 'User: arn:aws:iam::123456789012:user/svc is not authorized'
    with (
        patch.object(
            iam_service.iam,
            'create_user',
            side_effect=Exception(text),
        ),
        caplog.at_level(logging.WARNING, logger='web_service.services'),
    ):
        result = iam_service.create_user_and_policy('test-subject')
    assert result == {
        'status': 'failure',
        'message': (
            'Failed to create user and policy: '
            'IAM did not complete the request'
        ),
    }
    assert text in caplog.text


def test_create_user_and_policy_policy_creation_failure() -> None:
    """Test create_user_and_policy handles policy creation failure."""
    iam_service = IAMService(
        account_id='123456789012',
        region='us-east-1',
        cluster_name='test-cluster',
    )

    # Mock user creation succeeds, policy creation fails
    with (
        patch.object(iam_service.iam, 'create_user'),
        patch.object(
            iam_service.iam,
            'create_policy',
        ) as mock_create_policy,
    ):
        mock_create_policy.side_effect = Exception('Policy limit exceeded')

        result = iam_service.create_user_and_policy('test-subject')

        assert result['status'] == 'failure'
        assert 'Failed to create user and policy' in result['message']


def test_create_access_key_user_not_found() -> None:
    """Test create_access_key handles user not found."""
    iam_service = IAMService(
        account_id='123456789012',
        region='us-east-1',
        cluster_name='test-cluster',
    )

    # Mock IAM client to raise NoSuchEntity exception
    with patch.object(iam_service.iam, 'create_access_key') as mock_create_key:
        error_response = {
            'Error': {
                'Code': 'NoSuchEntity',
                'Message': 'User not found',
            },
        }
        mock_create_key.side_effect = ClientError(
            error_response,
            'CreateAccessKey',
        )

        result = iam_service.create_access_key('nonexistent-user')

        assert result['status'] == 'failure'
        assert 'Failed to create access key' in result['message']


def test_create_access_key_limit_exceeded() -> None:
    """Test create_access_key handles access key limit exceeded."""
    iam_service = IAMService(
        account_id='123456789012',
        region='us-east-1',
        cluster_name='test-cluster',
    )

    # Mock IAM client to raise LimitExceeded exception
    with patch.object(iam_service.iam, 'create_access_key') as mock_create_key:
        error_response = {
            'Error': {
                'Code': 'LimitExceeded',
                'Message': 'Access key limit exceeded',
            },
        }
        mock_create_key.side_effect = ClientError(
            error_response,
            'CreateAccessKey',
        )

        result = iam_service.create_access_key('test-user')

        assert result['status'] == 'failure'
        assert 'Failed to create access key' in result['message']


def test_delete_access_keys_no_keys() -> None:
    """Test delete_access_keys handles user with no keys (idempotent)."""
    iam_service = IAMService(
        account_id='123456789012',
        region='us-east-1',
        cluster_name='test-cluster',
    )

    # Mock list_access_keys to return empty list
    with patch.object(
        iam_service.iam,
        'list_access_keys',
    ) as mock_list_keys:
        mock_list_keys.return_value = {'AccessKeyMetadata': []}

        result = iam_service.delete_access_keys('test-user')

        # Should succeed (idempotent)
        assert result['status'] == 'success'


def test_delete_user_and_policy_partial_failure() -> None:
    """Test delete_user_and_policy handles partial deletion failures."""
    iam_service = IAMService(
        account_id='123456789012',
        region='us-east-1',
        cluster_name='test-cluster',
    )

    policy_arn = 'arn:aws:iam::123456789012:policy/msk-policy/test-user'

    # Every IAM call is stubbed, so none reaches AWS; an unexpected call
    # raises inside the method and turns the result into a failure
    with (
        patch.object(
            iam_service,
            'delete_access_keys',
        ) as mock_delete_keys,
        Stubber(iam_service.iam) as stubber,
    ):
        mock_delete_keys.return_value = {'status': 'success'}
        stubber.add_response(
            'detach_user_policy',
            {},
            {'UserName': 'test-user', 'PolicyArn': policy_arn},
        )
        stubber.add_response(
            'list_policy_versions',
            {
                'Versions': [
                    {'VersionId': 'v1', 'IsDefaultVersion': True},
                    {'VersionId': 'v2', 'IsDefaultVersion': False},
                ],
            },
            {'PolicyArn': policy_arn},
        )
        stubber.add_response(
            'delete_policy_version',
            {},
            {'PolicyArn': policy_arn, 'VersionId': 'v2'},
        )
        # delete_policy fails with NoSuchEntity
        stubber.add_client_error(
            'delete_policy',
            service_error_code='NoSuchEntity',
            service_message='Policy not found',
            expected_params={'PolicyArn': policy_arn},
        )
        stubber.add_response(
            'delete_user',
            {},
            {'UserName': 'test-user'},
        )

        result = iam_service.delete_user_and_policy('test-user')

        # The method suppresses NoSuchEntityException, so should succeed
        assert result['status'] == 'success'
        stubber.assert_no_pending_responses()


# ============================================================================
# DynamoDBService Error Path Tests
# ============================================================================


def test_store_key_table_not_found() -> None:
    """Test store_key handles table not found and creates table."""
    db_service = DynamoDBService(
        region='us-east-1',
        keys_table_name='test-diaspora-keys-table',
        users_table_name='test-diaspora-users-table',
        namespace_table_name='test-diaspora-namespace-table',
    )

    # Mock DynamoDB client with exceptions attribute
    mock_dynamodb = MagicMock()

    # Create a mock exceptions object with ResourceNotFoundException
    # that inherits from ClientError so it can be caught properly
    class MockResourceNotFoundError(ClientError):
        """Mock ResourceNotFoundError for testing."""

        def __init__(self, operation_name: str = 'PutItem'):
            error_response = {
                'Error': {
                    'Code': 'ResourceNotFoundException',
                    'Message': 'Table not found',
                },
            }
            super().__init__(error_response, operation_name)

    mock_exceptions = MagicMock()
    mock_exceptions.ResourceNotFoundException = MockResourceNotFoundError
    mock_dynamodb.exceptions = mock_exceptions
    db_service.dynamodb = mock_dynamodb

    # First call raises ResourceNotFoundError, second succeeds
    mock_dynamodb.put_item.side_effect = [
        MockResourceNotFoundError('PutItem'),
        None,  # Second call succeeds
    ]

    # Mock table creation
    with patch.object(db_service, '_create_keys_table') as mock_create_table:
        mock_create_table.return_value = None

        # Should not raise exception
        # pragma: allowlist secret
        db_service.store_key(
            subject='test-subject',
            access_key='AKIAIOSFODNN7EXAMPLE',  # pragma: allowlist secret
            secret_key='test-secret-key',  # pragma: allowlist secret
            create_date='2024-01-01T00:00:00',
        )

        # Should have called create_table and retried put_item
        mock_create_table.assert_called_once()
        assert mock_dynamodb.put_item.call_count == EXPECTED_RETRY_COUNT


def test_get_key_table_not_found() -> None:
    """Test get_key handles table not found gracefully."""
    db_service = DynamoDBService(
        region='us-east-1',
        keys_table_name='test-diaspora-keys-table',
        users_table_name='test-diaspora-users-table',
        namespace_table_name='test-diaspora-namespace-table',
    )

    # Mock DynamoDB client with exceptions attribute
    mock_dynamodb = MagicMock()

    # Create a mock exceptions object with ResourceNotFoundException
    # that inherits from ClientError so it can be caught properly
    class MockResourceNotFoundError(ClientError):
        """Mock ResourceNotFoundError for testing."""

        def __init__(self, operation_name: str = 'GetItem'):
            error_response = {
                'Error': {
                    'Code': 'ResourceNotFoundException',
                    'Message': 'Table not found',
                },
            }
            super().__init__(error_response, operation_name)

    mock_exceptions = MagicMock()
    mock_exceptions.ResourceNotFoundException = MockResourceNotFoundError
    mock_dynamodb.exceptions = mock_exceptions
    db_service.dynamodb = mock_dynamodb

    mock_dynamodb.get_item.side_effect = MockResourceNotFoundError('GetItem')

    # Should return None (gracefully handled)
    result = db_service.get_key('test-subject')
    assert result is None


def test_add_user_namespace_table_creation_retry() -> None:
    """Test add_user_namespace handles table creation and retry."""
    db_service = DynamoDBService(
        region='us-east-1',
        keys_table_name='test-diaspora-keys-table',
        users_table_name='test-diaspora-users-table',
        namespace_table_name='test-diaspora-namespace-table',
    )

    # Mock DynamoDB client with exceptions attribute
    mock_dynamodb = MagicMock()

    # Create a mock exceptions object with ResourceNotFoundException
    # that inherits from ClientError so it can be caught properly
    class MockResourceNotFoundError(ClientError):
        """Mock ResourceNotFoundError for testing."""

        def __init__(self, operation_name: str = 'UpdateItem'):
            error_response = {
                'Error': {
                    'Code': 'ResourceNotFoundException',
                    'Message': 'Table not found',
                },
            }
            super().__init__(error_response, operation_name)

    mock_exceptions = MagicMock()
    mock_exceptions.ResourceNotFoundException = MockResourceNotFoundError
    mock_dynamodb.exceptions = mock_exceptions
    db_service.dynamodb = mock_dynamodb

    # First call raises ResourceNotFoundError, second succeeds
    mock_dynamodb.update_item.side_effect = [
        MockResourceNotFoundError('UpdateItem'),
        None,  # Second call succeeds
    ]

    # Mock table creation
    with patch.object(db_service, '_create_users_table') as mock_create_table:
        mock_create_table.return_value = None

        # Should not raise exception
        db_service.add_user_namespace('test-subject', 'test-namespace')

        # Should have called create_table and retried update_item
        mock_create_table.assert_called_once()
        assert mock_dynamodb.update_item.call_count == EXPECTED_RETRY_COUNT


# ============================================================================
# KafkaService Error Path Tests
# ============================================================================


def test_create_topic_all_retries_fail() -> None:
    """Test create_topic handles all retry attempts failing."""
    kafka_service = KafkaService(
        bootstrap_servers='test-servers',
        region='us-east-1',
    )

    # Mock KafkaAdminClient to fail all attempts
    with patch(
        'web_service.services.KafkaAdminClient',
    ) as mock_admin_class:
        mock_admin = MagicMock()
        mock_admin_class.return_value = mock_admin

        mock_admin.create_topics.side_effect = KafkaError('Connection failed')

        result = kafka_service.create_topic('test-namespace', 'test-topic')

        assert result is not None
        assert result['status'] == 'failure'
        assert 'Failed to create Kafka topic' in result['message']
        assert 'after 3 attempts' in result['message']


def test_create_topic_closes_admin_client_after_each_attempt() -> None:
    """Test create_topic closes the admin client of every failed attempt."""
    kafka_service = KafkaService(
        bootstrap_servers='test-servers',
        region='us-east-1',
    )

    with patch(
        'web_service.services.KafkaAdminClient',
    ) as mock_admin_class:
        mock_admin = MagicMock()
        mock_admin_class.return_value = mock_admin
        mock_admin.create_topics.side_effect = KafkaError('Connection failed')

        kafka_service.create_topic('test-namespace', 'test-topic')

        assert mock_admin.close.call_count == mock_admin_class.call_count == 3
        _, kwargs = mock_admin_class.call_args
        assert kwargs['request_timeout_ms'] == ADMIN_REQUEST_TIMEOUT_MS
        _, kwargs = mock_admin.create_topics.call_args
        assert kwargs['timeout_ms'] == ADMIN_OPERATION_TIMEOUT_MS


def test_delete_topic_closes_admin_client_on_success() -> None:
    """Test delete_topic closes the admin client when the delete succeeds."""
    kafka_service = KafkaService(
        bootstrap_servers='test-servers',
        region='us-east-1',
    )

    with patch(
        'web_service.services.KafkaAdminClient',
    ) as mock_admin_class:
        mock_admin = MagicMock()
        mock_admin_class.return_value = mock_admin

        result = kafka_service.delete_topic('test-namespace', 'test-topic')

        assert result == {'status': 'success', 'message': 'Topic deleted'}
        mock_admin.close.assert_called_once()
        _, kwargs = mock_admin.delete_topics.call_args
        assert kwargs['timeout_ms'] == ADMIN_OPERATION_TIMEOUT_MS


def test_create_topic_timeout_error(caplog: pytest.LogCaptureFixture) -> None:
    """A create that times out logs the error and tells the client to retry."""
    kafka_service = KafkaService(
        bootstrap_servers='test-servers',
        region='us-east-1',
    )

    # Mock KafkaAdminClient to raise KafkaTimeoutError
    with patch(
        'web_service.services.KafkaAdminClient',
    ) as mock_admin_class:
        mock_admin = MagicMock()
        mock_admin_class.return_value = mock_admin

        # Simulate timeout on all attempts
        mock_admin.create_topics.side_effect = KafkaTimeoutError(
            'Request timed out',
        )

        with caplog.at_level(logging.WARNING, logger='web_service'):
            result = kafka_service.create_topic('test-namespace', 'test-topic')

        assert result == {
            'status': 'failure',
            'message': (
                'Failed to create Kafka topic test-namespace.test-topic '
                'after 3 attempts: the Kafka cluster did not complete it; '
                'retry shortly'
            ),
        }
        # The error goes to the log, not to the client
        assert 'Request timed out' in caplog.text


def test_create_topic_bootstrap_servers_none() -> None:
    """Test create_topic returns None when bootstrap_servers is None."""
    kafka_service = KafkaService(
        bootstrap_servers=None,
        region='us-east-1',
    )

    result = kafka_service.create_topic('test-namespace', 'test-topic')

    assert result is None


def test_delete_topic_all_retries_fail() -> None:
    """Test delete_topic handles all retry attempts failing."""
    kafka_service = KafkaService(
        bootstrap_servers='test-servers',
        region='us-east-1',
    )

    # Mock KafkaAdminClient to fail all attempts
    with patch(
        'web_service.services.KafkaAdminClient',
    ) as mock_admin_class:
        mock_admin = MagicMock()
        mock_admin_class.return_value = mock_admin

        mock_admin.delete_topics.side_effect = KafkaError('Connection failed')

        result = kafka_service.delete_topic('test-namespace', 'test-topic')

        assert result is not None
        assert result['status'] == 'failure'
        assert 'Failed to delete Kafka topic' in result['message']
        assert 'after 3 attempts' in result['message']


def test_recreate_topic_delete_fails() -> None:
    """Test recreate_topic handles delete failure."""
    kafka_service = KafkaService(
        bootstrap_servers='test-servers',
        region='us-east-1',
    )

    # Mock delete_topic to fail
    with patch.object(
        kafka_service,
        'delete_topic',
    ) as mock_delete:
        mock_delete.return_value = {
            'status': 'failure',
            'message': 'Failed to delete topic',
        }

        result = kafka_service.recreate_topic('test-namespace', 'test-topic')

        assert result is not None
        assert result['status'] == 'failure'
        assert 'Failed to delete topic' in result['message']


def test_recreate_topic_delete_fails_create_succeeds() -> None:
    """Test recreate_topic handles delete failure but create succeeds.

    This edge case tests the scenario where delete fails but create
    succeeds (unlikely but possible in practice).
    """
    kafka_service = KafkaService(
        bootstrap_servers='test-servers',
        region='us-east-1',
    )

    # Mock delete_topic to fail, but create_topic to succeed
    with (
        patch.object(
            kafka_service,
            'delete_topic',
        ) as mock_delete,
        patch.object(
            kafka_service,
            'create_topic',
        ) as mock_create,
    ):
        mock_delete.return_value = {
            'status': 'failure',
            'message': 'Failed to delete topic',
        }
        mock_create.return_value = {
            'status': 'success',
            'message': 'Topic created',
        }

        result = kafka_service.recreate_topic('test-namespace', 'test-topic')

        # Should return failure because delete failed
        assert result is not None
        assert result['status'] == 'failure'
        assert 'Failed to delete topic' in result['message']
        # Create should not be called if delete fails
        mock_create.assert_not_called()


def test_recreate_topic_create_fails() -> None:
    """Test recreate_topic handles create failure after successful delete."""
    kafka_service = KafkaService(
        bootstrap_servers='test-servers',
        region='us-east-1',
    )

    # Mock delete_topic to succeed, create_topic to fail
    with patch.object(
        kafka_service,
        'delete_topic',
    ) as mock_delete:
        mock_delete.return_value = {'status': 'success', 'message': 'Deleted'}

        with patch.object(
            kafka_service,
            'create_topic',
        ) as mock_create:
            mock_create.return_value = {
                'status': 'failure',
                'message': 'Failed to create topic',
            }

            with patch('time.sleep'):  # Skip sleep in test
                result = kafka_service.recreate_topic(
                    'test-namespace',
                    'test-topic',
                )

                assert result is not None
                assert result['status'] == 'failure'
                assert 'Failed to create topic' in result['message']


# ============================================================================
# NamespaceService Error Path Tests
# ============================================================================


def test_create_namespace_validation_failure() -> None:
    """Test create_namespace handles validation failure."""
    db_service = DynamoDBService(
        region='us-east-1',
        keys_table_name='test-diaspora-keys-table',
        users_table_name='test-diaspora-users-table',
        namespace_table_name='test-diaspora-namespace-table',
    )
    namespace_service = NamespaceService(dynamodb_service=db_service)

    # Test with invalid namespace name (too short)
    result = namespace_service.create_namespace('test-subject', 'ab')

    assert result['status'] == 'failure'
    assert 'between' in result['message'].lower()


def test_create_namespace_already_taken() -> None:
    """Test create_namespace handles namespace taken by another user."""
    db_service = DynamoDBService(
        region='us-east-1',
        keys_table_name='test-diaspora-keys-table',
        users_table_name='test-diaspora-users-table',
        namespace_table_name='test-diaspora-namespace-table',
    )
    namespace_service = NamespaceService(dynamodb_service=db_service)

    # Mock get_user_namespaces to return empty (user doesn't have it)
    with patch.object(
        db_service,
        'get_user_namespaces',
    ) as mock_get_user_ns:
        mock_get_user_ns.return_value = []

        # Mock get_global_namespaces to return namespace (taken by other user)
        with patch.object(
            db_service,
            'get_global_namespaces',
        ) as mock_get_global_ns:
            mock_get_global_ns.return_value = {'taken-namespace'}

            result = namespace_service.create_namespace(
                'test-subject',
                'taken-namespace',
            )

            assert result['status'] == 'failure'
            assert 'already taken' in result['message'].lower()


def test_create_topic_namespace_not_found() -> None:
    """Test create_topic handles namespace not found."""
    db_service = DynamoDBService(
        region='us-east-1',
        keys_table_name='test-diaspora-keys-table',
        users_table_name='test-diaspora-users-table',
        namespace_table_name='test-diaspora-namespace-table',
    )
    namespace_service = NamespaceService(dynamodb_service=db_service)

    # Mock get_user_namespaces to return empty (namespace doesn't exist)
    with patch.object(
        db_service,
        'get_user_namespaces',
    ) as mock_get_user_ns:
        mock_get_user_ns.return_value = []

        result = namespace_service.create_topic(
            'test-subject',
            'nonexistent-namespace',
            'test-topic',
        )

        assert result['status'] == 'failure'
        assert 'not found' in result['message'].lower()


# ============================================================================
# WebService Error Path Tests
# ============================================================================


def test_create_user_iam_fails_namespace_succeeds() -> None:
    """Test create_user handles IAM failure (namespace cleanup)."""
    mock_namespace = MagicMock(spec=NamespaceService)

    iam_service = MagicMock()
    iam_service.create_user_and_policy.return_value = {
        'status': 'failure',
        'message': 'IAM service unavailable',
    }

    mock_kafka = MagicMock()
    web_service = WebService(
        iam_service=iam_service,
        kafka_service=mock_kafka,
        namespace_service=mock_namespace,
    )

    result = web_service.create_user('test-subject')

    assert result['status'] == 'failure'
    assert 'Failed to create IAM user' in result['message']
    # Should not create namespace if IAM fails
    mock_namespace.create_namespace.assert_not_called()


def test_create_user_namespace_fails_cleanup() -> None:
    """Test create_user handles namespace failure and cleans up IAM."""
    mock_namespace = MagicMock(spec=NamespaceService)

    iam_service = MagicMock()
    iam_service.create_user_and_policy.return_value = {
        'status': 'success',
        'message': 'User created',
    }
    iam_service.delete_user_and_policy.return_value = {
        'status': 'success',
        'message': 'User deleted',
    }

    mock_namespace.generate_default.return_value = 'ns-test123'
    mock_namespace.create_namespace.return_value = {
        'status': 'failure',
        'message': 'Namespace creation failed',
    }

    mock_kafka = MagicMock()
    web_service = WebService(
        iam_service=iam_service,
        kafka_service=mock_kafka,
        namespace_service=mock_namespace,
    )

    result = web_service.create_user('test-subject')

    assert result['status'] == 'failure'
    assert 'Failed to create namespace' in result['message']
    # Should attempt to clean up IAM user
    iam_service.delete_user_and_policy.assert_called_once_with('test-subject')


def test_create_topic_dynamodb_succeeds_kafka_fails() -> None:
    """Test create_topic handles Kafka failure and cleans up DynamoDB."""
    namespace_service = MagicMock(spec=NamespaceService)
    namespace_service.add_topic.return_value = (
        {
            'status': 'success',
            'message': 'Topic created',
            'topics': ['test-topic'],
        },
        True,
    )
    namespace_service.delete_topic.return_value = {
        'status': 'success',
        'message': 'Topic deleted',
    }

    kafka_service = MagicMock()
    kafka_service.create_topic.return_value = {
        'status': 'failure',
        'message': 'Kafka connection failed',
    }

    mock_iam = MagicMock()
    web_service = WebService(
        iam_service=mock_iam,
        kafka_service=kafka_service,
        namespace_service=namespace_service,
    )

    result = web_service.create_topic(
        'test-subject',
        'test-namespace',
        'test-topic',
    )

    assert result['status'] == 'failure'
    assert 'Failed to create Kafka topic' in result['message']
    # Should attempt to clean up DynamoDB entry
    namespace_service.delete_topic.assert_called_once_with(
        'test-subject',
        'test-namespace',
        'test-topic',
    )


def test_delete_topic_kafka_fails_dynamodb_succeeds() -> None:
    """Test delete_topic handles Kafka failure but DynamoDB succeeds."""
    mock_iam = MagicMock()
    kafka_service = MagicMock()
    namespace_service = MagicMock(spec=NamespaceService)
    namespace_service.dynamodb = MagicMock()
    namespace_service.dynamodb.get_user_namespaces.return_value = [
        'test-namespace',
    ]

    kafka_service.delete_topic.return_value = {
        'status': 'failure',
        'message': 'Kafka connection failed',
    }

    namespace_service.delete_topic.return_value = {
        'status': 'success',
        'message': 'Topic deleted from DynamoDB',
        'topics': [],
    }

    web_service = WebService(
        iam_service=mock_iam,
        kafka_service=kafka_service,
        namespace_service=namespace_service,
    )

    result = web_service.delete_topic(
        'test-subject',
        'test-namespace',
        'test-topic',
    )

    # Should return failure because Kafka failed
    assert result['status'] == 'failure'
    assert 'Failed to delete Kafka topic' in result['message']
    # DynamoDB entry is kept so the Kafka topic isn't orphaned
    namespace_service.delete_topic.assert_not_called()


def test_delete_topic_namespace_of_another_user() -> None:
    """Test delete_topic never calls Kafka for a namespace not owned."""
    kafka_service = MagicMock()
    namespace_service = MagicMock(spec=NamespaceService)
    namespace_service.dynamodb = MagicMock()
    # The subject owns only its own namespace
    namespace_service.dynamodb.get_user_namespaces.return_value = [
        'ns-mine',
    ]

    web_service = WebService(
        iam_service=MagicMock(),
        kafka_service=kafka_service,
        namespace_service=namespace_service,
    )

    result = web_service.delete_topic(
        'test-subject',
        'ns-someone-else',
        'test-topic',
    )

    assert result['status'] == 'failure'
    assert 'Namespace ns-someone-else not found' in result['message']
    kafka_service.delete_topic.assert_not_called()
    namespace_service.delete_topic.assert_not_called()


def test_delete_topic_name_with_dot() -> None:
    """Test delete_topic never calls Kafka for a topic name with a dot."""
    kafka_service = MagicMock()
    namespace_service = MagicMock(spec=NamespaceService)
    namespace_service.dynamodb = MagicMock()
    namespace_service.dynamodb.get_user_namespaces.return_value = [
        'ns-mine',
    ]

    web_service = WebService(
        iam_service=MagicMock(),
        kafka_service=kafka_service,
        namespace_service=namespace_service,
    )

    result = web_service.delete_topic('test-subject', 'ns-mine', 'b.topic')

    assert result['status'] == 'failure'
    assert 'Invalid topic name b.topic' in result['message']
    kafka_service.delete_topic.assert_not_called()
    namespace_service.delete_topic.assert_not_called()


def test_delete_topic_owned_but_not_registered() -> None:
    """Test delete_topic still deletes an owner's topic missing in DynamoDB.

    A Kafka topic can outlive its DynamoDB entry (for example after a
    failed create's cleanup), so the owner must still be able to delete it.
    """
    kafka_service = MagicMock()
    kafka_service.delete_topic.return_value = {
        'status': 'success',
        'message': 'Topic deleted',
    }
    namespace_service = MagicMock(spec=NamespaceService)
    namespace_service.dynamodb = MagicMock()
    namespace_service.dynamodb.get_user_namespaces.return_value = [
        'test-namespace',
    ]
    namespace_service.delete_topic.return_value = {
        'status': 'success',
        'message': 'Topic test-topic not found in test-namespace',
        'topics': ['other-topic'],
    }

    web_service = WebService(
        iam_service=MagicMock(),
        kafka_service=kafka_service,
        namespace_service=namespace_service,
    )

    result = web_service.delete_topic(
        'test-subject',
        'test-namespace',
        'test-topic',
    )

    assert result['status'] == 'success'
    kafka_service.delete_topic.assert_called_once_with(
        'test-namespace',
        'test-topic',
    )
    namespace_service.delete_topic.assert_called_once_with(
        'test-subject',
        'test-namespace',
        'test-topic',
    )


def test_delete_topic_owned() -> None:
    """Test delete_topic deletes from Kafka, then DynamoDB, when owned."""
    kafka_service = MagicMock()
    kafka_service.delete_topic.return_value = {
        'status': 'success',
        'message': 'Topic deleted',
    }
    namespace_service = MagicMock(spec=NamespaceService)
    namespace_service.dynamodb = MagicMock()
    namespace_service.dynamodb.get_user_namespaces.return_value = [
        'test-namespace',
    ]
    namespace_service.delete_topic.return_value = {
        'status': 'success',
        'message': 'Topic test-topic deleted from test-namespace',
        'topics': [],
    }

    web_service = WebService(
        iam_service=MagicMock(),
        kafka_service=kafka_service,
        namespace_service=namespace_service,
    )

    result = web_service.delete_topic(
        'test-subject',
        'test-namespace',
        'test-topic',
    )

    assert result['status'] == 'success'
    kafka_service.delete_topic.assert_called_once_with(
        'test-namespace',
        'test-topic',
    )
    namespace_service.delete_topic.assert_called_once_with(
        'test-subject',
        'test-namespace',
        'test-topic',
    )


def test_recreate_topic_topic_not_found() -> None:
    """Test recreate_topic handles topic not found in DynamoDB."""
    mock_iam = MagicMock()
    mock_kafka = MagicMock()
    namespace_service = MagicMock(spec=NamespaceService)

    # Create a mock dynamodb attribute
    mock_dynamodb = MagicMock()
    mock_dynamodb.get_user_namespaces.return_value = ['test-namespace']
    mock_dynamodb.get_namespace_topics.return_value = []
    namespace_service.dynamodb = mock_dynamodb

    web_service = WebService(
        iam_service=mock_iam,
        kafka_service=mock_kafka,
        namespace_service=namespace_service,
    )

    result = web_service.recreate_topic(
        'test-subject',
        'test-namespace',
        'nonexistent-topic',
    )

    assert result['status'] == 'failure'
    assert 'not found' in result['message'].lower()


# ============================================================================
# Additional Error Path Tests - Remaining Coverage
# ============================================================================


def test_create_keys_table_already_exists() -> None:
    """Test _create_keys_table handles table already exists gracefully."""
    db_service = DynamoDBService(
        region='us-east-1',
        keys_table_name='test-diaspora-keys-table',
        users_table_name='test-diaspora-users-table',
        namespace_table_name='test-diaspora-namespace-table',
    )

    mock_dynamodb = MagicMock()

    # Create a mock exception for ResourceInUseException
    class MockResourceInUseError(ClientError):
        """Mock ResourceInUseException for testing."""

        def __init__(self, operation_name: str = 'CreateTable'):
            error_response = {
                'Error': {
                    'Code': 'ResourceInUseException',
                    'Message': 'Table already exists',
                },
            }
            super().__init__(error_response, operation_name)

    mock_exceptions = MagicMock()
    mock_exceptions.ResourceInUseException = MockResourceInUseError
    mock_dynamodb.exceptions = mock_exceptions
    db_service.dynamodb = mock_dynamodb

    # Mock create_table to raise ResourceInUseException
    mock_dynamodb.create_table.side_effect = MockResourceInUseError(
        'CreateTable',
    )

    # Should not raise exception (suppressed by contextlib.suppress)
    db_service._create_keys_table()

    # Should have attempted to create table
    mock_dynamodb.create_table.assert_called_once()


def test_create_users_table_already_exists() -> None:
    """Test _create_users_table handles table already exists gracefully."""
    db_service = DynamoDBService(
        region='us-east-1',
        keys_table_name='test-diaspora-keys-table',
        users_table_name='test-diaspora-users-table',
        namespace_table_name='test-diaspora-namespace-table',
    )

    mock_dynamodb = MagicMock()

    class MockResourceInUseError(ClientError):
        """Mock ResourceInUseException for testing."""

        def __init__(self, operation_name: str = 'CreateTable'):
            error_response = {
                'Error': {
                    'Code': 'ResourceInUseException',
                    'Message': 'Table already exists',
                },
            }
            super().__init__(error_response, operation_name)

    mock_exceptions = MagicMock()
    mock_exceptions.ResourceInUseException = MockResourceInUseError
    mock_dynamodb.exceptions = mock_exceptions
    db_service.dynamodb = mock_dynamodb

    mock_dynamodb.create_table.side_effect = MockResourceInUseError(
        'CreateTable',
    )

    # Should not raise exception
    db_service._create_users_table()

    mock_dynamodb.create_table.assert_called_once()


def test_create_namespace_table_already_exists() -> None:
    """Test _create_namespace_table handles table already exists gracefully."""
    db_service = DynamoDBService(
        region='us-east-1',
        keys_table_name='test-diaspora-keys-table',
        users_table_name='test-diaspora-users-table',
        namespace_table_name='test-diaspora-namespace-table',
    )

    mock_dynamodb = MagicMock()

    class MockResourceInUseError(ClientError):
        """Mock ResourceInUseException for testing."""

        def __init__(self, operation_name: str = 'CreateTable'):
            error_response = {
                'Error': {
                    'Code': 'ResourceInUseException',
                    'Message': 'Table already exists',
                },
            }
            super().__init__(error_response, operation_name)

    mock_exceptions = MagicMock()
    mock_exceptions.ResourceInUseException = MockResourceInUseError
    mock_dynamodb.exceptions = mock_exceptions
    db_service.dynamodb = mock_dynamodb

    mock_dynamodb.create_table.side_effect = MockResourceInUseError(
        'CreateTable',
    )

    # Should not raise exception
    db_service._create_namespace_table()

    mock_dynamodb.create_table.assert_called_once()


def test_add_namespace_topic_table_creation_retry() -> None:
    """Test add_namespace_topic handles table creation and retry."""
    db_service = DynamoDBService(
        region='us-east-1',
        keys_table_name='test-diaspora-keys-table',
        users_table_name='test-diaspora-users-table',
        namespace_table_name='test-diaspora-namespace-table',
    )

    mock_dynamodb = MagicMock()

    class MockResourceNotFoundError(ClientError):
        """Mock ResourceNotFoundError for testing."""

        def __init__(self, operation_name: str = 'UpdateItem'):
            error_response = {
                'Error': {
                    'Code': 'ResourceNotFoundException',
                    'Message': 'Table not found',
                },
            }
            super().__init__(error_response, operation_name)

    mock_exceptions = MagicMock()
    mock_exceptions.ResourceNotFoundException = MockResourceNotFoundError
    mock_dynamodb.exceptions = mock_exceptions
    db_service.dynamodb = mock_dynamodb

    # First call raises ResourceNotFoundError, second succeeds
    mock_dynamodb.update_item.side_effect = [
        MockResourceNotFoundError('UpdateItem'),
        None,  # Second call succeeds
    ]

    # Mock table creation
    with patch.object(
        db_service,
        '_create_namespace_table',
    ) as mock_create_table:
        mock_create_table.return_value = None

        # Should not raise exception
        db_service.add_namespace_topic('test-namespace', 'test-topic')

        # Should have called create_table and retried update_item
        mock_create_table.assert_called_once()
        assert mock_dynamodb.update_item.call_count == EXPECTED_RETRY_COUNT


def test_delete_namespace_with_topics() -> None:
    """Test delete_namespace handles namespace with topics."""
    # Create a real NamespaceService with mocked DynamoDBService
    mock_db_service = MagicMock(spec=DynamoDBService)
    namespace_service = NamespaceService(dynamodb_service=mock_db_service)

    # Mock that namespace exists and has topics
    mock_db_service.get_user_namespaces.return_value = ['ns-test']
    # Mock remove operations
    mock_db_service.remove_user_namespace.return_value = None
    mock_db_service.remove_global_namespace.return_value = None
    # After deletion, user has no namespaces
    mock_db_service.get_user_namespaces.side_effect = [
        ['ns-test'],  # First call: namespace exists
        [],  # Second call: after deletion, empty
    ]

    result = namespace_service.delete_namespace('test-subject', 'ns-test')

    assert result['status'] == 'success'
    assert 'deleted' in result['message'].lower()
    assert 'namespaces' in result
    # The namespace's topics item is deleted with it, so a later owner of
    # the name does not inherit its topics
    mock_db_service.delete_namespace_topics.assert_called_once_with('ns-test')


def test_delete_topic_namespace_not_found() -> None:
    """Test delete_topic handles namespace doesn't exist."""
    # Create a real NamespaceService with mocked DynamoDBService
    mock_db_service = MagicMock(spec=DynamoDBService)
    namespace_service = NamespaceService(dynamodb_service=mock_db_service)

    # Mock that namespace doesn't exist for user
    mock_db_service.get_user_namespaces.return_value = []

    result = namespace_service.delete_topic(
        'test-subject',
        'nonexistent-namespace',
        'test-topic',
    )

    assert result['status'] == 'failure'
    assert 'not found' in result['message'].lower()
    assert 'namespace' in result['message'].lower()


def test_list_namespace_and_topics_empty() -> None:
    """Test list_namespace_and_topics handles user with no namespaces."""
    # Create a real NamespaceService with mocked DynamoDBService
    mock_db_service = MagicMock(spec=DynamoDBService)
    namespace_service = NamespaceService(dynamodb_service=mock_db_service)

    # Mock that user has no namespaces
    mock_db_service.get_user_namespaces.return_value = []

    result = namespace_service.list_namespace_and_topics('test-subject')

    assert result['status'] == 'success'
    assert result['namespaces'] == {}
    assert '0 namespaces' in result['message']


def test_create_user_namespace_already_exists() -> None:
    """Test create_user handles namespace already exists scenario."""
    mock_iam = MagicMock()
    mock_iam.create_user_and_policy.return_value = {
        'status': 'success',
        'message': 'User created',
    }

    mock_namespace = MagicMock(spec=NamespaceService)
    mock_namespace.generate_default.return_value = 'ns-test123'
    # Namespace creation returns failure (already exists)
    mock_namespace.create_namespace.return_value = {
        'status': 'failure',
        'message': 'Namespace already taken: ns-test123',
    }

    web_service = WebService(
        iam_service=mock_iam,
        kafka_service=MagicMock(),
        namespace_service=mock_namespace,
    )

    result = web_service.create_user('test-subject')

    assert result['status'] == 'failure'
    assert 'Failed to create namespace' in result['message']
    # Should attempt to clean up IAM user
    mock_iam.delete_user_and_policy.assert_called_once_with('test-subject')


def test_delete_user_partial_failures() -> None:
    """Test delete_user handles partial failures."""
    mock_iam = MagicMock()
    mock_iam.delete_user_and_policy.return_value = {
        'status': 'failure',
        'message': 'IAM user not found',
    }

    mock_namespace = MagicMock(spec=NamespaceService)
    mock_namespace.dynamodb = MagicMock()
    mock_namespace.dynamodb.get_user_namespaces.return_value = ['ns-test123']
    mock_namespace.generate_default.return_value = 'ns-test123'
    mock_namespace.dynamodb.get_namespace_topics.return_value = []
    mock_namespace.delete_namespace.return_value = {
        'status': 'success',
        'message': 'Namespace deleted',
    }
    mock_namespace.dynamodb.delete_key.return_value = None

    web_service = WebService(
        iam_service=mock_iam,
        kafka_service=MagicMock(),
        namespace_service=mock_namespace,
    )

    result = web_service.delete_user('test-subject')

    # Should return failure if IAM deletion fails
    assert result['status'] == 'failure'
    assert 'Failed to delete IAM user' in result['message']


def test_create_key_iam_fails() -> None:
    """Test create_key handles IAM key creation failure."""
    mock_iam = MagicMock()
    mock_iam.delete_access_keys.return_value = {'status': 'success'}
    mock_iam.create_access_key.return_value = {
        'status': 'failure',
        'message': 'IAM limit exceeded',
    }

    mock_namespace = MagicMock(spec=NamespaceService)
    mock_namespace.dynamodb = MagicMock()
    # Mock get_key to return None (no existing key)
    mock_namespace.dynamodb.get_key.return_value = None

    web_service = WebService(
        iam_service=mock_iam,
        kafka_service=MagicMock(),
        namespace_service=mock_namespace,
    )

    # Mock create_user to succeed
    with patch.object(web_service, '_create_user') as mock_create_user:
        mock_create_user.return_value = {
            'status': 'success',
            'message': 'User created',
        }

        result = web_service.create_key('test-subject')

        assert result['status'] == 'failure'
        assert 'Failed to create IAM access key' in result['message']


def test_create_key_dynamodb_storage_fails() -> None:
    """Test create_key handles DynamoDB storage failure."""
    mock_iam = MagicMock()
    mock_iam.delete_access_keys.return_value = {'status': 'success'}
    mock_iam.create_access_key.return_value = {
        'status': 'success',
        'access_key': 'AKIAIOSFODNN7EXAMPLE',  # pragma: allowlist secret
        'secret_key': 'test-secret',  # pragma: allowlist secret
        'create_date': '2024-01-01T00:00:00',
    }

    mock_namespace = MagicMock(spec=NamespaceService)
    mock_namespace.dynamodb = MagicMock()
    # Mock get_key to return None (no existing key)
    mock_namespace.dynamodb.get_key.return_value = None
    # Mock store_key to raise exception
    mock_namespace.dynamodb.store_key.side_effect = Exception(
        'DynamoDB write failed',
    )

    web_service = WebService(
        iam_service=mock_iam,
        kafka_service=MagicMock(),
        namespace_service=mock_namespace,
    )

    with patch.object(web_service, '_create_user') as mock_create_user:
        mock_create_user.return_value = {
            'status': 'success',
            'message': 'User created',
        }

        # Should raise exception (not caught in create_key)
        with pytest.raises(Exception, match='DynamoDB write failed'):
            web_service.create_key('test-subject')


def test_create_topic_kafka_succeeds_dynamodb_fails() -> None:
    """Test create_topic handles Kafka succeeds but DynamoDB fails."""
    mock_kafka = MagicMock()
    mock_kafka.create_topic.return_value = {
        'status': 'success',
        'message': 'Topic created',
    }

    mock_namespace = MagicMock(spec=NamespaceService)
    # First call succeeds (creates in DynamoDB), but then fails on cleanup
    mock_namespace.add_topic.return_value = (
        {
            'status': 'success',
            'message': 'Topic created',
            'topics': ['test-topic'],
        },
        True,
    )
    # Mock delete_topic to fail (cleanup fails)
    mock_namespace.delete_topic.side_effect = Exception('Cleanup failed')

    web_service = WebService(
        iam_service=MagicMock(),
        kafka_service=mock_kafka,
        namespace_service=mock_namespace,
    )

    # This scenario is actually handled - if create_topic in namespace
    # succeeds, then kafka succeeds, so no cleanup needed.
    # Let's test the reverse:
    # DynamoDB succeeds, Kafka fails (already tested)
    # For this test, let's verify the cleanup path when Kafka fails
    mock_namespace.add_topic.return_value = (
        {
            'status': 'success',
            'message': 'Topic created',
            'topics': ['test-topic'],
        },
        True,
    )
    mock_kafka.create_topic.return_value = {
        'status': 'failure',
        'message': 'Kafka connection failed',
    }
    mock_namespace.delete_topic.return_value = {
        'status': 'success',
        'message': 'Topic deleted',
    }

    result = web_service.create_topic(
        'test-subject',
        'ns-test',
        'test-topic',
    )

    # Should return failure from Kafka
    assert result['status'] == 'failure'
    assert 'Kafka topic' in result['message']
    # Should attempt cleanup
    mock_namespace.delete_topic.assert_called_once()


def test_recreate_topic_namespace_not_found() -> None:
    """Test recreate_topic handles namespace doesn't exist."""
    mock_namespace = MagicMock(spec=NamespaceService)
    mock_namespace.dynamodb = MagicMock()
    # Mock that namespace doesn't exist
    mock_namespace.dynamodb.get_user_namespaces.return_value = []

    web_service = WebService(
        iam_service=MagicMock(),
        kafka_service=MagicMock(),
        namespace_service=mock_namespace,
    )

    result = web_service.recreate_topic(
        'test-subject',
        'nonexistent-namespace',
        'test-topic',
    )

    assert result['status'] == 'failure'
    assert 'not found' in result['message'].lower()
    assert 'namespace' in result['message'].lower()


def test_generate_user_policy_edge_cases() -> None:
    """Test generate_user_policy handles edge cases with subject formatting."""
    iam_service = IAMService(
        account_id='123456789012',
        region='us-east-1',
        cluster_name='test-cluster',
    )

    # Test with subject that has many dashes
    subject_many_dashes = '123e4567-e89b-12d3-a456-426614174000-extra-dashes'
    policy = iam_service.generate_user_policy(subject_many_dashes)

    assert 'Version' in policy
    assert 'Statement' in policy
    # Should extract last 12 chars after removing dashes
    subject_suffix = subject_many_dashes.replace('-', '')[-12:]
    assert f'ns-{subject_suffix}' in str(policy)

    # Test with very short subject
    short_subject = 'abc'
    policy_short = iam_service.generate_user_policy(short_subject)
    assert 'Version' in policy_short
    assert 'Statement' in policy_short

    # Test with subject without dashes
    subject_no_dashes = (
        '123e4567e89b12d3a456426614174000'  # pragma: allowlist secret
    )
    policy_no_dashes = iam_service.generate_user_policy(subject_no_dashes)
    assert 'Version' in policy_no_dashes
    # Should use last 12 chars
    assert subject_no_dashes[-12:] in str(policy_no_dashes)


# ============================================================================
# Management API Failure Paths
# ============================================================================


def _namespace_service_with_mock_db(
    namespaces: list[str],
    topics: list[str],
) -> tuple[NamespaceService, MagicMock]:
    """Build a real NamespaceService over a mocked DynamoDBService."""
    mock_db_service = MagicMock(spec=DynamoDBService)
    mock_db_service.get_user_namespaces.return_value = namespaces
    mock_db_service.get_namespace_topics.return_value = topics
    return NamespaceService(dynamodb_service=mock_db_service), mock_db_service


def _track_topics(mock_db: MagicMock, topics: set[str]) -> None:
    """Back a mocked DynamoDBService's topic calls with a set."""
    mock_db.get_namespace_topics.side_effect = lambda _ns: sorted(topics)
    mock_db.add_namespace_topic.side_effect = lambda _ns, t: topics.add(t)
    mock_db.remove_namespace_topic.side_effect = lambda _ns, t: topics.discard(
        t,
    )


def test_create_topic_kafka_fails_keeps_existing_entry() -> None:
    """Test a failed create of an already listed topic keeps its entry."""
    namespace_service, mock_db = _namespace_service_with_mock_db(
        ['test-namespace'],
        [],
    )
    topics = {'test-topic'}
    _track_topics(mock_db, topics)
    kafka_service = MagicMock()
    kafka_service.create_topic.return_value = {
        'status': 'failure',
        'message': 'Kafka connection failed',
    }
    web_service = WebService(
        iam_service=MagicMock(),
        kafka_service=kafka_service,
        namespace_service=namespace_service,
    )

    result = web_service.create_topic(
        'test-subject',
        'test-namespace',
        'test-topic',
    )

    assert result['status'] == 'failure'
    assert 'Failed to create Kafka topic' in result['message']
    # The entry was there before this call, so it stays
    assert topics == {'test-topic'}


def test_create_topic_kafka_fails_removes_added_entry() -> None:
    """Test a failed create of a new topic removes the entry it added."""
    namespace_service, mock_db = _namespace_service_with_mock_db(
        ['test-namespace'],
        [],
    )
    topics: set[str] = set()
    _track_topics(mock_db, topics)
    kafka_service = MagicMock()
    kafka_service.create_topic.return_value = {
        'status': 'failure',
        'message': 'Kafka connection failed',
    }
    web_service = WebService(
        iam_service=MagicMock(),
        kafka_service=kafka_service,
        namespace_service=namespace_service,
    )

    result = web_service.create_topic(
        'test-subject',
        'test-namespace',
        'test-topic',
    )

    assert result['status'] == 'failure'
    mock_db.add_namespace_topic.assert_called_once_with(
        'test-namespace',
        'test-topic',
    )
    # The entry this call added is gone again
    assert topics == set()


def test_delete_key_iam_fails() -> None:
    """Test delete_key reports failure and keeps the cached key."""
    mock_iam = MagicMock()
    mock_iam.delete_access_keys.return_value = {
        'status': 'failure',
        'message': (
            'Failed to delete access keys: IAM did not complete the request'
        ),
    }
    mock_namespace = MagicMock(spec=NamespaceService)
    mock_namespace.dynamodb = MagicMock()
    web_service = WebService(
        iam_service=mock_iam,
        kafka_service=MagicMock(),
        namespace_service=mock_namespace,
    )

    result = web_service.delete_key('test-subject')

    assert result['status'] == 'failure'
    assert 'Failed to delete IAM access keys' in result['message']
    assert 'IAM did not complete the request' in result['message']
    mock_namespace.dynamodb.delete_key.assert_not_called()


def test_create_key_old_key_delete_fails() -> None:
    """Test create_key fails when the old IAM keys can't be deleted."""
    mock_iam = MagicMock()
    mock_iam.delete_access_keys.return_value = {
        'status': 'failure',
        'message': (
            'Failed to delete access keys: IAM did not complete the request'
        ),
    }
    mock_namespace = MagicMock(spec=NamespaceService)
    mock_namespace.dynamodb = MagicMock()
    mock_namespace.dynamodb.get_key.return_value = None
    web_service = WebService(
        iam_service=mock_iam,
        kafka_service=MagicMock(),
        namespace_service=mock_namespace,
    )

    with patch.object(web_service, '_create_user') as mock_create_user:
        mock_create_user.return_value = {
            'status': 'success',
            'message': 'User created',
        }

        result = web_service.create_key('test-subject')

    assert result['status'] == 'failure'
    assert 'Failed to delete existing IAM access keys' in result['message']
    mock_iam.create_access_key.assert_not_called()
    mock_namespace.dynamodb.store_key.assert_not_called()


def test_delete_user_kafka_fails() -> None:
    """Test delete_user stops and keeps its records when Kafka fails."""
    mock_iam = MagicMock()
    kafka_service = MagicMock()
    kafka_service.delete_topic.return_value = {
        'status': 'failure',
        'message': 'Failed to delete Kafka topic ns-test123.topic-a',
    }
    mock_namespace = MagicMock(spec=NamespaceService)
    mock_namespace.dynamodb = MagicMock()
    mock_namespace.dynamodb.get_user_namespaces.return_value = ['ns-test123']
    mock_namespace.generate_default.return_value = 'ns-test123'
    mock_namespace.dynamodb.get_namespace_topics.return_value = [
        'topic-a',
        'topic-b',
    ]
    web_service = WebService(
        iam_service=mock_iam,
        kafka_service=kafka_service,
        namespace_service=mock_namespace,
    )

    result = web_service.delete_user('test-subject')

    assert result['status'] == 'failure'
    assert 'Failed to delete Kafka topic' in result['message']
    kafka_service.delete_topic.assert_called_once_with('ns-test123', 'topic-a')
    mock_namespace.delete_topic.assert_not_called()
    mock_namespace.delete_namespace.assert_not_called()
    mock_namespace.dynamodb.delete_key.assert_not_called()
    mock_iam.delete_user_and_policy.assert_not_called()


def test_delete_user_default_namespace_not_owned() -> None:
    """Test delete_user leaves a default namespace it doesn't own alone."""
    mock_iam = MagicMock()
    mock_iam.delete_user_and_policy.return_value = {
        'status': 'success',
        'message': 'IAM user deleted',
    }
    kafka_service = MagicMock()
    namespace_service, mock_db = _namespace_service_with_mock_db(
        [],
        ['someone-elses-topic'],
    )
    web_service = WebService(
        iam_service=mock_iam,
        kafka_service=kafka_service,
        namespace_service=namespace_service,
    )

    result = web_service.delete_user('test-subject')

    assert result['status'] == 'success'
    kafka_service.delete_topic.assert_not_called()
    mock_db.remove_namespace_topic.assert_not_called()
    mock_db.delete_namespace_topics.assert_not_called()
    mock_db.remove_global_namespace.assert_not_called()


def test_delete_namespace_topics_deletes_item() -> None:
    """Test delete_namespace_topics deletes the namespace's item."""
    db_service = DynamoDBService(
        region='us-east-1',
        keys_table_name='test-diaspora-keys-table',
        users_table_name='test-diaspora-users-table',
        namespace_table_name='test-diaspora-namespace-table',
    )
    with patch.object(db_service.dynamodb, 'delete_item') as mock_delete:
        db_service.delete_namespace_topics('ns-test')

    mock_delete.assert_called_once_with(
        TableName='test-diaspora-namespace-table',
        Key={'namespace': {'S': 'ns-test'}},
    )


def test_delete_namespace_topics_table_missing() -> None:
    """Test delete_namespace_topics is a no-op when the table is missing."""
    db_service = DynamoDBService(
        region='us-east-1',
        keys_table_name='test-diaspora-keys-table',
        users_table_name='test-diaspora-users-table',
        namespace_table_name='test-diaspora-namespace-table',
    )
    with Stubber(db_service.dynamodb) as stubber:
        stubber.add_client_error(
            'delete_item',
            service_error_code='ResourceNotFoundException',
        )

        db_service.delete_namespace_topics('ns-test')

        stubber.assert_no_pending_responses()


def test_recreate_topic_topic_still_exists() -> None:
    """Test recreate_topic fails if the topic still exists after retries."""
    kafka_service = KafkaService(
        bootstrap_servers='test-servers',
        region='us-east-1',
    )

    with (
        patch('web_service.services.KafkaAdminClient') as mock_admin_class,
        patch('time.sleep'),
    ):
        mock_admin = MagicMock()
        mock_admin_class.return_value = mock_admin
        mock_admin.create_topics.side_effect = TopicAlreadyExistsError(
            'Topic already exists',
        )

        result = kafka_service.recreate_topic('test-namespace', 'test-topic')

    assert result is not None
    assert result['status'] == 'failure'
    assert 'Failed to create Kafka topic' in result['message']
    assert 'after 3 attempts' in result['message']
    assert mock_admin.create_topics.call_count == 3


def test_recreate_topic_retries_while_topic_exists() -> None:
    """Test recreate_topic retries a create that finds the topic present."""
    kafka_service = KafkaService(
        bootstrap_servers='test-servers',
        region='us-east-1',
    )

    with (
        patch('web_service.services.KafkaAdminClient') as mock_admin_class,
        patch('time.sleep'),
    ):
        mock_admin = MagicMock()
        mock_admin_class.return_value = mock_admin
        mock_admin.create_topics.side_effect = [
            TopicAlreadyExistsError('Topic already exists'),
            None,
        ]

        result = kafka_service.recreate_topic('test-namespace', 'test-topic')

    assert result == {'status': 'success', 'message': 'Topic recreated'}
    assert mock_admin.create_topics.call_count == EXPECTED_RETRY_COUNT


def test_create_topic_already_exists_is_success() -> None:
    """Test create_topic still treats an existing topic as success."""
    kafka_service = KafkaService(
        bootstrap_servers='test-servers',
        region='us-east-1',
    )

    with patch('web_service.services.KafkaAdminClient') as mock_admin_class:
        mock_admin = MagicMock()
        mock_admin_class.return_value = mock_admin
        mock_admin.create_topics.side_effect = TopicAlreadyExistsError(
            'Topic already exists',
        )

        result = kafka_service.create_topic('test-namespace', 'test-topic')

    assert result == {'status': 'success', 'message': 'Topic already exists'}
    mock_admin.create_topics.assert_called_once()


def test_delete_topic_kafka_only_reports_deleted() -> None:
    """Test deleting an owner's Kafka-only topic reports its deletion."""
    kafka_service = MagicMock()
    kafka_service.delete_topic.return_value = {
        'status': 'success',
        'message': 'Topic deleted',
    }
    namespace_service, mock_db = _namespace_service_with_mock_db(
        ['test-namespace'],
        ['other-topic'],
    )
    web_service = WebService(
        iam_service=MagicMock(),
        kafka_service=kafka_service,
        namespace_service=namespace_service,
    )

    result = web_service.delete_topic(
        'test-subject',
        'test-namespace',
        'test-topic',
    )

    assert result == {
        'status': 'success',
        'message': 'Topic test-topic deleted from test-namespace',
        'topics': ['other-topic'],
    }
    mock_db.remove_namespace_topic.assert_not_called()


def test_delete_topic_absent_everywhere_reports_not_found() -> None:
    """Test deleting a topic absent from Kafka and DynamoDB says so."""
    kafka_service = MagicMock()
    kafka_service.delete_topic.return_value = {
        'status': 'success',
        'message': 'Topic does not exist',
    }
    namespace_service, _ = _namespace_service_with_mock_db(
        ['test-namespace'],
        [],
    )
    web_service = WebService(
        iam_service=MagicMock(),
        kafka_service=kafka_service,
        namespace_service=namespace_service,
    )

    result = web_service.delete_topic(
        'test-subject',
        'test-namespace',
        'test-topic',
    )

    assert result['status'] == 'success'
    assert result['message'] == 'Topic test-topic not found in test-namespace'


def test_delete_user_kafka_topic_already_gone() -> None:
    """Test delete_user goes on when Kafka says a topic doesn't exist."""
    mock_iam = MagicMock()
    mock_iam.delete_user_and_policy.return_value = {
        'status': 'success',
        'message': 'IAM user deleted',
    }
    kafka_service = MagicMock()
    kafka_service.delete_topic.return_value = {
        'status': 'success',
        'message': 'Topic does not exist',
    }
    mock_namespace = MagicMock(spec=NamespaceService)
    mock_namespace.dynamodb = MagicMock()
    mock_namespace.dynamodb.get_user_namespaces.return_value = ['ns-test123']
    mock_namespace.generate_default.return_value = 'ns-test123'
    mock_namespace.dynamodb.get_namespace_topics.return_value = ['topic-a']
    web_service = WebService(
        iam_service=mock_iam,
        kafka_service=kafka_service,
        namespace_service=mock_namespace,
    )

    result = web_service.delete_user('test-subject')

    assert result == {
        'status': 'success',
        'message': 'User test-subject deleted',
    }
    mock_namespace.delete_topic.assert_called_once_with(
        'test-subject',
        'ns-test123',
        'topic-a',
    )
    mock_namespace.delete_namespace.assert_called_once_with(
        'test-subject',
        'ns-test123',
    )
    mock_iam.delete_user_and_policy.assert_called_once_with('test-subject')


def test_delete_user_second_kafka_topic_fails() -> None:
    """Test delete_user keeps the rows from the first failed Kafka delete."""
    mock_iam = MagicMock()
    kafka_service = MagicMock()
    kafka_service.delete_topic.side_effect = [
        {'status': 'success', 'message': 'Topic deleted'},
        {
            'status': 'failure',
            'message': 'Failed to delete Kafka topic ns-test123.topic-b',
        },
    ]
    namespace_service, mock_db = _namespace_service_with_mock_db(
        ['ns-test123'],
        [],
    )
    topics = {'topic-a', 'topic-b'}
    _track_topics(mock_db, topics)
    web_service = WebService(
        iam_service=mock_iam,
        kafka_service=kafka_service,
        namespace_service=namespace_service,
    )

    with patch.object(
        namespace_service,
        'generate_default',
        return_value='ns-test123',
    ):
        result = web_service.delete_user('test-subject')

    assert result['status'] == 'failure'
    assert 'ns-test123.topic-b' in result['message']
    # The first topic's row went with it; the second's row and the
    # namespace stay, and IAM is untouched
    assert topics == {'topic-b'}
    mock_db.delete_namespace_topics.assert_not_called()
    mock_db.remove_user_namespace.assert_not_called()
    mock_iam.delete_user_and_policy.assert_not_called()


def test_delete_namespace_deletes_topics_item_first() -> None:
    """Test delete_namespace drops the topics item before the name."""
    namespace_service, mock_db = _namespace_service_with_mock_db(
        ['ns-test'],
        [],
    )

    namespace_service.delete_namespace('test-subject', 'ns-test')

    removals = [
        c
        for c in mock_db.mock_calls
        if c[0]
        in (
            'delete_namespace_topics',
            'remove_user_namespace',
            'remove_global_namespace',
        )
    ]
    assert removals == [
        call.delete_namespace_topics('ns-test'),
        call.remove_user_namespace('test-subject', 'ns-test'),
        call.remove_global_namespace('ns-test'),
    ]


def test_delete_namespace_topics_item_fails_keeps_name() -> None:
    """Test a failed topics item delete leaves the namespace owned."""
    namespace_service, mock_db = _namespace_service_with_mock_db(
        ['ns-test'],
        [],
    )
    mock_db.delete_namespace_topics.side_effect = Exception('DynamoDB down')

    with pytest.raises(Exception, match='DynamoDB down'):
        namespace_service.delete_namespace('test-subject', 'ns-test')

    mock_db.remove_user_namespace.assert_not_called()
    mock_db.remove_global_namespace.assert_not_called()


def test_delete_namespace_waits_for_topic_changes() -> None:
    """Test delete_namespace runs under the lock topic changes take."""
    namespace_service, mock_db = _namespace_service_with_mock_db(
        ['ns-test'],
        [],
    )
    results: list[dict[str, object]] = []

    with namespace_service._global_lock:
        worker = threading.Thread(
            target=lambda: results.append(
                namespace_service.delete_namespace('test-subject', 'ns-test'),
            ),
        )
        worker.start()
        worker.join(timeout=0.2)
        # Held off while a topic change (here, the test) holds the lock
        assert worker.is_alive()
        mock_db.get_user_namespaces.assert_not_called()
        mock_db.delete_namespace_topics.assert_not_called()

    worker.join(timeout=5)
    assert not worker.is_alive()
    assert results[0]['status'] == 'success'
    mock_db.delete_namespace_topics.assert_called_once_with('ns-test')


def test_create_topic_exists_after_timeout_is_success() -> None:
    """Test a topic found after this call's timed-out attempt is a success.

    The timed-out attempt may have created the topic before failing.
    """
    kafka_service = KafkaService(
        bootstrap_servers='test-servers',
        region='us-east-1',
    )

    with (
        patch('web_service.services.KafkaAdminClient') as mock_admin_class,
        patch('time.sleep') as mock_sleep,
    ):
        mock_admin = MagicMock()
        mock_admin_class.return_value = mock_admin
        mock_admin.create_topics.side_effect = [
            KafkaTimeoutError('Request timed out'),
            TopicAlreadyExistsError('Topic already exists'),
        ]

        result = kafka_service.create_topic(
            'test-namespace',
            'test-topic',
            exists_ok=False,
        )

    assert result is not None
    assert result['status'] == 'success'
    assert mock_admin.create_topics.call_count == EXPECTED_RETRY_COUNT
    mock_sleep.assert_not_called()


def test_create_topic_still_exists_waits_and_fails() -> None:
    """Test a topic that stays present is retried after waits, then fails."""
    kafka_service = KafkaService(
        bootstrap_servers='test-servers',
        region='us-east-1',
    )

    with (
        patch('web_service.services.KafkaAdminClient') as mock_admin_class,
        patch('time.sleep') as mock_sleep,
    ):
        mock_admin = MagicMock()
        mock_admin_class.return_value = mock_admin
        mock_admin.create_topics.side_effect = TopicAlreadyExistsError(
            'Topic already exists',
        )

        result = kafka_service.create_topic(
            'test-namespace',
            'test-topic',
            exists_ok=False,
        )

    assert result is not None
    assert result['status'] == 'failure'
    assert 'after 3 attempts' in result['message']
    assert mock_admin.create_topics.call_count == 3
    # A wait before each retry, none after the last attempt
    assert mock_sleep.call_args_list == [call(2), call(2)]


def _list_two_keys(stubber: Stubber) -> None:
    """Stub list_access_keys to return two keys for test-user."""
    stubber.add_response(
        'list_access_keys',
        {
            'AccessKeyMetadata': [
                {
                    'UserName': 'test-user',
                    'AccessKeyId': 'TESTKEYID00000000001',
                    'Status': 'Active',
                },
                {
                    'UserName': 'test-user',
                    'AccessKeyId': 'TESTKEYID00000000002',
                    'Status': 'Active',
                },
            ],
        },
        {'UserName': 'test-user'},
    )


def test_delete_access_keys_first_key_already_gone() -> None:
    """Test delete_access_keys deletes the rest when one key is gone."""
    iam_service = IAMService(
        account_id='123456789012',
        region='us-east-1',
        cluster_name='test-cluster',
    )

    with Stubber(iam_service.iam) as stubber:
        _list_two_keys(stubber)
        stubber.add_client_error(
            'delete_access_key',
            service_error_code='NoSuchEntity',
            expected_params={
                'UserName': 'test-user',
                'AccessKeyId': 'TESTKEYID00000000001',
            },
        )
        stubber.add_response(
            'delete_access_key',
            {},
            {'UserName': 'test-user', 'AccessKeyId': 'TESTKEYID00000000002'},
        )

        result = iam_service.delete_access_keys('test-user')

        assert result['status'] == 'success'
        stubber.assert_no_pending_responses()


def test_delete_access_keys_second_key_fails() -> None:
    """Test delete_access_keys fails when a key can't be deleted."""
    iam_service = IAMService(
        account_id='123456789012',
        region='us-east-1',
        cluster_name='test-cluster',
    )

    with Stubber(iam_service.iam) as stubber:
        _list_two_keys(stubber)
        stubber.add_client_error(
            'delete_access_key',
            service_error_code='NoSuchEntity',
            expected_params={
                'UserName': 'test-user',
                'AccessKeyId': 'TESTKEYID00000000001',
            },
        )
        stubber.add_client_error(
            'delete_access_key',
            service_error_code='ServiceFailure',
            service_message='Internal failure',
            expected_params={
                'UserName': 'test-user',
                'AccessKeyId': 'TESTKEYID00000000002',
            },
        )

        result = iam_service.delete_access_keys('test-user')

        assert result['status'] == 'failure'
        assert 'Failed to delete access keys' in result['message']
        stubber.assert_no_pending_responses()


def _iam_success() -> MagicMock:
    """An IAM service mock whose calls all succeed."""
    mock_iam = MagicMock()
    mock_iam.create_user_and_policy.return_value = {'status': 'success'}
    mock_iam.delete_access_keys.return_value = {'status': 'success'}
    mock_iam.create_access_key.return_value = {
        'status': 'success',
        'access_key': 'AKIAIOSFODNN7EXAMPLE',  # pragma: allowlist secret
        'secret_key': 'test-secret',  # pragma: allowlist secret
        'create_date': '2024-01-01T00:00:00',
    }
    return mock_iam


def test_delete_key_waits_for_create_key() -> None:
    """A delete_key during create_key runs after it, so no revoked key stays.

    Without the lock, the delete would clear the cache before create_key
    stores the key it made, and the next create_key would return that
    revoked key as an existing one.
    """
    mock_iam = _iam_success()
    mock_db = MagicMock()
    mock_db.get_key.return_value = None
    mock_namespace = MagicMock(spec=NamespaceService)
    mock_namespace.dynamodb = mock_db
    web_service = WebService(
        iam_service=mock_iam,
        kafka_service=MagicMock(),
        namespace_service=mock_namespace,
    )
    order: list[str] = []
    mock_db.store_key.side_effect = lambda **_kw: order.append('store')
    mock_db.delete_key.side_effect = lambda _s: order.append('delete')

    in_create = threading.Event()
    release = threading.Event()

    def _slow_create_access_key(_subject: str) -> dict[str, str]:
        in_create.set()
        release.wait(timeout=5)
        return _iam_success().create_access_key.return_value

    mock_iam.create_access_key.side_effect = _slow_create_access_key

    with patch.object(web_service, '_create_user') as mock_create_user:
        mock_create_user.return_value = {'status': 'success'}
        creator = threading.Thread(
            daemon=True,
            target=web_service.create_key,
            args=('test-subject',),
        )
        creator.start()
        assert in_create.wait(timeout=5)
        deleter = threading.Thread(
            daemon=True,
            target=web_service.delete_key,
            args=('test-subject',),
        )
        deleter.start()
        deleter.join(timeout=0.2)
        # The delete is held back while create_key runs
        assert deleter.is_alive()
        assert order == []
        release.set()
        creator.join(timeout=5)
        deleter.join(timeout=5)

    assert order == ['store', 'delete']


def test_create_user_takes_the_service_lock() -> None:
    """create_user waits while another user or key operation runs."""
    mock_iam = _iam_success()
    namespace_service = NamespaceService(dynamodb_service=MagicMock())
    web_service = WebService(
        iam_service=mock_iam,
        kafka_service=MagicMock(),
        namespace_service=namespace_service,
    )
    web_service._global_lock.acquire()
    try:
        worker = threading.Thread(
            daemon=True,
            target=web_service.create_user,
            args=('test-subject',),
        )
        worker.start()
        worker.join(timeout=0.2)
        assert worker.is_alive()
        mock_iam.create_user_and_policy.assert_not_called()
    finally:
        web_service._global_lock.release()
    worker.join(timeout=5)
    assert not worker.is_alive()
    mock_iam.create_user_and_policy.assert_called_once_with('test-subject')


def test_create_key_creating_the_user_does_not_deadlock() -> None:
    """create_key creates the user under the lock it already holds."""
    mock_iam = _iam_success()
    mock_db = MagicMock()
    mock_db.get_key.return_value = None
    namespace_service = NamespaceService(dynamodb_service=mock_db)
    web_service = WebService(
        iam_service=mock_iam,
        kafka_service=MagicMock(),
        namespace_service=namespace_service,
    )
    results: list[dict[str, object]] = []
    worker = threading.Thread(
        daemon=True,
        target=lambda: results.append(web_service.create_key('test-subject')),
    )
    worker.start()
    worker.join(timeout=5)
    assert not worker.is_alive(), 'create_key deadlocked'
    assert results[0]['status'] == 'success'
    assert results[0]['fresh'] is True
    mock_iam.create_user_and_policy.assert_called_once_with('test-subject')
    # Both locks are free afterwards
    assert not web_service._global_lock.locked()
    assert not namespace_service._global_lock.locked()
