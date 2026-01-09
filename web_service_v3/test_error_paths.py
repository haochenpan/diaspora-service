"""Error path tests for web_service_v3 services.

Most tests use mocks to simulate specific error conditions that are difficult
to reproduce with real AWS services (e.g., IAM exceptions, network failures).
Some tests have integration variants that use real AWS services where the
error conditions can be naturally tested.

To run only unit tests (mocked): pytest web_service_v3/test_error_paths.py
To run only integration tests: pytest web_service_v3/test_error_paths.py -m integration
"""

from __future__ import annotations

from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from web_service_v3.services import DynamoDBService
from web_service_v3.services import IAMService
from web_service_v3.services import KafkaService
from web_service_v3.services import NamespaceService
from web_service_v3.services import WebService


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


def test_create_user_and_policy_policy_creation_failure() -> None:
    """Test create_user_and_policy handles policy creation failure."""
    iam_service = IAMService(
        account_id='123456789012',
        region='us-east-1',
        cluster_name='test-cluster',
    )

    # Mock user creation succeeds, policy creation fails
    with patch.object(iam_service.iam, 'create_user'):
        with patch.object(
            iam_service.iam,
            'create_policy',
        ) as mock_create_policy:
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
        from botocore.exceptions import ClientError

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
        from botocore.exceptions import ClientError

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

    # Mock delete_access_keys to succeed, but delete_policy to fail
    with patch.object(
        iam_service,
        'delete_access_keys',
    ) as mock_delete_keys:
        mock_delete_keys.return_value = {'status': 'success'}

        with patch.object(
            iam_service.iam,
            'detach_user_policy',
        ):
            with patch.object(
                iam_service.iam,
                'delete_policy',
            ) as mock_delete_policy:
                from botocore.exceptions import ClientError

                error_response = {
                    'Error': {
                        'Code': 'NoSuchEntity',
                        'Message': 'Policy not found',
                    },
                }
                mock_delete_policy.side_effect = ClientError(
                    error_response,
                    'DeletePolicy',
                )

                # Should still succeed due to suppress
                result = iam_service.delete_user_and_policy('test-user')
                # The method suppresses NoSuchEntityException, so should succeed
                assert result['status'] == 'success'


# ============================================================================
# DynamoDBService Error Path Tests
# ============================================================================


def test_store_key_table_not_found() -> None:
    """Test store_key handles table not found and creates table."""
    db_service = DynamoDBService(
        region='us-east-1',
        keys_table_name='test-keys-table',
        users_table_name='test-users-table',
        namespace_table_name='test-namespace-table',
    )

    # Mock DynamoDB client with exceptions attribute
    from botocore.exceptions import ClientError

    mock_dynamodb = MagicMock()
    # Create a mock exceptions object with ResourceNotFoundException
    # that inherits from ClientError so it can be caught properly
    class MockResourceNotFoundException(ClientError):
        """Mock ResourceNotFoundException for testing."""

        def __init__(self, operation_name: str = 'PutItem'):
            error_response = {
                'Error': {
                    'Code': 'ResourceNotFoundException',
                    'Message': 'Table not found',
                },
            }
            super().__init__(error_response, operation_name)

    mock_exceptions = MagicMock()
    mock_exceptions.ResourceNotFoundException = MockResourceNotFoundException
    mock_dynamodb.exceptions = mock_exceptions
    db_service.dynamodb = mock_dynamodb

    # First call raises ResourceNotFoundException, second succeeds
    mock_dynamodb.put_item.side_effect = [
        MockResourceNotFoundException('PutItem'),
        None,  # Second call succeeds
    ]

    # Mock table creation
    with patch.object(db_service, '_create_keys_table') as mock_create_table:
        mock_create_table.return_value = None

        # Should not raise exception
        db_service.store_key(
            subject='test-subject',
            access_key='AKIAIOSFODNN7EXAMPLE',
            secret_key='test-secret-key',
            create_date='2024-01-01T00:00:00',
        )

        # Should have called create_table and retried put_item
        mock_create_table.assert_called_once()
        assert mock_dynamodb.put_item.call_count == 2


def test_get_key_table_not_found() -> None:
    """Test get_key handles table not found gracefully."""
    db_service = DynamoDBService(
        region='us-east-1',
        keys_table_name='test-keys-table',
        users_table_name='test-users-table',
        namespace_table_name='test-namespace-table',
    )

    # Mock DynamoDB client with exceptions attribute
    from botocore.exceptions import ClientError

    mock_dynamodb = MagicMock()
    # Create a mock exceptions object with ResourceNotFoundException
    # that inherits from ClientError so it can be caught properly
    class MockResourceNotFoundException(ClientError):
        """Mock ResourceNotFoundException for testing."""

        def __init__(self, operation_name: str = 'GetItem'):
            error_response = {
                'Error': {
                    'Code': 'ResourceNotFoundException',
                    'Message': 'Table not found',
                },
            }
            super().__init__(error_response, operation_name)

    mock_exceptions = MagicMock()
    mock_exceptions.ResourceNotFoundException = MockResourceNotFoundException
    mock_dynamodb.exceptions = mock_exceptions
    db_service.dynamodb = mock_dynamodb

    mock_dynamodb.get_item.side_effect = MockResourceNotFoundException('GetItem')

    # Should return None (gracefully handled)
    result = db_service.get_key('test-subject')
    assert result is None


def test_add_user_namespace_table_creation_retry() -> None:
    """Test add_user_namespace handles table creation and retry."""
    db_service = DynamoDBService(
        region='us-east-1',
        keys_table_name='test-keys-table',
        users_table_name='test-users-table',
        namespace_table_name='test-namespace-table',
    )

    # Mock DynamoDB client with exceptions attribute
    from botocore.exceptions import ClientError

    mock_dynamodb = MagicMock()
    # Create a mock exceptions object with ResourceNotFoundException
    # that inherits from ClientError so it can be caught properly
    class MockResourceNotFoundException(ClientError):
        """Mock ResourceNotFoundException for testing."""

        def __init__(self, operation_name: str = 'UpdateItem'):
            error_response = {
                'Error': {
                    'Code': 'ResourceNotFoundException',
                    'Message': 'Table not found',
                },
            }
            super().__init__(error_response, operation_name)

    mock_exceptions = MagicMock()
    mock_exceptions.ResourceNotFoundException = MockResourceNotFoundException
    mock_dynamodb.exceptions = mock_exceptions
    db_service.dynamodb = mock_dynamodb

    # First call raises ResourceNotFoundException, second succeeds
    mock_dynamodb.update_item.side_effect = [
        MockResourceNotFoundException('UpdateItem'),
        None,  # Second call succeeds
    ]

    # Mock table creation
    with patch.object(db_service, '_create_users_table') as mock_create_table:
        mock_create_table.return_value = None

        # Should not raise exception
        db_service.add_user_namespace('test-subject', 'test-namespace')

        # Should have called create_table and retried update_item
        mock_create_table.assert_called_once()
        assert mock_dynamodb.update_item.call_count == 2


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
        'web_service_v3.services.KafkaAdminClient',
    ) as mock_admin_class:
        mock_admin = MagicMock()
        mock_admin_class.return_value = mock_admin

        from kafka.errors import KafkaError

        mock_admin.create_topics.side_effect = KafkaError('Connection failed')

        result = kafka_service.create_topic('test-namespace', 'test-topic')

        assert result is not None
        assert result['status'] == 'failure'
        assert 'Failed to create Kafka topic' in result['message']
        assert 'after 3 attempts' in result['message']


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
        'web_service_v3.services.KafkaAdminClient',
    ) as mock_admin_class:
        mock_admin = MagicMock()
        mock_admin_class.return_value = mock_admin

        from kafka.errors import KafkaError

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
        keys_table_name='test-keys-table',
        users_table_name='test-users-table',
        namespace_table_name='test-namespace-table',
    )
    namespace_service = NamespaceService(dynamodb_service=db_service)

    # Test with invalid namespace name (too short)
    result = namespace_service.create_namespace('test-subject', 'ab')

    assert result['status'] == 'failure'
    assert 'between' in result['message'].lower()


def test_create_namespace_already_taken() -> None:
    """Test create_namespace handles namespace already taken by another user."""
    db_service = DynamoDBService(
        region='us-east-1',
        keys_table_name='test-keys-table',
        users_table_name='test-users-table',
        namespace_table_name='test-namespace-table',
    )
    namespace_service = NamespaceService(dynamodb_service=db_service)

    # Mock get_user_namespaces to return empty (user doesn't have it)
    with patch.object(
        db_service,
        'get_user_namespaces',
    ) as mock_get_user_ns:
        mock_get_user_ns.return_value = []

        # Mock get_global_namespaces to return namespace (taken by another user)
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
        keys_table_name='test-keys-table',
        users_table_name='test-users-table',
        namespace_table_name='test-namespace-table',
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
    mock_iam = MagicMock()
    mock_kafka = MagicMock()
    mock_db = MagicMock()
    mock_namespace = MagicMock(spec=NamespaceService)

    iam_service = MagicMock()
    iam_service.create_user_and_policy.return_value = {
        'status': 'failure',
        'message': 'IAM service unavailable',
    }

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
    mock_iam = MagicMock()
    mock_kafka = MagicMock()
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
    mock_iam = MagicMock()
    mock_kafka = MagicMock()
    mock_db = MagicMock()
    mock_namespace = MagicMock(spec=NamespaceService)

    namespace_service = MagicMock(spec=NamespaceService)
    namespace_service.create_topic.return_value = {
        'status': 'success',
        'message': 'Topic created',
        'topics': ['test-topic'],
    }
    namespace_service.delete_topic.return_value = {
        'status': 'success',
        'message': 'Topic deleted',
    }

    kafka_service = MagicMock()
    kafka_service.create_topic.return_value = {
        'status': 'failure',
        'message': 'Kafka connection failed',
    }

    web_service = WebService(
        iam_service=mock_iam,
        kafka_service=kafka_service,
        namespace_service=namespace_service,
    )

    result = web_service.create_topic('test-subject', 'test-namespace', 'test-topic')

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
