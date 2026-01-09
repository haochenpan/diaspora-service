"""Unit tests for web_service_v3 WebService."""

from __future__ import annotations

import contextlib
import json
import logging
import os
import warnings
from typing import Any

import pytest

from web_service_v3.services import DynamoDBService
from web_service_v3.services import IAMService
from web_service_v3.services import KafkaService
from web_service_v3.services import NamespaceService
from web_service_v3.services import WebService

# Suppress verbose logging and warnings from external libraries
logging.getLogger('kafka').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
# Suppress deprecation warnings (especially from kafka library)
warnings.filterwarnings('ignore', category=DeprecationWarning)
warnings.filterwarnings(
    'ignore',
    message='.*ssl.PROTOCOL_TLS.*',
    category=DeprecationWarning,
)

# ============================================================================
# Test Constants
# ============================================================================

EXPECTED_TOPICS_COUNT = 2

# ============================================================================
# Test Fixtures
# ============================================================================


@pytest.fixture
def iam_service() -> IAMService:
    """Create an IAMService instance with real AWS services."""
    account_id = os.getenv('AWS_ACCOUNT_ID')
    region = os.getenv('AWS_ACCOUNT_REGION')
    cluster_name = os.getenv('MSK_CLUSTER_NAME')

    if not all([account_id, region, cluster_name]):
        missing = [
            var
            for var, val in [
                ('AWS_ACCOUNT_ID', account_id),
                ('AWS_ACCOUNT_REGION', region),
                ('MSK_CLUSTER_NAME', cluster_name),
            ]
            if val is None
        ]
        raise ValueError(
            f'Missing required environment variables: {", ".join(missing)}',
        )

    return IAMService(
        account_id=account_id or '',
        region=region or '',
        cluster_name=cluster_name or '',
    )


@pytest.fixture
def kafka_service() -> KafkaService:
    """Create a KafkaService instance with real AWS services."""
    bootstrap_servers = os.getenv('DEFAULT_SERVERS')
    region = os.getenv('AWS_ACCOUNT_REGION')

    if not region:
        raise ValueError(
            'Missing required environment variable: AWS_ACCOUNT_REGION',
        )

    return KafkaService(
        bootstrap_servers=bootstrap_servers,
        region=region,
    )


@pytest.fixture
def db_service() -> DynamoDBService:
    """Create a DynamoDBService instance with real AWS services."""
    region = os.getenv('AWS_ACCOUNT_REGION')
    keys_table_name = 'test-diaspora-keys-table'
    users_table_name = 'test-diaspora-users-table'
    namespace_table_name = 'test-diaspora-namespace-table'

    if not region:
        raise ValueError(
            'Missing required environment variable: AWS_ACCOUNT_REGION',
        )

    return DynamoDBService(
        region=region,
        keys_table_name=keys_table_name,
        users_table_name=users_table_name,
        namespace_table_name=namespace_table_name,
    )


@pytest.fixture
def namespace_service(db_service: DynamoDBService) -> NamespaceService:
    """Create a NamespaceService instance."""
    return NamespaceService(dynamodb_service=db_service)


@pytest.fixture
def web_service(
    iam_service: IAMService,
    kafka_service: KafkaService,
    namespace_service: NamespaceService,
) -> WebService:
    """Create a WebService instance."""
    return WebService(
        iam_service=iam_service,
        kafka_service=kafka_service,
        namespace_service=namespace_service,
    )


@pytest.fixture
def random_subject() -> str:
    """Generate a random subject for each test."""
    return f'test-subject-{os.urandom(8).hex()}'


@pytest.fixture
def cleanup_user(
    web_service: WebService,
    iam_service: IAMService,
    namespace_service: NamespaceService,
) -> Any:
    """Fixture that provides cleanup function for test users."""
    created_users: list[str] = []

    def _cleanup(subject: str) -> None:
        """Mark a user for cleanup."""
        if subject not in created_users:
            created_users.append(subject)

    yield _cleanup

    # Cleanup all created users
    for subject in created_users:
        with contextlib.suppress(Exception):
            # Try to delete via web_service first
            web_service.delete_user(subject)
        with contextlib.suppress(Exception):
            # Fallback: manual cleanup
            namespace = namespace_service.generate_default(subject)
            namespace_service.delete_namespace(subject, namespace)
            iam_service.delete_user_and_policy(subject)


# ============================================================================
# Integration Tests with Real AWS Services
# ============================================================================


@pytest.mark.integration
def test_create_user_success(
    web_service: WebService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test create_user success case."""
    print(
        f'\n[test_create_user_success] Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # Create user
    result = web_service.create_user(random_subject)
    print('  Create user result:')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'message' in result
    assert 'subject' in result
    assert 'namespace' in result
    assert result['subject'] == random_subject
    assert result['namespace'].startswith('ns-')


@pytest.mark.integration
def test_create_user_idempotent(
    web_service: WebService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test create_user is idempotent."""
    print(
        f'\n[test_create_user_idempotent] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # Create user first time
    result1 = web_service.create_user(random_subject)
    print('  First create result:')
    print(json.dumps(result1, indent=2, default=str))

    assert result1['status'] == 'success'

    # Create user second time (should succeed as idempotent)
    result2 = web_service.create_user(random_subject)
    print('  Second create result:')
    print(json.dumps(result2, indent=2, default=str))

    # Assertions
    assert result2['status'] == 'success'
    assert result2['subject'] == random_subject
    assert result2['namespace'] == result1['namespace']


@pytest.mark.integration
def test_delete_user_success(
    web_service: WebService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test delete_user success case (user has only default namespace)."""
    print(
        f'\n[test_delete_user_success] Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # Create user first
    create_result = web_service.create_user(random_subject)
    assert create_result['status'] == 'success'

    # Create a key to ensure it's cached in DynamoDB
    create_key_result = web_service.create_key(random_subject)
    assert create_key_result['status'] == 'success'

    # Verify key exists in DynamoDB before deletion
    stored_key_before = web_service.namespace_service.dynamodb.get_key(
        random_subject,
    )
    assert stored_key_before is not None

    # Delete user (should succeed as user has only default namespace)
    result = web_service.delete_user(random_subject)
    print('  Delete user result:')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'message' in result
    assert random_subject in result['message']

    # Verify cached key is deleted from DynamoDB
    stored_key_after = web_service.namespace_service.dynamodb.get_key(
        random_subject,
    )
    assert stored_key_after is None


@pytest.mark.integration
def test_delete_user_with_topics(
    web_service: WebService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test delete_user deletes topics in default namespace."""
    print(
        f'\n[test_delete_user_with_topics] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # Create user first
    create_result = web_service.create_user(random_subject)
    assert create_result['status'] == 'success'
    namespace = create_result['namespace']

    # Create topics in the default namespace
    topic1 = 'test-topic-1'
    topic2 = 'test-topic-2'
    create_topic1 = web_service.namespace_service.create_topic(
        random_subject,
        namespace,
        topic1,
    )
    assert create_topic1['status'] == 'success'
    create_topic2 = web_service.namespace_service.create_topic(
        random_subject,
        namespace,
        topic2,
    )
    assert create_topic2['status'] == 'success'

    # Verify topics exist
    topics_before = (
        web_service.namespace_service.dynamodb.get_namespace_topics(
            namespace,
        )
    )
    assert len(topics_before) == EXPECTED_TOPICS_COUNT
    assert topic1 in topics_before
    assert topic2 in topics_before

    # Create a key to ensure it's cached in DynamoDB
    create_key_result = web_service.create_key(random_subject)
    assert create_key_result['status'] == 'success'

    # Verify key exists in DynamoDB before deletion
    stored_key_before = web_service.namespace_service.dynamodb.get_key(
        random_subject,
    )
    assert stored_key_before is not None

    # Delete user (should delete topics, namespace, cached keys, and IAM user)
    result = web_service.delete_user(random_subject)
    print('  Delete user result:')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    assert isinstance(result, dict)
    assert result['status'] == 'success'

    # Verify topics are deleted
    topics_after = web_service.namespace_service.dynamodb.get_namespace_topics(
        namespace,
    )
    assert len(topics_after) == 0

    # Verify cached key is deleted from DynamoDB
    stored_key_after = web_service.namespace_service.dynamodb.get_key(
        random_subject,
    )
    assert stored_key_after is None


@pytest.mark.integration
def test_delete_user_deletes_cached_key(
    web_service: WebService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test delete_user deletes cached key from DynamoDB."""
    print(
        f'\n[test_delete_user_deletes_cached_key] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # Create user first
    create_result = web_service.create_user(random_subject)
    assert create_result['status'] == 'success'

    # Create a key to ensure it's cached in DynamoDB
    create_key_result = web_service.create_key(random_subject)
    assert create_key_result['status'] == 'success'
    created_access_key = create_key_result['access_key']
    created_secret_key = create_key_result['secret_key']

    # Verify key exists in DynamoDB before deletion
    stored_key_before = web_service.namespace_service.dynamodb.get_key(
        random_subject,
    )
    assert stored_key_before is not None
    assert stored_key_before['access_key'] == created_access_key
    assert stored_key_before['secret_key'] == created_secret_key

    # Delete user
    result = web_service.delete_user(random_subject)
    print('  Delete user result:')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    assert isinstance(result, dict)
    assert result['status'] == 'success'

    # Verify cached key is deleted from DynamoDB
    stored_key_after = web_service.namespace_service.dynamodb.get_key(
        random_subject,
    )
    assert stored_key_after is None, (
        'Cached key should be deleted from DynamoDB '
        'when user is deleted'
    )


@pytest.mark.integration
def test_delete_user_with_namespaces(
    web_service: WebService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test delete_user fails when user has namespaces."""
    print(
        f'\n[test_delete_user_with_namespaces] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # Create user first
    create_result = web_service.create_user(random_subject)
    assert create_result['status'] == 'success'

    # Create an additional namespace
    additional_namespace = f'additional-ns-{os.urandom(4).hex()}'
    namespace_result = web_service.namespace_service.create_namespace(
        random_subject,
        additional_namespace,
    )
    assert namespace_result['status'] == 'success'

    # Try to delete user (should fail as user has namespaces)
    result = web_service.delete_user(random_subject)
    print('  Delete user result:')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    assert isinstance(result, dict)
    assert result['status'] == 'failure'
    assert 'message' in result
    assert 'namespace' in result['message'].lower()
    assert 'delete' in result['message'].lower()

    # Cleanup manually
    web_service.namespace_service.delete_namespace(
        random_subject,
        additional_namespace,
    )
    web_service.delete_user(random_subject)


@pytest.mark.integration
def test_delete_user_nonexistent(
    web_service: WebService,
    random_subject: str,
) -> None:
    """Test delete_user with non-existent user."""
    print(
        f'\n[test_delete_user_nonexistent] '
        f'Testing with subject: {random_subject}',
    )

    # Try to delete non-existent user
    result = web_service.delete_user(random_subject)
    print('  Delete user result:')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    # Should succeed (idempotent) or fail gracefully
    assert isinstance(result, dict)
    assert 'status' in result
    assert 'message' in result


@pytest.mark.integration
def test_full_lifecycle(
    web_service: WebService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test full lifecycle: create user, delete user."""
    print(
        f'\n[test_full_lifecycle] Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # 1. Create user
    create_result = web_service.create_user(random_subject)
    assert create_result['status'] == 'success'
    assert 'namespace' in create_result
    namespace = create_result['namespace']

    # 2. Verify user exists in IAM
    # (We can't directly check, but if create succeeded, it exists)

    # 3. Verify namespace exists
    user_namespaces = (
        web_service.namespace_service.dynamodb.get_user_namespaces(
            random_subject,
        )
    )
    assert namespace in user_namespaces

    # 4. Create a topic in the namespace
    topic = 'test-topic-1'
    create_topic_result = web_service.namespace_service.create_topic(
        random_subject,
        namespace,
        topic,
    )
    assert create_topic_result['status'] == 'success'

    # 5. Verify topic exists
    topics_before = (
        web_service.namespace_service.dynamodb.get_namespace_topics(
            namespace,
        )
    )
    assert topic in topics_before

    # 5a. Create a key to ensure it's cached in DynamoDB
    create_key_result = web_service.create_key(random_subject)
    assert create_key_result['status'] == 'success'

    # 5b. Verify key exists in DynamoDB before deletion
    stored_key_before = web_service.namespace_service.dynamodb.get_key(
        random_subject,
    )
    assert stored_key_before is not None

    # 6. Delete user (should delete topic, namespace, cached keys,
    # and IAM user)
    delete_result = web_service.delete_user(random_subject)
    assert delete_result['status'] == 'success'

    # 7. Verify namespace is deleted
    user_namespaces_after = (
        web_service.namespace_service.dynamodb.get_user_namespaces(
            random_subject,
        )
    )
    assert namespace not in user_namespaces_after

    # 8. Verify topic is deleted
    topics_after = web_service.namespace_service.dynamodb.get_namespace_topics(
        namespace,
    )
    assert topic not in topics_after

    # 9. Verify cached key is deleted from DynamoDB
    stored_key_after = web_service.namespace_service.dynamodb.get_key(
        random_subject,
    )
    assert stored_key_after is None

    print('  Full lifecycle test completed successfully')


@pytest.mark.integration
def test_create_key_success(
    web_service: WebService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test create_key success case."""
    print(
        f'\n[test_create_key_success] Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # Create key (should create user, namespace, and key)
    result = web_service.create_key(random_subject)
    print('  Create key result:')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'message' in result
    assert 'access_key' in result
    assert 'secret_key' in result
    assert 'create_date' in result
    assert len(result['access_key']) > 0
    assert len(result['secret_key']) > 0


@pytest.mark.integration
def test_create_key_force_refresh(
    web_service: WebService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test create_key force refresh (always creates new key)."""
    print(
        f'\n[test_create_key_force_refresh] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # Create key first time
    result1 = web_service.create_key(random_subject)
    print('  First create key result:')
    print(json.dumps(result1, indent=2, default=str))
    assert result1['status'] == 'success'
    first_access_key = result1['access_key']
    first_secret_key = result1['secret_key']

    # Create key second time (should force refresh)
    result2 = web_service.create_key(random_subject)
    print('  Second create key result (force refresh):')
    print(json.dumps(result2, indent=2, default=str))
    assert result2['status'] == 'success'
    second_access_key = result2['access_key']
    second_secret_key = result2['secret_key']

    # Assertions - keys should be different
    assert first_access_key != second_access_key
    assert first_secret_key != second_secret_key

    # Verify new key is stored in DynamoDB
    stored_key = web_service.namespace_service.dynamodb.get_key(
        random_subject,
    )
    assert stored_key is not None
    assert stored_key['access_key'] == second_access_key
    assert stored_key['secret_key'] == second_secret_key


@pytest.mark.integration
def test_get_key_existing(
    web_service: WebService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test get_key when key exists in DynamoDB."""
    print(
        f'\n[test_get_key_existing] Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # Create key first
    create_result = web_service.create_key(random_subject)
    assert create_result['status'] == 'success'
    created_access_key = create_result['access_key']
    created_secret_key = create_result['secret_key']

    # Get key (should retrieve from DynamoDB)
    result = web_service.get_key(random_subject)
    print('  Get key result:')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'message' in result
    assert 'access_key' in result
    assert 'secret_key' in result
    assert 'create_date' in result
    assert result['access_key'] == created_access_key
    assert result['secret_key'] == created_secret_key


@pytest.mark.integration
def test_get_key_nonexistent(
    web_service: WebService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test get_key when key doesn't exist (should create)."""
    print(
        f'\n[test_get_key_nonexistent] Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # Get key when it doesn't exist (should create user, namespace, and key)
    result = web_service.get_key(random_subject)
    print('  Get key result (created):')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'message' in result
    assert 'access_key' in result
    assert 'secret_key' in result
    assert 'create_date' in result
    assert len(result['access_key']) > 0
    assert len(result['secret_key']) > 0

    # Verify key is stored in DynamoDB
    stored_key = web_service.namespace_service.dynamodb.get_key(
        random_subject,
    )
    assert stored_key is not None
    assert stored_key['access_key'] == result['access_key']
    assert stored_key['secret_key'] == result['secret_key']


@pytest.mark.integration
def test_delete_key_success(
    web_service: WebService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test delete_key success case."""
    print(
        f'\n[test_delete_key_success] Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # Create key first
    create_result = web_service.create_key(random_subject)
    assert create_result['status'] == 'success'

    # Verify key exists in DynamoDB
    stored_key_before = web_service.namespace_service.dynamodb.get_key(
        random_subject,
    )
    assert stored_key_before is not None

    # Delete key
    result = web_service.delete_key(random_subject)
    print('  Delete key result:')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'message' in result

    # Verify key is deleted from DynamoDB
    stored_key_after = web_service.namespace_service.dynamodb.get_key(
        random_subject,
    )
    assert stored_key_after is None


@pytest.mark.integration
def test_delete_key_idempotent(
    web_service: WebService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test delete_key is idempotent."""
    print(
        f'\n[test_delete_key_idempotent] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # Create key first
    create_result = web_service.create_key(random_subject)
    assert create_result['status'] == 'success'

    # Delete key first time
    result1 = web_service.delete_key(random_subject)
    print('  First delete key result:')
    print(json.dumps(result1, indent=2, default=str))
    assert result1['status'] == 'success'

    # Delete key second time (should succeed as idempotent)
    result2 = web_service.delete_key(random_subject)
    print('  Second delete key result (idempotent):')
    print(json.dumps(result2, indent=2, default=str))
    assert result2['status'] == 'success'


@pytest.mark.integration
def test_key_lifecycle(
    web_service: WebService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test full key lifecycle: create, get, delete."""
    print(
        f'\n[test_key_lifecycle] Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # 1. Create key
    create_result = web_service.create_key(random_subject)
    print('  1. Create key result:')
    print(json.dumps(create_result, indent=2, default=str))
    assert create_result['status'] == 'success'
    created_access_key = create_result['access_key']
    created_secret_key = create_result['secret_key']

    # 2. Get key (should retrieve from DynamoDB)
    get_result = web_service.get_key(random_subject)
    print('  2. Get key result:')
    print(json.dumps(get_result, indent=2, default=str))
    assert get_result['status'] == 'success'
    assert get_result['access_key'] == created_access_key
    assert get_result['secret_key'] == created_secret_key

    # 3. Create key again (force refresh - should create new key)
    refresh_result = web_service.create_key(random_subject)
    print('  3. Create key (force refresh) result:')
    print(json.dumps(refresh_result, indent=2, default=str))
    assert refresh_result['status'] == 'success'
    assert refresh_result['access_key'] != created_access_key
    assert refresh_result['secret_key'] != created_secret_key

    # 4. Get key again (should retrieve new key)
    get_result2 = web_service.get_key(random_subject)
    print('  4. Get key (after refresh) result:')
    print(json.dumps(get_result2, indent=2, default=str))
    assert get_result2['status'] == 'success'
    assert get_result2['access_key'] == refresh_result['access_key']
    assert get_result2['secret_key'] == refresh_result['secret_key']

    # 5. Delete key
    delete_result = web_service.delete_key(random_subject)
    print('  5. Delete key result:')
    print(json.dumps(delete_result, indent=2, default=str))
    assert delete_result['status'] == 'success'

    # 6. Get key after delete (should create new key)
    get_result3 = web_service.get_key(random_subject)
    print('  6. Get key (after delete, should create) result:')
    print(json.dumps(get_result3, indent=2, default=str))
    assert get_result3['status'] == 'success'
    assert get_result3['access_key'] != refresh_result['access_key']

    print('  Key lifecycle test completed successfully')


@pytest.mark.integration
def test_create_topic_success(
    web_service: WebService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test create_topic success case."""
    print(
        f'\n[test_create_topic_success] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # Create user first
    create_result = web_service.create_user(random_subject)
    assert create_result['status'] == 'success'
    namespace = create_result['namespace']
    topic = 'test-topic-1'

    # Create topic
    result = web_service.create_topic(random_subject, namespace, topic)
    print('  Create topic result:')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'message' in result
    assert 'topics' in result
    assert topic in result['topics']

    # Verify topic exists in DynamoDB
    topics = web_service.namespace_service.dynamodb.get_namespace_topics(
        namespace,
    )
    assert topic in topics


@pytest.mark.integration
def test_create_topic_idempotent(
    web_service: WebService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test create_topic is idempotent."""
    print(
        f'\n[test_create_topic_idempotent] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # Create user first
    create_result = web_service.create_user(random_subject)
    assert create_result['status'] == 'success'
    namespace = create_result['namespace']
    topic = 'test-topic-1'

    # Create topic first time
    result1 = web_service.create_topic(random_subject, namespace, topic)
    print('  First create topic result:')
    print(json.dumps(result1, indent=2, default=str))
    assert result1['status'] == 'success'
    assert topic in result1['topics']

    # Create topic second time (should succeed as idempotent)
    result2 = web_service.create_topic(random_subject, namespace, topic)
    print('  Second create topic result (idempotent):')
    print(json.dumps(result2, indent=2, default=str))
    assert result2['status'] == 'success'
    assert topic in result2['topics']
    assert result2['topics'] == result1['topics']


@pytest.mark.integration
def test_create_topic_nonexistent_namespace(
    web_service: WebService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test create_topic fails when namespace doesn't exist."""
    print(
        f'\n[test_create_topic_nonexistent_namespace] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # Create user first
    create_result = web_service.create_user(random_subject)
    assert create_result['status'] == 'success'

    # Try to create topic in non-existent namespace
    nonexistent_namespace = 'nonexistent-namespace'
    topic = 'test-topic-1'
    result = web_service.create_topic(
        random_subject,
        nonexistent_namespace,
        topic,
    )
    print('  Create topic result:')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    assert isinstance(result, dict)
    assert result['status'] == 'failure'
    assert 'message' in result
    assert 'namespace' in result['message'].lower()


@pytest.mark.integration
def test_delete_topic_success(
    web_service: WebService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test delete_topic success case."""
    print(
        f'\n[test_delete_topic_success] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # Create user first
    create_result = web_service.create_user(random_subject)
    assert create_result['status'] == 'success'
    namespace = create_result['namespace']
    topic = 'test-topic-1'

    # Create topic first
    create_topic_result = web_service.create_topic(
        random_subject,
        namespace,
        topic,
    )
    assert create_topic_result['status'] == 'success'

    # Verify topic exists in DynamoDB
    topics_before = (
        web_service.namespace_service.dynamodb.get_namespace_topics(
            namespace,
        )
    )
    assert topic in topics_before

    # Delete topic
    result = web_service.delete_topic(random_subject, namespace, topic)
    print('  Delete topic result:')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'message' in result
    assert 'topics' in result
    assert topic not in result['topics']

    # Verify topic is deleted from DynamoDB
    topics_after = web_service.namespace_service.dynamodb.get_namespace_topics(
        namespace,
    )
    assert topic not in topics_after


@pytest.mark.integration
def test_delete_topic_idempotent(
    web_service: WebService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test delete_topic is idempotent."""
    print(
        f'\n[test_delete_topic_idempotent] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # Create user first
    create_result = web_service.create_user(random_subject)
    assert create_result['status'] == 'success'
    namespace = create_result['namespace']
    topic = 'test-topic-1'

    # Create topic first
    create_topic_result = web_service.create_topic(
        random_subject,
        namespace,
        topic,
    )
    assert create_topic_result['status'] == 'success'

    # Delete topic first time
    result1 = web_service.delete_topic(random_subject, namespace, topic)
    print('  First delete topic result:')
    print(json.dumps(result1, indent=2, default=str))
    assert result1['status'] == 'success'
    assert topic not in result1['topics']

    # Delete topic second time (should succeed as idempotent)
    result2 = web_service.delete_topic(random_subject, namespace, topic)
    print('  Second delete topic result (idempotent):')
    print(json.dumps(result2, indent=2, default=str))
    assert result2['status'] == 'success'
    assert topic not in result2['topics']


@pytest.mark.integration
def test_delete_topic_nonexistent_namespace(
    web_service: WebService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test delete_topic fails when namespace doesn't exist."""
    print(
        f'\n[test_delete_topic_nonexistent_namespace] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # Create user first
    create_result = web_service.create_user(random_subject)
    assert create_result['status'] == 'success'

    # Try to delete topic from non-existent namespace
    nonexistent_namespace = 'nonexistent-namespace'
    topic = 'test-topic-1'
    result = web_service.delete_topic(
        random_subject,
        nonexistent_namespace,
        topic,
    )
    print('  Delete topic result:')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    assert isinstance(result, dict)
    assert result['status'] == 'failure'
    assert 'message' in result
    assert 'namespace' in result['message'].lower()


@pytest.mark.integration
def test_topic_lifecycle(
    web_service: WebService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test full topic lifecycle: create, delete."""
    print(
        f'\n[test_topic_lifecycle] Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # 1. Create user
    create_result = web_service.create_user(random_subject)
    assert create_result['status'] == 'success'
    namespace = create_result['namespace']
    topic1 = 'test-topic-1'
    topic2 = 'test-topic-2'

    # 2. Create first topic
    create_topic1_result = web_service.create_topic(
        random_subject,
        namespace,
        topic1,
    )
    print('  1. Create topic 1 result:')
    print(json.dumps(create_topic1_result, indent=2, default=str))
    assert create_topic1_result['status'] == 'success'
    assert topic1 in create_topic1_result['topics']

    # 3. Create second topic
    create_topic2_result = web_service.create_topic(
        random_subject,
        namespace,
        topic2,
    )
    print('  2. Create topic 2 result:')
    print(json.dumps(create_topic2_result, indent=2, default=str))
    assert create_topic2_result['status'] == 'success'
    assert topic2 in create_topic2_result['topics']

    # 4. Verify both topics exist in DynamoDB
    topics_before = (
        web_service.namespace_service.dynamodb.get_namespace_topics(
            namespace,
        )
    )
    assert topic1 in topics_before
    assert topic2 in topics_before

    # 5. Delete first topic
    delete_topic1_result = web_service.delete_topic(
        random_subject,
        namespace,
        topic1,
    )
    print('  3. Delete topic 1 result:')
    print(json.dumps(delete_topic1_result, indent=2, default=str))
    assert delete_topic1_result['status'] == 'success'
    assert topic1 not in delete_topic1_result['topics']
    assert topic2 in delete_topic1_result['topics']

    # 6. Verify only topic2 exists in DynamoDB
    topics_after = web_service.namespace_service.dynamodb.get_namespace_topics(
        namespace,
    )
    assert topic1 not in topics_after
    assert topic2 in topics_after

    # 7. Delete second topic
    delete_topic2_result = web_service.delete_topic(
        random_subject,
        namespace,
        topic2,
    )
    print('  4. Delete topic 2 result:')
    print(json.dumps(delete_topic2_result, indent=2, default=str))
    assert delete_topic2_result['status'] == 'success'
    assert topic2 not in delete_topic2_result['topics']

    # 8. Verify no topics exist in DynamoDB
    topics_final = web_service.namespace_service.dynamodb.get_namespace_topics(
        namespace,
    )
    assert len(topics_final) == 0

    print('  Topic lifecycle test completed successfully')


@pytest.mark.integration
def test_recreate_topic_success(
    web_service: WebService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test recreate_topic success case."""
    print(
        f'\n[test_recreate_topic_success] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # Create user first
    create_result = web_service.create_user(random_subject)
    assert create_result['status'] == 'success'
    namespace = create_result['namespace']
    topic = 'test-topic-1'

    # Create topic first
    create_topic_result = web_service.create_topic(
        random_subject,
        namespace,
        topic,
    )
    assert create_topic_result['status'] == 'success'

    # Verify topic exists in DynamoDB
    topics_before = (
        web_service.namespace_service.dynamodb.get_namespace_topics(
            namespace,
        )
    )
    assert topic in topics_before

    # Recreate topic
    result = web_service.recreate_topic(random_subject, namespace, topic)
    print('  Recreate topic result:')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'message' in result

    # Verify topic still exists in DynamoDB after recreation
    topics_after = web_service.namespace_service.dynamodb.get_namespace_topics(
        namespace,
    )
    assert topic in topics_after


@pytest.mark.integration
def test_recreate_topic_idempotent(
    web_service: WebService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test recreate_topic is idempotent."""
    print(
        f'\n[test_recreate_topic_idempotent] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # Create user first
    create_result = web_service.create_user(random_subject)
    assert create_result['status'] == 'success'
    namespace = create_result['namespace']
    topic = 'test-topic-1'

    # Create topic first
    create_topic_result = web_service.create_topic(
        random_subject,
        namespace,
        topic,
    )
    assert create_topic_result['status'] == 'success'

    # Recreate topic first time
    result1 = web_service.recreate_topic(random_subject, namespace, topic)
    print('  First recreate topic result:')
    print(json.dumps(result1, indent=2, default=str))
    assert result1['status'] == 'success'

    # Recreate topic second time (should succeed as idempotent)
    result2 = web_service.recreate_topic(random_subject, namespace, topic)
    print('  Second recreate topic result (idempotent):')
    print(json.dumps(result2, indent=2, default=str))
    assert result2['status'] == 'success'


@pytest.mark.integration
def test_recreate_topic_nonexistent_namespace(
    web_service: WebService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test recreate_topic fails when namespace doesn't exist."""
    print(
        f'\n[test_recreate_topic_nonexistent_namespace] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # Create user first
    create_result = web_service.create_user(random_subject)
    assert create_result['status'] == 'success'

    # Try to recreate topic in non-existent namespace
    nonexistent_namespace = 'nonexistent-namespace'
    topic = 'test-topic-1'
    result = web_service.recreate_topic(
        random_subject,
        nonexistent_namespace,
        topic,
    )
    print('  Recreate topic result:')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    assert isinstance(result, dict)
    assert result['status'] == 'failure'
    assert 'message' in result
    assert 'namespace' in result['message'].lower()


@pytest.mark.integration
def test_recreate_topic_nonexistent_topic(
    web_service: WebService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test recreate_topic fails when topic doesn't exist."""
    print(
        f'\n[test_recreate_topic_nonexistent_topic] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # Create user first
    create_result = web_service.create_user(random_subject)
    assert create_result['status'] == 'success'
    namespace = create_result['namespace']

    # Try to recreate non-existent topic
    nonexistent_topic = 'nonexistent-topic'
    result = web_service.recreate_topic(
        random_subject,
        namespace,
        nonexistent_topic,
    )
    print('  Recreate topic result:')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    assert isinstance(result, dict)
    assert result['status'] == 'failure'
    assert 'message' in result
    assert 'topic' in result['message'].lower()


@pytest.mark.integration
def test_recreate_topic_lifecycle(
    web_service: WebService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test recreate_topic in full lifecycle."""
    print(
        f'\n[test_recreate_topic_lifecycle] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # 1. Create user
    create_result = web_service.create_user(random_subject)
    assert create_result['status'] == 'success'
    namespace = create_result['namespace']
    topic = 'test-topic-1'

    # 2. Create topic
    create_topic_result = web_service.create_topic(
        random_subject,
        namespace,
        topic,
    )
    print('  1. Create topic result:')
    print(json.dumps(create_topic_result, indent=2, default=str))
    assert create_topic_result['status'] == 'success'
    assert topic in create_topic_result['topics']

    # 3. Verify topic exists in DynamoDB
    topics_before = (
        web_service.namespace_service.dynamodb.get_namespace_topics(
            namespace,
        )
    )
    assert topic in topics_before

    # 4. Recreate topic
    recreate_result = web_service.recreate_topic(
        random_subject,
        namespace,
        topic,
    )
    print('  2. Recreate topic result:')
    print(json.dumps(recreate_result, indent=2, default=str))
    assert recreate_result['status'] == 'success'

    # 5. Verify topic still exists in DynamoDB after recreation
    topics_after = web_service.namespace_service.dynamodb.get_namespace_topics(
        namespace,
    )
    assert topic in topics_after

    # 6. Delete topic
    delete_result = web_service.delete_topic(random_subject, namespace, topic)
    print('  3. Delete topic result:')
    print(json.dumps(delete_result, indent=2, default=str))
    assert delete_result['status'] == 'success'

    print('  Recreate topic lifecycle test completed successfully')
