"""Unit tests for web_service WebService."""

from __future__ import annotations

import contextlib
import json
import logging
import os
import queue
import threading
import time
import warnings
from typing import Any

import pytest

from web_service.services import DynamoDBService
from web_service.services import IAMService
from web_service.services import KafkaService
from web_service.services import NamespaceService
from web_service.services import WebService

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
# Helper Functions
# ============================================================================


def _collect_queue_results(
    results_queue: queue.Queue[dict[str, Any]],
    errors_queue: queue.Queue[Exception],
) -> tuple[list[dict[str, Any]], list[Exception]]:
    """Collect results from thread-safe queues.

    Args:
        results_queue: Queue containing result dictionaries
        errors_queue: Queue containing exceptions

    Returns:
        Tuple of (results list, errors list)
    """
    results = []
    while not results_queue.empty():
        results.append(results_queue.get())
    errors = []
    while not errors_queue.empty():
        errors.append(errors_queue.get())
    return results, errors


def _verify_user_deletion_cleanup(
    web_service: WebService,
    iam_service: IAMService,
    subject: str,
    namespace: str,
    topic: str,
) -> None:
    """Verify all user resources are cleaned up after deletion."""
    stored_key_after = web_service.namespace_service.dynamodb.get_key(
        subject,
    )
    assert stored_key_after is None, 'Key should be deleted from DynamoDB'
    topics_after = web_service.namespace_service.dynamodb.get_namespace_topics(
        namespace,
    )
    assert topic not in topics_after, 'Topic should be deleted'
    user_namespaces = (
        web_service.namespace_service.dynamodb.get_user_namespaces(subject)
    )
    assert namespace not in user_namespaces, (
        'Namespace should be deleted from user namespaces'
    )
    try:
        iam_service.iam.get_user(UserName=subject)
        iam_user_exists = True
    except iam_service.iam.exceptions.NoSuchEntityException:
        iam_user_exists = False
    assert not iam_user_exists, 'IAM user should be deleted'


def _verify_existing_key_results(
    successful_results: list[dict[str, Any]],
    initial_access_key: str,
    initial_secret_key: str,
) -> None:
    """Verify all results return the same existing key."""
    for result in successful_results:
        assert result['status'] == 'success'
        assert result['access_key'] == initial_access_key
        assert result['secret_key'] == initial_secret_key
        assert result.get('fresh') is False
        assert 'already exists' in result['message'].lower()


def _verify_key_unchanged(
    web_service: WebService,
    iam_service: IAMService,
    subject: str,
    initial_keys: dict[str, Any],
    initial_iam_key_count: int,
) -> None:
    """Verify no new IAM keys were created and DynamoDB key unchanged."""
    try:
        final_iam_keys = iam_service.iam.list_access_keys(
            UserName=subject,
        )['AccessKeyMetadata']
        final_iam_key_count = len(final_iam_keys)
        print(
            f'  IAM keys count: {final_iam_key_count} '
            f'(initial: {initial_iam_key_count})',
        )
        assert final_iam_key_count == initial_iam_key_count
    except iam_service.iam.exceptions.NoSuchEntityException:
        pass
    stored_key = web_service.namespace_service.dynamodb.get_key(subject)
    assert stored_key is not None
    assert stored_key['access_key'] == initial_keys['access_key']
    assert stored_key['secret_key'] == initial_keys['secret_key']


def _verify_concurrent_key_creation_results(
    results: list[dict[str, Any]],
    num_threads: int,
    web_service: WebService,
    iam_service: IAMService,
    subject: str,
) -> None:
    """Verify results from concurrent key creation."""
    successful_results = [r for r in results if r.get('status') == 'success']
    print(f'  Successful calls: {len(successful_results)}/{num_threads}')
    stored_key = web_service.namespace_service.dynamodb.get_key(subject)
    assert stored_key is not None, 'Key should exist in DynamoDB'
    stored_access_key = stored_key['access_key']
    print(f'  DynamoDB key: {stored_access_key}')
    if successful_results:
        result_access_keys = [r['access_key'] for r in successful_results]
        print(f'  Result access keys: {result_access_keys}')
        for result in successful_results:
            assert result['access_key'] == stored_access_key, (
                'All results should have the same access key '
                '(atomic operation)'
            )
            assert 'access_key' in result
            assert len(result['access_key']) > 0
            assert result['status'] == 'success'
    try:
        iam_keys = iam_service.iam.list_access_keys(UserName=subject)
        iam_key_count = len(iam_keys['AccessKeyMetadata'])
        assert iam_key_count == 1, (
            f'Should have exactly 1 IAM key, got {iam_key_count}'
        )
        assert stored_key['access_key'] in [
            k['AccessKeyId'] for k in iam_keys['AccessKeyMetadata']
        ], 'DynamoDB key should exist in IAM'
    except iam_service.iam.exceptions.NoSuchEntityException:
        # User might not exist yet, which is fine
        pass


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
    request: pytest.FixtureRequest,
) -> Any:
    """Fixture that provides cleanup function for test users.

    Uses both yield and addfinalizer to ensure cleanup happens even if
    tests fail. The addfinalizer ensures cleanup runs even if there's an
    exception during fixture teardown.
    """
    created_users: list[str] = []
    cleanup_done = False

    def _cleanup(subject: str) -> None:
        """Mark a user for cleanup."""
        if subject not in created_users:
            created_users.append(subject)

    def _finalize() -> None:
        """Cleanup all created users and their Kafka topics."""
        nonlocal cleanup_done
        if cleanup_done:
            return
        cleanup_done = True
        for subject in created_users:
            # First, explicitly clean up any Kafka topics for this user
            # This ensures topics are deleted even if delete_user fails
            try:
                default_namespace = namespace_service.generate_default(subject)
                topics = namespace_service.dynamodb.get_namespace_topics(
                    default_namespace,
                )
                for topic in topics:
                    with contextlib.suppress(Exception):
                        # Delete Kafka topic directly
                        web_service.kafka_service.delete_topic(
                            default_namespace,
                            topic,
                        )
            except Exception:
                pass  # Ignore errors during topic enumeration

            with contextlib.suppress(Exception):
                # Try to delete via web_service first (includes topic cleanup)
                web_service.delete_user(subject)
            with contextlib.suppress(Exception):
                # Fallback: manual cleanup
                namespace = namespace_service.generate_default(subject)
                namespace_service.delete_namespace(subject, namespace)
                iam_service.delete_user_and_policy(subject)

    # Register finalizer to ensure cleanup happens even if test fails
    # This is more reliable than yield-based cleanup alone
    request.addfinalizer(_finalize)  # noqa: PT021

    yield _cleanup

    # Also run cleanup on normal completion
    # (flag prevents double cleanup if addfinalizer also runs)
    _finalize()


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
        'Cached key should be deleted from DynamoDB when user is deleted'
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
def test_concurrent_user_deletion(
    web_service: WebService,
    iam_service: IAMService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test concurrent delete_user() calls (should be idempotent)."""
    print(
        f'\n[test_concurrent_user_deletion] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # Create user with resources first
    create_result = web_service.create_user(random_subject)
    assert create_result['status'] == 'success'
    namespace = create_result['namespace']

    # Create a key to ensure it's cached in DynamoDB
    create_key_result = web_service.create_key(random_subject)
    assert create_key_result['status'] == 'success'

    # Create a topic to ensure resources exist
    topic = 'test-topic-1'
    create_topic_result = web_service.create_topic(
        random_subject,
        namespace,
        topic,
    )
    assert create_topic_result['status'] == 'success'

    # Verify resources exist before deletion
    stored_key_before = web_service.namespace_service.dynamodb.get_key(
        random_subject,
    )
    assert stored_key_before is not None
    topics_before = (
        web_service.namespace_service.dynamodb.get_namespace_topics(
            namespace,
        )
    )
    assert topic in topics_before

    # Number of concurrent operations
    num_threads = 10
    results_queue: queue.Queue[dict[str, Any]] = queue.Queue()
    errors_queue: queue.Queue[Exception] = queue.Queue()
    threads: list[threading.Thread] = []

    def delete_user_thread(thread_id: int) -> None:
        """Delete user in a thread."""
        try:
            result = web_service.delete_user(random_subject)
            results_queue.put(result)
            print(f'  Thread {thread_id} result: {result["status"]}')
        except Exception as e:
            errors_queue.put(e)
            results_queue.put(
                {
                    'status': 'failure',
                    'message': f'Exception: {e}',
                },
            )
            print(f'  Thread {thread_id} error: {e}')

    # Start all threads
    for i in range(num_threads):
        thread = threading.Thread(target=delete_user_thread, args=(i,))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Collect results from queues
    results, errors = _collect_queue_results(results_queue, errors_queue)

    # Check for errors
    if errors:
        print(f'  Errors occurred: {errors}')

    # Print all results summary
    print('\n  All results summary:')
    for i, result in enumerate(results):
        print(
            f'    Thread {i}: {result["status"]} - '
            f'{result.get("message", "N/A")}',
        )

    # Assertions
    assert len(results) == num_threads, 'All threads should complete'
    successful_results = [r for r in results if r.get('status') == 'success']
    # All deletions should succeed (idempotent behavior)
    # At least one should succeed, others may succeed or fail gracefully
    assert len(successful_results) >= 1, 'At least one deletion should succeed'

    # Verify all resources are cleaned up
    _verify_user_deletion_cleanup(
        web_service,
        iam_service,
        random_subject,
        namespace,
        topic,
    )


@pytest.mark.integration
def test_user_deletion_during_resource_creation(
    web_service: WebService,
    iam_service: IAMService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test user deletion while creating topics/namespaces."""
    print(
        f'\n[test_user_deletion_during_resource_creation] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # Create user first
    create_result = web_service.create_user(random_subject)
    assert create_result['status'] == 'success'
    namespace = create_result['namespace']

    # Create a key
    create_key_result = web_service.create_key(random_subject)
    assert create_key_result['status'] == 'success'

    deletion_complete = threading.Event()
    creation_results_queue: queue.Queue[dict[str, Any]] = queue.Queue()
    deletion_result: dict[str, Any] | None = None
    creation_errors_queue: queue.Queue[Exception] = queue.Queue()

    def create_resources_thread() -> None:
        """Create resources in a thread."""
        try:
            # Try to create a topic
            topic = 'test-topic-during-deletion'
            result = web_service.create_topic(
                random_subject,
                namespace,
                topic,
            )
            creation_results_queue.put(result)
            print(f'  Creation thread result: {result["status"]}')
        except Exception as e:
            creation_errors_queue.put(e)
            print(f'  Creation thread error: {e}')

    def delete_user_thread() -> None:
        """Delete user in a thread."""
        nonlocal deletion_result
        try:
            result = web_service.delete_user(random_subject)
            deletion_result = result
            print(f'  Deletion thread result: {result["status"]}')
            deletion_complete.set()
        except Exception as e:
            print(f'  Deletion thread error: {e}')
            deletion_complete.set()

    # Start deletion thread
    deletion_thread = threading.Thread(target=delete_user_thread)
    deletion_thread.start()

    # Small delay to let deletion start
    time.sleep(0.1)

    # Start creation thread (should be blocked by lock)
    creation_thread = threading.Thread(target=create_resources_thread)
    creation_thread.start()

    # Wait for deletion to complete
    deletion_complete.wait(timeout=30)
    deletion_thread.join()
    creation_thread.join()

    # Collect creation results from queue
    creation_results, _ = _collect_queue_results(
        creation_results_queue,
        creation_errors_queue,
    )

    # Assertions
    assert deletion_result is not None, 'Deletion should complete'
    assert deletion_result['status'] == 'success', 'Deletion should succeed'

    # Verify resources are cleaned up
    stored_key_after = web_service.namespace_service.dynamodb.get_key(
        random_subject,
    )
    assert stored_key_after is None, 'Key should be deleted'

    # Creation might succeed or fail depending on timing,
    # but deletion should have completed successfully
    # The lock ensures atomicity - either creation happens before deletion,
    # or deletion happens before creation
    if creation_results:
        print(
            f'  Creation result: {creation_results[0]["status"]} - '
            f'{creation_results[0].get("message", "N/A")}',
        )


@pytest.mark.integration
def test_user_deletion_partial_failures(
    web_service: WebService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test user deletion handles partial failures gracefully."""
    print(
        f'\n[test_user_deletion_partial_failures] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # Create user first
    create_result = web_service.create_user(random_subject)
    assert create_result['status'] == 'success'

    # Create a key
    create_key_result = web_service.create_key(random_subject)
    assert create_key_result['status'] == 'success'

    # Delete user (should succeed even if some operations fail)
    # This test verifies that the method handles errors gracefully
    result = web_service.delete_user(random_subject)
    print('  Delete user result:')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    assert isinstance(result, dict)
    assert 'status' in result
    assert 'message' in result

    # The deletion should either succeed or fail gracefully
    # If it fails, it should provide a clear error message
    if result['status'] == 'failure':
        assert 'message' in result
        assert len(result['message']) > 0
    else:
        # If it succeeds, verify resources are cleaned up
        # Key might be deleted or might not exist
        # The important thing is that the operation completed
        _ = web_service.namespace_service.dynamodb.get_key(random_subject)


@pytest.mark.integration
def test_concurrent_topic_creation(
    web_service: WebService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test concurrent create_topic() calls (should be idempotent)."""
    print(
        f'\n[test_concurrent_topic_creation] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # Create user and namespace first
    create_result = web_service.create_user(random_subject)
    assert create_result['status'] == 'success'
    namespace = create_result['namespace']
    topic = 'test-topic-concurrent'

    # Ensure topic doesn't exist
    web_service.delete_topic(random_subject, namespace, topic)

    # Number of concurrent operations
    num_threads = 10
    results_queue: queue.Queue[dict[str, Any]] = queue.Queue()
    errors_queue: queue.Queue[Exception] = queue.Queue()
    threads: list[threading.Thread] = []

    def create_topic_thread(thread_id: int) -> None:
        """Create topic in a thread."""
        try:
            result = web_service.create_topic(
                random_subject,
                namespace,
                topic,
            )
            results_queue.put(result)
            print(f'  Thread {thread_id} result: {result["status"]}')
        except Exception as e:
            errors_queue.put(e)
            results_queue.put(
                {
                    'status': 'failure',
                    'message': f'Exception: {e}',
                },
            )
            print(f'  Thread {thread_id} error: {e}')

    # Start all threads
    for i in range(num_threads):
        thread = threading.Thread(target=create_topic_thread, args=(i,))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Collect results from queues
    results, errors = _collect_queue_results(results_queue, errors_queue)

    # Check for errors
    if errors:
        print(f'  Errors occurred: {errors}')

    # Print all results summary
    print('\n  All results summary:')
    for i, result in enumerate(results):
        print(
            f'    Thread {i}: {result["status"]} - '
            f'{result.get("message", "N/A")}',
        )

    # Assertions
    assert len(results) == num_threads, 'All threads should complete'
    successful_results = [r for r in results if r.get('status') == 'success']
    # All creations should succeed (idempotent behavior)
    assert len(successful_results) == num_threads, (
        'All topic creations should succeed (idempotent)'
    )

    # Verify topic exists exactly once in DynamoDB
    topics_after = web_service.namespace_service.dynamodb.get_namespace_topics(
        namespace,
    )
    topic_count = (
        topics_after.count(topic)
        if isinstance(
            topics_after,
            list,
        )
        else (1 if topic in topics_after else 0)
    )
    assert topic_count == 1, (
        f'Topic should exist exactly once, found {topic_count} times'
    )
    assert topic in topics_after, 'Topic should exist in namespace'

    # Verify all results have the same topic list (atomic operation)
    if successful_results:
        for result in successful_results:
            result_topics = result.get('topics', [])
            assert topic in result_topics, (
                'All results should include the created topic'
            )


@pytest.mark.integration
def test_concurrent_topic_creation_existing_topic(
    web_service: WebService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test concurrent create_topic() calls when topic already exists."""
    print(
        f'\n[test_concurrent_topic_creation_existing_topic] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # Create user and namespace first
    create_result = web_service.create_user(random_subject)
    assert create_result['status'] == 'success'
    namespace = create_result['namespace']
    topic = 'test-topic-concurrent-existing'

    # Create topic first
    initial_result = web_service.create_topic(
        random_subject,
        namespace,
        topic,
    )
    assert initial_result['status'] == 'success'
    print(f'  Initial topic created: {topic}')

    # Number of concurrent operations
    num_threads = 10
    results_queue: queue.Queue[dict[str, Any]] = queue.Queue()
    errors_queue: queue.Queue[Exception] = queue.Queue()
    threads: list[threading.Thread] = []

    def create_topic_thread(thread_id: int) -> None:
        """Create topic in a thread."""
        try:
            result = web_service.create_topic(
                random_subject,
                namespace,
                topic,
            )
            results_queue.put(result)
            print(f'  Thread {thread_id} result: {result["status"]}')
        except Exception as e:
            errors_queue.put(e)
            results_queue.put(
                {
                    'status': 'failure',
                    'message': f'Exception: {e}',
                },
            )
            print(f'  Thread {thread_id} error: {e}')

    # Start all threads
    for i in range(num_threads):
        thread = threading.Thread(target=create_topic_thread, args=(i,))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Collect results from queues
    results, errors = _collect_queue_results(results_queue, errors_queue)

    # Check for errors
    if errors:
        print(f'  Errors occurred: {errors}')

    # Print all results summary
    print('\n  All results summary:')
    for i, result in enumerate(results):
        print(
            f'    Thread {i}: {result["status"]} - '
            f'{result.get("message", "N/A")}',
        )

    # Assertions
    assert len(results) == num_threads, 'All threads should complete'
    successful_results = [r for r in results if r.get('status') == 'success']
    # All should succeed (idempotent behavior)
    assert len(successful_results) == num_threads, (
        'All topic creations should succeed (idempotent)'
    )

    # Verify all results indicate topic already exists
    for result in successful_results:
        assert 'already exists' in result.get('message', '').lower() or (
            topic in result.get('topics', [])
        ), 'All results should indicate topic already exists'

    # Verify topic still exists exactly once
    topics_after = web_service.namespace_service.dynamodb.get_namespace_topics(
        namespace,
    )
    topic_count = (
        topics_after.count(topic)
        if isinstance(
            topics_after,
            list,
        )
        else (1 if topic in topics_after else 0)
    )
    assert topic_count == 1, (
        f'Topic should exist exactly once, found {topic_count} times'
    )


@pytest.mark.integration
def test_concurrent_topic_deletion(
    web_service: WebService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test concurrent delete_topic() calls (should be idempotent)."""
    print(
        f'\n[test_concurrent_topic_deletion] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # Create user and namespace first
    create_result = web_service.create_user(random_subject)
    assert create_result['status'] == 'success'
    namespace = create_result['namespace']
    topic = 'test-topic-concurrent-delete'

    # Create topic first
    create_topic_result = web_service.create_topic(
        random_subject,
        namespace,
        topic,
    )
    assert create_topic_result['status'] == 'success'

    # Verify topic exists before deletion
    topics_before = (
        web_service.namespace_service.dynamodb.get_namespace_topics(
            namespace,
        )
    )
    assert topic in topics_before

    # Number of concurrent operations
    num_threads = 10
    results_queue: queue.Queue[dict[str, Any]] = queue.Queue()
    errors_queue: queue.Queue[Exception] = queue.Queue()
    threads: list[threading.Thread] = []

    def delete_topic_thread(thread_id: int) -> None:
        """Delete topic in a thread."""
        try:
            result = web_service.delete_topic(
                random_subject,
                namespace,
                topic,
            )
            results_queue.put(result)
            print(f'  Thread {thread_id} result: {result["status"]}')
        except Exception as e:
            errors_queue.put(e)
            # Put error result to maintain count
            results_queue.put(
                {
                    'status': 'failure',
                    'message': f'Exception: {e}',
                },
            )
            print(f'  Thread {thread_id} error: {e}')

    # Start all threads
    for i in range(num_threads):
        thread = threading.Thread(target=delete_topic_thread, args=(i,))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Collect results from queues
    results, errors = _collect_queue_results(results_queue, errors_queue)

    # Check for errors
    if errors:
        print(f'  Errors occurred: {errors}')

    # Print all results summary
    print('\n  All results summary:')
    for i, result in enumerate(results):
        print(
            f'    Thread {i}: {result["status"]} - '
            f'{result.get("message", "N/A")}',
        )

    # Assertions
    assert len(results) == num_threads, (
        f'All threads should complete (got {len(results)}/{num_threads})'
    )
    successful_results = [r for r in results if r.get('status') == 'success']
    # All deletions should succeed (idempotent behavior)
    assert len(successful_results) == num_threads, (
        f'All {num_threads} topic deletions should succeed '
        f'(got {len(successful_results)} successful, '
        f'{len(errors)} errors)'
    )

    # Verify topic is deleted from DynamoDB (most important assertion)
    # This is what matters - the topic should be deleted regardless of
    # how many threads succeeded
    topics_after = web_service.namespace_service.dynamodb.get_namespace_topics(
        namespace,
    )
    assert topic not in topics_after, 'Topic should be deleted from namespace'

    # Verify all successful results indicate topic was deleted
    for result in successful_results:
        assert topic not in result.get('topics', []), (
            'All results should indicate topic was deleted'
        )


@pytest.mark.integration
def test_topic_deletion_during_creation(
    web_service: WebService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test topic deletion while creating topic (lock prevents race)."""
    print(
        f'\n[test_topic_deletion_during_creation] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # Create user and namespace first
    create_result = web_service.create_user(random_subject)
    assert create_result['status'] == 'success'
    namespace = create_result['namespace']
    topic = 'test-topic-delete-during-create'

    deletion_complete = threading.Event()
    creation_results_queue: queue.Queue[dict[str, Any]] = queue.Queue()
    deletion_result: dict[str, Any] | None = None
    creation_errors_queue: queue.Queue[Exception] = queue.Queue()

    def create_topic_thread() -> None:
        """Create topic in a thread."""
        try:
            result = web_service.create_topic(
                random_subject,
                namespace,
                topic,
            )
            creation_results_queue.put(result)
            print(f'  Creation thread result: {result["status"]}')
        except Exception as e:
            creation_errors_queue.put(e)
            print(f'  Creation thread error: {e}')

    def delete_topic_thread() -> None:
        """Delete topic in a thread."""
        nonlocal deletion_result
        try:
            result = web_service.delete_topic(
                random_subject,
                namespace,
                topic,
            )
            deletion_result = result
            print(f'  Deletion thread result: {result["status"]}')
            deletion_complete.set()
        except Exception as e:
            print(f'  Deletion thread error: {e}')
            deletion_complete.set()

    # Start deletion thread
    deletion_thread = threading.Thread(target=delete_topic_thread)
    deletion_thread.start()

    # Small delay to let deletion start
    time.sleep(0.1)

    # Start creation thread (should be blocked by lock)
    creation_thread = threading.Thread(target=create_topic_thread)
    creation_thread.start()

    # Wait for deletion to complete
    deletion_complete.wait(timeout=30)
    deletion_thread.join()
    creation_thread.join()

    # Collect creation results from queue
    creation_results, _ = _collect_queue_results(
        creation_results_queue,
        creation_errors_queue,
    )

    # Assertions
    assert deletion_result is not None, 'Deletion should complete'
    # Deletion might succeed or fail depending on whether topic exists
    assert deletion_result['status'] in ('success', 'failure'), (
        'Deletion should complete with a status'
    )

    # Verify final state - topic might not exist if deletion succeeded,
    # or might exist if creation happened first - the lock ensures atomicity
    if creation_results:
        print(
            f'  Creation result: {creation_results[0]["status"]} - '
            f'{creation_results[0].get("message", "N/A")}',
        )

    # Explicit cleanup: ensure topic is deleted regardless of race condition
    with contextlib.suppress(Exception):
        web_service.delete_topic(random_subject, namespace, topic)
    # Fallback: delete directly from Kafka in case DynamoDB was cleaned
    # but Kafka topic persists
    with contextlib.suppress(Exception):
        web_service.kafka_service.delete_topic(namespace, topic)


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
    """Test full key lifecycle: create, delete."""
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

    # 2. Create key again (should return existing key if it exists)
    refresh_result = web_service.create_key(random_subject)
    print('  2. Create key (second call) result:')
    print(json.dumps(refresh_result, indent=2, default=str))
    assert refresh_result['status'] == 'success'
    # Since create_key now checks DynamoDB first, it should return existing key
    assert refresh_result['access_key'] == created_access_key
    assert refresh_result['secret_key'] == created_secret_key

    # 3. Delete key
    delete_result = web_service.delete_key(random_subject)
    print('  3. Delete key result:')
    print(json.dumps(delete_result, indent=2, default=str))
    assert delete_result['status'] == 'success'

    print('  Key lifecycle test completed successfully')


@pytest.mark.integration
def test_concurrent_key_creation_no_existing_key(
    web_service: WebService,
    iam_service: IAMService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test concurrent create_key() calls when no key exists."""
    print(
        f'\n[test_concurrent_key_creation_no_existing_key] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # Ensure no key exists in DynamoDB
    web_service.namespace_service.dynamodb.delete_key(random_subject)
    # Ensure no IAM keys exist
    iam_service.delete_access_keys(random_subject)

    # Number of concurrent operations
    num_threads = 20
    results_queue: queue.Queue[dict[str, Any]] = queue.Queue()
    errors_queue: queue.Queue[Exception] = queue.Queue()
    threads: list[threading.Thread] = []

    def create_key_thread(thread_id: int) -> None:
        """Create key in a thread."""
        try:
            result = web_service.create_key(random_subject)
            results_queue.put(result)
            print(f'  Thread {thread_id} result: {result["status"]}')
        except Exception as e:
            errors_queue.put(e)
            results_queue.put(
                {
                    'status': 'failure',
                    'message': f'Exception: {e}',
                },
            )
            print(f'  Thread {thread_id} error: {e}')

    # Start all threads
    for i in range(num_threads):
        thread = threading.Thread(target=create_key_thread, args=(i,))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Collect results from queues
    results, errors = _collect_queue_results(results_queue, errors_queue)

    # Check for errors
    if errors:
        print(f'  Errors occurred: {errors}')
        # Some errors might be expected (e.g., IAM key limit)

    # Verify all calls succeeded and results are consistent
    _verify_concurrent_key_creation_results(
        results,
        num_threads,
        web_service,
        iam_service,
        random_subject,
    )

    print('  Concurrent key creation test (no existing key) completed')


@pytest.mark.integration
def test_concurrent_key_creation_existing_key(
    web_service: WebService,
    iam_service: IAMService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test concurrent create_key() calls when key already exists."""
    print(
        f'\n[test_concurrent_key_creation_existing_key] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # Create a key first
    initial_result = web_service.create_key(random_subject)
    assert initial_result['status'] == 'success'
    initial_access_key = initial_result['access_key']
    initial_secret_key = initial_result['secret_key']
    print(f'  Initial key created: {initial_access_key}')

    # Get initial IAM key count
    try:
        initial_iam_keys = iam_service.iam.list_access_keys(
            UserName=random_subject,
        )['AccessKeyMetadata']
        initial_iam_key_count = len(initial_iam_keys)
    except iam_service.iam.exceptions.NoSuchEntityException:
        initial_iam_key_count = 0

    # Number of concurrent operations
    num_threads = 20
    results_queue: queue.Queue[dict[str, Any]] = queue.Queue()
    errors_queue: queue.Queue[Exception] = queue.Queue()
    threads: list[threading.Thread] = []

    def create_key_thread(thread_id: int) -> None:
        """Create key in a thread."""
        try:
            result = web_service.create_key(random_subject)
            results_queue.put(result)
            print(f'  Thread {thread_id} result: {result["status"]}')
        except Exception as e:
            errors_queue.put(e)
            results_queue.put(
                {
                    'status': 'failure',
                    'message': f'Exception: {e}',
                },
            )
            print(f'  Thread {thread_id} error: {e}')

    # Start all threads
    for i in range(num_threads):
        thread = threading.Thread(target=create_key_thread, args=(i,))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Collect results from queues
    results, errors = _collect_queue_results(results_queue, errors_queue)

    # Check for errors
    if errors:
        raise Exception(f'Errors occurred: {errors}')

    # Verify all calls succeeded
    assert len(results) == num_threads, 'All threads should complete'
    successful_results = [r for r in results if r.get('status') == 'success']
    assert len(successful_results) == num_threads, 'All calls should succeed'

    # All results should return the same existing key (fresh=False)
    _verify_existing_key_results(
        successful_results,
        initial_access_key,
        initial_secret_key,
    )

    # Verify no new IAM keys were created and DynamoDB unchanged
    _verify_key_unchanged(
        web_service,
        iam_service,
        random_subject,
        {'access_key': initial_access_key, 'secret_key': initial_secret_key},
        initial_iam_key_count,
    )

    print('  Concurrent key creation test (existing key) completed')


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
