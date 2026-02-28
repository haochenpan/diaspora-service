"""Unit tests for web_service DynamoDBService."""

from __future__ import annotations

import contextlib
import json
import os
import threading
from datetime import datetime
from typing import Any

import pytest

from web_service.services import DynamoDBService

# ============================================================================
# Test Constants
# ============================================================================

EXPECTED_NAMESPACES_COUNT_3 = 3
EXPECTED_NAMESPACES_COUNT_2 = 2
EXPECTED_TOPICS_COUNT_3 = 3
EXPECTED_TOPICS_COUNT_2 = 2

# ============================================================================
# Test Fixtures
# ============================================================================


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
def random_subject() -> str:
    """Generate a random subject for each test."""
    return f'test-subject-{os.urandom(8).hex()}'


@pytest.fixture
def random_namespace() -> str:
    """Generate a random namespace for each test."""
    return f'test-namespace-{os.urandom(8).hex()}'


@pytest.fixture
def cleanup_data(db_service: DynamoDBService) -> Any:
    """Fixture that provides cleanup function for test data."""
    created_subjects: list[str] = []
    created_namespaces: list[str] = []

    def _cleanup_subject(subject: str) -> None:
        """Mark a subject for cleanup."""
        if subject not in created_subjects:
            created_subjects.append(subject)

    def _cleanup_namespace(namespace: str) -> None:
        """Mark a namespace for cleanup."""
        if namespace not in created_namespaces:
            created_namespaces.append(namespace)

    yield {'subject': _cleanup_subject, 'namespace': _cleanup_namespace}

    # Cleanup all created data
    for subject in created_subjects:
        with contextlib.suppress(Exception):
            db_service.delete_key(subject)
        # Remove all user namespaces individually
        with contextlib.suppress(Exception):
            namespaces = db_service.get_user_namespaces(subject)
            for namespace in namespaces:
                db_service.remove_user_namespace(subject, namespace)
    for namespace in created_namespaces:
        # Remove all namespace topics individually
        with contextlib.suppress(Exception):
            topics = db_service.get_namespace_topics(namespace)
            for topic in topics:
                db_service.remove_namespace_topic(namespace, topic)
    # Remove all global namespaces individually
    with contextlib.suppress(Exception):
        global_namespaces = db_service.get_global_namespaces()
        for namespace in global_namespaces:
            db_service.remove_global_namespace(namespace)


# ============================================================================
# Integration Tests with Real AWS Services
# ============================================================================


@pytest.mark.integration
def test_store_key(
    db_service: DynamoDBService,
    random_subject: str,
    cleanup_data: Any,
) -> None:
    """Test store_key with real DynamoDB service."""
    print(
        f'\n[test_store_key] Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_data['subject'](random_subject)

    # Store key
    access_key = 'aaaaa'
    secret_key = 'bbbbb'  # pragma: allowlist secret

    create_date = datetime.now().isoformat()

    db_service.store_key(
        subject=random_subject,
        access_key=access_key,
        secret_key=secret_key,
        create_date=create_date,
    )
    print(f'  Stored key for subject: {random_subject}')

    # Verify it was stored
    result = db_service.get_key(random_subject)
    print('  Retrieved key:')
    # Mask secret key for security
    if result:
        masked_result = result.copy()
        if 'secret_key' in masked_result:
            masked_result['secret_key'] = '***MASKED***'
        print(json.dumps(masked_result, indent=2, default=str))

    # Assertions
    assert result is not None
    assert result['access_key'] == access_key
    assert result['secret_key'] == secret_key
    assert result['create_date'] == create_date


@pytest.mark.integration
def test_get_key_nonexistent(
    db_service: DynamoDBService,
    random_subject: str,
) -> None:
    """Test get_key with nonexistent subject."""
    print(
        f'\n[test_get_key_nonexistent] Testing with subject: {random_subject}',
    )

    # Try to get non-existent key
    result = db_service.get_key(random_subject)
    print(f'  Retrieved key: {result}')

    # Assertions
    assert result is None


@pytest.mark.integration
def test_delete_key(
    db_service: DynamoDBService,
    random_subject: str,
    cleanup_data: Any,
) -> None:
    """Test delete_key with real DynamoDB service."""
    print(
        f'\n[test_delete_key] Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_data['subject'](random_subject)

    # Store key first
    db_service.store_key(
        subject=random_subject,
        access_key='aaaaa',
        secret_key='bbbbb',  # pragma: allowlist secret
        create_date=datetime.now().isoformat(),
    )

    # Verify it exists
    assert db_service.get_key(random_subject) is not None

    # Delete key
    db_service.delete_key(random_subject)
    print(f'  Deleted key for subject: {random_subject}')

    # Verify it's gone
    result = db_service.get_key(random_subject)
    print(f'  Retrieved key after delete: {result}')

    # Assertions
    assert result is None


@pytest.mark.integration
def test_get_user_namespaces_nonexistent(
    db_service: DynamoDBService,
    random_subject: str,
) -> None:
    """Test get_user_namespaces with nonexistent subject."""
    print(
        f'\n[test_get_user_namespaces_nonexistent] '
        f'Testing with subject: {random_subject}',
    )

    # Try to get non-existent namespaces
    result = db_service.get_user_namespaces(random_subject)
    print(f'  Retrieved namespaces: {result}')

    # Assertions
    assert isinstance(result, list)
    assert len(result) == 0


@pytest.mark.integration
def test_remove_user_namespace(
    db_service: DynamoDBService,
    random_subject: str,
    cleanup_data: Any,
) -> None:
    """Test remove_user_namespace with real DynamoDB service."""
    print(
        f'\n[test_remove_user_namespace] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_data['subject'](random_subject)

    # Store user namespaces first
    db_service.add_user_namespace(random_subject, 'namespace1')
    db_service.add_user_namespace(random_subject, 'namespace2')

    # Verify it exists
    namespaces = db_service.get_user_namespaces(random_subject)
    assert len(namespaces) == EXPECTED_NAMESPACES_COUNT_2

    # Remove one namespace
    db_service.remove_user_namespace(random_subject, 'namespace1')
    print(f'  Removed namespace1 for subject: {random_subject}')

    # Verify it's removed
    result = db_service.get_user_namespaces(random_subject)
    print(f'  Retrieved namespaces after remove: {result}')

    # Assertions
    assert isinstance(result, list)
    assert len(result) == EXPECTED_NAMESPACES_COUNT_2 - 1
    assert 'namespace2' in result
    assert 'namespace1' not in result


@pytest.mark.integration
def test_add_namespace_topic(
    db_service: DynamoDBService,
    random_namespace: str,
    cleanup_data: Any,
) -> None:
    """Test add_namespace_topic with real DynamoDB service."""
    print(
        f'\n[test_add_namespace_topic] '
        f'Testing with namespace: {random_namespace}',
    )

    # Mark for cleanup
    cleanup_data['namespace'](random_namespace)

    # Store namespace topics using add_namespace_topic
    topics = ['topic1', 'topic2', 'topic3']
    for topic in topics:
        db_service.add_namespace_topic(
            namespace=random_namespace,
            topic=topic,
        )
    print(f'  Stored topics: {topics}')

    # Verify it was stored
    result = db_service.get_namespace_topics(random_namespace)
    print('  Retrieved topics:')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    assert isinstance(result, list)
    assert len(result) == EXPECTED_TOPICS_COUNT_3
    assert set(result) == set(topics)


@pytest.mark.integration
def test_get_namespace_topics_nonexistent(
    db_service: DynamoDBService,
    random_namespace: str,
) -> None:
    """Test get_namespace_topics with nonexistent namespace."""
    print(
        f'\n[test_get_namespace_topics_nonexistent] '
        f'Testing with namespace: {random_namespace}',
    )

    # Try to get non-existent topics
    result = db_service.get_namespace_topics(random_namespace)
    print(f'  Retrieved topics: {result}')

    # Assertions
    assert isinstance(result, list)
    assert len(result) == 0


@pytest.mark.integration
def test_remove_namespace_topic(
    db_service: DynamoDBService,
    random_namespace: str,
    cleanup_data: Any,
) -> None:
    """Test remove_namespace_topic with real DynamoDB service."""
    print(
        f'\n[test_remove_namespace_topic] '
        f'Testing with namespace: {random_namespace}',
    )

    # Mark for cleanup
    cleanup_data['namespace'](random_namespace)

    # Store namespace topics first
    db_service.add_namespace_topic(
        namespace=random_namespace,
        topic='topic1',
    )
    db_service.add_namespace_topic(
        namespace=random_namespace,
        topic='topic2',
    )

    # Verify it exists
    topics = db_service.get_namespace_topics(random_namespace)
    assert len(topics) == EXPECTED_TOPICS_COUNT_2

    # Remove one topic
    db_service.remove_namespace_topic(random_namespace, 'topic1')
    print(f'  Removed topic1 for namespace: {random_namespace}')

    # Verify it's removed
    result = db_service.get_namespace_topics(random_namespace)
    print(f'  Retrieved topics after remove: {result}')

    # Assertions
    assert isinstance(result, list)
    assert len(result) == EXPECTED_TOPICS_COUNT_2 - 1
    assert 'topic2' in result
    assert 'topic1' not in result


@pytest.mark.integration
def test_get_global_namespaces_nonexistent(
    db_service: DynamoDBService,
) -> None:
    """Test get_global_namespaces when none exist."""
    print('\n[test_get_global_namespaces_nonexistent] Testing')

    # Try to get non-existent global namespaces
    result = db_service.get_global_namespaces()
    print(f'  Retrieved global namespaces: {result}')

    # Assertions
    assert isinstance(result, set)
    assert len(result) == 0


@pytest.mark.integration
def test_remove_global_namespace(
    db_service: DynamoDBService,
    cleanup_data: Any,
) -> None:
    """Test remove_global_namespace with real DynamoDB service."""
    print('\n[test_remove_global_namespace] Testing')

    # Store global namespaces first
    db_service.add_global_namespace('global1')
    db_service.add_global_namespace('global2')

    # Verify it exists
    global_namespaces = db_service.get_global_namespaces()
    assert len(global_namespaces) == EXPECTED_NAMESPACES_COUNT_2

    # Remove one namespace
    db_service.remove_global_namespace('global1')
    print('  Removed global1')

    # Verify it's removed
    result = db_service.get_global_namespaces()
    print(f'  Retrieved global namespaces after remove: {result}')

    # Assertions
    assert isinstance(result, set)
    assert len(result) == EXPECTED_NAMESPACES_COUNT_2 - 1
    assert 'global2' in result
    assert 'global1' not in result


@pytest.mark.integration
def test_full_lifecycle(
    db_service: DynamoDBService,
    random_subject: str,
    cleanup_data: Any,
) -> None:
    """Test full lifecycle: store key, store namespaces, delete all."""
    print(f'\n[test_full_lifecycle] Testing with subject: {random_subject}')

    # Mark for cleanup
    cleanup_data['subject'](random_subject)

    # 1. Store key
    create_date = datetime.now().isoformat()
    db_service.store_key(
        subject=random_subject,
        access_key='aaaaa',
        secret_key='bbbbb',  # pragma: allowlist secret
        create_date=create_date,
    )
    key_result = db_service.get_key(random_subject)
    assert key_result is not None
    assert key_result['access_key'] == 'aaaaa'

    # 2. Store user namespaces
    db_service.add_user_namespace(random_subject, 'ns1')
    db_service.add_user_namespace(random_subject, 'ns2')
    namespaces_result = db_service.get_user_namespaces(random_subject)
    assert len(namespaces_result) == EXPECTED_NAMESPACES_COUNT_2
    assert set(namespaces_result) == {'ns1', 'ns2'}

    # 3. Store global namespaces
    db_service.add_global_namespace('global1')
    db_service.add_global_namespace('global2')
    global_result = db_service.get_global_namespaces()
    assert len(global_result) == EXPECTED_NAMESPACES_COUNT_2
    assert global_result == {'global1', 'global2'}

    # 4. Delete key
    db_service.delete_key(random_subject)
    assert db_service.get_key(random_subject) is None

    # 5. Remove user namespaces
    db_service.remove_user_namespace(random_subject, 'ns1')
    db_service.remove_user_namespace(random_subject, 'ns2')
    assert len(db_service.get_user_namespaces(random_subject)) == 0

    # 6. Remove global namespaces
    db_service.remove_global_namespace('global1')
    db_service.remove_global_namespace('global2')
    assert len(db_service.get_global_namespaces()) == 0

    # 7. Store namespace topics
    random_namespace = f'test-namespace-{os.urandom(8).hex()}'
    db_service.add_namespace_topic(
        namespace=random_namespace,
        topic='topic1',
    )
    db_service.add_namespace_topic(
        namespace=random_namespace,
        topic='topic2',
    )
    topics_result = db_service.get_namespace_topics(random_namespace)
    assert len(topics_result) == EXPECTED_TOPICS_COUNT_2
    assert set(topics_result) == {'topic1', 'topic2'}

    # 8. Remove namespace topics
    db_service.remove_namespace_topic(random_namespace, 'topic1')
    db_service.remove_namespace_topic(random_namespace, 'topic2')
    assert len(db_service.get_namespace_topics(random_namespace)) == 0

    print('  Full lifecycle test completed successfully')


@pytest.mark.integration
def test_concurrent_add_user_namespace(
    db_service: DynamoDBService,
    random_subject: str,
    cleanup_data: Any,
) -> None:
    """Test race condition: concurrent add_user_namespace operations."""
    print(
        f'\n[test_concurrent_add_user_namespace] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_data['subject'](random_subject)

    # Number of concurrent operations
    num_threads = 10
    namespaces_per_thread = 5
    total_expected = num_threads * namespaces_per_thread

    errors: list[Exception] = []
    threads: list[threading.Thread] = []

    def add_namespaces(thread_id: int) -> None:
        """Add namespaces in a thread."""
        try:
            for i in range(namespaces_per_thread):
                namespace = f'ns-{thread_id}-{i}'
                db_service.add_user_namespace(random_subject, namespace)
        except Exception as e:
            errors.append(e)

    # Start all threads
    for i in range(num_threads):
        thread = threading.Thread(target=add_namespaces, args=(i,))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Check for errors
    if errors:
        raise Exception(f'Errors occurred: {errors}')

    # Verify all namespaces were added (sets deduplicate, so we check count)
    result = db_service.get_user_namespaces(random_subject)
    print(
        f'  Added {len(result)} unique namespaces '
        f'(expected ~{total_expected})',
    )

    # Assertions
    assert isinstance(result, list)
    # All namespaces should be present (no race condition losses)
    assert len(result) == total_expected
    # Verify specific namespaces exist
    for thread_id in range(num_threads):
        for i in range(namespaces_per_thread):
            namespace = f'ns-{thread_id}-{i}'
            assert namespace in result, f'Missing namespace: {namespace}'


@pytest.mark.integration
def test_concurrent_add_global_namespace(
    db_service: DynamoDBService,
    cleanup_data: Any,
) -> None:
    """Test race condition: concurrent add_global_namespace operations."""
    print('\n[test_concurrent_add_global_namespace] Testing')

    # Number of concurrent operations
    num_threads = 10
    namespaces_per_thread = 5
    total_expected = num_threads * namespaces_per_thread

    errors: list[Exception] = []
    threads: list[threading.Thread] = []

    def add_namespaces(thread_id: int) -> None:
        """Add global namespaces in a thread."""
        try:
            for i in range(namespaces_per_thread):
                namespace = f'global-{thread_id}-{i}'
                db_service.add_global_namespace(namespace)
        except Exception as e:
            errors.append(e)

    # Start all threads
    for i in range(num_threads):
        thread = threading.Thread(target=add_namespaces, args=(i,))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Check for errors
    if errors:
        raise Exception(f'Errors occurred: {errors}')

    # Verify all namespaces were added
    result = db_service.get_global_namespaces()
    print(f'  Added {len(result)} unique global namespaces')

    # Assertions
    assert isinstance(result, set)
    # All namespaces should be present (no race condition losses)
    assert len(result) == total_expected
    # Verify specific namespaces exist
    for thread_id in range(num_threads):
        for i in range(namespaces_per_thread):
            namespace = f'global-{thread_id}-{i}'
            assert namespace in result, f'Missing namespace: {namespace}'

    # Cleanup
    for namespace in result:
        with contextlib.suppress(Exception):
            db_service.remove_global_namespace(namespace)


@pytest.mark.integration
def test_concurrent_add_namespace_topic(
    db_service: DynamoDBService,
    random_namespace: str,
    cleanup_data: Any,
) -> None:
    """Test race condition: concurrent add_namespace_topic operations."""
    print(
        f'\n[test_concurrent_add_namespace_topic] '
        f'Testing with namespace: {random_namespace}',
    )

    # Mark for cleanup
    cleanup_data['namespace'](random_namespace)

    # Number of concurrent operations
    num_threads = 10
    topics_per_thread = 5
    total_expected = num_threads * topics_per_thread

    errors: list[Exception] = []
    threads: list[threading.Thread] = []

    def add_topics(thread_id: int) -> None:
        """Add topics in a thread."""
        try:
            for i in range(topics_per_thread):
                topic = f'topic-{thread_id}-{i}'
                db_service.add_namespace_topic(random_namespace, topic)
        except Exception as e:
            errors.append(e)

    # Start all threads
    for i in range(num_threads):
        thread = threading.Thread(target=add_topics, args=(i,))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Check for errors
    if errors:
        raise Exception(f'Errors occurred: {errors}')

    # Verify all topics were added
    result = db_service.get_namespace_topics(random_namespace)
    print(f'  Added {len(result)} unique topics (expected {total_expected})')

    # Assertions
    assert isinstance(result, list)
    # All topics should be present (no race condition losses)
    assert len(result) == total_expected
    # Verify specific topics exist
    for thread_id in range(num_threads):
        for i in range(topics_per_thread):
            topic = f'topic-{thread_id}-{i}'
            assert topic in result, f'Missing topic: {topic}'


@pytest.mark.integration
def test_concurrent_remove_operations(
    db_service: DynamoDBService,
    random_subject: str,
    random_namespace: str,
    cleanup_data: Any,
) -> None:
    """Test race condition: concurrent remove operations."""
    print(
        f'\n[test_concurrent_remove_operations] '
        f'Testing with subject: {random_subject}, '
        f'namespace: {random_namespace}',
    )

    # Mark for cleanup
    cleanup_data['subject'](random_subject)
    cleanup_data['namespace'](random_namespace)

    # Setup: Add items first
    num_items = 20
    for i in range(num_items):
        db_service.add_user_namespace(random_subject, f'ns-{i}')
        db_service.add_global_namespace(f'global-{i}')
        db_service.add_namespace_topic(random_namespace, f'topic-{i}')

    # Number of concurrent remove operations
    num_threads = 5
    items_per_thread = 4  # Each thread removes 4 items

    errors: list[Exception] = []
    threads: list[threading.Thread] = []

    def remove_items(thread_id: int) -> None:
        """Remove items in a thread."""
        try:
            for i in range(items_per_thread):
                item_id = thread_id * items_per_thread + i
                if item_id < num_items:
                    db_service.remove_user_namespace(
                        random_subject,
                        f'ns-{item_id}',
                    )
                    db_service.remove_global_namespace(f'global-{item_id}')
                    db_service.remove_namespace_topic(
                        random_namespace,
                        f'topic-{item_id}',
                    )
        except Exception as e:
            errors.append(e)

    # Start all threads
    for i in range(num_threads):
        thread = threading.Thread(target=remove_items, args=(i,))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Check for errors
    if errors:
        raise Exception(f'Errors occurred: {errors}')

    # Verify all items were removed
    user_namespaces = db_service.get_user_namespaces(random_subject)
    global_namespaces = db_service.get_global_namespaces()
    topics = db_service.get_namespace_topics(random_namespace)

    print(f'  Remaining user namespaces: {len(user_namespaces)}')
    print(f'  Remaining global namespaces: {len(global_namespaces)}')
    print(f'  Remaining topics: {len(topics)}')

    # Assertions - all items should be removed (idempotent operations)
    expected_remaining = max(0, num_items - (num_threads * items_per_thread))
    assert len(user_namespaces) == expected_remaining
    assert len(global_namespaces) == expected_remaining
    assert len(topics) == expected_remaining
