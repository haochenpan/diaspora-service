"""Unit tests for web_service_v3 DynamoDBService."""

from __future__ import annotations

import contextlib
import json
import os
from datetime import datetime
from typing import Any

import pytest

from web_service_v3.services import DynamoDBService

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
    keys_table_name = 'test-keys-table'
    users_table_name = 'test-users-table'
    namespace_table_name = 'test-namespace-table'

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
        with contextlib.suppress(Exception):
            db_service.delete_user_namespace(subject)
    for namespace in created_namespaces:
        with contextlib.suppress(Exception):
            db_service.delete_namespace_topics(namespace)
    with contextlib.suppress(Exception):
        db_service.delete_global_namespaces()


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
def test_put_user_namespace(
    db_service: DynamoDBService,
    random_subject: str,
    cleanup_data: Any,
) -> None:
    """Test put_user_namespace with real DynamoDB service."""
    print(
        f'\n[test_put_user_namespace] Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_data['subject'](random_subject)

    # Store user namespaces
    namespaces = ['namespace1', 'namespace2', 'namespace3']
    db_service.put_user_namespace(
        subject=random_subject,
        namespaces=namespaces,
    )
    print(f'  Stored namespaces: {namespaces}')

    # Verify it was stored
    result = db_service.get_user_namespaces(random_subject)
    print('  Retrieved namespaces:')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    assert isinstance(result, list)
    assert len(result) == EXPECTED_NAMESPACES_COUNT_3
    assert set(result) == set(namespaces)


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
def test_delete_user_namespace(
    db_service: DynamoDBService,
    random_subject: str,
    cleanup_data: Any,
) -> None:
    """Test delete_user_namespace with real DynamoDB service."""
    print(
        f'\n[test_delete_user_namespace] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_data['subject'](random_subject)

    # Store user namespaces first
    db_service.put_user_namespace(
        subject=random_subject,
        namespaces=['namespace1', 'namespace2'],
    )

    # Verify it exists
    assert len(db_service.get_user_namespaces(random_subject)) > 0

    # Delete user namespace
    db_service.delete_user_namespace(random_subject)
    print(f'  Deleted namespaces for subject: {random_subject}')

    # Verify it's gone
    result = db_service.get_user_namespaces(random_subject)
    print(f'  Retrieved namespaces after delete: {result}')

    # Assertions
    assert isinstance(result, list)
    assert len(result) == 0


@pytest.mark.integration
def test_put_namespace_topics(
    db_service: DynamoDBService,
    random_namespace: str,
    cleanup_data: Any,
) -> None:
    """Test put_namespace_topics with real DynamoDB service."""
    print(
        f'\n[test_put_namespace_topics] '
        f'Testing with namespace: {random_namespace}',
    )

    # Mark for cleanup
    cleanup_data['namespace'](random_namespace)

    # Store namespace topics
    topics = ['topic1', 'topic2', 'topic3']
    db_service.put_namespace_topics(
        namespace=random_namespace,
        topics=topics,
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
def test_delete_namespace_topics(
    db_service: DynamoDBService,
    random_namespace: str,
    cleanup_data: Any,
) -> None:
    """Test delete_namespace_topics with real DynamoDB service."""
    print(
        f'\n[test_delete_namespace_topics] '
        f'Testing with namespace: {random_namespace}',
    )

    # Mark for cleanup
    cleanup_data['namespace'](random_namespace)

    # Store namespace topics first
    db_service.put_namespace_topics(
        namespace=random_namespace,
        topics=['topic1', 'topic2'],
    )

    # Verify it exists
    assert len(db_service.get_namespace_topics(random_namespace)) > 0

    # Delete namespace topics
    db_service.delete_namespace_topics(random_namespace)
    print(f'  Deleted topics for namespace: {random_namespace}')

    # Verify it's gone
    result = db_service.get_namespace_topics(random_namespace)
    print(f'  Retrieved topics after delete: {result}')

    # Assertions
    assert isinstance(result, list)
    assert len(result) == 0


@pytest.mark.integration
def test_put_global_namespaces(
    db_service: DynamoDBService,
    cleanup_data: Any,
) -> None:
    """Test put_global_namespaces with real DynamoDB service."""
    print('\n[test_put_global_namespaces] Testing global namespaces')

    # Store global namespaces
    namespaces = {'global1', 'global2', 'global3'}
    db_service.put_global_namespaces(namespaces)
    print(f'  Stored global namespaces: {sorted(namespaces)}')

    # Verify it was stored
    result = db_service.get_global_namespaces()
    print('  Retrieved global namespaces:')
    print(json.dumps(sorted(result), indent=2, default=str))

    # Assertions
    assert isinstance(result, set)
    assert len(result) == EXPECTED_NAMESPACES_COUNT_3
    assert result == namespaces


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
def test_delete_global_namespaces(
    db_service: DynamoDBService,
    cleanup_data: Any,
) -> None:
    """Test delete_global_namespaces with real DynamoDB service."""
    print('\n[test_delete_global_namespaces] Testing')

    # Store global namespaces first
    db_service.put_global_namespaces({'global1', 'global2'})

    # Verify it exists
    assert len(db_service.get_global_namespaces()) > 0

    # Delete global namespaces
    db_service.delete_global_namespaces()
    print('  Deleted global namespaces')

    # Verify it's gone
    result = db_service.get_global_namespaces()
    print(f'  Retrieved global namespaces after delete: {result}')

    # Assertions
    assert isinstance(result, set)
    assert len(result) == 0


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
    db_service.put_user_namespace(
        subject=random_subject,
        namespaces=['ns1', 'ns2'],
    )
    namespaces_result = db_service.get_user_namespaces(random_subject)
    assert len(namespaces_result) == EXPECTED_NAMESPACES_COUNT_2
    assert set(namespaces_result) == {'ns1', 'ns2'}

    # 3. Store global namespaces
    db_service.put_global_namespaces({'global1', 'global2'})
    global_result = db_service.get_global_namespaces()
    assert len(global_result) == EXPECTED_NAMESPACES_COUNT_2
    assert global_result == {'global1', 'global2'}

    # 4. Delete key
    db_service.delete_key(random_subject)
    assert db_service.get_key(random_subject) is None

    # 5. Delete user namespace
    db_service.delete_user_namespace(random_subject)
    assert len(db_service.get_user_namespaces(random_subject)) == 0

    # 6. Delete global namespaces
    db_service.delete_global_namespaces()
    assert len(db_service.get_global_namespaces()) == 0

    # 7. Store namespace topics
    random_namespace = f'test-namespace-{os.urandom(8).hex()}'
    db_service.put_namespace_topics(
        namespace=random_namespace,
        topics=['topic1', 'topic2'],
    )
    topics_result = db_service.get_namespace_topics(random_namespace)
    assert len(topics_result) == EXPECTED_TOPICS_COUNT_2
    assert set(topics_result) == {'topic1', 'topic2'}

    # 8. Delete namespace topics
    db_service.delete_namespace_topics(random_namespace)
    assert len(db_service.get_namespace_topics(random_namespace)) == 0

    print('  Full lifecycle test completed successfully')
