"""Unit tests for web_service_v3 NamespaceService."""

from __future__ import annotations

import contextlib
import json
import os
from typing import Any

import pytest

from web_service_v3.services import DynamoDBService
from web_service_v3.services import NamespaceService

# ============================================================================
# Test Constants
# ============================================================================

EXPECTED_NAMESPACE_LENGTH = 15  # 'ns-' + 12 chars
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
def namespace_service(db_service: DynamoDBService) -> NamespaceService:
    """Create a NamespaceService instance."""
    return NamespaceService(dynamodb_service=db_service)


@pytest.fixture
def random_subject() -> str:
    """Generate a random subject for each test."""
    return f'test-subject-{os.urandom(8).hex()}'


@pytest.fixture
def cleanup_data(
    namespace_service: NamespaceService,
    db_service: DynamoDBService,
) -> Any:
    """Fixture that provides cleanup function for test data."""
    created_subjects: list[str] = []
    created_namespaces: list[tuple[str, str]] = []

    def _cleanup_subject(subject: str) -> None:
        """Mark a subject for cleanup."""
        if subject not in created_subjects:
            created_subjects.append(subject)

    def _cleanup_namespace(subject: str, namespace: str) -> None:
        """Mark a namespace for cleanup."""
        namespace_key = (subject, namespace)
        if namespace_key not in created_namespaces:
            created_namespaces.append(namespace_key)

    yield {
        'subject': _cleanup_subject,
        'namespace': _cleanup_namespace,
    }

    # Cleanup all created data
    for subject, namespace in created_namespaces:
        with contextlib.suppress(Exception):
            # Delete all topics first
            topics = db_service.get_namespace_topics(namespace)
            for topic in topics:
                namespace_service.delete_topic(subject, namespace, topic)
            # Delete namespace
            namespace_service.delete_namespace(subject, namespace)
    for subject in created_subjects:
        with contextlib.suppress(Exception):
            db_service.delete_key(subject)
        # Remove all user namespaces individually
        with contextlib.suppress(Exception):
            namespaces = db_service.get_user_namespaces(subject)
            for namespace in namespaces:
                db_service.remove_user_namespace(subject, namespace)
    # Remove all global namespaces individually
    with contextlib.suppress(Exception):
        global_namespaces = db_service.get_global_namespaces()
        for namespace in global_namespaces:
            db_service.remove_global_namespace(namespace)


# ============================================================================
# Integration Tests with Real AWS Services
# ============================================================================


@pytest.mark.integration
def test_validate_name(
    namespace_service: NamespaceService,
) -> None:
    """Test validate_name with comprehensive edge cases.

    Covers all validation scenarios including:
    - Empty strings and boundary values
    - Length constraints (min/max)
    - Special characters and invalid patterns
    - Valid patterns (dash, underscore, numbers, mixed case)
    - Unicode and whitespace characters
    """
    print('\n[test_validate_name] Testing name validation')

    # ========================================================================
    # Valid names
    # ========================================================================
    assert namespace_service.validate_name('valid-ns-123') is None
    assert namespace_service.validate_name('Valid_NS_123') is None
    assert namespace_service.validate_name('abc') is None  # Min length (3)
    assert namespace_service.validate_name('a' * 32) is None  # Max (32)
    assert namespace_service.validate_name('valid-name') is None  # Dash OK
    assert namespace_service.validate_name('valid_name') is None  # Underscore OK
    assert namespace_service.validate_name('valid123name') is None  # Numbers OK
    assert namespace_service.validate_name('ValidName123') is None  # Mixed case OK

    # ========================================================================
    # Invalid: Empty and too short
    # ========================================================================
    result = namespace_service.validate_name('')  # Empty string
    assert result is not None
    assert result['status'] == 'failure'
    assert 'between' in result['message'].lower()

    result = namespace_service.validate_name('ab')  # Too short (2 chars)
    assert result is not None
    assert result['status'] == 'failure'
    assert 'between' in result['message'].lower()

    # ========================================================================
    # Invalid: Too long
    # ========================================================================
    result = namespace_service.validate_name('a' * 33)  # Too long (33 chars)
    assert result is not None
    assert result['status'] == 'failure'
    assert 'between' in result['message'].lower()

    # ========================================================================
    # Invalid: Special characters
    # ========================================================================
    invalid_special_chars = [
        'test@name',
        'test.name',
        'test name',  # Space
        'test!name',
        'test#name',
    ]
    for name in invalid_special_chars:
        result = namespace_service.validate_name(name)
        assert result is not None, f'{name} should be invalid'
        assert result['status'] == 'failure'
        assert (
            'letters' in result['message'].lower()
            or 'characters' in result['message'].lower()
        )

    # ========================================================================
    # Invalid: Starts/ends with dash or underscore
    # ========================================================================
    result = namespace_service.validate_name('-invalid')  # Starts with dash
    assert result is not None
    assert result['status'] == 'failure'
    assert 'start' in result['message'].lower()

    result = namespace_service.validate_name('invalid-')  # Ends with dash
    assert result is not None
    assert result['status'] == 'failure'
    msg_lower = result['message'].lower()
    assert 'end' in msg_lower or 'start' in msg_lower

    result = namespace_service.validate_name('_invalid')  # Starts with underscore
    assert result is not None
    assert result['status'] == 'failure'
    assert 'start' in result['message'].lower()

    result = namespace_service.validate_name('invalid_')  # Ends underscore
    assert result is not None
    assert result['status'] == 'failure'
    msg_lower = result['message'].lower()
    assert 'end' in msg_lower or 'start' in msg_lower

    # ========================================================================
    # Invalid: Unicode characters
    # ========================================================================
    unicode_names = ['测试', 'тест', 'テスト', '🎉test']
    for name in unicode_names:
        result = namespace_service.validate_name(name)
        assert result is not None, f'{name} should be invalid'
        assert result['status'] == 'failure'

    # ========================================================================
    # Invalid: Whitespace and control characters
    # ========================================================================
    result = namespace_service.validate_name('   ')  # Whitespace only
    assert result is not None
    assert result['status'] == 'failure'

    result = namespace_service.validate_name('test\nname')  # Newline
    assert result is not None
    assert result['status'] == 'failure'

    result = namespace_service.validate_name('test\tname')  # Tab
    assert result is not None
    assert result['status'] == 'failure'

    # ========================================================================
    # Boundary value tests
    # ========================================================================
    boundary_cases = [
        ('ab', False),  # Too short (2 chars)
        ('abc', True),  # Minimum valid (3 chars)
        ('a' * 32, True),  # Maximum valid (32 chars)
        ('a' * 33, False),  # Too long (33 chars)
    ]

    for name, should_be_valid in boundary_cases:
        result = namespace_service.validate_name(name)
        if should_be_valid:
            assert result is None, f'{name} should be valid'
        else:
            assert result is not None, f'{name} should be invalid'
            assert result['status'] == 'failure'

    print('  All validation tests passed')


@pytest.mark.integration
def test_generate_default(
    namespace_service: NamespaceService,
    random_subject: str,
) -> None:
    """Test generate_default namespace name."""
    print(
        f'\n[test_generate_default] Testing with subject: {random_subject}',
    )

    namespace = namespace_service.generate_default(random_subject)
    print(f'  Generated namespace: {namespace}')

    # Assertions
    assert namespace.startswith('ns-')
    assert len(namespace) == EXPECTED_NAMESPACE_LENGTH
    assert random_subject.replace('-', '')[-12:] in namespace


@pytest.mark.integration
def test_create_namespace(
    namespace_service: NamespaceService,
    random_subject: str,
    cleanup_data: Any,
) -> None:
    """Test create_namespace with real DynamoDB service."""
    print(
        f'\n[test_create_namespace] Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_data['subject'](random_subject)

    namespace = namespace_service.generate_default(random_subject)
    cleanup_data['namespace'](random_subject, namespace)

    # Create namespace
    result = namespace_service.create_namespace(
        subject=random_subject,
        namespace=namespace,
    )
    print('  Create namespace result:')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'message' in result
    assert 'namespaces' in result
    assert namespace in result['namespaces']


@pytest.mark.integration
def test_create_namespace_idempotent(
    namespace_service: NamespaceService,
    random_subject: str,
    cleanup_data: Any,
) -> None:
    """Test create_namespace is idempotent."""
    print(
        f'\n[test_create_namespace_idempotent] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_data['subject'](random_subject)

    namespace = namespace_service.generate_default(random_subject)
    cleanup_data['namespace'](random_subject, namespace)

    # Create namespace first time
    result1 = namespace_service.create_namespace(
        subject=random_subject,
        namespace=namespace,
    )
    print('  First create result:')
    print(json.dumps(result1, indent=2, default=str))

    # Create namespace second time (should succeed as idempotent)
    result2 = namespace_service.create_namespace(
        subject=random_subject,
        namespace=namespace,
    )
    print('  Second create result:')
    print(json.dumps(result2, indent=2, default=str))

    # Assertions
    assert result1['status'] == 'success'
    assert result2['status'] == 'success'
    assert 'already exists' in result2['message'].lower()


@pytest.mark.integration
def test_delete_namespace(
    namespace_service: NamespaceService,
    random_subject: str,
    cleanup_data: Any,
) -> None:
    """Test delete_namespace with real DynamoDB service."""
    print(
        f'\n[test_delete_namespace] Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_data['subject'](random_subject)

    namespace = namespace_service.generate_default(random_subject)

    # Create namespace first
    create_result = namespace_service.create_namespace(
        subject=random_subject,
        namespace=namespace,
    )
    assert create_result['status'] == 'success'

    # Delete namespace
    result = namespace_service.delete_namespace(
        subject=random_subject,
        namespace=namespace,
    )
    print('  Delete namespace result:')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'message' in result
    assert 'namespaces' in result
    assert namespace not in result['namespaces']


@pytest.mark.integration
def test_delete_namespace_idempotent(
    namespace_service: NamespaceService,
    random_subject: str,
    cleanup_data: Any,
) -> None:
    """Test delete_namespace is idempotent."""
    print(
        f'\n[test_delete_namespace_idempotent] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_data['subject'](random_subject)

    namespace = namespace_service.generate_default(random_subject)

    # Delete non-existent namespace (should succeed as idempotent)
    result = namespace_service.delete_namespace(
        subject=random_subject,
        namespace=namespace,
    )
    print('  Delete result:')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'not found' in result['message'].lower()


@pytest.mark.integration
def test_create_topic(
    namespace_service: NamespaceService,
    random_subject: str,
    cleanup_data: Any,
) -> None:
    """Test create_topic with real DynamoDB service."""
    print(
        f'\n[test_create_topic] Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_data['subject'](random_subject)

    namespace = namespace_service.generate_default(random_subject)
    cleanup_data['namespace'](random_subject, namespace)

    # Create namespace first
    namespace_service.create_namespace(random_subject, namespace)

    # Create topic
    topic = 'test-topic-1'
    result = namespace_service.create_topic(
        subject=random_subject,
        namespace=namespace,
        topic=topic,
    )
    print('  Create topic result:')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'message' in result
    assert 'topics' in result
    assert topic in result['topics']


@pytest.mark.integration
def test_create_topic_idempotent(
    namespace_service: NamespaceService,
    random_subject: str,
    cleanup_data: Any,
) -> None:
    """Test create_topic is idempotent."""
    print(
        f'\n[test_create_topic_idempotent] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_data['subject'](random_subject)

    namespace = namespace_service.generate_default(random_subject)
    cleanup_data['namespace'](random_subject, namespace)

    # Create namespace first
    namespace_service.create_namespace(random_subject, namespace)

    topic = 'test-topic-1'

    # Create topic first time
    result1 = namespace_service.create_topic(
        subject=random_subject,
        namespace=namespace,
        topic=topic,
    )

    # Create topic second time (should succeed as idempotent)
    result2 = namespace_service.create_topic(
        subject=random_subject,
        namespace=namespace,
        topic=topic,
    )
    print('  Second create result:')
    print(json.dumps(result2, indent=2, default=str))

    # Assertions
    assert result1['status'] == 'success'
    assert result2['status'] == 'success'
    assert 'already exists' in result2['message'].lower()


@pytest.mark.integration
def test_delete_topic(
    namespace_service: NamespaceService,
    random_subject: str,
    cleanup_data: Any,
) -> None:
    """Test delete_topic with real DynamoDB service."""
    print(
        f'\n[test_delete_topic] Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_data['subject'](random_subject)

    namespace = namespace_service.generate_default(random_subject)
    cleanup_data['namespace'](random_subject, namespace)

    # Create namespace and topic first
    namespace_service.create_namespace(random_subject, namespace)
    topic = 'test-topic-1'
    namespace_service.create_topic(random_subject, namespace, topic)

    # Delete topic
    result = namespace_service.delete_topic(
        subject=random_subject,
        namespace=namespace,
        topic=topic,
    )
    print('  Delete topic result:')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'message' in result
    assert 'topics' in result
    assert topic not in result['topics']


@pytest.mark.integration
def test_delete_topic_idempotent(
    namespace_service: NamespaceService,
    random_subject: str,
    cleanup_data: Any,
) -> None:
    """Test delete_topic is idempotent."""
    print(
        f'\n[test_delete_topic_idempotent] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_data['subject'](random_subject)

    namespace = namespace_service.generate_default(random_subject)
    cleanup_data['namespace'](random_subject, namespace)

    # Create namespace first
    namespace_service.create_namespace(random_subject, namespace)

    # Delete non-existent topic (should succeed as idempotent)
    result = namespace_service.delete_topic(
        subject=random_subject,
        namespace=namespace,
        topic='non-existent-topic',
    )
    print('  Delete result:')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'not found' in result['message'].lower()


@pytest.mark.integration
def test_list_namespace_and_topics(
    namespace_service: NamespaceService,
    random_subject: str,
    cleanup_data: Any,
) -> None:
    """Test list_namespace_and_topics with real DynamoDB service."""
    print(
        f'\n[test_list_namespace_and_topics] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_data['subject'](random_subject)

    namespace = namespace_service.generate_default(random_subject)
    cleanup_data['namespace'](random_subject, namespace)

    # Create namespace and topics
    namespace_service.create_namespace(random_subject, namespace)
    namespace_service.create_topic(random_subject, namespace, 'topic1')
    namespace_service.create_topic(random_subject, namespace, 'topic2')

    # List namespaces and topics
    result = namespace_service.list_namespace_and_topics(random_subject)
    print('  List result:')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'message' in result
    assert 'namespaces' in result
    assert namespace in result['namespaces']
    assert 'topic1' in result['namespaces'][namespace]
    assert 'topic2' in result['namespaces'][namespace]


@pytest.mark.integration
def test_full_lifecycle(
    namespace_service: NamespaceService,
    random_subject: str,
    cleanup_data: Any,
) -> None:
    """Test full lifecycle.

    Creates namespace, creates topics, deletes topics, deletes namespace.
    """
    print(
        f'\n[test_full_lifecycle] Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_data['subject'](random_subject)

    namespace = namespace_service.generate_default(random_subject)
    cleanup_data['namespace'](random_subject, namespace)

    # 1. Create namespace
    create_result = namespace_service.create_namespace(
        random_subject,
        namespace,
    )
    assert create_result['status'] == 'success'

    # 2. Create topics
    topic1 = 'topic1'
    topic2 = 'topic2'
    create_topic1 = namespace_service.create_topic(
        random_subject,
        namespace,
        topic1,
    )
    assert create_topic1['status'] == 'success'
    create_topic2 = namespace_service.create_topic(
        random_subject,
        namespace,
        topic2,
    )
    assert create_topic2['status'] == 'success'

    # 3. List namespaces and topics
    list_result = namespace_service.list_namespace_and_topics(random_subject)
    assert list_result['status'] == 'success'
    assert len(list_result['namespaces'][namespace]) == EXPECTED_TOPICS_COUNT_2

    # 4. Delete topic
    delete_topic_result = namespace_service.delete_topic(
        random_subject,
        namespace,
        topic1,
    )
    assert delete_topic_result['status'] == 'success'
    assert topic1 not in delete_topic_result['topics']

    # 5. Delete namespace
    delete_result = namespace_service.delete_namespace(
        random_subject,
        namespace,
    )
    assert delete_result['status'] == 'success'
    assert namespace not in delete_result['namespaces']

    print('  Full lifecycle test completed successfully')
