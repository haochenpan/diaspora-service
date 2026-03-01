"""Edge case tests for web_service services.

These tests use real AWS services (DynamoDB) to verify edge case handling
in actual service interactions.
"""

from __future__ import annotations

import os

import pytest
from botocore.exceptions import ClientError

from web_service.services import DynamoDBService
from web_service.services import NamespaceService

# Test constants
EXPECTED_NAMESPACE_LENGTH_SHORT = 6  # 'ns-' + 3 chars
EXPECTED_NAMESPACE_LENGTH_STANDARD = 15  # 'ns-' + 12 chars
EXPECTED_NAMESPACE_LENGTH_MIN = 4  # 'ns-' + 1 char

# ============================================================================
# NamespaceService Edge Cases
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


# ============================================================================
# generate_default Edge Cases
# ============================================================================


def test_generate_default_short_subject(
    namespace_service: NamespaceService,
) -> None:
    """Test generate_default with short subject."""
    short_subject = 'abc'
    namespace = namespace_service.generate_default(short_subject)

    assert namespace.startswith('ns-')
    # When subject is shorter than 12 chars, uses all available chars
    assert namespace == 'ns-abc'
    assert len(namespace) == EXPECTED_NAMESPACE_LENGTH_SHORT


def test_generate_default_long_subject(
    namespace_service: NamespaceService,
) -> None:
    """Test generate_default with long subject."""
    long_subject = 'a' * 100
    namespace = namespace_service.generate_default(long_subject)

    assert namespace.startswith('ns-')
    assert len(namespace) == EXPECTED_NAMESPACE_LENGTH_STANDARD


def test_generate_default_subject_with_dashes(
    namespace_service: NamespaceService,
) -> None:
    """Test generate_default with subject containing dashes."""
    subject_with_dashes = '123e4567-e89b-12d3-a456-426614174000'
    namespace = namespace_service.generate_default(subject_with_dashes)

    assert namespace.startswith('ns-')
    # Dashes should be removed
    assert '-' not in namespace[3:]  # After 'ns-' prefix


def test_generate_default_subject_without_dashes(
    namespace_service: NamespaceService,
) -> None:
    """Test generate_default with subject without dashes."""
    subject_no_dashes = (
        '123e4567e89b12d3a456426614174000'  # pragma: allowlist secret
    )
    namespace = namespace_service.generate_default(subject_no_dashes)

    assert namespace.startswith('ns-')
    assert subject_no_dashes[-12:] in namespace


# ============================================================================
# DynamoDBService Edge Cases
# ============================================================================


@pytest.mark.integration
def test_get_key_nonexistent_subject(db_service: DynamoDBService) -> None:
    """Test get_key with nonexistent subject using real AWS service."""
    # Use a random subject that doesn't exist
    random_subject = f'nonexistent-{os.urandom(8).hex()}'

    result = db_service.get_key(random_subject)

    assert result is None


@pytest.mark.integration
def test_get_user_namespaces_nonexistent_subject(
    db_service: DynamoDBService,
) -> None:
    """Test get_user_namespaces with nonexistent subject using real AWS."""
    random_subject = f'nonexistent-{os.urandom(8).hex()}'

    result = db_service.get_user_namespaces(random_subject)

    assert isinstance(result, list)
    assert len(result) == 0


@pytest.mark.integration
def test_get_namespace_topics_nonexistent_namespace(
    db_service: DynamoDBService,
) -> None:
    """Test get_namespace_topics with nonexistent namespace using real AWS."""
    random_namespace = f'nonexistent-{os.urandom(8).hex()}'

    result = db_service.get_namespace_topics(random_namespace)

    assert isinstance(result, list)
    assert len(result) == 0


@pytest.mark.integration
def test_get_global_namespaces_empty(db_service: DynamoDBService) -> None:
    """Test get_global_namespaces when empty using real AWS service."""
    result = db_service.get_global_namespaces()

    assert isinstance(result, set)
    assert len(result) == 0


# ============================================================================
# Input Edge Cases
# ============================================================================


@pytest.mark.integration
def test_store_key_empty_strings(db_service: DynamoDBService) -> None:
    """Test store_key with empty strings (edge case).

    DynamoDB doesn't allow empty strings for key attributes.
    This test verifies the error handling with real AWS service.
    """
    with pytest.raises(ClientError) as exc_info:
        db_service.store_key(
            subject='',
            access_key='',
            secret_key='',
            create_date='',
        )

    # Should raise ValidationException
    assert exc_info.value.response['Error']['Code'] in [
        'ValidationException',
        'ResourceNotFoundException',
    ]


@pytest.mark.integration
def test_add_user_namespace_empty_strings(db_service: DynamoDBService) -> None:
    """Test add_user_namespace with empty strings (edge case).

    DynamoDB doesn't allow empty strings for key attributes.
    This test verifies the error handling with real AWS service.
    """
    with pytest.raises(ClientError) as exc_info:
        db_service.add_user_namespace('', '')

    # Should raise ValidationException or ResourceNotFoundException
    assert exc_info.value.response['Error']['Code'] in [
        'ValidationException',
        'ResourceNotFoundException',
    ]


@pytest.mark.integration
def test_add_namespace_topic_empty_strings(
    db_service: DynamoDBService,
) -> None:
    """Test add_namespace_topic with empty strings (edge case).

    DynamoDB doesn't allow empty strings for key attributes.
    This test verifies the error handling with real AWS service.
    """
    with pytest.raises(ClientError) as exc_info:
        db_service.add_namespace_topic('', '')

    # Should raise ValidationException or ResourceNotFoundException
    assert exc_info.value.response['Error']['Code'] in [
        'ValidationException',
        'ResourceNotFoundException',
    ]


# ============================================================================
# Boundary Value Tests (generate_default)
# ============================================================================


def test_generate_default_boundary_subjects(
    namespace_service: NamespaceService,
) -> None:
    """Test generate_default with boundary subject lengths."""
    # Very short subject (1 char)
    short_subject = 'a'
    namespace1 = namespace_service.generate_default(short_subject)
    assert namespace1.startswith('ns-')
    assert namespace1 == 'ns-a'
    assert len(namespace1) == EXPECTED_NAMESPACE_LENGTH_MIN

    # Exactly 12 characters (no dashes)
    subject_12 = 'a' * 12
    namespace2 = namespace_service.generate_default(subject_12)
    assert namespace2 == f'ns-{subject_12}'
    assert len(namespace2) == EXPECTED_NAMESPACE_LENGTH_STANDARD

    # Very long subject
    long_subject = 'a' * 1000
    namespace3 = namespace_service.generate_default(long_subject)
    assert namespace3.startswith('ns-')
    assert len(namespace3) == EXPECTED_NAMESPACE_LENGTH_STANDARD
    # Should use last 12 characters
    assert namespace3[3:] == long_subject[-12:]


# ============================================================================
# Topic Name Length Boundary Tests
# ============================================================================


def test_validate_name_topic_length_boundaries(
    namespace_service: NamespaceService,
) -> None:
    """Test validate_name with topic name length boundaries (3-32 chars)."""
    # Too short: 2 chars
    result = namespace_service.validate_name('ab')
    assert result is not None
    assert result['status'] == 'failure'
    assert 'between' in result['message'].lower()

    # Minimum: 3 chars
    result = namespace_service.validate_name('abc')
    assert result is None  # Valid

    # Maximum: 32 chars
    max_name = 'a' * 32
    result = namespace_service.validate_name(max_name)
    assert result is None  # Valid

    # Too long: 33 chars
    too_long = 'a' * 33
    result = namespace_service.validate_name(too_long)
    assert result is not None
    assert result['status'] == 'failure'
    assert 'between' in result['message'].lower()


def test_validate_name_very_long_string(
    namespace_service: NamespaceService,
) -> None:
    """Test validate_name with very long strings beyond max length."""
    # Test with extremely long string (1000 chars)
    very_long = 'a' * 1000
    result = namespace_service.validate_name(very_long)
    assert result is not None
    assert result['status'] == 'failure'
    assert 'between' in result['message'].lower()
    assert '32' in result['message']  # Should mention max length


def test_validate_name_none_value(
    namespace_service: NamespaceService,
) -> None:
    """Test validate_name with None value (where applicable).

    Note: validate_name expects a string, so None would cause TypeError.
    This test verifies the behavior when None is passed.
    """
    # validate_name expects a string, so None should raise TypeError
    with pytest.raises((TypeError, AttributeError)):
        namespace_service.validate_name(None)
