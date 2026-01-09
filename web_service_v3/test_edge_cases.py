"""Edge case tests for web_service_v3 services.

These tests use real AWS services (DynamoDB) to verify edge case handling
in actual service interactions.
"""

from __future__ import annotations

import os

import pytest

from web_service_v3.services import DynamoDBService
from web_service_v3.services import NamespaceService


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
# Name Validation Edge Cases
# ============================================================================


def test_validate_name_empty_string(namespace_service: NamespaceService) -> None:
    """Test validate_name with empty string."""
    result = namespace_service.validate_name('')

    assert result is not None
    assert result['status'] == 'failure'
    assert 'between' in result['message'].lower()


def test_validate_name_too_short(namespace_service: NamespaceService) -> None:
    """Test validate_name with name too short (2 characters)."""
    result = namespace_service.validate_name('ab')

    assert result is not None
    assert result['status'] == 'failure'
    assert 'between' in result['message'].lower()


def test_validate_name_min_length(namespace_service: NamespaceService) -> None:
    """Test validate_name with minimum valid length (3 characters)."""
    result = namespace_service.validate_name('abc')

    assert result is None  # Valid


def test_validate_name_max_length(namespace_service: NamespaceService) -> None:
    """Test validate_name with maximum valid length (32 characters)."""
    result = namespace_service.validate_name('a' * 32)

    assert result is None  # Valid


def test_validate_name_too_long(namespace_service: NamespaceService) -> None:
    """Test validate_name with name too long (33 characters)."""
    result = namespace_service.validate_name('a' * 33)

    assert result is not None
    assert result['status'] == 'failure'
    assert 'between' in result['message'].lower()


def test_validate_name_special_characters(namespace_service: NamespaceService) -> None:
    """Test validate_name with special characters."""
    invalid_names = ['test@name', 'test.name', 'test name', 'test!name', 'test#name']

    for name in invalid_names:
        result = namespace_service.validate_name(name)
        assert result is not None
        assert result['status'] == 'failure'
        assert 'letters' in result['message'].lower() or 'characters' in result['message'].lower()


def test_validate_name_starts_with_dash(namespace_service: NamespaceService) -> None:
    """Test validate_name with name starting with dash."""
    result = namespace_service.validate_name('-invalid')

    assert result is not None
    assert result['status'] == 'failure'
    assert 'start' in result['message'].lower()


def test_validate_name_ends_with_dash(namespace_service: NamespaceService) -> None:
    """Test validate_name with name ending with dash."""
    result = namespace_service.validate_name('invalid-')

    assert result is not None
    assert result['status'] == 'failure'
    assert 'end' in result['message'].lower() or 'start' in result['message'].lower()


def test_validate_name_starts_with_underscore(
    namespace_service: NamespaceService,
) -> None:
    """Test validate_name with name starting with underscore."""
    result = namespace_service.validate_name('_invalid')

    assert result is not None
    assert result['status'] == 'failure'
    assert 'start' in result['message'].lower()


def test_validate_name_ends_with_underscore(
    namespace_service: NamespaceService,
) -> None:
    """Test validate_name with name ending with underscore."""
    result = namespace_service.validate_name('invalid_')

    assert result is not None
    assert result['status'] == 'failure'
    assert 'end' in result['message'].lower() or 'start' in result['message'].lower()


def test_validate_name_valid_with_dash(namespace_service: NamespaceService) -> None:
    """Test validate_name with valid name containing dash."""
    result = namespace_service.validate_name('valid-name')

    assert result is None  # Valid


def test_validate_name_valid_with_underscore(
    namespace_service: NamespaceService,
) -> None:
    """Test validate_name with valid name containing underscore."""
    result = namespace_service.validate_name('valid_name')

    assert result is None  # Valid


def test_validate_name_valid_with_numbers(namespace_service: NamespaceService) -> None:
    """Test validate_name with valid name containing numbers."""
    result = namespace_service.validate_name('valid123name')

    assert result is None  # Valid


def test_validate_name_valid_mixed_case(namespace_service: NamespaceService) -> None:
    """Test validate_name with valid mixed case name."""
    result = namespace_service.validate_name('ValidName123')

    assert result is None  # Valid


# ============================================================================
# generate_default Edge Cases
# ============================================================================


def test_generate_default_short_subject(namespace_service: NamespaceService) -> None:
    """Test generate_default with short subject."""
    short_subject = 'abc'
    namespace = namespace_service.generate_default(short_subject)

    assert namespace.startswith('ns-')
    # When subject is shorter than 12 chars, uses all available chars
    assert namespace == 'ns-abc'
    assert len(namespace) == 6  # 'ns-' + 3 chars (not padded to 12)


def test_generate_default_long_subject(namespace_service: NamespaceService) -> None:
    """Test generate_default with long subject."""
    long_subject = 'a' * 100
    namespace = namespace_service.generate_default(long_subject)

    assert namespace.startswith('ns-')
    assert len(namespace) == 15  # 'ns-' + 12 chars (last 12 chars of subject)


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
    subject_no_dashes = '123e4567e89b12d3a456426614174000'
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
    """Test get_user_namespaces with nonexistent subject using real AWS service."""
    random_subject = f'nonexistent-{os.urandom(8).hex()}'

    result = db_service.get_user_namespaces(random_subject)

    assert isinstance(result, list)
    assert len(result) == 0


@pytest.mark.integration
def test_get_namespace_topics_nonexistent_namespace(
    db_service: DynamoDBService,
) -> None:
    """Test get_namespace_topics with nonexistent namespace using real AWS service."""
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
    # DynamoDB doesn't allow empty strings for key attributes
    from botocore.exceptions import ClientError

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
    # DynamoDB doesn't allow empty strings for key attributes
    from botocore.exceptions import ClientError

    with pytest.raises(ClientError) as exc_info:
        db_service.add_user_namespace('', '')

    # Should raise ValidationException or ResourceNotFoundException
    assert exc_info.value.response['Error']['Code'] in [
        'ValidationException',
        'ResourceNotFoundException',
    ]


@pytest.mark.integration
def test_add_namespace_topic_empty_strings(db_service: DynamoDBService) -> None:
    """Test add_namespace_topic with empty strings (edge case).

    DynamoDB doesn't allow empty strings for key attributes.
    This test verifies the error handling with real AWS service.
    """
    # DynamoDB doesn't allow empty strings for key attributes
    from botocore.exceptions import ClientError

    with pytest.raises(ClientError) as exc_info:
        db_service.add_namespace_topic('', '')

    # Should raise ValidationException or ResourceNotFoundException
    assert exc_info.value.response['Error']['Code'] in [
        'ValidationException',
        'ResourceNotFoundException',
    ]


# ============================================================================
# Unicode and Special Character Edge Cases
# ============================================================================


def test_validate_name_unicode_characters(namespace_service: NamespaceService) -> None:
    """Test validate_name with Unicode characters."""
    unicode_names = ['测试', 'тест', 'テスト', '🎉test']

    for name in unicode_names:
        result = namespace_service.validate_name(name)
        assert result is not None
        assert result['status'] == 'failure'


def test_validate_name_whitespace_only(namespace_service: NamespaceService) -> None:
    """Test validate_name with whitespace-only string."""
    result = namespace_service.validate_name('   ')

    assert result is not None
    assert result['status'] == 'failure'


def test_validate_name_newline_characters(namespace_service: NamespaceService) -> None:
    """Test validate_name with newline characters."""
    result = namespace_service.validate_name('test\nname')

    assert result is not None
    assert result['status'] == 'failure'


def test_validate_name_tab_characters(namespace_service: NamespaceService) -> None:
    """Test validate_name with tab characters."""
    result = namespace_service.validate_name('test\tname')

    assert result is not None
    assert result['status'] == 'failure'


# ============================================================================
# Boundary Value Tests
# ============================================================================


def test_validate_name_boundary_values(namespace_service: NamespaceService) -> None:
    """Test validate_name with boundary values."""
    # Test all boundary cases
    test_cases = [
        ('ab', False),  # Too short (2 chars)
        ('abc', True),  # Minimum valid (3 chars)
        ('a' * 32, True),  # Maximum valid (32 chars)
        ('a' * 33, False),  # Too long (33 chars)
    ]

    for name, should_be_valid in test_cases:
        result = namespace_service.validate_name(name)
        if should_be_valid:
            assert result is None, f'{name} should be valid'
        else:
            assert result is not None, f'{name} should be invalid'
            assert result['status'] == 'failure'


def test_generate_default_boundary_subjects(
    namespace_service: NamespaceService,
) -> None:
    """Test generate_default with boundary subject lengths."""
    # Very short subject (1 char)
    short_subject = 'a'
    namespace1 = namespace_service.generate_default(short_subject)
    assert namespace1.startswith('ns-')
    assert namespace1 == 'ns-a'
    assert len(namespace1) == 4  # 'ns-' + 1 char (not padded)

    # Exactly 12 characters (no dashes)
    subject_12 = 'a' * 12
    namespace2 = namespace_service.generate_default(subject_12)
    assert namespace2 == f'ns-{subject_12}'
    assert len(namespace2) == 15  # 'ns-' + 12 chars

    # Very long subject
    long_subject = 'a' * 1000
    namespace3 = namespace_service.generate_default(long_subject)
    assert namespace3.startswith('ns-')
    assert len(namespace3) == 15  # 'ns-' + 12 chars
    # Should use last 12 characters
    assert namespace3[3:] == long_subject[-12:]
