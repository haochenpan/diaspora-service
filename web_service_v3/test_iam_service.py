"""Unit tests for web_service_v3 IAMService."""

from __future__ import annotations

import contextlib
import json
import os
from typing import Any

import pytest

from web_service_v3.services import IAMService

# ============================================================================
# Test Constants
# ============================================================================

# Number of resources in IAM policy (group, cluster, transactional-id, topic)
POLICY_RESOURCE_COUNT = 4

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
def random_subject() -> str:
    """Generate a random subject for each test."""
    return f'test-subject-{os.urandom(8).hex()}'


@pytest.fixture
def cleanup_user(iam_service: IAMService) -> Any:
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
            iam_service.delete_user_and_policy(subject)


# ============================================================================
# Integration Tests with Real AWS Services
# ============================================================================


@pytest.mark.integration
def test_generate_user_policy(
    iam_service: IAMService,
    random_subject: str,
) -> None:
    """Test generate_user_policy with real IAM service."""
    print(
        f'\n[test_generate_user_policy] '
        f'Testing with subject: {random_subject}',
    )

    # Generate policy
    policy = iam_service.generate_user_policy(random_subject)
    print('  Generated policy:')
    print(json.dumps(policy, indent=2, default=str))

    # Assertions
    assert isinstance(policy, dict)
    assert policy['Version'] == '2012-10-17'
    assert 'Statement' in policy
    assert len(policy['Statement']) == 1
    assert policy['Statement'][0]['Effect'] == 'Allow'
    assert 'kafka-cluster:Connect' in policy['Statement'][0]['Action']
    # Policy should have 4 resources: group, cluster, transactional-id, topic
    assert len(policy['Statement'][0]['Resource']) == POLICY_RESOURCE_COUNT


@pytest.mark.integration
def test_create_user_and_policy(
    iam_service: IAMService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test create_user_and_policy with real AWS services."""
    print(
        f'\n[test_create_user_and_policy] '
        f'Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # Create user and policy
    result = iam_service.create_user_and_policy(random_subject)
    print('  Create result:')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'message' in result
    assert random_subject in result['message']


@pytest.mark.integration
def test_create_access_key(
    iam_service: IAMService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test create_access_key with real AWS services."""
    print(f'\n[test_create_access_key] Testing with subject: {random_subject}')

    # Mark for cleanup
    cleanup_user(random_subject)

    # Create user first
    iam_service.create_user_and_policy(random_subject)

    # Create access key
    result = iam_service.create_access_key(random_subject)
    print('  Create key result:')
    # Mask secret key for security
    masked_result = result.copy()
    if 'secret_key' in masked_result:
        masked_result['secret_key'] = '***MASKED***'
    print(json.dumps(masked_result, indent=2, default=str))

    # Assertions
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'message' in result
    assert 'access_key' in result
    assert 'secret_key' in result
    assert 'create_date' in result
    assert random_subject in result['message']


@pytest.mark.integration
def test_delete_access_keys(
    iam_service: IAMService,
    random_subject: str,
    cleanup_user: Any,
) -> None:
    """Test delete_access_keys with real AWS services."""
    print(
        f'\n[test_delete_access_keys] Testing with subject: {random_subject}',
    )

    # Mark for cleanup
    cleanup_user(random_subject)

    # Create user and access key first
    iam_service.create_user_and_policy(random_subject)
    iam_service.create_access_key(random_subject)

    # Delete access keys
    result = iam_service.delete_access_keys(random_subject)
    print('  Delete keys result:')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'message' in result
    assert random_subject in result['message']


@pytest.mark.integration
def test_delete_user_and_policy(
    iam_service: IAMService,
    random_subject: str,
) -> None:
    """Test delete_user_and_policy with real AWS services."""
    print(
        f'\n[test_delete_user_and_policy] '
        f'Testing with subject: {random_subject}',
    )

    # Create user, policy, and access key first
    iam_service.create_user_and_policy(random_subject)
    iam_service.create_access_key(random_subject)

    # Delete user and policy
    result = iam_service.delete_user_and_policy(random_subject)
    print('  Delete result:')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'message' in result
    assert random_subject in result['message']


@pytest.mark.integration
def test_full_lifecycle(
    iam_service: IAMService,
    random_subject: str,
) -> None:
    """Test full lifecycle: create user, create key, delete key, delete."""
    print(f'\n[test_full_lifecycle] Testing with subject: {random_subject}')

    # 1. Create user and policy
    create_result = iam_service.create_user_and_policy(random_subject)
    assert create_result['status'] == 'success'

    # 2. Create access key
    key_result = iam_service.create_access_key(random_subject)
    assert key_result['status'] == 'success'
    assert 'access_key' in key_result

    # 3. Delete access key
    delete_keys_result = iam_service.delete_access_keys(random_subject)
    assert delete_keys_result['status'] == 'success'

    # 4. Delete user and policy
    delete_result = iam_service.delete_user_and_policy(random_subject)
    assert delete_result['status'] == 'success'

    print(
        '  Full lifecycle test completed successfully',
    )
