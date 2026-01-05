"""Unit tests for web_service_v3 routes."""

from __future__ import annotations

import contextlib
import json
import os
from typing import Any
from unittest.mock import patch

import pytest
from _pytest.monkeypatch import MonkeyPatch

from web_service.utils import EnvironmentChecker
from web_service_v3.utils import AWSManagerV3

# ============================================================================
# Environment Variable Tests
# ============================================================================


EnvironmentChecker.check_env_variables(
    'AWS_ACCESS_KEY_ID',
    'AWS_SECRET_ACCESS_KEY',
    'SERVER_CLIENT_ID',
    'SERVER_SECRET',
    'AWS_ACCOUNT_ID',
    'AWS_ACCOUNT_REGION',
    'MSK_CLUSTER_NAME',
)


# ============================================================================
# Integration Tests with Real AWS Services
# ============================================================================


@pytest.fixture
def aws_manager(monkeypatch: MonkeyPatch) -> AWSManagerV3:
    """Create an AWSManagerV3 instance with real AWS services."""
    required_vars = {
        'AWS_ACCOUNT_ID': os.getenv('AWS_ACCOUNT_ID'),
        'AWS_ACCOUNT_REGION': os.getenv('AWS_ACCOUNT_REGION'),
        'MSK_CLUSTER_NAME': os.getenv('MSK_CLUSTER_NAME'),
    }

    missing_vars = [
        var for var, value in required_vars.items() if value is None
    ]
    if missing_vars:
        raise ValueError(
            f'Missing required environment variables: '
            f'{", ".join(missing_vars)}. '
            'Please set all required environment variables '
            'before running tests.',
        )

    # At this point, we know all required vars are not None
    assert required_vars['AWS_ACCOUNT_ID'] is not None
    assert required_vars['AWS_ACCOUNT_REGION'] is not None
    assert required_vars['MSK_CLUSTER_NAME'] is not None

    return AWSManagerV3(
        account_id=required_vars['AWS_ACCOUNT_ID'],
        region=required_vars['AWS_ACCOUNT_REGION'],
        cluster_name=required_vars['MSK_CLUSTER_NAME'],
        iam_public=os.getenv('DEFAULT_SERVERS'),
        keys_table_name='diaspora-keys-test',
        users_table_name='diaspora-users-test',
        namespace_table_name='diaspora-namespaces-test',
    )


@pytest.fixture
def random_subject() -> str:
    """Generate a random subject for each test."""
    return f'test-subject-{os.urandom(8).hex()}'


def test_create_user(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test create_user with real AWS services."""
    print(f'\n[test_create_user] Testing with user: {random_subject}')
    # Create user should not raise an exception
    result = aws_manager.create_user(random_subject)
    print(f'  Result: {result}')

    # Assert on result
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert random_subject in result['message']
    assert result['user_created'] == 'True'
    assert result['policy_created'] == 'True'
    assert result['policy_attached'] == 'True'

    # Verify user was created by checking it exists
    try:
        user = aws_manager.iam.get_user(UserName=random_subject)
        assert user['User']['UserName'] == random_subject
    except aws_manager.iam.exceptions.NoSuchEntityException:
        pytest.fail('User was not created')

    # Verify policy was created
    policy_arn = (
        f'arn:aws:iam::{aws_manager.account_id}:'
        f'policy/msk-policy/{random_subject}'
    )
    try:
        policy = aws_manager.iam.get_policy(PolicyArn=policy_arn)
        assert policy['Policy']['PolicyName'] == random_subject
    except aws_manager.iam.exceptions.NoSuchEntityException:
        pytest.fail('Policy was not created')

    # Cleanup: delete the user
    with contextlib.suppress(Exception):
        aws_manager.delete_user(random_subject)


def test_create_user_idempotent(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test create_user is idempotent (can be called multiple times)."""
    print(
        f'\n[test_create_user_idempotent] Testing with user: {random_subject}',
    )
    # Create user first time
    result1 = aws_manager.create_user(random_subject)
    print(f'  First create result: {result1}')

    # Assert on first create result
    assert isinstance(result1, dict)
    assert result1['status'] == 'success'
    assert random_subject in result1['message']
    assert result1['user_created'] == 'True'
    assert result1['policy_created'] == 'True'
    assert result1['policy_attached'] == 'True'

    # Create user second time should not raise an exception
    result2 = aws_manager.create_user(random_subject)
    print(f'  Second create result: {result2}')

    # Assert on second create result (idempotent - user/policy already exist)
    assert isinstance(result2, dict)
    assert result2['status'] == 'success'
    assert random_subject in result2['message']
    assert result2['user_created'] == 'False'  # Already exists
    assert result2['policy_created'] == 'False'  # Already exists
    assert result2['policy_attached'] == 'True'  # Still attached

    # Verify user still exists
    try:
        user = aws_manager.iam.get_user(UserName=random_subject)
        assert user['User']['UserName'] == random_subject
    except aws_manager.iam.exceptions.NoSuchEntityException:
        pytest.fail('User was deleted after second create call')

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_user(random_subject)


def test_delete_user(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test delete_user with real AWS services."""
    print(f'\n[test_delete_user] Testing with user: {random_subject}')
    # First create a user
    create_result = aws_manager.create_user(random_subject)
    print(f'  Create result: {create_result}')

    # Assert on create result
    assert isinstance(create_result, dict)
    assert create_result['status'] == 'success'
    assert random_subject in create_result['message']

    # Verify user exists
    try:
        aws_manager.iam.get_user(UserName=random_subject)
    except aws_manager.iam.exceptions.NoSuchEntityException:
        pytest.fail('User was not created before delete test')

    # Delete user should not raise an exception
    delete_result = aws_manager.delete_user(random_subject)
    print(f'  Delete result: {delete_result}')

    # Assert on delete result
    assert isinstance(delete_result, dict)
    assert delete_result['status'] == 'success'
    assert random_subject in delete_result['message']
    assert delete_result['policy_detached'] == 'True'
    assert delete_result['policy_deleted'] == 'True'
    assert delete_result['user_deleted'] == 'True'

    # Verify user was deleted
    with pytest.raises(aws_manager.iam.exceptions.NoSuchEntityException):
        aws_manager.iam.get_user(UserName=random_subject)

    # Verify policy was deleted
    policy_arn = (
        f'arn:aws:iam::{aws_manager.account_id}:'
        f'policy/msk-policy/{random_subject}'
    )
    with pytest.raises(aws_manager.iam.exceptions.NoSuchEntityException):
        aws_manager.iam.get_policy(PolicyArn=policy_arn)


def test_delete_user_not_found(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test delete_user handles user not found gracefully."""
    print(
        f'\n[test_delete_user_not_found] Testing with user: {random_subject}',
    )
    # Ensure user doesn't exist
    with contextlib.suppress(Exception):
        aws_manager.delete_user(random_subject)

    # Try to delete non-existent user should not raise an exception
    result = aws_manager.delete_user(random_subject)
    print(f'  Delete result: {result}')

    # Assert on delete result (user doesn't exist, so all operations are False)
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert random_subject in result['message']
    assert result['keys_deleted'] == 'False'
    assert result['policy_detached'] == 'False'
    assert result['policy_deleted'] == 'False'
    assert result['user_deleted'] == 'False'


def test_create_and_delete_user_cycle(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test creating and deleting a user in a cycle."""
    print(
        f'\n[test_create_and_delete_user_cycle] '
        f'Testing with user: {random_subject}',
    )
    # Create user
    create_result1 = aws_manager.create_user(random_subject)
    print(f'  First create result: {create_result1}')

    # Assert on first create result
    assert isinstance(create_result1, dict)
    assert create_result1['status'] == 'success'
    assert random_subject in create_result1['message']
    assert create_result1['user_created'] == 'True'
    assert create_result1['policy_created'] == 'True'
    assert create_result1['policy_attached'] == 'True'

    # Verify it exists
    try:
        aws_manager.iam.get_user(UserName=random_subject)
    except aws_manager.iam.exceptions.NoSuchEntityException:
        pytest.fail('User was not created')

    # Delete user
    delete_result = aws_manager.delete_user(random_subject)
    print(f'  Delete result: {delete_result}')

    # Assert on delete result
    assert isinstance(delete_result, dict)
    assert delete_result['status'] == 'success'
    assert random_subject in delete_result['message']
    assert delete_result['policy_detached'] == 'True'
    assert delete_result['policy_deleted'] == 'True'
    assert delete_result['user_deleted'] == 'True'

    # Verify it's deleted
    with pytest.raises(aws_manager.iam.exceptions.NoSuchEntityException):
        aws_manager.iam.get_user(UserName=random_subject)

    # Create again should work
    create_result2 = aws_manager.create_user(random_subject)
    print(f'  Second create result: {create_result2}')

    # Assert on second create result
    assert isinstance(create_result2, dict)
    assert create_result2['status'] == 'success'
    assert random_subject in create_result2['message']
    assert create_result2['user_created'] == 'True'
    assert create_result2['policy_created'] == 'True'
    assert create_result2['policy_attached'] == 'True'

    # Verify it exists again
    try:
        aws_manager.iam.get_user(UserName=random_subject)
    except aws_manager.iam.exceptions.NoSuchEntityException:
        pytest.fail('User was not created in second cycle')

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_user(random_subject)


def test_delete_user_with_access_keys(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test delete_user when user has access keys."""
    print(
        f'\n[test_delete_user_with_access_keys] '
        f'Testing with user: {random_subject}',
    )
    # Create user
    create_result = aws_manager.create_user(random_subject)
    print(f'  Create result: {create_result}')

    # Create access keys for the user
    key_response = aws_manager.iam.create_access_key(UserName=random_subject)
    access_key_id = key_response['AccessKey']['AccessKeyId']
    print(f'  Created access key: {access_key_id}')

    # Verify access key exists
    keys = aws_manager.iam.list_access_keys(UserName=random_subject)
    assert len(keys['AccessKeyMetadata']) > 0

    # Delete user should delete access keys too
    delete_result = aws_manager.delete_user(random_subject)
    print(f'  Delete result: {delete_result}')

    # Assert on delete result
    assert isinstance(delete_result, dict)
    assert delete_result['status'] == 'success'
    assert random_subject in delete_result['message']
    assert delete_result['keys_deleted'] == 'True'
    assert delete_result['policy_detached'] == 'True'
    assert delete_result['policy_deleted'] == 'True'
    assert delete_result['user_deleted'] == 'True'

    # Verify user was deleted
    with pytest.raises(aws_manager.iam.exceptions.NoSuchEntityException):
        aws_manager.iam.get_user(UserName=random_subject)


def test_delete_user_idempotent(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test delete_user is idempotent (can be called multiple times)."""
    print(
        f'\n[test_delete_user_idempotent] Testing with user: {random_subject}',
    )
    # Create user first
    create_result = aws_manager.create_user(random_subject)
    print(f'  Create result: {create_result}')

    # Delete user first time
    delete_result1 = aws_manager.delete_user(random_subject)
    print(f'  First delete result: {delete_result1}')

    # Assert on first delete result
    assert isinstance(delete_result1, dict)
    assert delete_result1['status'] == 'success'
    assert delete_result1['user_deleted'] == 'True'

    # Delete user second time should not raise an exception
    delete_result2 = aws_manager.delete_user(random_subject)
    print(f'  Second delete result: {delete_result2}')

    # Assert on second delete result (idempotent - user already deleted)
    assert isinstance(delete_result2, dict)
    assert delete_result2['status'] == 'success'
    assert random_subject in delete_result2['message']
    assert delete_result2['keys_deleted'] == 'False'
    assert delete_result2['policy_detached'] == 'False'
    assert delete_result2['policy_deleted'] == 'False'
    assert delete_result2['user_deleted'] == 'False'

    # Verify user is still deleted
    with pytest.raises(aws_manager.iam.exceptions.NoSuchEntityException):
        aws_manager.iam.get_user(UserName=random_subject)


def test_create_user_existing_policy(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test create_user when policy already exists but user doesn't."""
    print(
        f'\n[test_create_user_existing_policy] '
        f'Testing with user: {random_subject}',
    )
    # Manually create policy first
    policy_arn = (
        f'arn:aws:iam::{aws_manager.account_id}:'
        f'policy/msk-policy/{random_subject}'
    )
    try:
        aws_manager.iam.create_policy(
            PolicyName=random_subject,
            Path='/msk-policy/',
            PolicyDocument=json.dumps(
                aws_manager.naming.iam_user_policy(),
            ),
        )
        print(f'  Created policy manually: {policy_arn}')
    except aws_manager.iam.exceptions.EntityAlreadyExistsException:
        pass  # Policy might already exist

    # Now create user - should attach existing policy
    result = aws_manager.create_user(random_subject)
    print(f'  Create result: {result}')

    # Assert on result
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert random_subject in result['message']
    assert result['user_created'] == 'True'
    assert result['policy_created'] == 'False'  # Policy already existed
    assert result['policy_attached'] == 'True'

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_user(random_subject)


def test_delete_user_detached_policy(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test delete_user when policy is already detached."""
    print(
        f'\n[test_delete_user_detached_policy] '
        f'Testing with user: {random_subject}',
    )
    # Create user
    create_result = aws_manager.create_user(random_subject)
    print(f'  Create result: {create_result}')

    # Manually detach policy
    policy_arn = (
        f'arn:aws:iam::{aws_manager.account_id}:'
        f'policy/msk-policy/{random_subject}'
    )
    try:
        aws_manager.iam.detach_user_policy(
            UserName=random_subject,
            PolicyArn=policy_arn,
        )
        print('  Detached policy manually')
    except aws_manager.iam.exceptions.NoSuchEntityException:
        pass

    # Delete user should still work
    delete_result = aws_manager.delete_user(random_subject)
    print(f'  Delete result: {delete_result}')

    # Assert on delete result
    assert isinstance(delete_result, dict)
    assert delete_result['status'] == 'success'
    assert random_subject in delete_result['message']
    assert delete_result['policy_detached'] == 'False'  # Already detached
    assert delete_result['policy_deleted'] == 'True'
    assert delete_result['user_deleted'] == 'True'

    # Verify user was deleted
    with pytest.raises(aws_manager.iam.exceptions.NoSuchEntityException):
        aws_manager.iam.get_user(UserName=random_subject)


# ============================================================================
# Key Management Tests
# ============================================================================


def test_create_key(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test create_key with real AWS services."""
    print(f'\n[test_create_key] Testing with user: {random_subject}')
    # Create key should create user if needed
    result = aws_manager.create_key(random_subject)
    print(f'  Result: {result}')

    # Assert on result
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert random_subject in result['message']
    assert 'access_key' in result
    assert 'secret_key' in result
    assert 'create_date' in result
    assert result['username'] == random_subject
    assert 'endpoint' in result

    # Verify user was created
    try:
        user = aws_manager.iam.get_user(UserName=random_subject)
        assert user['User']['UserName'] == random_subject
    except aws_manager.iam.exceptions.NoSuchEntityException:
        pytest.fail('User was not created')

    # Verify access key exists in IAM
    keys = aws_manager.iam.list_access_keys(UserName=random_subject)
    assert len(keys['AccessKeyMetadata']) == 1
    assert keys['AccessKeyMetadata'][0]['AccessKeyId'] == result['access_key']

    # Verify key is stored in DynamoDB
    stored_key = aws_manager._get_key_from_dynamodb(random_subject)
    assert stored_key is not None
    assert stored_key['access_key'] == result['access_key']
    assert stored_key['secret_key'] == result['secret_key']
    assert stored_key['create_date'] == result['create_date']

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_key(random_subject)
        aws_manager.delete_user(random_subject)


@pytest.mark.integration
def test_delete_user_with_policy_versions(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test delete_user deletes non-default policy versions."""
    print(
        f'\n[test_delete_user_with_policy_versions] '
        f'Testing with user: {random_subject}',
    )
    # Create user (creates policy)
    create_result = aws_manager.create_user(random_subject)
    assert create_result['status'] == 'success'

    # Get policy ARN (using same construction as in utils.py)
    policy_arn = aws_manager.iam_policy_arn_prefix + random_subject

    # Create a new policy version (non-default)
    # First, get the current policy document
    policy_versions = aws_manager.iam.list_policy_versions(
        PolicyArn=policy_arn,
    )
    default_version = next(
        v for v in policy_versions['Versions'] if v['IsDefaultVersion']
    )
    policy_doc = aws_manager.iam.get_policy_version(
        PolicyArn=policy_arn,
        VersionId=default_version['VersionId'],
    )['PolicyVersion']['Document']

    # Create a new version with modified document
    modified_doc = policy_doc.copy()
    # Create new version (this will make the old one non-default)
    aws_manager.iam.create_policy_version(
        PolicyArn=policy_arn,
        PolicyDocument=json.dumps(modified_doc),
        SetAsDefault=True,
    )

    # Verify we have multiple versions
    policy_versions = aws_manager.iam.list_policy_versions(
        PolicyArn=policy_arn,
    )
    non_default_versions = [
        v for v in policy_versions['Versions'] if not v['IsDefaultVersion']
    ]
    assert len(non_default_versions) > 0

    # Delete user should delete non-default versions
    delete_result = aws_manager.delete_user(random_subject)
    print(f'  Delete result: {delete_result}')

    assert isinstance(delete_result, dict)
    assert delete_result['status'] == 'success'
    assert delete_result['policy_deleted'] == 'True'

    # Verify user is deleted
    with pytest.raises(aws_manager.iam.exceptions.NoSuchEntityException):
        aws_manager.iam.get_user(UserName=random_subject)


def test_create_key_idempotent(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test create_key is idempotent (can be called multiple times)."""
    print(
        f'\n[test_create_key_idempotent] Testing with user: {random_subject}',
    )
    # Create key first time
    result1 = aws_manager.create_key(random_subject)
    print(f'  First create result: {result1}')
    first_access_key = result1['access_key']

    # Assert on first result
    assert isinstance(result1, dict)
    assert result1['status'] == 'success'
    assert 'access_key' in result1
    assert 'secret_key' in result1
    assert 'create_date' in result1

    # Create key second time should delete old and create new
    result2 = aws_manager.create_key(random_subject)
    print(f'  Second create result: {result2}')
    second_access_key = result2['access_key']

    # Assert on second result
    assert isinstance(result2, dict)
    assert result2['status'] == 'success'
    assert 'access_key' in result2
    assert 'secret_key' in result2
    assert 'create_date' in result2
    # Access key should be different (old one was deleted)
    assert first_access_key != second_access_key
    # Create dates may be the same if created in the same second
    # (both are valid ISO format dates)
    assert 'create_date' in result1
    assert 'create_date' in result2

    # Verify only one key exists
    keys = aws_manager.iam.list_access_keys(UserName=random_subject)
    assert len(keys['AccessKeyMetadata']) == 1
    assert keys['AccessKeyMetadata'][0]['AccessKeyId'] == second_access_key

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_key(random_subject)
        aws_manager.delete_user(random_subject)


@pytest.mark.integration
def test_get_key_without_create_date(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test get_key handles DynamoDB entry without create_date (old entry)."""
    print(
        f'\n[test_get_key_without_create_date] '
        f'Testing with user: {random_subject}',
    )
    # Create user first
    aws_manager.create_user(random_subject)

    # Create access key in IAM
    key_response = aws_manager.iam.create_access_key(UserName=random_subject)
    old_access_key = key_response['AccessKey']['AccessKeyId']
    old_secret_key = key_response['AccessKey']['SecretAccessKey']

    # Manually store key in DynamoDB without create_date (simulating old entry)
    aws_manager.dynamodb.put_item(
        TableName=aws_manager.keys_table_name,
        Item={
            'subject': {'S': random_subject},
            'access_key': {'S': old_access_key},
            'secret_key': {'S': old_secret_key},
            # No create_date field
        },
    )

    # Get key should handle missing create_date gracefully
    result = aws_manager.get_key(random_subject)
    print(f'  Get key result: {result}')

    # Should return key (may create new or return existing)
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'access_key' in result
    assert 'secret_key' in result
    # create_date may or may not be present depending on path taken

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_key(random_subject)
        aws_manager.delete_user(random_subject)


@pytest.mark.integration
def test_get_key_stale_dynamodb_entry(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test get_key handles key in DynamoDB but not in IAM."""
    print(
        f'\n[test_get_key_stale_dynamodb_entry] '
        f'Testing with user: {random_subject}',
    )
    # Create user and key first
    create_result = aws_manager.create_key(random_subject)
    old_access_key = create_result['access_key']

    # Delete key from IAM but leave it in DynamoDB (stale entry)
    aws_manager.iam.delete_access_key(
        UserName=random_subject,
        AccessKeyId=old_access_key,
    )

    # Get key should detect stale entry and create new one
    get_result = aws_manager.get_key(random_subject)
    print(f'  Get key result: {get_result}')

    # Should create new key
    assert isinstance(get_result, dict)
    assert get_result['status'] == 'success'
    assert get_result['access_key'] != old_access_key
    assert get_result.get('retrieved_from_dynamodb') is False

    # Verify new key is in DynamoDB
    stored_key = aws_manager._get_key_from_dynamodb(random_subject)
    assert stored_key is not None
    assert stored_key['access_key'] == get_result['access_key']

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_key(random_subject)
        aws_manager.delete_user(random_subject)


@pytest.mark.integration
def test_delete_key_no_keys_exception(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test delete_key when user exists but has no keys."""
    print(
        f'\n[test_delete_key_no_keys_exception] '
        f'Testing with user: {random_subject}',
    )
    # Create user but no keys
    aws_manager.create_user(random_subject)

    # Verify no keys exist
    keys = aws_manager.iam.list_access_keys(UserName=random_subject)
    assert len(keys['AccessKeyMetadata']) == 0

    # Delete key should handle NoSuchEntityException gracefully
    delete_result = aws_manager.delete_key(random_subject)
    print(f'  Delete result: {delete_result}')

    # Should succeed with keys_deleted=False
    assert isinstance(delete_result, dict)
    assert delete_result['status'] == 'success'
    assert delete_result['keys_deleted'] == 'False'

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_user(random_subject)


@pytest.mark.integration
def test_delete_key_with_existing_keys(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test delete_key when user has existing keys."""
    print(
        f'\n[test_delete_key_with_existing_keys] '
        f'Testing with user: {random_subject}',
    )
    # Create user and key
    aws_manager.create_user(random_subject)
    create_result = aws_manager.create_key(random_subject)
    print(f'  Created key: {create_result.get("access_key", "N/A")[:20]}...')

    # Verify key exists in IAM
    keys = aws_manager.iam.list_access_keys(UserName=random_subject)
    assert len(keys['AccessKeyMetadata']) == 1
    access_key_id = keys['AccessKeyMetadata'][0]['AccessKeyId']

    # Verify key exists in DynamoDB
    stored_key = aws_manager._get_key_from_dynamodb(random_subject)
    assert stored_key is not None
    assert stored_key['access_key'] == access_key_id

    # Delete key
    delete_result = aws_manager.delete_key(random_subject)
    print(f'  Delete result: {delete_result}')

    # Verify deletion was successful
    assert isinstance(delete_result, dict)
    assert delete_result['status'] == 'success'
    assert delete_result['keys_deleted'] == 'True'

    # Verify key is deleted from IAM
    keys_after = aws_manager.iam.list_access_keys(UserName=random_subject)
    assert len(keys_after['AccessKeyMetadata']) == 0

    # Verify key is deleted from DynamoDB
    stored_key_after = aws_manager._get_key_from_dynamodb(random_subject)
    assert stored_key_after is None

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_user(random_subject)


@pytest.mark.integration
def test_get_key_from_dynamodb_table_not_exists(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test _get_key_from_dynamodb handles ResourceNotFoundException."""
    print(
        f'\n[test_get_key_from_dynamodb_table_not_exists] '
        f'Testing with user: {random_subject}',
    )
    # Delete test table if it exists
    try:
        aws_manager.dynamodb.delete_table(
            TableName=aws_manager.keys_table_name,
        )
        aws_manager.dynamodb.get_waiter('table_not_exists').wait(
            TableName=aws_manager.keys_table_name,
        )
    except Exception:
        pass  # Table doesn't exist, which is fine

    # Try to get key from non-existent table
    result = aws_manager._get_key_from_dynamodb(random_subject)
    assert result is None

    # Cleanup - recreate table if needed
    with contextlib.suppress(Exception):
        aws_manager.create_key(random_subject)
        aws_manager.delete_key(random_subject)
        aws_manager.delete_user(random_subject)


@pytest.mark.integration
def test_create_keys_table_auto_creation(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test that DynamoDB table is created automatically if needed."""
    print(
        f'\n[test_create_keys_table_auto_creation] '
        f'Testing with user: {random_subject}',
    )
    # Delete test table if it exists (to test auto-creation)
    try:
        aws_manager.dynamodb.delete_table(
            TableName=aws_manager.keys_table_name,
        )
        # Wait for table to be deleted
        aws_manager.dynamodb.get_waiter('table_not_exists').wait(
            TableName=aws_manager.keys_table_name,
        )
    except Exception:
        pass  # Table doesn't exist, which is fine

    # Create key - should auto-create table
    result = aws_manager.create_key(random_subject)
    print(f'  Create key result: {result}')

    # Should succeed
    assert isinstance(result, dict)
    assert result['status'] == 'success'

    # Verify table exists by trying to get item
    stored_key = aws_manager._get_key_from_dynamodb(random_subject)
    assert stored_key is not None

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_key(random_subject)
        aws_manager.delete_user(random_subject)


# ============================================================================
# Namespace Management Tests
# ============================================================================


@pytest.mark.integration
def test_create_namespace(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test create_namespace with real AWS services."""
    print(f'\n[test_create_namespace] Testing with user: {random_subject}')
    # Create user first
    aws_manager.create_user(random_subject)

    # Create namespace
    namespace = f'test-ns-{random_subject[-8:]}'
    result = aws_manager.create_namespace(random_subject, namespace)
    print(f'  Result: {result}')

    # Assert on result
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert random_subject in result['message']
    assert namespace in result['namespaces']

    # Verify namespace is in user record
    user_record = aws_manager._get_user_record(random_subject)
    assert user_record is not None
    assert namespace in user_record['namespaces']

    # Verify namespace is in global registry
    global_namespaces = aws_manager._get_global_namespaces()
    assert namespace in global_namespaces

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_namespace(random_subject, namespace)
        aws_manager.delete_user(random_subject)


@pytest.mark.integration
def test_create_namespace_idempotent(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test create_namespace is idempotent."""
    print(
        f'\n[test_create_namespace_idempotent] '
        f'Testing with user: {random_subject}',
    )
    # Create user first
    aws_manager.create_user(random_subject)

    namespace = f'test-ns-{random_subject[-8:]}'

    # Create namespace first time
    result1 = aws_manager.create_namespace(random_subject, namespace)
    print(f'  First create result: {result1}')
    assert result1['status'] == 'success'
    assert namespace in result1['namespaces']

    # Create namespace second time (should be idempotent)
    result2 = aws_manager.create_namespace(random_subject, namespace)
    print(f'  Second create result: {result2}')
    assert result2['status'] == 'success'
    assert namespace in result2['namespaces']
    assert result2['namespaces'] == result1['namespaces']

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_namespace(random_subject, namespace)
        aws_manager.delete_user(random_subject)


@pytest.mark.integration
def test_create_namespace_already_taken(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test create_namespace fails when namespace is taken by another user."""
    print(
        f'\n[test_create_namespace_already_taken] '
        f'Testing with user: {random_subject}',
    )
    # Create two users
    subject1 = random_subject
    subject2 = f'{random_subject}-2'
    aws_manager.create_user(subject1)
    aws_manager.create_user(subject2)

    namespace = f'test-ns-{random_subject[-8:]}'

    # First user creates namespace
    result1 = aws_manager.create_namespace(subject1, namespace)
    print(f'  First user create result: {result1}')
    assert result1['status'] == 'success'

    # Second user tries to create same namespace (should fail)
    with pytest.raises(ValueError, match='already taken'):
        aws_manager.create_namespace(subject2, namespace)

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_namespace(subject1, namespace)
        aws_manager.delete_user(subject1)
        aws_manager.delete_user(subject2)


@pytest.mark.integration
def test_create_namespace_invalid_name(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test create_namespace with invalid namespace names."""
    print(
        f'\n[test_create_namespace_invalid_name] '
        f'Testing with user: {random_subject}',
    )
    # Create user first
    aws_manager.create_user(random_subject)

    # Test various invalid names
    invalid_names = [
        'ab',  # Too short
        'a' * 33,  # Too long
        'test-namespace!',  # Invalid character
        'test namespace',  # Space not allowed
        '-test-namespace',  # Starts with hyphen
        'test-namespace-',  # Ends with hyphen
        '_test-namespace',  # Starts with underscore
        'test-namespace_',  # Ends with underscore
    ]

    for invalid_name in invalid_names:
        with pytest.raises(ValueError, match='Namespace name'):
            aws_manager.create_namespace(random_subject, invalid_name)

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_user(random_subject)


@pytest.mark.integration
def test_delete_namespace(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test delete_namespace with real AWS services."""
    print(f'\n[test_delete_namespace] Testing with user: {random_subject}')
    # Create user and namespace first
    aws_manager.create_user(random_subject)
    namespace = f'test-ns-{random_subject[-8:]}'
    aws_manager.create_namespace(random_subject, namespace)

    # Verify namespace exists
    user_record = aws_manager._get_user_record(random_subject)
    assert user_record is not None
    assert namespace in user_record['namespaces']

    # Delete namespace
    result = aws_manager.delete_namespace(random_subject, namespace)
    print(f'  Delete result: {result}')

    # Assert on result
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert random_subject in result['message']
    assert namespace not in result['namespaces']

    # Verify namespace removed from user record
    user_record_after = aws_manager._get_user_record(random_subject)
    if user_record_after:
        assert namespace not in user_record_after.get('namespaces', [])
    else:
        # User record was deleted (no namespaces left)
        assert result['namespaces'] == []

    # Verify namespace removed from global registry
    global_namespaces = aws_manager._get_global_namespaces()
    assert namespace not in global_namespaces

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_user(random_subject)


@pytest.mark.integration
def test_delete_namespace_not_found(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test delete_namespace when namespace doesn't exist."""
    print(
        f'\n[test_delete_namespace_not_found] '
        f'Testing with user: {random_subject}',
    )
    # Create user and a namespace (so user has namespaces)
    aws_manager.create_user(random_subject)
    existing_namespace = f'test-ns-existing-{random_subject[-8:]}'
    aws_manager.create_namespace(random_subject, existing_namespace)

    # Try to delete non-existent namespace (idempotent, should succeed)
    result = aws_manager.delete_namespace(random_subject, 'non-existent-ns')
    print(f'  Delete result: {result}')

    # Should succeed (idempotent behavior)
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'not found' in result['message']

    # Verify existing namespace is still there
    user_record = aws_manager._get_user_record(random_subject)
    assert user_record is not None
    assert existing_namespace in user_record['namespaces']

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_namespace(random_subject, existing_namespace)
        aws_manager.delete_user(random_subject)


@pytest.mark.integration
def test_delete_namespace_user_no_namespaces(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test delete_namespace when user has no namespaces."""
    print(
        f'\n[test_delete_namespace_user_no_namespaces] '
        f'Testing with user: {random_subject}',
    )
    # Create user but no namespace
    aws_manager.create_user(random_subject)

    # Try to delete namespace (idempotent, should succeed)
    result = aws_manager.delete_namespace(random_subject, 'some-namespace')
    print(f'  Delete result: {result}')

    # Should succeed (idempotent behavior)
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'No namespaces found' in result['message']
    assert result['namespaces'] == []

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_user(random_subject)


@pytest.mark.integration
def test_delete_namespace_owned_by_another_user(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test delete_namespace fails when namespace is owned by another user."""
    print(
        f'\n[test_delete_namespace_owned_by_another_user] '
        f'Testing with user: {random_subject}',
    )
    # Create two users
    subject1 = random_subject
    subject2 = f'{random_subject}-2'
    aws_manager.create_user(subject1)
    aws_manager.create_user(subject2)

    # Use unique namespace name based on random_subject
    namespace = f'test-ns-owned-{random_subject[-8:]}'

    # First user creates namespace
    result1 = aws_manager.create_namespace(subject1, namespace)
    print(f'  First user create result: {result1}')
    assert result1['status'] == 'success'
    assert namespace in result1['namespaces']

    # Verify namespace is in first user's record
    user_record1 = aws_manager._get_user_record(subject1)
    assert user_record1 is not None
    assert namespace in user_record1['namespaces']

    # Second user creates their own namespace (so they have namespaces)
    other_namespace = f'test-ns-other-{random_subject[-8:]}'
    result2 = aws_manager.create_namespace(subject2, other_namespace)
    print(f'  Second user create result: {result2}')
    assert result2['status'] == 'success'

    # Second user tries to delete namespace owned by first user
    # (idempotent, should succeed but namespace not found for subject2)
    result = aws_manager.delete_namespace(subject2, namespace)
    print(f'  Second user delete result: {result}')

    # Should succeed (idempotent behavior)
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'not found' in result['message']

    # Verify namespace is still owned by first user
    user_record1_after = aws_manager._get_user_record(subject1)
    assert user_record1_after is not None
    assert namespace in user_record1_after['namespaces']

    # Verify namespace still exists for first user
    user_record1_after = aws_manager._get_user_record(subject1)
    assert user_record1_after is not None
    assert namespace in user_record1_after['namespaces']

    # Verify namespace still in global registry
    global_namespaces = aws_manager._get_global_namespaces()
    assert namespace in global_namespaces

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_namespace(subject1, namespace)
        aws_manager.delete_namespace(subject2, other_namespace)
        aws_manager.delete_user(subject1)
        aws_manager.delete_user(subject2)


@pytest.mark.integration
def test_create_and_delete_namespace_cycle(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test creating and deleting namespaces in a cycle."""
    print(
        f'\n[test_create_and_delete_namespace_cycle] '
        f'Testing with user: {random_subject}',
    )
    # Create user
    aws_manager.create_user(random_subject)

    namespace1 = f'test-ns-cycle-1-{random_subject[-8:]}'
    namespace2 = f'test-ns-cycle-2-{random_subject[-8:]}'

    # Create first namespace
    result1 = aws_manager.create_namespace(random_subject, namespace1)
    assert result1['status'] == 'success'
    assert namespace1 in result1['namespaces']

    # Create second namespace
    result2 = aws_manager.create_namespace(random_subject, namespace2)
    assert result2['status'] == 'success'
    assert namespace1 in result2['namespaces']
    assert namespace2 in result2['namespaces']

    # Delete first namespace
    result3 = aws_manager.delete_namespace(random_subject, namespace1)
    assert result3['status'] == 'success'
    assert namespace1 not in result3['namespaces']
    assert namespace2 in result3['namespaces']

    # Verify first namespace can be recreated (globally unique)
    result4 = aws_manager.create_namespace(random_subject, namespace1)
    assert result4['status'] == 'success'
    assert namespace1 in result4['namespaces']
    assert namespace2 in result4['namespaces']

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_namespace(random_subject, namespace1)
        aws_manager.delete_namespace(random_subject, namespace2)
        aws_manager.delete_user(random_subject)


@pytest.mark.integration
def test_namespace_validation_rules(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test namespace name validation rules."""
    print(
        f'\n[test_namespace_validation_rules] '
        f'Testing with user: {random_subject}',
    )
    # Create user first
    aws_manager.create_user(random_subject)

    # Valid names (make them unique to avoid conflicts)
    suffix = random_subject[-6:]  # Use 6 chars to leave room for prefix
    valid_names = [
        f'abc-{suffix}',  # Minimum length (3 + 1 + 6 = 10 chars)
        f'{"a" * 26}{suffix}',  # Maximum length (26 + 6 = 32 chars)
        f'test-ns-{suffix}',  # With dash
        f'test_ns_{suffix}',  # With underscore
        f'TestNs{suffix}',  # Mixed case
        f'123test{suffix}',  # Starts with number
    ]
    # Ensure max length name is exactly 32 chars
    max_len_name = f'{"a" * 26}{suffix}'
    assert len(max_len_name) == 32  # noqa: PLR2004

    for valid_name in valid_names:
        try:
            result = aws_manager.create_namespace(random_subject, valid_name)
            assert result['status'] == 'success'
            # Clean up
            aws_manager.delete_namespace(random_subject, valid_name)
        except Exception as e:
            pytest.fail(f'Valid name {valid_name} failed: {e}')

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_user(random_subject)


# ============================================================================
# Topic Management Tests
# ============================================================================


@pytest.mark.integration
def test_create_topic(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test create_topic with real AWS services."""
    print(f'\n[test_create_topic] Testing with user: {random_subject}')
    # Create user and namespace first
    aws_manager.create_user(random_subject)
    namespace = f'test-ns-{random_subject[-8:]}'
    aws_manager.create_namespace(random_subject, namespace)

    # Create topic
    topic = 'test-topic-123'
    result = aws_manager.create_topic(random_subject, namespace, topic)
    print(f'  Result: {result}')

    # Assert on result
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert namespace in result['message']
    assert topic in result['message']
    assert result['namespace'] == namespace
    assert result['topic'] == topic

    # Verify topic is in namespace record
    topics = aws_manager._get_namespace_topics(namespace)
    assert topic in topics

    # Verify topic ARN is in IAM policy
    policy_arn = aws_manager.iam_policy_arn_prefix + random_subject
    policy = aws_manager._get_iam_policy(policy_arn)
    topic_arn = f'{aws_manager.topic_arn_prefix}{namespace}.{topic}'
    assert topic_arn in policy['Statement'][0]['Resource']

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_topic(random_subject, namespace, topic)
        aws_manager.delete_namespace(random_subject, namespace)
        aws_manager.delete_user(random_subject)


@pytest.mark.integration
def test_create_topic_idempotent(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test create_topic is idempotent."""
    print(
        f'\n[test_create_topic_idempotent] '
        f'Testing with user: {random_subject}',
    )
    # Create user and namespace first
    aws_manager.create_user(random_subject)
    namespace = f'test-ns-{random_subject[-8:]}'
    aws_manager.create_namespace(random_subject, namespace)

    topic = 'test-topic-456'

    # Create topic first time
    result1 = aws_manager.create_topic(random_subject, namespace, topic)
    print(f'  First create result: {result1}')
    assert result1['status'] == 'success'
    assert result1['topic'] == topic

    # Create topic second time (should be idempotent)
    result2 = aws_manager.create_topic(random_subject, namespace, topic)
    print(f'  Second create result: {result2}')
    assert result2['status'] == 'success'
    assert result2['topic'] == topic

    # Verify topic still exists
    topics = aws_manager._get_namespace_topics(namespace)
    assert topic in topics
    assert topics.count(topic) == 1  # Should only appear once

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_topic(random_subject, namespace, topic)
        aws_manager.delete_namespace(random_subject, namespace)
        aws_manager.delete_user(random_subject)


@pytest.mark.integration
def test_create_topic_invalid_name(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test create_topic with invalid topic names."""
    print(
        f'\n[test_create_topic_invalid_name] '
        f'Testing with user: {random_subject}',
    )
    # Create user and namespace first
    aws_manager.create_user(random_subject)
    namespace = f'test-ns-{random_subject[-8:]}'
    aws_manager.create_namespace(random_subject, namespace)

    # Test various invalid names
    invalid_names = [
        'ab',  # Too short
        'a' * 33,  # Too long
        'test-topic!',  # Invalid character
        'test topic',  # Space not allowed
        '-test-topic',  # Starts with hyphen
        'test-topic-',  # Ends with hyphen
        '_test-topic',  # Starts with underscore
        'test-topic_',  # Ends with underscore
    ]

    for invalid_name in invalid_names:
        with pytest.raises(ValueError, match='Namespace name'):
            aws_manager.create_topic(
                random_subject,
                namespace,
                invalid_name,
            )

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_namespace(random_subject, namespace)
        aws_manager.delete_user(random_subject)


@pytest.mark.integration
def test_create_topic_namespace_not_owned(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test create_topic fails when namespace doesn't belong to user."""
    print(
        f'\n[test_create_topic_namespace_not_owned] '
        f'Testing with user: {random_subject}',
    )
    # Create two users
    subject1 = random_subject
    subject2 = f'{random_subject}-2'
    aws_manager.create_user(subject1)
    aws_manager.create_user(subject2)

    namespace = f'test-ns-{random_subject[-8:]}'
    topic = 'test-topic-789'

    # First user creates namespace
    aws_manager.create_namespace(subject1, namespace)

    # Second user creates their own namespace (so they have namespaces)
    other_namespace = f'test-ns-other-{random_subject[-8:]}'
    aws_manager.create_namespace(subject2, other_namespace)

    # Second user tries to create topic in first user's namespace (should fail)
    with pytest.raises(ValueError, match='not found'):
        aws_manager.create_topic(subject2, namespace, topic)

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_namespace(subject1, namespace)
        aws_manager.delete_namespace(subject2, other_namespace)
        aws_manager.delete_user(subject1)
        aws_manager.delete_user(subject2)


@pytest.mark.integration
def test_create_topic_user_no_namespaces(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test create_topic when user has no namespaces."""
    print(
        f'\n[test_create_topic_user_no_namespaces] '
        f'Testing with user: {random_subject}',
    )
    # Create user but no namespace
    aws_manager.create_user(random_subject)

    namespace = f'test-ns-{random_subject[-8:]}'
    topic = 'test-topic'

    # Try to create topic (should fail)
    with pytest.raises(ValueError, match='No namespaces found'):
        aws_manager.create_topic(random_subject, namespace, topic)

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_user(random_subject)


@pytest.mark.integration
def test_delete_topic(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test delete_topic with real AWS services."""
    print(f'\n[test_delete_topic] Testing with user: {random_subject}')
    # Create user, namespace, and topic first
    aws_manager.create_user(random_subject)
    namespace = f'test-ns-{random_subject[-8:]}'
    aws_manager.create_namespace(random_subject, namespace)
    topic = 'test-topic-delete'
    aws_manager.create_topic(random_subject, namespace, topic)

    # Verify topic exists
    topics = aws_manager._get_namespace_topics(namespace)
    assert topic in topics

    # Delete topic
    result = aws_manager.delete_topic(random_subject, namespace, topic)
    print(f'  Delete result: {result}')

    # Assert on result
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert namespace in result['message']
    assert topic in result['message']
    assert result['namespace'] == namespace
    assert result['topic'] == topic

    # Verify topic removed from namespace record
    topics_after = aws_manager._get_namespace_topics(namespace)
    assert topic not in topics_after

    # Verify topic ARN removed from IAM policy
    policy_arn = aws_manager.iam_policy_arn_prefix + random_subject
    policy = aws_manager._get_iam_policy(policy_arn)
    topic_arn = f'{aws_manager.topic_arn_prefix}{namespace}.{topic}'
    assert topic_arn not in policy['Statement'][0]['Resource']

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_namespace(random_subject, namespace)
        aws_manager.delete_user(random_subject)


@pytest.mark.integration
def test_delete_topic_not_found(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test delete_topic when topic doesn't exist."""
    print(
        f'\n[test_delete_topic_not_found] Testing with user: {random_subject}',
    )
    # Create user and namespace (with a topic)
    aws_manager.create_user(random_subject)
    namespace = f'test-ns-{random_subject[-8:]}'
    aws_manager.create_namespace(random_subject, namespace)
    existing_topic = 'test-topic-existing'
    aws_manager.create_topic(random_subject, namespace, existing_topic)

    # Try to delete non-existent topic (should succeed, idempotent)
    result = aws_manager.delete_topic(
        random_subject,
        namespace,
        'non-existent-topic',
    )
    print(f'  Delete result: {result}')
    assert result['status'] == 'success'
    assert 'not found' in result['message']

    # Verify existing topic is still there
    topics = aws_manager._get_namespace_topics(namespace)
    assert existing_topic in topics

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_topic(random_subject, namespace, existing_topic)
        aws_manager.delete_namespace(random_subject, namespace)
        aws_manager.delete_user(random_subject)


@pytest.mark.integration
def test_delete_topic_namespace_not_owned(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test delete_topic fails when namespace doesn't belong to user."""
    print(
        f'\n[test_delete_topic_namespace_not_owned] '
        f'Testing with user: {random_subject}',
    )
    # Create two users
    subject1 = random_subject
    subject2 = f'{random_subject}-2'
    aws_manager.create_user(subject1)
    aws_manager.create_user(subject2)

    namespace = f'test-ns-{random_subject[-8:]}'
    topic = 'test-topic-owned-by-other'

    # First user creates namespace and topic
    aws_manager.create_namespace(subject1, namespace)
    aws_manager.create_topic(subject1, namespace, topic)

    # Second user creates their own namespace (so they have namespaces)
    other_namespace = f'test-ns-other-{random_subject[-8:]}'
    aws_manager.create_namespace(subject2, other_namespace)

    # Second user tries to delete topic from first user's namespace
    # (idempotent, should succeed but namespace not found for subject2)
    result = aws_manager.delete_topic(subject2, namespace, topic)
    print(f'  Second user delete result: {result}')

    # Should succeed (idempotent behavior)
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'not found' in result['message']

    # Verify topic is still in first user's namespace
    topics_after = aws_manager._get_namespace_topics(namespace)
    assert topic in topics_after

    # Verify topic still exists for first user
    topics = aws_manager._get_namespace_topics(namespace)
    assert topic in topics

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_topic(subject1, namespace, topic)
        aws_manager.delete_namespace(subject1, namespace)
        aws_manager.delete_namespace(subject2, other_namespace)
        aws_manager.delete_user(subject1)
        aws_manager.delete_user(subject2)


@pytest.mark.integration
def test_create_and_delete_topic_cycle(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test creating and deleting topics in a cycle."""
    print(
        f'\n[test_create_and_delete_topic_cycle] '
        f'Testing with user: {random_subject}',
    )
    # Create user and namespace
    aws_manager.create_user(random_subject)
    namespace = f'test-ns-{random_subject[-8:]}'
    aws_manager.create_namespace(random_subject, namespace)

    topic1 = 'test-topic-cycle-1'
    topic2 = 'test-topic-cycle-2'

    # Create first topic
    result1 = aws_manager.create_topic(random_subject, namespace, topic1)
    assert result1['status'] == 'success'

    # Create second topic
    result2 = aws_manager.create_topic(random_subject, namespace, topic2)
    assert result2['status'] == 'success'

    # Verify both topics exist
    topics = aws_manager._get_namespace_topics(namespace)
    assert topic1 in topics
    assert topic2 in topics

    # Delete first topic
    result3 = aws_manager.delete_topic(random_subject, namespace, topic1)
    assert result3['status'] == 'success'

    # Verify first topic removed, second still exists
    topics_after = aws_manager._get_namespace_topics(namespace)
    assert topic1 not in topics_after
    assert topic2 in topics_after

    # Recreate first topic
    result4 = aws_manager.create_topic(random_subject, namespace, topic1)
    assert result4['status'] == 'success'

    # Verify both topics exist again
    topics_final = aws_manager._get_namespace_topics(namespace)
    assert topic1 in topics_final
    assert topic2 in topics_final

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_topic(random_subject, namespace, topic1)
        aws_manager.delete_topic(random_subject, namespace, topic2)
        aws_manager.delete_namespace(random_subject, namespace)
        aws_manager.delete_user(random_subject)


@pytest.mark.integration
def test_topic_validation_rules(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test topic name validation rules."""
    print(
        f'\n[test_topic_validation_rules] Testing with user: {random_subject}',
    )
    # Create user and namespace first
    aws_manager.create_user(random_subject)
    namespace = f'test-ns-{random_subject[-8:]}'
    aws_manager.create_namespace(random_subject, namespace)

    # Valid names
    valid_names = [
        'abc',  # Minimum length
        'a' * 32,  # Maximum length
        'test-topic-123',  # With dash
        'test_topic_123',  # With underscore
        'TestTopic123',  # Mixed case
        '123test',  # Starts with number
    ]

    for valid_name in valid_names:
        try:
            result = aws_manager.create_topic(
                random_subject,
                namespace,
                valid_name,
            )
            assert result['status'] == 'success'
            # Clean up
            aws_manager.delete_topic(random_subject, namespace, valid_name)
        except Exception as e:
            pytest.fail(f'Valid name {valid_name} failed: {e}')

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_namespace(random_subject, namespace)
        aws_manager.delete_user(random_subject)


@pytest.mark.integration
def test_topic_uniqueness_under_namespace(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test that topics must be unique under a namespace."""
    print(
        f'\n[test_topic_uniqueness_under_namespace] '
        f'Testing with user: {random_subject}',
    )
    # Create user and namespace
    aws_manager.create_user(random_subject)
    namespace = f'test-ns-{random_subject[-8:]}'
    aws_manager.create_namespace(random_subject, namespace)

    topic = 'test-topic-unique'

    # Create topic first time
    result1 = aws_manager.create_topic(random_subject, namespace, topic)
    assert result1['status'] == 'success'

    # Create same topic again (should be idempotent, not error)
    result2 = aws_manager.create_topic(random_subject, namespace, topic)
    assert result2['status'] == 'success'
    assert 'already exists' in result2['message']

    # Verify topic appears only once in namespace
    topics = aws_manager._get_namespace_topics(namespace)
    assert topics.count(topic) == 1

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_topic(random_subject, namespace, topic)
        aws_manager.delete_namespace(random_subject, namespace)
        aws_manager.delete_user(random_subject)


@pytest.mark.integration
def test_create_topic_kafka_failure_after_retries(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test create_topic returns failure when Kafka topic creation fails."""
    print(
        f'\n[test_create_topic_kafka_failure_after_retries] '
        f'Testing with user: {random_subject}',
    )
    # Create user and namespace first
    aws_manager.create_user(random_subject)
    namespace = f'test-ns-{random_subject[-8:]}'
    aws_manager.create_namespace(random_subject, namespace)

    # Mock _create_kafka_topic to return failure after retries
    def mock_create_kafka_topic_failure(
        self: AWSManagerV3,
        namespace: str,
        topic: str,
    ) -> dict[str, Any]:
        return {
            'status': 'failure',
            'message': (
                'Failed to create Kafka topic test-ns.123 after '
                '3 attempts: Connection timeout'
            ),
        }

    topic = 'test-topic-123'
    with patch.object(
        AWSManagerV3,
        '_create_kafka_topic',
        mock_create_kafka_topic_failure,
    ):
        result = aws_manager.create_topic(random_subject, namespace, topic)
        print(f'  Result: {result}')

        # Should return failure status
        assert isinstance(result, dict)
        assert result['status'] == 'failure'
        assert 'message' in result
        assert 'Failed to create Kafka topic' in result['message']
        assert result['namespace'] == namespace
        assert result['topic'] == topic

        # Note: Topic IS added to namespace record before Kafka check
        # (this is the current implementation behavior)
        # Verify topic is in namespace record
        topics = aws_manager._get_namespace_topics(namespace)
        assert topic in topics

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_topic(random_subject, namespace, topic)
        aws_manager.delete_namespace(random_subject, namespace)
        aws_manager.delete_user(random_subject)


@pytest.mark.integration
def test_create_topic_kafka_success(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test create_topic succeeds when _create_kafka_topic succeeds."""
    print(
        f'\n[test_create_topic_kafka_success] '
        f'Testing with user: {random_subject}',
    )
    # Create user and namespace first
    aws_manager.create_user(random_subject)
    namespace = f'test-ns-{random_subject[-8:]}'
    aws_manager.create_namespace(random_subject, namespace)

    # Mock _create_kafka_topic to return success
    def mock_create_kafka_topic_success(
        self: AWSManagerV3,
        namespace: str,
        topic: str,
    ) -> dict[str, Any]:
        return {'status': 'success'}

    topic = 'test-topic-456'
    with patch.object(
        AWSManagerV3,
        '_create_kafka_topic',
        mock_create_kafka_topic_success,
    ):
        result = aws_manager.create_topic(random_subject, namespace, topic)
        print(f'  Result: {result}')

        # Should succeed when Kafka creation succeeds
        assert isinstance(result, dict)
        assert result['status'] == 'success'
        assert result['namespace'] == namespace
        assert result['topic'] == topic

        # Verify topic is in namespace record
        topics = aws_manager._get_namespace_topics(namespace)
        assert topic in topics

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_topic(random_subject, namespace, topic)
        aws_manager.delete_namespace(random_subject, namespace)
        aws_manager.delete_user(random_subject)


@pytest.mark.integration
def test_create_topic_kafka_no_endpoint(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test create_topic when iam_public is None (no Kafka endpoint)."""
    print(
        f'\n[test_create_topic_kafka_no_endpoint] '
        f'Testing with user: {random_subject}',
    )
    # Create user and namespace first
    aws_manager.create_user(random_subject)
    namespace = f'test-ns-{random_subject[-8:]}'
    aws_manager.create_namespace(random_subject, namespace)

    # Save original iam_public
    original_iam_public = aws_manager.iam_public

    # Set iam_public to None
    aws_manager.iam_public = None

    topic = 'test-topic-789'
    result = aws_manager.create_topic(random_subject, namespace, topic)
    print(f'  Result: {result}')

    # Should succeed (Kafka creation is skipped when iam_public is None)
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert result['namespace'] == namespace
    assert result['topic'] == topic

    # Verify topic was added to namespace record
    topics = aws_manager._get_namespace_topics(namespace)
    assert topic in topics

    # Restore original iam_public
    aws_manager.iam_public = original_iam_public

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_topic(random_subject, namespace, topic)
        aws_manager.delete_namespace(random_subject, namespace)
        aws_manager.delete_user(random_subject)


# ============================================================================
# List Namespaces Tests
# ============================================================================


@pytest.mark.integration
def test_list_namespaces(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test list_namespaces with real AWS services."""
    print(f'\n[test_list_namespaces] Testing with user: {random_subject}')
    # Create user and namespaces with topics
    aws_manager.create_user(random_subject)
    namespace1 = f'test-ns-1-{random_subject[-8:]}'
    namespace2 = f'test-ns-2-{random_subject[-8:]}'
    aws_manager.create_namespace(random_subject, namespace1)
    aws_manager.create_namespace(random_subject, namespace2)

    # Create topics in namespaces
    topic1 = 'test-topic-1'
    topic2 = 'test-topic-2'
    aws_manager.create_topic(random_subject, namespace1, topic1)
    aws_manager.create_topic(random_subject, namespace1, topic2)
    aws_manager.create_topic(random_subject, namespace2, topic1)

    # List namespaces
    result = aws_manager.list_namespaces(random_subject)
    print(f'  Result: {result}')

    # Assert on result
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert random_subject in result['message']
    assert 'namespaces' in result
    assert isinstance(result['namespaces'], dict)

    # Verify namespaces and topics
    namespaces = result['namespaces']
    assert namespace1 in namespaces
    assert namespace2 in namespaces
    assert topic1 in namespaces[namespace1]
    assert topic2 in namespaces[namespace1]
    assert topic1 in namespaces[namespace2]
    assert len(namespaces[namespace1]) == 2  # noqa: PLR2004
    assert len(namespaces[namespace2]) == 1

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_topic(random_subject, namespace1, topic1)
        aws_manager.delete_topic(random_subject, namespace1, topic2)
        aws_manager.delete_topic(random_subject, namespace2, topic1)
        aws_manager.delete_namespace(random_subject, namespace1)
        aws_manager.delete_namespace(random_subject, namespace2)
        aws_manager.delete_user(random_subject)


@pytest.mark.integration
def test_list_namespaces_empty(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test list_namespaces when user has no namespaces."""
    print(
        f'\n[test_list_namespaces_empty] Testing with user: {random_subject}',
    )
    # Create user but no namespaces
    aws_manager.create_user(random_subject)

    # List namespaces
    result = aws_manager.list_namespaces(random_subject)
    print(f'  Result: {result}')

    # Assert on result
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'No namespaces found' in result['message']
    assert result['namespaces'] == {}

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_user(random_subject)


@pytest.mark.integration
def test_list_namespaces_with_empty_namespace(
    aws_manager: AWSManagerV3,
    random_subject: str,
) -> None:
    """Test list_namespaces when namespace has no topics."""
    print(
        f'\n[test_list_namespaces_with_empty_namespace] '
        f'Testing with user: {random_subject}',
    )
    # Create user and namespace but no topics
    aws_manager.create_user(random_subject)
    namespace = f'test-ns-empty-{random_subject[-8:]}'
    aws_manager.create_namespace(random_subject, namespace)

    # List namespaces
    result = aws_manager.list_namespaces(random_subject)
    print(f'  Result: {result}')

    # Assert on result
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert namespace in result['namespaces']
    assert result['namespaces'][namespace] == []

    # Cleanup
    with contextlib.suppress(Exception):
        aws_manager.delete_namespace(random_subject, namespace)
        aws_manager.delete_user(random_subject)
