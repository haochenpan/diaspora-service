"""Utility functions for web service v3."""

from __future__ import annotations

import contextlib
import json
import re
import secrets
import string
import threading
from typing import Any

import boto3
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from kafka.admin import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.errors import UnknownTopicOrPartitionError
from kafka.sasl.oauth import AbstractTokenProvider

from web_service.utils import AWSManager

# Namespace validation constants
MIN_NAMESPACE_LENGTH = 3
MAX_NAMESPACE_LENGTH = 32

# Global namespaces tracking key
GLOBAL_NAMESPACES_KEY = '__global_namespaces__'


class MSKTokenProvider(AbstractTokenProvider):
    """Provide tokens for MSK authentication."""

    def __init__(self, region: str) -> None:
        """Initialize with AWS region."""
        self.region = region

    def token(self) -> str:
        """Generate and return an MSK auth token."""
        token, _ = MSKAuthTokenProvider.generate_auth_token(self.region)
        return token


class AWSManagerV3:
    """Manage AWS resources for Diaspora Service V3 - simplified version."""

    def __init__(  # noqa: PLR0913
        self,
        account_id: str,
        region: str,
        cluster_name: str,
        iam_public: str | None,
        keys_table_name: str | None = None,
        users_table_name: str | None = None,
        namespace_table_name: str | None = None,
    ) -> None:
        """Initialize AWSManagerV3 with AWS credentials and configuration.

        Args:
            account_id: AWS account ID
            region: AWS region
            cluster_name: MSK cluster name
            iam_public: IAM public endpoint (optional)
            keys_table_name: DynamoDB table name for keys
                (defaults to 'diaspora-keys')
            users_table_name: DynamoDB table name for user records
                (defaults to 'diaspora-users')
            namespace_table_name: DynamoDB table name for namespace/topic
                records (defaults to 'diaspora-namespaces')
        """
        self.account_id = account_id
        self.region = region
        self.cluster_name = cluster_name
        self.iam_public = iam_public

        # Import NamingManager from web_service.utils
        self.naming = AWSManager.NamingManager(
            'msk',
            account_id,
            region,
            cluster_name,
        )

        # Initialize AWS clients
        self.iam = boto3.client('iam')
        self.dynamodb = boto3.client('dynamodb', region_name=region)
        self.lock = threading.Lock()

        # IAM policy ARN prefix
        self.iam_policy_arn_prefix = (
            f'arn:aws:iam::{account_id}:policy/msk-policy/'
        )

        # DynamoDB table name for storing keys
        self.keys_table_name = keys_table_name or 'diaspora-keys'

        # DynamoDB table name for storing user records (namespaces)
        self.users_table_name = users_table_name or 'diaspora-users'

        # DynamoDB table name for storing namespace/topic records
        self.namespace_table_name = (
            namespace_table_name or 'diaspora-namespaces'
        )

    def _ensure_user_exists(self, subject: str) -> dict[str, bool]:
        """Ensure IAM user exists with policy attached (idempotent).

        This method does NOT use a lock - caller must hold lock.
        Returns dict with flags indicating what was created.
        """
        user_created = False
        policy_created = False
        policy_attached = False

        # 1. Create IAM user
        with contextlib.suppress(
            self.iam.exceptions.EntityAlreadyExistsException,
        ):
            self.iam.create_user(Path='/msk-users/', UserName=subject)
            user_created = True

        # 2. Create IAM policy
        with contextlib.suppress(
            self.iam.exceptions.EntityAlreadyExistsException,
        ):
            self.iam.create_policy(
                PolicyName=subject,
                Path='/msk-policy/',
                PolicyDocument=json.dumps(
                    self.naming.iam_user_policy_v3(subject),
                ),
            )
            policy_created = True

        # 3. Attach the created policy to user
        with contextlib.suppress(
            self.iam.exceptions.EntityAlreadyExistsException,
        ):
            self.iam.attach_user_policy(
                UserName=subject,
                PolicyArn=self.iam_policy_arn_prefix + subject,
            )
            policy_attached = True

        # 4. Update IAM policy to refresh with latest policy document
        # This ensures the policy is always up-to-date and cleans up old
        # versions
        with contextlib.suppress(
            self.iam.exceptions.NoSuchEntityException,
        ):
            self._update_iam_policy(subject)

        return {
            'user_created': user_created,
            'policy_created': policy_created,
            'policy_attached': policy_attached,
        }

    def _ensure_namespace_exists(self, subject: str) -> dict[str, Any]:
        """Ensure namespace exists for the user (idempotent).

        This method does NOT use a lock - caller must hold lock.
        Tries to create namespace with subject-based name first.
        If that fails (namespace already taken), retries with random
        namespaces.

        Returns:
            dict with 'status': 'success' and 'namespace' if successful,
            or 'status': 'failure' and 'message' if all retries failed.
        """
        # Try to create namespace with subject-based name first
        namespace = f'ns-{subject.replace("-", "")[-12:]}'
        namespace_result = self._create_namespace_internal(subject, namespace)

        # If namespace creation failed, retry with random namespaces
        # Only retry if the error is "namespace already taken"
        max_retries = 10
        retry_count = 0
        while (
            namespace_result.get('status') != 'success'
            and retry_count < max_retries
        ):
            error_message = namespace_result.get('message', '').lower()
            # Only retry if namespace is already taken
            if 'already taken' not in error_message:
                break

            # Generate random 12-character alphanumeric string
            # Using lowercase and numbers to ensure valid namespace
            random_suffix = ''.join(
                secrets.choice(string.ascii_lowercase + string.digits)
                for _ in range(12)
            )
            namespace = f'ns-{random_suffix}'
            namespace_result = self._create_namespace_internal(
                subject,
                namespace,
            )
            retry_count += 1

        # If still failed after all retries, return failure
        if namespace_result.get('status') != 'success':
            return {
                'status': 'failure',
                'message': namespace_result.get(
                    'message',
                    'Unknown error',
                ),
            }

        # Namespace creation succeeded
        return {
            'status': 'success',
            'namespace': namespace,
        }

    def create_user(self, subject: str) -> dict[str, str]:
        """Create an IAM user with policy for the given subject.

        Also creates and registers the namespace for the user.
        If the default namespace (based on subject UUID) fails, retries with
        randomly generated namespaces until one succeeds.
        """
        with self.lock:
            result = self._ensure_user_exists(subject)
            namespace_result = self._ensure_namespace_exists(subject)

            # If namespace creation failed, still return user creation info
            if namespace_result.get('status') != 'success':
                response = {
                    'status': 'success',
                    'message': f'IAM user created for {subject}',
                    'user_created': str(result['user_created']),
                    'policy_created': str(result['policy_created']),
                    'policy_attached': str(result['policy_attached']),
                    'namespace_created': 'false',
                    'namespace_error': namespace_result.get(
                        'message',
                        'Unknown error',
                    ),
                }
                return response

            # Namespace creation succeeded
            response = {
                'status': 'success',
                'message': f'IAM user created for {subject}',
                'user_created': str(result['user_created']),
                'policy_created': str(result['policy_created']),
                'policy_attached': str(result['policy_attached']),
                'namespace_created': 'true',
                'namespace': namespace_result['namespace'],
            }

            return response

    def delete_user(self, subject: str) -> dict[str, str]:
        """Delete the IAM user and all associated resources."""
        with self.lock:
            keys_deleted = False
            policy_detached = False
            policy_deleted = False
            user_deleted = False
            namespaces_deleted = False

            # 1. Delete all namespaces for the user
            user_record = self._get_user_record(subject)
            if user_record and 'namespaces' in user_record:
                namespaces = user_record['namespaces'].copy()
                for namespace in namespaces:
                    # Use internal method since we already hold the lock
                    self._delete_namespace_internal(subject, namespace)
                if namespaces:
                    namespaces_deleted = True

            # 2. Delete existing access keys if any
            try:
                existing_keys = self.iam.list_access_keys(UserName=subject)
                for key in existing_keys['AccessKeyMetadata']:
                    self.iam.delete_access_key(
                        UserName=subject,
                        AccessKeyId=key['AccessKeyId'],
                    )
                    keys_deleted = True
            except self.iam.exceptions.NoSuchEntityException:
                pass  # user doesn't exist or has no keys

            # 3. Detach IAM policy
            try:
                policy_arn = self.iam_policy_arn_prefix + subject
                self.iam.detach_user_policy(
                    UserName=subject,
                    PolicyArn=policy_arn,
                )
                policy_detached = True
            except self.iam.exceptions.NoSuchEntityException:
                pass  # policy not attached

            # 4. Delete IAM policy (including all versions)
            try:
                policy_arn = self.iam_policy_arn_prefix + subject
                # Delete non-default policy versions first
                self._delete_policy_versions(policy_arn)
                # Delete the policy itself
                self.iam.delete_policy(PolicyArn=policy_arn)
                policy_deleted = True
            except self.iam.exceptions.NoSuchEntityException:
                pass  # policy doesn't exist

            # 5. Delete IAM user
            try:
                self.iam.delete_user(UserName=subject)
                user_deleted = True
            except self.iam.exceptions.NoSuchEntityException:
                pass  # user doesn't exist

            return {
                'status': 'success',
                'message': f'IAM user deleted for {subject}',
                'namespaces_deleted': str(namespaces_deleted),
                'keys_deleted': str(keys_deleted),
                'policy_detached': str(policy_detached),
                'policy_deleted': str(policy_deleted),
                'user_deleted': str(user_deleted),
            }

    def _store_key_in_dynamodb(
        self,
        subject: str,
        access_key: str,
        secret_key: str,
        create_date: str,
    ) -> None:
        """Store access key in DynamoDB.

        This method does NOT use a lock - caller must hold lock.
        create_date should be ISO format string.
        """
        try:
            self.dynamodb.put_item(
                TableName=self.keys_table_name,
                Item={
                    'subject': {'S': subject},
                    'access_key': {'S': access_key},
                    'secret_key': {'S': secret_key},
                    'create_date': {'S': create_date},
                },
            )
        except self.dynamodb.exceptions.ResourceNotFoundException:
            # Table doesn't exist, create it
            self._create_keys_table()
            # Retry storing the key
            self.dynamodb.put_item(
                TableName=self.keys_table_name,
                Item={
                    'subject': {'S': subject},
                    'access_key': {'S': access_key},
                    'secret_key': {'S': secret_key},
                    'create_date': {'S': create_date},
                },
            )

    def _get_key_from_dynamodb(self, subject: str) -> dict[str, str] | None:
        """Get access key from DynamoDB.

        This method does NOT use a lock - caller must hold lock.
        Returns dict with access_key, secret_key, and create_date if found,
        None otherwise.
        """
        try:
            response = self.dynamodb.get_item(
                TableName=self.keys_table_name,
                Key={'subject': {'S': subject}},
            )
            if 'Item' in response:
                result = {
                    'access_key': response['Item']['access_key']['S'],
                    'secret_key': response['Item']['secret_key']['S'],
                }
                # create_date might not exist for old entries
                if 'create_date' in response['Item']:
                    result['create_date'] = response['Item']['create_date'][
                        'S'
                    ]
                return result
        except self.dynamodb.exceptions.ResourceNotFoundException:
            # Table doesn't exist yet
            pass
        return None

    def _delete_key_from_dynamodb(self, subject: str) -> None:
        """Delete access key from DynamoDB.

        This method does NOT use a lock - caller must hold lock.
        """
        with contextlib.suppress(
            self.dynamodb.exceptions.ResourceNotFoundException,
        ):
            self.dynamodb.delete_item(
                TableName=self.keys_table_name,
                Key={'subject': {'S': subject}},
            )

    def _create_keys_table(self) -> None:
        """Create DynamoDB table for storing keys if it doesn't exist."""
        try:
            self.dynamodb.create_table(
                TableName=self.keys_table_name,
                KeySchema=[
                    {'AttributeName': 'subject', 'KeyType': 'HASH'},
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'subject', 'AttributeType': 'S'},
                ],
                BillingMode='PAY_PER_REQUEST',
            )
            # Wait for table to be created
            self.dynamodb.get_waiter('table_exists').wait(
                TableName=self.keys_table_name,
            )
        except self.dynamodb.exceptions.ResourceInUseException:
            pass  # Table already exists

    def create_key(self, subject: str) -> dict[str, str]:
        """Create an access key for a user, creating user if needed."""
        with self.lock:
            # Ensure user exists first (idempotent)
            self._ensure_user_exists(subject)
            # Ensure namespace exists (idempotent)
            self._ensure_namespace_exists(subject)

            # Delete existing keys from IAM if there are any
            with contextlib.suppress(
                self.iam.exceptions.NoSuchEntityException,
            ):
                existing_keys = self.iam.list_access_keys(UserName=subject)
                for key in existing_keys['AccessKeyMetadata']:
                    self.iam.delete_access_key(
                        UserName=subject,
                        AccessKeyId=key['AccessKeyId'],
                    )

            # Delete existing key from DynamoDB if any
            self._delete_key_from_dynamodb(subject)

            # Create new key
            new_key = self.iam.create_access_key(UserName=subject)
            access_key = new_key['AccessKey']['AccessKeyId']
            secret_key = new_key['AccessKey']['SecretAccessKey']
            create_date = new_key['AccessKey']['CreateDate'].isoformat()

            # Store key in DynamoDB
            self._store_key_in_dynamodb(
                subject,
                access_key,
                secret_key,
                create_date,
            )

            return {
                'status': 'success',
                'message': f'Access key created for {subject}',
                'username': subject,
                'access_key': access_key,
                'secret_key': secret_key,
                'create_date': create_date,
                'endpoint': self.iam_public or '',
            }

    def get_key(self, subject: str) -> dict[str, Any]:
        """Get access key from DynamoDB or create if needed."""
        with self.lock:
            # Ensure user exists first (idempotent)
            self._ensure_user_exists(subject)
            # Ensure namespace exists (idempotent)
            self._ensure_namespace_exists(subject)

            # Try to retrieve key from DynamoDB first
            stored_key = self._get_key_from_dynamodb(subject)
            if stored_key:
                # Verify the key still exists in IAM
                try:
                    keys = self.iam.list_access_keys(UserName=subject)
                    access_key_ids = [
                        k['AccessKeyId'] for k in keys['AccessKeyMetadata']
                    ]
                    if stored_key['access_key'] in access_key_ids:
                        # Key exists in both DynamoDB and IAM, return it
                        result = {
                            'status': 'success',
                            'message': f'Access key retrieved for {subject}',
                            'username': subject,
                            'access_key': stored_key['access_key'],
                            'secret_key': stored_key['secret_key'],
                            'endpoint': self.iam_public or '',
                            'retrieved_from_dynamodb': True,
                        }
                        # Include create_date if available
                        if 'create_date' in stored_key:
                            result['create_date'] = stored_key['create_date']
                        return result
                except self.iam.exceptions.NoSuchEntityException:
                    pass  # User doesn't exist (shouldn't happen)

                # Key in DynamoDB but not in IAM, delete from DynamoDB
                self._delete_key_from_dynamodb(subject)

            # No key in DynamoDB or key invalid, create new one
            # Delete existing keys from IAM if there are any
            with contextlib.suppress(
                self.iam.exceptions.NoSuchEntityException,
            ):
                existing_keys = self.iam.list_access_keys(UserName=subject)
                for key in existing_keys['AccessKeyMetadata']:
                    self.iam.delete_access_key(
                        UserName=subject,
                        AccessKeyId=key['AccessKeyId'],
                    )

            # Create new key
            new_key = self.iam.create_access_key(UserName=subject)
            access_key = new_key['AccessKey']['AccessKeyId']
            secret_key = new_key['AccessKey']['SecretAccessKey']
            create_date = new_key['AccessKey']['CreateDate'].isoformat()

            # Store key in DynamoDB
            self._store_key_in_dynamodb(
                subject,
                access_key,
                secret_key,
                create_date,
            )

            return {
                'status': 'success',
                'message': f'Access key retrieved/created for {subject}',
                'username': subject,
                'access_key': access_key,
                'secret_key': secret_key,
                'create_date': create_date,
                'endpoint': self.iam_public or '',
                'retrieved_from_dynamodb': False,
            }

    def delete_key(self, subject: str) -> dict[str, str]:
        """Delete access keys for a user."""
        with self.lock:
            # Check if user exists
            try:
                self.iam.get_user(UserName=subject)
            except self.iam.exceptions.NoSuchEntityException:
                return {
                    'status': 'error',
                    'message': f'User {subject} does not exist.',
                }

            # Delete existing keys from IAM
            keys_deleted = False
            try:
                existing_keys = self.iam.list_access_keys(UserName=subject)
                for key in existing_keys['AccessKeyMetadata']:
                    self.iam.delete_access_key(
                        UserName=subject,
                        AccessKeyId=key['AccessKeyId'],
                    )
                    keys_deleted = True
            except self.iam.exceptions.NoSuchEntityException:
                pass  # No keys exist

            # Delete key from DynamoDB
            self._delete_key_from_dynamodb(subject)

            return {
                'status': 'success',
                'message': f'Access keys deleted for {subject}',
                'keys_deleted': str(keys_deleted),
            }

    def _delete_policy_versions(self, policy_arn: str) -> None:
        """Delete all non-default policy versions."""
        with contextlib.suppress(
            self.iam.exceptions.NoSuchEntityException,
        ):
            existing_policies = self.iam.list_policy_versions(
                PolicyArn=policy_arn,
            )
            for ver in existing_policies['Versions']:
                if not ver['IsDefaultVersion']:
                    self.iam.delete_policy_version(
                        PolicyArn=policy_arn,
                        VersionId=ver['VersionId'],
                    )

    def _update_iam_policy(self, subject: str) -> None:
        """Update IAM policy by creating a new version and deleting old ones.

        This method refreshes the policy with the latest policy document from
        iam_user_policy_v3, creates a new version, and deletes old non-default
        versions to avoid hitting the version quota.

        This method does NOT use a lock - caller must hold lock.

        Args:
            subject: User subject ID
        """
        policy_arn = self.iam_policy_arn_prefix + subject
        with contextlib.suppress(
            self.iam.exceptions.NoSuchEntityException,
        ):
            # Get the latest policy document
            latest_policy = self.naming.iam_user_policy_v3(subject)

            # Create a new policy version with the latest policy
            self.iam.create_policy_version(
                PolicyArn=policy_arn,
                PolicyDocument=json.dumps(latest_policy),
                SetAsDefault=True,
            )

            # Delete old non-default versions
            self._delete_policy_versions(policy_arn)

    def _validate_namespace_name(
        self,
        namespace: str,
    ) -> dict[str, Any] | None:
        """Validate namespace name according to rules.

        Rules:
        - 3-32 characters
        - Only lowercase, uppercase, numbers, dash, underscore
        - Cannot start or end with hyphen or underscore

        Returns:
            None if valid, or dict with 'status': 'failure' and 'message'
            if invalid
        """
        if not (
            MIN_NAMESPACE_LENGTH <= len(namespace) <= MAX_NAMESPACE_LENGTH
        ):
            return {
                'status': 'failure',
                'message': (
                    f'Namespace name must be between {MIN_NAMESPACE_LENGTH} '
                    f'and {MAX_NAMESPACE_LENGTH} characters'
                ),
            }
        if not re.match(r'^[a-zA-Z0-9_-]+$', namespace):
            return {
                'status': 'failure',
                'message': (
                    'Namespace name can only contain letters, numbers, '
                    'dash, and underscore'
                ),
            }
        if namespace[0] in ('-', '_') or namespace[-1] in ('-', '_'):
            return {
                'status': 'failure',
                'message': (
                    'Namespace name cannot start or end with hyphen or '
                    'underscore'
                ),
            }
        return None

    def _create_users_table(self) -> None:
        """Create DynamoDB table for user records if it doesn't exist."""
        try:
            self.dynamodb.create_table(
                TableName=self.users_table_name,
                KeySchema=[
                    {'AttributeName': 'subject', 'KeyType': 'HASH'},
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'subject', 'AttributeType': 'S'},
                ],
                BillingMode='PAY_PER_REQUEST',
            )
            # Wait for table to be created
            self.dynamodb.get_waiter('table_exists').wait(
                TableName=self.users_table_name,
            )
        except self.dynamodb.exceptions.ResourceInUseException:
            pass  # Table already exists

    def _get_user_record(self, subject: str) -> dict[str, Any] | None:
        """Get user record from DynamoDB.

        This method does NOT use a lock - caller must hold lock.
        Returns dict with namespaces list if found, None otherwise.
        """
        try:
            response = self.dynamodb.get_item(
                TableName=self.users_table_name,
                Key={'subject': {'S': subject}},
            )
            if 'Item' in response:
                result: dict[str, Any] = {}
                if 'namespaces' in response['Item']:
                    result['namespaces'] = [
                        ns['S'] for ns in response['Item']['namespaces']['L']
                    ]
                return result
        except self.dynamodb.exceptions.ResourceNotFoundException:
            # Table doesn't exist yet
            pass
        return None

    def _get_global_namespaces(self) -> set[str]:
        """Get all globally registered namespace names.

        This method does NOT use a lock - caller must hold lock.
        Returns set of namespace names.
        """
        try:
            response = self.dynamodb.get_item(
                TableName=self.users_table_name,
                Key={'subject': {'S': GLOBAL_NAMESPACES_KEY}},
            )
            if 'Item' in response and 'namespaces' in response['Item']:
                return {ns['S'] for ns in response['Item']['namespaces']['L']}
        except self.dynamodb.exceptions.ResourceNotFoundException:
            # Table doesn't exist yet
            pass
        return set()

    def _update_global_namespaces(self, namespaces: set[str]) -> None:
        """Update global namespaces registry in DynamoDB.

        This method does NOT use a lock - caller must hold lock.
        """
        try:
            self.dynamodb.put_item(
                TableName=self.users_table_name,
                Item={
                    'subject': {'S': GLOBAL_NAMESPACES_KEY},
                    'namespaces': {
                        'L': [{'S': ns} for ns in sorted(namespaces)],
                    },
                },
            )
        except self.dynamodb.exceptions.ResourceNotFoundException:
            # Table doesn't exist, create it
            self._create_users_table()
            # Retry storing the record
            self.dynamodb.put_item(
                TableName=self.users_table_name,
                Item={
                    'subject': {'S': GLOBAL_NAMESPACES_KEY},
                    'namespaces': {
                        'L': [{'S': ns} for ns in sorted(namespaces)],
                    },
                },
            )

    def _update_user_namespaces(
        self,
        subject: str,
        namespaces: list[str],
    ) -> None:
        """Update user record with namespaces in DynamoDB.

        This method does NOT use a lock - caller must hold lock.
        """
        try:
            self.dynamodb.put_item(
                TableName=self.users_table_name,
                Item={
                    'subject': {'S': subject},
                    'namespaces': {
                        'L': [{'S': ns} for ns in sorted(set(namespaces))],
                    },
                },
            )
        except self.dynamodb.exceptions.ResourceNotFoundException:
            # Table doesn't exist, create it
            self._create_users_table()
            # Retry storing the record
            self.dynamodb.put_item(
                TableName=self.users_table_name,
                Item={
                    'subject': {'S': subject},
                    'namespaces': {
                        'L': [{'S': ns} for ns in sorted(set(namespaces))],
                    },
                },
            )

    def _delete_user_record(self, subject: str) -> None:
        """Delete user record from DynamoDB.

        This method does NOT use a lock - caller must hold lock.
        """
        with contextlib.suppress(
            self.dynamodb.exceptions.ResourceNotFoundException,
        ):
            self.dynamodb.delete_item(
                TableName=self.users_table_name,
                Key={'subject': {'S': subject}},
            )

    def _create_namespace_internal(
        self,
        subject: str,
        namespace: str,
    ) -> dict[str, Any]:
        """Create a namespace for a user (internal, no lock).

        This method does NOT use a lock - caller must hold lock.
        Namespace names must be globally unique.

        Args:
            subject: User subject ID
            namespace: Namespace name to create

        Returns:
            dict with status, message, and namespaces list (on success)
            or status 'failure' with message (if namespace already taken)
        """
        # Validate namespace name
        validation_error = self._validate_namespace_name(namespace)
        if validation_error:
            validation_error['namespace'] = namespace
            return validation_error

        # Get global namespaces registry
        global_namespaces = self._get_global_namespaces()

        # Get existing namespaces for this user
        user_record = self._get_user_record(subject)
        existing_namespaces = (
            user_record['namespaces'] if user_record else []
        )

        # Check if namespace is already taken by another user
        if namespace in global_namespaces:
            if namespace not in existing_namespaces:
                return {
                    'status': 'failure',
                    'message': f'Namespace already taken: {namespace}',
                    'namespace': namespace,
                }
            # Namespace already owned by this user, return existing list
            return {
                'status': 'success',
                'message': f'Namespace already exists for {subject}',
                'namespaces': existing_namespaces,
            }

        # Add namespace to user's list
        all_namespaces = sorted({*existing_namespaces, namespace})

        # Update global registry
        updated_global = global_namespaces | {namespace}
        self._update_global_namespaces(updated_global)

        # Update user record
        self._update_user_namespaces(subject, all_namespaces)

        return {
            'status': 'success',
            'message': f'Namespace created for {subject}',
            'namespaces': all_namespaces,
        }

    def create_namespace(
        self,
        subject: str,
        namespace: str,
    ) -> dict[str, Any]:
        """Create a namespace for a user.

        Namespace names must be globally unique.

        Args:
            subject: User subject ID
            namespace: Namespace name to create

        Returns:
            dict with status, message, and namespaces list (on success)
            or status 'failure' with message (if namespace already taken)
        """
        with self.lock:
            return self._create_namespace_internal(subject, namespace)

    def _delete_namespace_internal(
        self,
        subject: str,
        namespace: str,
    ) -> dict[str, Any]:
        """Delete a namespace for a user (internal, no lock).

        This method does NOT use a lock - caller must hold lock.

        Args:
            subject: User subject ID
            namespace: Namespace name to delete

        Returns:
            dict with status, message, and remaining namespaces list.
            Returns success message even if namespace doesn't exist.
        """
        # Get existing namespaces
        user_record = self._get_user_record(subject)
        if not user_record or 'namespaces' not in user_record:
            # No namespaces found, already deleted (idempotent)
            return {
                'status': 'success',
                'message': f'No namespaces found for {subject}',
                'namespaces': [],
            }

        existing_namespaces = user_record['namespaces']

        if namespace not in existing_namespaces:
            # Namespace doesn't exist, already deleted (idempotent)
            return {
                'status': 'success',
                'message': (
                    f'Namespace {namespace} not found for {subject}'
                ),
                'namespaces': existing_namespaces,
            }

        # Remove namespace from user's list
        remaining_namespaces = [
            ns for ns in existing_namespaces if ns != namespace
        ]

        if remaining_namespaces:
            # Update with remaining namespaces
            self._update_user_namespaces(subject, remaining_namespaces)
        else:
            # Delete record if no namespaces left
            self._delete_user_record(subject)

        # Update global registry
        global_namespaces = self._get_global_namespaces()
        updated_global = global_namespaces - {namespace}
        if updated_global:
            self._update_global_namespaces(updated_global)
        else:
            # Delete global record if empty
            self._delete_user_record(GLOBAL_NAMESPACES_KEY)

        return {
            'status': 'success',
            'message': f'Namespace deleted for {subject}',
            'namespaces': remaining_namespaces,
        }

    def delete_namespace(
        self,
        subject: str,
        namespace: str,
    ) -> dict[str, Any]:
        """Delete a namespace for a user (idempotent).

        Args:
            subject: User subject ID
            namespace: Namespace name to delete

        Returns:
            dict with status, message, and remaining namespaces list.
            Returns success message even if namespace doesn't exist.
        """
        with self.lock:
            return self._delete_namespace_internal(subject, namespace)

    def _create_namespace_table(self) -> None:
        """Create DynamoDB table for namespace/topic records.

        Creates table if it doesn't exist.
        """
        try:
            self.dynamodb.create_table(
                TableName=self.namespace_table_name,
                KeySchema=[
                    {'AttributeName': 'namespace', 'KeyType': 'HASH'},
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'namespace', 'AttributeType': 'S'},
                ],
                BillingMode='PAY_PER_REQUEST',
            )
            self.dynamodb.get_waiter('table_exists').wait(
                TableName=self.namespace_table_name,
            )
        except self.dynamodb.exceptions.ResourceInUseException:
            pass

    def _get_namespace_topics(self, namespace: str) -> list[str]:
        """Get list of topics for a namespace from DynamoDB.

        This method does NOT use a lock - caller must hold lock.
        Returns empty list if namespace not found.
        """
        try:
            response = self.dynamodb.get_item(
                TableName=self.namespace_table_name,
                Key={'namespace': {'S': namespace}},
            )
            if 'Item' in response and 'topics' in response['Item']:
                return [t['S'] for t in response['Item']['topics']['L']]
        except self.dynamodb.exceptions.ResourceNotFoundException:
            pass
        return []

    def _update_namespace_topics(
        self,
        namespace: str,
        topics: list[str],
    ) -> None:
        """Update namespace record with topics in DynamoDB.

        This method does NOT use a lock - caller must hold lock.
        """
        try:
            self.dynamodb.put_item(
                TableName=self.namespace_table_name,
                Item={
                    'namespace': {'S': namespace},
                    'topics': {
                        'L': [{'S': t} for t in sorted(set(topics))],
                    },
                },
            )
        except self.dynamodb.exceptions.ResourceNotFoundException:
            self._create_namespace_table()
            self.dynamodb.put_item(
                TableName=self.namespace_table_name,
                Item={
                    'namespace': {'S': namespace},
                    'topics': {
                        'L': [{'S': t} for t in sorted(set(topics))],
                    },
                },
            )

    def _delete_namespace_topics(self, namespace: str) -> None:
        """Delete namespace record from DynamoDB.

        This method does NOT use a lock - caller must hold lock.
        """
        with contextlib.suppress(
            self.dynamodb.exceptions.ResourceNotFoundException,
        ):
            self.dynamodb.delete_item(
                TableName=self.namespace_table_name,
                Key={'namespace': {'S': namespace}},
            )

    def _create_kafka_topic(
        self,
        namespace: str,
        topic: str,
    ) -> dict[str, Any] | None:
        """Create a Kafka topic (idempotent).

        This method does NOT use a lock - caller must hold lock.

        Args:
            namespace: Namespace name
            topic: Topic name

        Returns:
            dict with 'status': 'success' on success, or
            dict with 'status': 'failure' and 'message' on failure
            after retries, or None if Kafka endpoint is not configured.

        The actual Kafka topic name will be '{namespace}.{topic}'.
        """
        if not self.iam_public:
            # No Kafka endpoint configured, skip topic creation
            return None

        kafka_topic_name = f'{namespace}.{topic}'
        max_retries = 3
        last_exception: Exception | None = None

        for attempt in range(max_retries):
            try:
                admin = KafkaAdminClient(
                    bootstrap_servers=self.iam_public,
                    security_protocol='SASL_SSL',
                    sasl_mechanism='OAUTHBEARER',
                    sasl_oauth_token_provider=MSKTokenProvider(self.region),
                )
                topic_list = [
                    NewTopic(
                        name=kafka_topic_name,
                        num_partitions=1,
                        replication_factor=2,
                    ),
                ]
                admin.create_topics(new_topics=topic_list)
                return {'status': 'success'}  # Success
            except TopicAlreadyExistsError:
                # Topic already exists, this is idempotent
                return {'status': 'success'}
            except Exception as e:
                last_exception = e
                if attempt == max_retries - 1:
                    # Last attempt failed, return failure status
                    return {
                        'status': 'failure',
                        'message': (
                            f'Failed to create Kafka topic '
                            f'{kafka_topic_name} after '
                            f'{max_retries} attempts: {e!s}'
                        ),
                    }
                # Retry on other exceptions
                continue

        # This should never be reached, but for type safety
        return {
            'status': 'failure',
            'message': (
                f'Failed to create Kafka topic {kafka_topic_name}: '
                f'{str(last_exception) if last_exception else "Unknown error"}'
            ),
        }

    def _delete_kafka_topic(
        self,
        namespace: str,
        topic: str,
    ) -> dict[str, Any] | None:
        """Delete a Kafka topic (idempotent).

        This method does NOT use a lock - caller must hold lock.

        Args:
            namespace: Namespace name
            topic: Topic name

        Returns:
            dict with 'status': 'success' on success, or
            dict with 'status': 'failure' and 'message' on failure
            after retries, or None if Kafka endpoint is not configured.

        The actual Kafka topic name will be '{namespace}.{topic}'.
        """
        if not self.iam_public:
            # No Kafka endpoint configured, skip topic deletion
            return None

        kafka_topic_name = f'{namespace}.{topic}'
        max_retries = 3
        last_exception: Exception | None = None

        for attempt in range(max_retries):
            try:
                admin = KafkaAdminClient(
                    bootstrap_servers=self.iam_public,
                    security_protocol='SASL_SSL',
                    sasl_mechanism='OAUTHBEARER',
                    sasl_oauth_token_provider=MSKTokenProvider(self.region),
                )
                admin.delete_topics(topics=[kafka_topic_name])
                return {'status': 'success'}  # Success
            except UnknownTopicOrPartitionError:
                # Topic doesn't exist, this is idempotent
                return {'status': 'success'}
            except Exception as e:
                last_exception = e
                if attempt == max_retries - 1:
                    # Last attempt failed, return failure status
                    return {
                        'status': 'failure',
                        'message': (
                            f'Failed to delete Kafka topic '
                            f'{kafka_topic_name} after '
                            f'{max_retries} attempts: {e!s}'
                        ),
                    }
                # Retry on other exceptions
                continue

        # This should never be reached, but for type safety
        return {
            'status': 'failure',
            'message': (
                f'Failed to delete Kafka topic {kafka_topic_name}: '
                f'{str(last_exception) if last_exception else "Unknown error"}'
            ),
        }

    def create_topic(  # noqa: PLR0911
        self,
        subject: str,
        namespace: str,
        topic: str,
    ) -> dict[str, Any]:
        """Create a topic under a namespace.

        Args:
            subject: User subject ID
            namespace: Namespace name
            topic: Topic name

        Returns:
            dict with status, message, and topic info (on success)
            or status 'failure' with message (if validation fails or
            namespace not owned by user)
        """
        with self.lock:
            # Validate topic name (same rules as namespace)
            validation_error = self._validate_namespace_name(topic)
            if validation_error:
                validation_error['namespace'] = namespace
                validation_error['topic'] = topic
                return validation_error

            # Verify namespace belongs to user
            user_record = self._get_user_record(subject)
            if not user_record or 'namespaces' not in user_record:
                return {
                    'status': 'failure',
                    'message': f'No namespaces found for {subject}',
                    'namespace': namespace,
                    'topic': topic,
                }
            if namespace not in user_record['namespaces']:
                return {
                    'status': 'failure',
                    'message': (
                        f'Namespace {namespace} not found for {subject}'
                    ),
                    'namespace': namespace,
                    'topic': topic,
                }

            # Get existing topics for namespace
            existing_topics = self._get_namespace_topics(namespace)

            # Check if topic already exists (must be unique under namespace)
            if topic in existing_topics:
                # Topic already exists, ensure user exists and create Kafka
                # topic (idempotent)
                self._ensure_user_exists(subject)
                kafka_result = self._create_kafka_topic(namespace, topic)
                if kafka_result and kafka_result.get('status') == 'failure':
                    return {
                        'status': 'failure',
                        'message': kafka_result.get(
                            'message',
                            'Unknown error',
                        ),
                        'namespace': namespace,
                        'topic': topic,
                    }
                return {
                    'status': 'success',
                    'message': (
                        f'Topic {topic} already exists in '
                        f'namespace {namespace}'
                    ),
                    'namespace': namespace,
                    'topic': topic,
                }

            # Add topic to namespace
            all_topics = sorted({*existing_topics, topic})
            self._update_namespace_topics(namespace, all_topics)

            # Ensure user exists
            self._ensure_user_exists(subject)

            # Create Kafka topic (idempotent)
            kafka_result = self._create_kafka_topic(namespace, topic)
            if kafka_result and kafka_result.get('status') == 'failure':
                return {
                    'status': 'failure',
                    'message': kafka_result.get(
                        'message',
                        'Unknown error',
                    ),
                    'namespace': namespace,
                    'topic': topic,
                }

            return {
                'status': 'success',
                'message': f'Topic {topic} created in namespace {namespace}',
                'namespace': namespace,
                'topic': topic,
            }

    def delete_topic(
        self,
        subject: str,
        namespace: str,
        topic: str,
    ) -> dict[str, Any]:
        """Delete a topic from a namespace (idempotent).

        Args:
            subject: User subject ID
            namespace: Namespace name
            topic: Topic name

        Returns:
            dict with status and message.
            Returns success message even if namespace or topic doesn't exist.
        """
        with self.lock:
            # Verify namespace belongs to user
            user_record = self._get_user_record(subject)
            if not user_record or 'namespaces' not in user_record:
                # No namespaces found, already deleted (idempotent)
                # Still try to delete from Kafka
                self._delete_kafka_topic(namespace, topic)
                return {
                    'status': 'success',
                    'message': f'No namespaces found for {subject}',
                    'namespace': namespace,
                    'topic': topic,
                }
            if namespace not in user_record['namespaces']:
                # Namespace doesn't exist, already deleted (idempotent)
                # Still try to delete from Kafka
                self._delete_kafka_topic(namespace, topic)
                return {
                    'status': 'success',
                    'message': (
                        f'Namespace {namespace} not found for {subject}'
                    ),
                    'namespace': namespace,
                    'topic': topic,
                }

            # Get existing topics for namespace
            existing_topics = self._get_namespace_topics(namespace)

            # Check if topic exists
            if topic not in existing_topics:
                # Topic doesn't exist, delete from Kafka anyway (idempotent)
                self._delete_kafka_topic(namespace, topic)
                return {
                    'status': 'success',
                    'message': (
                        f'Topic {topic} not found in namespace {namespace}'
                    ),
                    'namespace': namespace,
                    'topic': topic,
                }

            # Remove topic from namespace
            remaining_topics = [t for t in existing_topics if t != topic]
            if remaining_topics:
                self._update_namespace_topics(namespace, remaining_topics)
            else:
                self._delete_namespace_topics(namespace)

            # Delete Kafka topic (idempotent)
            kafka_result = self._delete_kafka_topic(namespace, topic)
            if kafka_result and kafka_result.get('status') == 'failure':
                return {
                    'status': 'failure',
                    'message': kafka_result.get(
                        'message',
                        'Unknown error',
                    ),
                    'namespace': namespace,
                    'topic': topic,
                }

            return {
                'status': 'success',
                'message': f'Topic {topic} deleted from namespace {namespace}',
                'namespace': namespace,
                'topic': topic,
            }

    def list_namespaces(
        self,
        subject: str,
    ) -> dict[str, Any]:
        """List all namespaces owned by a user and their topics.

        Args:
            subject: User subject ID

        Returns:
            dict with status, message, and namespaces dict
            (namespace -> topics)  # noqa: E501
        """
        with self.lock:
            # Get user record
            user_record = self._get_user_record(subject)
            if not user_record or 'namespaces' not in user_record:
                return {
                    'status': 'success',
                    'message': f'No namespaces found for {subject}',
                    'namespaces': {},
                }

            namespaces = user_record['namespaces']
            result: dict[str, list[str]] = {}

            # Get topics for each namespace
            for namespace in namespaces:
                topics = self._get_namespace_topics(namespace)
                result[namespace] = topics

            return {
                'status': 'success',
                'message': f'Namespaces retrieved for {subject}',
                'namespaces': result,
            }

    def recreate_topic(  # noqa: PLR0911
        self,
        subject: str,
        namespace: str,
        topic: str,
    ) -> dict[str, Any]:
        """Recreate a topic by deleting and recreating it via KafkaAdminClient.

        Authenticates the client with namespace and topic access, then:
        1. Verifies the subject owns the namespace
        2. Verifies the topic exists in the namespace
        3. Creates a KafkaAdminClient
        4. Deletes the Kafka topic
        5. Recreates the Kafka topic

        Args:
            subject: User subject ID
            namespace: Namespace name
            topic: Topic name

        Returns:
            dict with status, message, namespace, and topic info
        """
        with self.lock:
            # Verify namespace belongs to user
            user_record = self._get_user_record(subject)
            if not user_record or 'namespaces' not in user_record:
                return {
                    'status': 'failure',
                    'message': f'No namespaces found for {subject}',
                    'namespace': namespace,
                    'topic': topic,
                }
            if namespace not in user_record['namespaces']:
                return {
                    'status': 'failure',
                    'message': (
                        f'Namespace {namespace} not found for {subject}'
                    ),
                    'namespace': namespace,
                    'topic': topic,
                }

            # Verify topic exists in namespace
            namespace_topics = self._get_namespace_topics(namespace)
            if topic not in namespace_topics:
                return {
                    'status': 'failure',
                    'message': (
                        f'Topic {topic} not found in namespace {namespace} '
                        f'for {subject}'
                    ),
                    'namespace': namespace,
                    'topic': topic,
                }

            # Check if Kafka endpoint is configured
            if not self.iam_public:
                return {
                    'status': 'failure',
                    'message': 'Kafka endpoint not configured',
                    'namespace': namespace,
                    'topic': topic,
                }

            kafka_topic_name = f'{namespace}.{topic}'

            try:
                # Create KafkaAdminClient
                admin = KafkaAdminClient(
                    bootstrap_servers=self.iam_public,
                    security_protocol='SASL_SSL',
                    sasl_mechanism='OAUTHBEARER',
                    sasl_oauth_token_provider=MSKTokenProvider(self.region),
                )

                # Delete the topic
                with contextlib.suppress(UnknownTopicOrPartitionError):
                    admin.delete_topics(topics=[kafka_topic_name])

                # Recreate the topic
                topic_list = [
                    NewTopic(
                        name=kafka_topic_name,
                        num_partitions=1,
                        replication_factor=2,
                    ),
                ]
                admin.create_topics(new_topics=topic_list)

                return {
                    'status': 'success',
                    'message': (
                        f'Topic {topic} recreated in namespace {namespace}'
                    ),
                    'namespace': namespace,
                    'topic': topic,
                }
            except TopicAlreadyExistsError:
                # Topic was recreated successfully
                return {
                    'status': 'success',
                    'message': (
                        f'Topic {topic} recreated in namespace {namespace}'
                    ),
                    'namespace': namespace,
                    'topic': topic,
                }
            except Exception as e:
                return {
                    'status': 'failure',
                    'message': (
                        f'Failed to recreate topic {topic} in namespace '
                        f'{namespace}: {e!s}'
                    ),
                    'namespace': namespace,
                    'topic': topic,
                }
