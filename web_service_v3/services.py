"""Service classes for AWS resource management."""

from __future__ import annotations

import contextlib
import json
import re
import secrets
import string
from typing import Any

from kafka.admin import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.errors import UnknownTopicOrPartitionError

from web_service_v3.responses import create_failure_result
from web_service_v3.responses import create_success_result
from web_service_v3.utils import MSKTokenProvider

# Namespace validation constants
MIN_NAMESPACE_LENGTH = 3
MAX_NAMESPACE_LENGTH = 32

# Global namespaces tracking key
GLOBAL_NAMESPACES_KEY = '__global_namespaces__'


class IAMService:
    """Service for managing IAM users and policies."""

    def __init__(
        self,
        iam_client: Any,
        account_id: str,
        region: str,
        cluster_name: str,
    ) -> None:
        """Initialize IAM service.

        Args:
            iam_client: Boto3 IAM client
            account_id: AWS account ID
            region: AWS region
            cluster_name: MSK cluster name
        """
        self.iam = iam_client
        self.account_id = account_id
        self.region = region
        self.cluster_name = cluster_name
        self.iam_policy_arn_prefix = (
            f'arn:aws:iam::{account_id}:policy/msk-policy/'
        )

    def generate_user_policy(self, subject: str) -> dict[str, Any]:
        """Generate an IAM user policy for Kafka access (v3).

        Namespace routes are disabled - uses ns-<last 12 digits>.* pattern.

        Args:
            subject: User subject ID

        Returns:
            IAM policy document as a dictionary
        """
        # Extract last 12 characters from subject (removing dashes)
        subject_suffix = subject.replace('-', '')[-12:]
        topic_arn = (
            f'arn:aws:kafka:{self.region}:{self.account_id}:'
            f'topic/{self.cluster_name}/*/ns-{subject_suffix}.*'
        )
        return {
            'Version': '2012-10-17',
            'Statement': [
                {
                    'Effect': 'Allow',
                    'Action': [
                        'kafka-cluster:Connect',
                        'kafka-cluster:DescribeTopic',
                        'kafka-cluster:ReadData',
                        'kafka-cluster:WriteData',
                        'kafka-cluster:DescribeGroup',
                        'kafka-cluster:AlterGroup',
                        'kafka-cluster:WriteDataIdempotently',
                        'kafka-cluster:DescribeTransactionalId',
                        'kafka-cluster:AlterTransactionalId',
                    ],
                    'Resource': [
                        f'arn:aws:kafka:{self.region}:{self.account_id}:group/{self.cluster_name}/*',
                        f'arn:aws:kafka:{self.region}:{self.account_id}:cluster/{self.cluster_name}/*',
                        f'arn:aws:kafka:{self.region}:{self.account_id}:transactional-id/{self.cluster_name}/*',
                        topic_arn,
                    ],
                },
            ],
        }

    def ensure_user_exists(self, subject: str) -> dict[str, bool]:
        """Ensure IAM user exists with policy attached (idempotent).

        This method does NOT use a lock - caller must hold lock.
        Returns dict with flags indicating what was created.

        Args:
            subject: User subject ID

        Returns:
            Dictionary with creation flags
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
                    self.generate_user_policy(subject),
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
        # Note: update_policy already handles NoSuchEntityException internally
        self.update_policy(subject)

        return {
            'user_created': user_created,
            'policy_created': policy_created,
            'policy_attached': policy_attached,
        }

    def update_policy(self, subject: str) -> None:
        """Update IAM policy by creating a new version and deleting old ones.

        This method refreshes the policy with the latest policy document,
        creates a new version, and deletes old non-default versions to avoid
        hitting the version quota.

        This method does NOT use a lock - caller must hold lock.

        Args:
            subject: User subject ID
        """
        policy_arn = self.iam_policy_arn_prefix + subject
        with contextlib.suppress(
            self.iam.exceptions.NoSuchEntityException,
        ):
            # Get the latest policy document
            latest_policy = self.generate_user_policy(subject)

            # Create a new policy version with the latest policy
            self.iam.create_policy_version(
                PolicyArn=policy_arn,
                PolicyDocument=json.dumps(latest_policy),
                SetAsDefault=True,
            )

            # Delete old non-default versions
            self.delete_policy_versions(policy_arn)

    def delete_policy_versions(self, policy_arn: str) -> None:
        """Delete all non-default policy versions.

        Args:
            policy_arn: Policy ARN
        """
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

    def delete_user(self, subject: str) -> dict[str, Any]:
        """Delete IAM user and all associated resources.

        Args:
            subject: User subject ID

        Returns:
            dict with deletion status
        """
        namespaces_deleted = False  # Will be set by caller
        keys_deleted = False
        policy_detached = False
        policy_deleted = False
        user_deleted = False

        # 1. Delete existing access keys if any
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

        # 2. Detach IAM policy
        try:
            policy_arn = self.iam_policy_arn_prefix + subject
            self.iam.detach_user_policy(
                UserName=subject,
                PolicyArn=policy_arn,
            )
            policy_detached = True
        except self.iam.exceptions.NoSuchEntityException:
            pass  # policy not attached

        # 3. Delete IAM policy (including all versions)
        try:
            policy_arn = self.iam_policy_arn_prefix + subject
            # Delete non-default policy versions first
            self.delete_policy_versions(policy_arn)
            # Delete the policy itself
            self.iam.delete_policy(PolicyArn=policy_arn)
            policy_deleted = True
        except self.iam.exceptions.NoSuchEntityException:
            pass  # policy doesn't exist

        # 4. Delete IAM user
        try:
            self.iam.delete_user(UserName=subject)
            user_deleted = True
        except self.iam.exceptions.NoSuchEntityException:
            pass  # user doesn't exist

        return {
            'status': 'success',
            'message': f'IAM user deleted for {subject}',
            'namespaces_deleted': namespaces_deleted,
            'keys_deleted': keys_deleted,
            'policy_detached': policy_detached,
            'policy_deleted': policy_deleted,
            'user_deleted': user_deleted,
        }

    def create_access_key(self, subject: str) -> dict[str, str]:
        """Create an access key for a user.

        Args:
            subject: User subject ID

        Returns:
            Dictionary with access_key, secret_key, and create_date
        """
        new_key = self.iam.create_access_key(UserName=subject)
        return {
            'access_key': new_key['AccessKey']['AccessKeyId'],
            'secret_key': new_key['AccessKey']['SecretAccessKey'],
            'create_date': new_key['AccessKey']['CreateDate'].isoformat(),
        }

    def delete_access_keys(self, subject: str) -> bool:
        """Delete all access keys for a user.

        Args:
            subject: User subject ID

        Returns:
            True if any keys were deleted, False otherwise
        """
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
        return keys_deleted

    def list_access_keys(self, subject: str) -> list[str]:
        """List access key IDs for a user.

        Args:
            subject: User subject ID

        Returns:
            List of access key IDs
        """
        try:
            keys = self.iam.list_access_keys(UserName=subject)
            return [k['AccessKeyId'] for k in keys['AccessKeyMetadata']]
        except self.iam.exceptions.NoSuchEntityException:
            return []

    def user_exists(self, subject: str) -> bool:
        """Check if user exists.

        Args:
            subject: User subject ID

        Returns:
            True if user exists, False otherwise
        """
        try:
            self.iam.get_user(UserName=subject)
            return True
        except self.iam.exceptions.NoSuchEntityException:
            return False


class DynamoDBService:
    """Service for managing DynamoDB operations."""

    def __init__(
        self,
        dynamodb_client: Any,
        keys_table_name: str,
        users_table_name: str,
        namespace_table_name: str,
    ) -> None:
        """Initialize DynamoDB service.

        Args:
            dynamodb_client: Boto3 DynamoDB client
            keys_table_name: Table name for keys
            users_table_name: Table name for user records
            namespace_table_name: Table name for namespace/topic records
        """
        self.dynamodb = dynamodb_client
        self.keys_table_name = keys_table_name
        self.users_table_name = users_table_name
        self.namespace_table_name = namespace_table_name

    def store_key(
        self,
        subject: str,
        access_key: str,
        secret_key: str,
        create_date: str,
    ) -> None:
        """Store access key in DynamoDB.

        This method does NOT use a lock - caller must hold lock.
        create_date should be ISO format string.

        Args:
            subject: User subject ID
            access_key: Access key ID
            secret_key: Secret access key
            create_date: Creation date in ISO format
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

    def get_key(self, subject: str) -> dict[str, str] | None:
        """Get access key from DynamoDB.

        This method does NOT use a lock - caller must hold lock.
        Returns dict with access_key, secret_key, and create_date if found,
        None otherwise.

        Args:
            subject: User subject ID

        Returns:
            Dictionary with key information or None
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

    def delete_key(self, subject: str) -> None:
        """Delete access key from DynamoDB.

        This method does NOT use a lock - caller must hold lock.

        Args:
            subject: User subject ID
        """
        with contextlib.suppress(
            self.dynamodb.exceptions.ResourceNotFoundException,
        ):
            self.dynamodb.delete_item(
                TableName=self.keys_table_name,
                Key={'subject': {'S': subject}},
            )

    def get_user_record(self, subject: str) -> dict[str, Any] | None:
        """Get user record from DynamoDB.

        This method does NOT use a lock - caller must hold lock.
        Returns dict with namespaces list if found, None otherwise.

        Args:
            subject: User subject ID

        Returns:
            Dictionary with user record or None
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

    def get_global_namespaces(self) -> set[str]:
        """Get all globally registered namespace names.

        This method does NOT use a lock - caller must hold lock.
        Returns set of namespace names.

        Returns:
            Set of namespace names
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

    def update_global_namespaces(self, namespaces: set[str]) -> None:
        """Update global namespaces registry in DynamoDB.

        This method does NOT use a lock - caller must hold lock.

        Args:
            namespaces: Set of namespace names
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

    def update_user_namespaces(
        self,
        subject: str,
        namespaces: list[str],
    ) -> None:
        """Update user record with namespaces in DynamoDB.

        This method does NOT use a lock - caller must hold lock.

        Args:
            subject: User subject ID
            namespaces: List of namespace names
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

    def delete_user_record(self, subject: str) -> None:
        """Delete user record from DynamoDB.

        This method does NOT use a lock - caller must hold lock.

        Args:
            subject: User subject ID
        """
        with contextlib.suppress(
            self.dynamodb.exceptions.ResourceNotFoundException,
        ):
            self.dynamodb.delete_item(
                TableName=self.users_table_name,
                Key={'subject': {'S': subject}},
            )

    def get_namespace_topics(self, namespace: str) -> list[str]:
        """Get list of topics for a namespace from DynamoDB.

        This method does NOT use a lock - caller must hold lock.
        Returns empty list if namespace not found.

        Args:
            namespace: Namespace name

        Returns:
            List of topic names
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

    def update_namespace_topics(
        self,
        namespace: str,
        topics: list[str],
    ) -> None:
        """Update namespace record with topics in DynamoDB.

        This method does NOT use a lock - caller must hold lock.

        Args:
            namespace: Namespace name
            topics: List of topic names
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

    def delete_namespace_topics(self, namespace: str) -> None:
        """Delete namespace record from DynamoDB.

        This method does NOT use a lock - caller must hold lock.

        Args:
            namespace: Namespace name
        """
        with contextlib.suppress(
            self.dynamodb.exceptions.ResourceNotFoundException,
        ):
            self.dynamodb.delete_item(
                TableName=self.namespace_table_name,
                Key={'namespace': {'S': namespace}},
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

    def _create_namespace_table(self) -> None:
        """Create DynamoDB table for namespace/topic records."""
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


class KafkaService:
    """Service for managing Kafka/MSK operations."""

    def __init__(self, bootstrap_servers: str | None, region: str) -> None:
        """Initialize Kafka service.

        Args:
            bootstrap_servers: Kafka bootstrap servers (can be None)
            region: AWS region
        """
        self.bootstrap_servers = bootstrap_servers
        self.region = region

    def create_topic(
        self,
        namespace: str,
        topic: str,
    ) -> dict[str, Any] | None:
        """Create a Kafka topic (idempotent).

        Args:
            namespace: Namespace name
            topic: Topic name

        Returns:
            dict with status and message on success/failure, or None if
            Kafka endpoint is not configured.

        The actual Kafka topic name will be '{namespace}.{topic}'.
        """
        if not self.bootstrap_servers:
            # No Kafka endpoint configured, skip topic creation
            return None

        kafka_topic_name = f'{namespace}.{topic}'
        max_retries = 3
        last_exception: Exception | None = None

        for attempt in range(max_retries):
            try:
                admin = KafkaAdminClient(
                    bootstrap_servers=self.bootstrap_servers,
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
                return create_success_result('Topic created')
            except TopicAlreadyExistsError:
                # Topic already exists, this is idempotent
                return create_success_result('Topic already exists')
            except Exception as e:
                last_exception = e
                if attempt == max_retries - 1:
                    # Last attempt failed, return failure status
                    return create_failure_result(
                        f'Failed to create Kafka topic {kafka_topic_name} '
                        f'after {max_retries} attempts: {e!s}',
                    )
                # Retry on other exceptions
                continue

        # This should never be reached, but for type safety
        return create_failure_result(
            f'Failed to create Kafka topic {kafka_topic_name}: '
            f'{str(last_exception) if last_exception else "Unknown error"}',
        )

    def delete_topic(
        self,
        namespace: str,
        topic: str,
    ) -> dict[str, Any] | None:
        """Delete a Kafka topic (idempotent).

        Args:
            namespace: Namespace name
            topic: Topic name

        Returns:
            dict with status and message on success/failure, or None if
            Kafka endpoint is not configured.

        The actual Kafka topic name will be '{namespace}.{topic}'.
        """
        if not self.bootstrap_servers:
            # No Kafka endpoint configured, skip topic deletion
            return None

        kafka_topic_name = f'{namespace}.{topic}'
        max_retries = 3
        last_exception: Exception | None = None

        for attempt in range(max_retries):
            try:
                admin = KafkaAdminClient(
                    bootstrap_servers=self.bootstrap_servers,
                    security_protocol='SASL_SSL',
                    sasl_mechanism='OAUTHBEARER',
                    sasl_oauth_token_provider=MSKTokenProvider(self.region),
                )
                admin.delete_topics(topics=[kafka_topic_name])
                return create_success_result('Topic deleted')
            except UnknownTopicOrPartitionError:
                # Topic doesn't exist, this is idempotent
                return create_success_result('Topic does not exist')
            except Exception as e:
                last_exception = e
                if attempt == max_retries - 1:
                    # Last attempt failed, return failure status
                    return create_failure_result(
                        f'Failed to delete Kafka topic {kafka_topic_name} '
                        f'after {max_retries} attempts: {e!s}',
                    )
                # Retry on other exceptions
                continue

        # This should never be reached, but for type safety
        return create_failure_result(
            f'Failed to delete Kafka topic {kafka_topic_name}: '
            f'{str(last_exception) if last_exception else "Unknown error"}',
        )


class NamespaceService:
    """Service for managing namespace validation and business logic."""

    def __init__(self, dynamodb_service: DynamoDBService) -> None:
        """Initialize namespace service.

        Args:
            dynamodb_service: DynamoDB service instance
        """
        self.dynamodb = dynamodb_service

    def validate(self, namespace: str) -> dict[str, Any] | None:
        """Validate namespace name according to rules.

        Rules:
        - 3-32 characters
        - Only lowercase, uppercase, numbers, dash, underscore
        - Cannot start or end with hyphen or underscore

        Args:
            namespace: Namespace name to validate

        Returns:
            None if valid, or dict with failure status if invalid
        """
        if not (
            MIN_NAMESPACE_LENGTH <= len(namespace) <= MAX_NAMESPACE_LENGTH
        ):
            return create_failure_result(
                f'Namespace name must be between {MIN_NAMESPACE_LENGTH} '
                f'and {MAX_NAMESPACE_LENGTH} characters',
            )
        if not re.match(r'^[a-zA-Z0-9_-]+$', namespace):
            return create_failure_result(
                'Namespace name can only contain letters, numbers, '
                'dash, and underscore',
            )
        if namespace[0] in ('-', '_') or namespace[-1] in ('-', '_'):
            return create_failure_result(
                'Namespace name cannot start or end with hyphen or underscore',
            )
        return None

    def generate_default(self, subject: str) -> str:
        """Generate default namespace name from subject.

        Args:
            subject: User subject ID

        Returns:
            Default namespace name
        """
        return f'ns-{subject.replace("-", "")[-12:]}'

    def generate_random(self) -> str:
        """Generate random namespace name.

        Returns:
            Random namespace name
        """
        random_suffix = ''.join(
            secrets.choice(string.ascii_lowercase + string.digits)
            for _ in range(12)
        )
        return f'ns-{random_suffix}'

    def ensure_exists(self, subject: str) -> dict[str, Any]:
        """Ensure namespace exists for the user (idempotent).

        This method does NOT use a lock - caller must hold lock.
        Tries to create namespace with subject-based name first.
        If that fails (namespace already taken), retries with random
        namespaces.

        Args:
            subject: User subject ID

        Returns:
            dict with 'status': 'success' and 'namespace' if successful,
            or 'status': 'failure' and 'message' if all retries failed.
        """
        # Try to create namespace with subject-based name first
        namespace = self.generate_default(subject)
        namespace_result = self.create(subject, namespace)

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
            namespace = self.generate_random()
            namespace_result = self.create(subject, namespace)
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

    def create(
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
        validation_error = self.validate(namespace)
        if validation_error:
            validation_error_dict = {
                'status': 'failure',
                'message': validation_error['message'],
            }
            validation_error_dict['namespace'] = namespace
            return validation_error_dict

        # Get global namespaces registry
        global_namespaces = self.dynamodb.get_global_namespaces()

        # Get existing namespaces for this user
        user_record = self.dynamodb.get_user_record(subject)
        existing_namespaces = user_record['namespaces'] if user_record else []

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
        self.dynamodb.update_global_namespaces(updated_global)

        # Update user record
        self.dynamodb.update_user_namespaces(subject, all_namespaces)

        return {
            'status': 'success',
            'message': f'Namespace created for {subject}',
            'namespaces': all_namespaces,
        }

    def delete(
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
        user_record = self.dynamodb.get_user_record(subject)
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
                'message': (f'Namespace {namespace} not found for {subject}'),
                'namespaces': existing_namespaces,
            }

        # Remove namespace from user's list
        remaining_namespaces = [
            ns for ns in existing_namespaces if ns != namespace
        ]

        if remaining_namespaces:
            # Update with remaining namespaces
            self.dynamodb.update_user_namespaces(subject, remaining_namespaces)
        else:
            # Delete record if no namespaces left
            self.dynamodb.delete_user_record(subject)

        # Update global registry
        global_namespaces = self.dynamodb.get_global_namespaces()
        updated_global = global_namespaces - {namespace}
        if updated_global:
            self.dynamodb.update_global_namespaces(updated_global)
        else:
            # Delete global record if empty
            self.dynamodb.delete_user_record(GLOBAL_NAMESPACES_KEY)

        return {
            'status': 'success',
            'message': f'Namespace deleted for {subject}',
            'namespaces': remaining_namespaces,
        }
