"""Service classes for AWS resource management."""

from __future__ import annotations

import contextlib
import json
import os
import re
import threading
import time
from typing import Any

import boto3
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from kafka.admin import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.errors import UnknownTopicOrPartitionError
from kafka.sasl.oauth import AbstractTokenProvider

from web_service.utils import EnvironmentChecker


class IAMService:
    """Service for managing IAM users and policies."""

    def __init__(
        self,
        account_id: str,
        region: str,
        cluster_name: str,
    ) -> None:
        """Initialize IAM service.

        Args:
            account_id: AWS account ID
            region: AWS region
            cluster_name: MSK cluster name
        """
        self.iam = boto3.client('iam')
        self.account_id = account_id
        self.region = region
        self.cluster_name = cluster_name
        self.policy_arn_prefix = (
            f'arn:aws:iam::{account_id}:policy/msk-policy/'
        )
        self.kafka_arn_prefix = f'arn:aws:kafka:{region}:{account_id}'

    def _get_policy_arn(self, subject: str) -> str:
        """Get policy ARN for a subject."""
        return self.policy_arn_prefix + subject

    def generate_user_policy(self, subject: str) -> dict[str, Any]:
        """Generate an IAM user policy for Kafka access.

        Args:
            subject: User subject ID

        Returns:
            IAM policy document as a dictionary
        """
        subject_suffix = subject.replace('-', '')[-12:]
        base = f'{self.kafka_arn_prefix}'
        cluster = f'/{self.cluster_name}/*'
        resources = [
            f'{base}:group{cluster}',
            f'{base}:cluster{cluster}',
            f'{base}:transactional-id{cluster}',
            f'{base}:topic{cluster}/ns-{subject_suffix}.*',
        ]
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
                    'Resource': resources,
                },
            ],
        }

    def create_user_and_policy(self, subject: str) -> dict[str, Any]:
        """Create IAM user with policy attached (idempotent).

        Args:
            subject: User subject ID

        Returns:
            Dictionary with status (success/failure) and message
        """
        try:
            with contextlib.suppress(
                self.iam.exceptions.EntityAlreadyExistsException,
            ):
                self.iam.create_user(
                    Path='/msk-users/',
                    UserName=subject,
                )

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

            with contextlib.suppress(
                self.iam.exceptions.EntityAlreadyExistsException,
            ):
                self.iam.attach_user_policy(
                    UserName=subject,
                    PolicyArn=self._get_policy_arn(subject),
                )

            return {
                'status': 'success',
                'message': f'User and policy created for {subject}',
            }
        except Exception as e:
            return {
                'status': 'failure',
                'message': f'Failed to create user and policy: {e!s}',
            }

    def delete_user_and_policy(self, subject: str) -> dict[str, Any]:
        """Delete IAM user and all associated resources.

        Args:
            subject: User subject ID

        Returns:
            dict with status (success/failure) and message
        """
        try:
            # Delete access keys
            self.delete_access_keys(subject)

            # Detach and delete policy
            policy_arn = self._get_policy_arn(subject)
            with contextlib.suppress(
                self.iam.exceptions.NoSuchEntityException,
            ):
                self.iam.detach_user_policy(
                    UserName=subject,
                    PolicyArn=policy_arn,
                )

            with contextlib.suppress(
                self.iam.exceptions.NoSuchEntityException,
            ):
                # Delete non-default policy versions first
                versions = self.iam.list_policy_versions(
                    PolicyArn=policy_arn,
                )['Versions']
                for ver in versions:
                    if not ver['IsDefaultVersion']:
                        self.iam.delete_policy_version(
                            PolicyArn=policy_arn,
                            VersionId=ver['VersionId'],
                        )
                # Delete the policy itself
                self.iam.delete_policy(PolicyArn=policy_arn)

            # Delete user
            with contextlib.suppress(
                self.iam.exceptions.NoSuchEntityException,
            ):
                self.iam.delete_user(UserName=subject)

            return {
                'status': 'success',
                'message': f'IAM user deleted for {subject}',
            }
        except Exception as e:
            return {
                'status': 'failure',
                'message': f'Failed to delete user and policy: {e!s}',
            }

    def create_access_key(self, subject: str) -> dict[str, Any]:
        """Create an access key for a user.

        Args:
            subject: User subject ID

        Returns:
            Dictionary with status (success/failure), message,
            access_key, secret_key, and create_date
        """
        try:
            response = self.iam.create_access_key(UserName=subject)
            key = response['AccessKey']
            return {
                'status': 'success',
                'message': f'Access key created for {subject}',
                'access_key': key['AccessKeyId'],
                'secret_key': key['SecretAccessKey'],
                'create_date': key['CreateDate'].isoformat(),
            }
        except Exception as e:
            return {
                'status': 'failure',
                'message': f'Failed to create access key: {e!s}',
            }

    def delete_access_keys(self, subject: str) -> dict[str, Any]:
        """Delete all access keys for a user.

        Args:
            subject: User subject ID

        Returns:
            Dictionary with status (success/failure) and message
        """
        try:
            with contextlib.suppress(
                self.iam.exceptions.NoSuchEntityException,
            ):
                keys = self.iam.list_access_keys(
                    UserName=subject,
                )['AccessKeyMetadata']
                for key in keys:
                    self.iam.delete_access_key(
                        UserName=subject,
                        AccessKeyId=key['AccessKeyId'],
                    )
            return {
                'status': 'success',
                'message': f'Access keys deleted for {subject}',
            }
        except Exception as e:
            return {
                'status': 'failure',
                'message': f'Failed to delete access keys: {e!s}',
            }


# Global namespaces tracking key
GLOBAL_NAMESPACES_KEY = '__global_namespaces__'


class DynamoDBService:
    """Service for managing DynamoDB operations."""

    def __init__(
        self,
        region: str,
        keys_table_name: str,
        users_table_name: str,
        namespace_table_name: str,
    ) -> None:
        """Initialize DynamoDB service.

        Args:
            region: AWS region
            keys_table_name: Table name for keys
            users_table_name: Table name for user records
            namespace_table_name: Table name for namespace/topic records
        """
        self.dynamodb = boto3.client('dynamodb', region_name=region)
        self.keys_table_name = keys_table_name
        self.users_table_name = users_table_name
        self.namespace_table_name = namespace_table_name

    # ========================================================================
    # Key Management
    # ========================================================================

    def store_key(
        self,
        subject: str,
        access_key: str,
        secret_key: str,
        create_date: str,
    ) -> None:
        """Store access key in DynamoDB.

        Args:
            subject: User subject ID
            access_key: Access key ID
            secret_key: Secret access key
            create_date: Creation date in ISO format
        """
        item = {
            'subject': {'S': subject},
            'access_key': {'S': access_key},
            'secret_key': {'S': secret_key},
            'create_date': {'S': create_date},
        }
        try:
            self.dynamodb.put_item(
                TableName=self.keys_table_name,
                Item=item,
            )
        except self.dynamodb.exceptions.ResourceNotFoundException:
            self._create_keys_table()
            self.dynamodb.put_item(
                TableName=self.keys_table_name,
                Item=item,
            )

    def get_key(self, subject: str) -> dict[str, str] | None:
        """Get access key from DynamoDB.

        Args:
            subject: User subject ID

        Returns:
            Dictionary with access_key, secret_key, and create_date if found,
            None otherwise
        """
        with contextlib.suppress(
            self.dynamodb.exceptions.ResourceNotFoundException,
        ):
            response = self.dynamodb.get_item(
                TableName=self.keys_table_name,
                Key={'subject': {'S': subject}},
            )
            if 'Item' in response:
                return {
                    'access_key': response['Item']['access_key']['S'],
                    'secret_key': response['Item']['secret_key']['S'],
                    'create_date': response['Item']['create_date']['S'],
                }
        return None

    def delete_key(self, subject: str) -> None:
        """Delete access key from DynamoDB.

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

    # ========================================================================
    # User Namespace Management
    # ========================================================================

    def get_user_namespaces(self, subject: str) -> list[str]:
        """Get user namespaces from DynamoDB.

        Args:
            subject: User subject ID

        Returns:
            List of namespace names (sorted), empty list if not found
        """
        with contextlib.suppress(
            self.dynamodb.exceptions.ResourceNotFoundException,
        ):
            response = self.dynamodb.get_item(
                TableName=self.users_table_name,
                Key={'subject': {'S': subject}},
            )
            if 'Item' in response and 'namespaces' in response['Item']:
                return sorted(response['Item']['namespaces']['SS'])
        return []

    def add_user_namespace(
        self,
        subject: str,
        namespace: str,
    ) -> None:
        """Atomically add a namespace to user namespaces in DynamoDB.

        This method uses DynamoDB's ADD operation on a String Set (SS)
        to atomically add a namespace. This prevents race conditions when
        multiple namespaces are added concurrently.

        Args:
            subject: User subject ID
            namespace: Namespace name to add
        """
        try:
            # Use ADD operation on a set - atomic and idempotent
            self.dynamodb.update_item(
                TableName=self.users_table_name,
                Key={'subject': {'S': subject}},
                UpdateExpression='ADD namespaces :namespace',
                ExpressionAttributeValues={
                    ':namespace': {'SS': [namespace]},
                },
            )
        except self.dynamodb.exceptions.ResourceNotFoundException:
            # Table doesn't exist, create it and retry
            self._create_users_table()
            self.dynamodb.update_item(
                TableName=self.users_table_name,
                Key={'subject': {'S': subject}},
                UpdateExpression='ADD namespaces :namespace',
                ExpressionAttributeValues={
                    ':namespace': {'SS': [namespace]},
                },
            )

    def remove_user_namespace(
        self,
        subject: str,
        namespace: str,
    ) -> None:
        """Atomically remove a namespace from user namespaces in DynamoDB.

        This method uses DynamoDB's DELETE operation on a String Set (SS)
        to atomically remove a namespace. This is fully atomic and prevents
        race conditions when multiple namespaces are removed concurrently.
        The operation is idempotent - deleting a non-existent namespace
        is a no-op.

        Args:
            subject: User subject ID
            namespace: Namespace name to remove
        """
        # Use DELETE operation on a set - fully atomic and idempotent
        with contextlib.suppress(
            self.dynamodb.exceptions.ResourceNotFoundException,
        ):
            self.dynamodb.update_item(
                TableName=self.users_table_name,
                Key={'subject': {'S': subject}},
                UpdateExpression='DELETE namespaces :namespace',
                ExpressionAttributeValues={
                    ':namespace': {'SS': [namespace]},
                },
            )

    # ========================================================================
    # Global Namespace Management
    # ========================================================================

    def get_global_namespaces(self) -> set[str]:
        """Get all globally registered namespace names.

        Returns:
            Set of namespace names
        """
        with contextlib.suppress(
            self.dynamodb.exceptions.ResourceNotFoundException,
        ):
            response = self.dynamodb.get_item(
                TableName=self.users_table_name,
                Key={'subject': {'S': GLOBAL_NAMESPACES_KEY}},
            )
            if 'Item' in response and 'namespaces' in response['Item']:
                return set(response['Item']['namespaces']['SS'])
        return set()

    def add_global_namespace(self, namespace: str) -> None:
        """Atomically add a namespace to global namespaces in DynamoDB.

        This method uses DynamoDB's ADD operation on a String Set (SS)
        to atomically add a namespace. This prevents race conditions when
        multiple namespaces are added concurrently.

        Args:
            namespace: Namespace name to add
        """
        try:
            # Use ADD operation on a set - atomic and idempotent
            self.dynamodb.update_item(
                TableName=self.users_table_name,
                Key={'subject': {'S': GLOBAL_NAMESPACES_KEY}},
                UpdateExpression='ADD namespaces :namespace',
                ExpressionAttributeValues={
                    ':namespace': {'SS': [namespace]},
                },
            )
        except self.dynamodb.exceptions.ResourceNotFoundException:
            # Table doesn't exist, create it and retry
            self._create_users_table()
            self.dynamodb.update_item(
                TableName=self.users_table_name,
                Key={'subject': {'S': GLOBAL_NAMESPACES_KEY}},
                UpdateExpression='ADD namespaces :namespace',
                ExpressionAttributeValues={
                    ':namespace': {'SS': [namespace]},
                },
            )

    def remove_global_namespace(self, namespace: str) -> None:
        """Atomically remove a namespace from global namespaces in DynamoDB.

        This method uses DynamoDB's DELETE operation on a String Set (SS)
        to atomically remove a namespace. This is fully atomic and prevents
        race conditions when multiple namespaces are removed concurrently.
        The operation is idempotent - deleting a non-existent namespace
        is a no-op.

        Args:
            namespace: Namespace name to remove
        """
        # Use DELETE operation on a set - fully atomic and idempotent
        with contextlib.suppress(
            self.dynamodb.exceptions.ResourceNotFoundException,
        ):
            self.dynamodb.update_item(
                TableName=self.users_table_name,
                Key={'subject': {'S': GLOBAL_NAMESPACES_KEY}},
                UpdateExpression='DELETE namespaces :namespace',
                ExpressionAttributeValues={
                    ':namespace': {'SS': [namespace]},
                },
            )

    # ========================================================================
    # Namespace Topics Management
    # ========================================================================

    def add_namespace_topic(
        self,
        namespace: str,
        topic: str,
    ) -> None:
        """Atomically add a topic to namespace topics in DynamoDB.

        This method uses DynamoDB's ADD operation on a String Set (SS)
        to atomically add a topic. This prevents race conditions when
        multiple topics are added concurrently.

        Args:
            namespace: Namespace name
            topic: Topic name to add
        """
        try:
            # Use ADD operation on a set - atomic and idempotent
            self.dynamodb.update_item(
                TableName=self.namespace_table_name,
                Key={'namespace': {'S': namespace}},
                UpdateExpression='ADD topics :topic',
                ExpressionAttributeValues={
                    ':topic': {'SS': [topic]},
                },
            )
        except self.dynamodb.exceptions.ResourceNotFoundException:
            # Table doesn't exist, create it and retry
            self._create_namespace_table()
            self.dynamodb.update_item(
                TableName=self.namespace_table_name,
                Key={'namespace': {'S': namespace}},
                UpdateExpression='ADD topics :topic',
                ExpressionAttributeValues={
                    ':topic': {'SS': [topic]},
                },
            )

    def get_namespace_topics(self, namespace: str) -> list[str]:
        """Get list of topics for a namespace from DynamoDB.

        Args:
            namespace: Namespace name

        Returns:
            List of topic names (sorted), empty list if not found
        """
        with contextlib.suppress(
            self.dynamodb.exceptions.ResourceNotFoundException,
        ):
            response = self.dynamodb.get_item(
                TableName=self.namespace_table_name,
                Key={'namespace': {'S': namespace}},
            )
            if 'Item' in response and 'topics' in response['Item']:
                return sorted(response['Item']['topics']['SS'])
        return []

    def remove_namespace_topic(
        self,
        namespace: str,
        topic: str,
    ) -> None:
        """Atomically remove a topic from namespace topics in DynamoDB.

        This method uses DynamoDB's DELETE operation on a String Set (SS)
        to atomically remove a topic. This is fully atomic and prevents
        race conditions when multiple topics are removed concurrently.
        The operation is idempotent - deleting a non-existent topic
        is a no-op.

        Args:
            namespace: Namespace name
            topic: Topic name to remove
        """
        # Use DELETE operation on a set - fully atomic and idempotent
        with contextlib.suppress(
            self.dynamodb.exceptions.ResourceNotFoundException,
        ):
            self.dynamodb.update_item(
                TableName=self.namespace_table_name,
                Key={'namespace': {'S': namespace}},
                UpdateExpression='DELETE topics :topic',
                ExpressionAttributeValues={
                    ':topic': {'SS': [topic]},
                },
            )

    # ========================================================================
    # Table Management
    # ========================================================================

    def _create_keys_table(self) -> None:
        """Create DynamoDB table for storing keys if it doesn't exist."""
        with contextlib.suppress(
            self.dynamodb.exceptions.ResourceInUseException,
        ):
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
            self.dynamodb.get_waiter('table_exists').wait(
                TableName=self.keys_table_name,
            )

    def _create_users_table(self) -> None:
        """Create DynamoDB table for user records if it doesn't exist."""
        with contextlib.suppress(
            self.dynamodb.exceptions.ResourceInUseException,
        ):
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
            self.dynamodb.get_waiter('table_exists').wait(
                TableName=self.users_table_name,
            )

    def _create_namespace_table(self) -> None:
        """Create DynamoDB table for namespace/topic records."""
        with contextlib.suppress(
            self.dynamodb.exceptions.ResourceInUseException,
        ):
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


class MSKTokenProvider(AbstractTokenProvider):
    """Provide tokens for MSK authentication."""

    def __init__(self, region: str) -> None:
        """Initialize with AWS region.

        Args:
            region: AWS region
        """
        self.region = region

    def token(self) -> str:
        """Generate and return an MSK auth token.

        Returns:
            MSK authentication token
        """
        token, _ = MSKAuthTokenProvider.generate_auth_token(self.region)
        return token


class KafkaService:
    """Service for managing Kafka/MSK operations."""

    def __init__(
        self,
        bootstrap_servers: str | None,
        region: str,
    ) -> None:
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
            Dictionary with status (success/failure) and message,
            or None if Kafka endpoint is not configured.

        The actual Kafka topic name will be '{namespace}.{topic}'.
        """
        if not self.bootstrap_servers:
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
                return {
                    'status': 'success',
                    'message': 'Topic created',
                }
            except TopicAlreadyExistsError:
                return {
                    'status': 'success',
                    'message': 'Topic already exists',
                }
            except Exception as e:
                last_exception = e
                if attempt == max_retries - 1:
                    return {
                        'status': 'failure',
                        'message': (
                            f'Failed to create Kafka topic '
                            f'{kafka_topic_name} after {max_retries} '
                            f'attempts: {e!s}'
                        ),
                    }
                continue

        return {
            'status': 'failure',
            'message': (
                f'Failed to create Kafka topic {kafka_topic_name}: '
                f'{str(last_exception) if last_exception else "Unknown error"}'
            ),
        }

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
            Dictionary with status (success/failure) and message,
            or None if Kafka endpoint is not configured.

        The actual Kafka topic name will be '{namespace}.{topic}'.
        """
        if not self.bootstrap_servers:
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
                return {
                    'status': 'success',
                    'message': 'Topic deleted',
                }
            except UnknownTopicOrPartitionError:
                return {
                    'status': 'success',
                    'message': 'Topic does not exist',
                }
            except Exception as e:
                last_exception = e
                if attempt == max_retries - 1:
                    return {
                        'status': 'failure',
                        'message': (
                            f'Failed to delete Kafka topic '
                            f'{kafka_topic_name} after {max_retries} '
                            f'attempts: {e!s}'
                        ),
                    }
                continue

        return {
            'status': 'failure',
            'message': (
                f'Failed to delete Kafka topic {kafka_topic_name}: '
                f'{str(last_exception) if last_exception else "Unknown error"}'
            ),
        }

    def recreate_topic(
        self,
        namespace: str,
        topic: str,
    ) -> dict[str, Any] | None:
        """Recreate a Kafka topic by deleting and recreating it.

        This method calls delete_topic (idempotent) and then
        create_topic (idempotent).

        Args:
            namespace: Namespace name
            topic: Topic name

        Returns:
            Dictionary with status (success/failure) and message,
            or None if Kafka endpoint is not configured.
        """
        if not self.bootstrap_servers:
            return None

        # Delete topic (idempotent)
        delete_result = self.delete_topic(namespace, topic)
        if delete_result and delete_result['status'] == 'failure':
            return delete_result

        # Wait 5 seconds before recreating
        time.sleep(5)

        # Create topic (idempotent)
        create_result = self.create_topic(namespace, topic)
        if create_result and create_result['status'] == 'failure':
            return create_result

        return {
            'status': 'success',
            'message': 'Topic recreated',
        }


# Namespace validation constants
MIN_NAMESPACE_LENGTH = 3
MAX_NAMESPACE_LENGTH = 32


class NamespaceService:
    """Service for managing namespace validation and business logic."""

    def __init__(self, dynamodb_service: DynamoDBService) -> None:
        """Initialize namespace service.

        Args:
            dynamodb_service: DynamoDB service instance
        """
        self.dynamodb = dynamodb_service
        # Per-namespace locks for atomic create_namespace() operations
        self._namespace_creation_locks: dict[str, threading.Lock] = {}
        self._locks_lock = threading.Lock()  # Protects the locks dictionary

    @classmethod
    def validate_name(cls, name: str) -> dict[str, str] | None:
        """Validate namespace or topic name according to rules.

        Rules:
        - 3-32 characters
        - Only lowercase, uppercase, numbers, dash, underscore
        - Cannot start or end with hyphen or underscore

        Args:
            name: Namespace or topic name to validate

        Returns:
            None if valid, or dict with status 'failure' and message if invalid
        """
        if not (MIN_NAMESPACE_LENGTH <= len(name) <= MAX_NAMESPACE_LENGTH):
            return {
                'status': 'failure',
                'message': (
                    f'Name must be between {MIN_NAMESPACE_LENGTH} '
                    f'and {MAX_NAMESPACE_LENGTH} characters'
                ),
            }
        if not re.match(r'^[a-zA-Z0-9_-]+$', name):
            return {
                'status': 'failure',
                'message': (
                    'Name can only contain letters, numbers, '
                    'dash, and underscore'
                ),
            }
        if name[0] in ('-', '_') or name[-1] in ('-', '_'):
            return {
                'status': 'failure',
                'message': (
                    'Name cannot start or end with hyphen or underscore'
                ),
            }
        return None

    def generate_default(self, subject: str) -> str:
        """Generate default namespace name from subject.

        Args:
            subject: User subject ID

        Returns:
            Default namespace name
        """
        return f'ns-{subject.replace("-", "")[-12:]}'

    def _get_namespace_lock(self, namespace: str) -> threading.Lock:
        """Get or create a lock for a specific namespace.

        Args:
            namespace: Namespace name

        Returns:
            Lock object for the namespace
        """
        # Double-checked locking pattern for thread-safe lock creation
        if namespace not in self._namespace_creation_locks:
            with self._locks_lock:
                if namespace not in self._namespace_creation_locks:
                    self._namespace_creation_locks[namespace] = (
                        threading.Lock()
                    )
        return self._namespace_creation_locks[namespace]

    def create_namespace(
        self,
        subject: str,
        namespace: str,
    ) -> dict[str, Any]:
        """Create a namespace for a user (idempotent).

        Namespace names must be globally unique.
        Safe to call multiple times with the same parameters.

        This method is atomic per namespace - concurrent calls for the same
        namespace are serialized using a per-namespace lock.

        Args:
            subject: User subject ID
            namespace: Namespace name to create

        Returns:
            Dictionary with status, message, and namespaces list (on success)
            or status 'failure' with message (if namespace already taken)
        """
        validation_error = self.validate_name(namespace)
        if validation_error:
            return validation_error

        # Get per-namespace lock to ensure atomicity
        lock = self._get_namespace_lock(namespace)
        with lock:
            existing_namespaces = self.dynamodb.get_user_namespaces(subject)

            # If namespace already exists for this user, return success
            # (idempotent)
            if namespace in existing_namespaces:
                return {
                    'status': 'success',
                    'message': f'Namespace already exists for {subject}',
                    'namespaces': existing_namespaces,
                }

            # Check if namespace is taken by another user
            global_namespaces = self.dynamodb.get_global_namespaces()
            if namespace in global_namespaces:
                return {
                    'status': 'failure',
                    'message': f'Namespace already taken: {namespace}',
                }

            # Atomically add namespace to global and user lists
            # This prevents race conditions when multiple calls happen
            # concurrently
            self.dynamodb.add_global_namespace(namespace)
            self.dynamodb.add_user_namespace(subject, namespace)

            # Read back the updated list (already sorted and deduplicated)
            all_namespaces = self.dynamodb.get_user_namespaces(subject)

            return {
                'status': 'success',
                'message': f'Namespace created for {subject}',
                'namespaces': all_namespaces,
            }

    def delete_namespace(
        self,
        subject: str,
        namespace: str,
    ) -> dict[str, Any]:
        """Delete a namespace for a user (idempotent).

        Safe to call multiple times with the same parameters.

        Args:
            subject: User subject ID
            namespace: Namespace name to delete

        Returns:
            Dictionary with status, message, and remaining namespaces list.
            Returns success message even if namespace doesn't exist.
        """
        existing_namespaces = self.dynamodb.get_user_namespaces(subject)

        # If namespace doesn't exist, return success (idempotent)
        if namespace not in existing_namespaces:
            return {
                'status': 'success',
                'message': f'Namespace {namespace} not found for {subject}',
                'namespaces': existing_namespaces,
            }

        # Atomically remove namespace from user and global lists
        # These methods handle the RMW pattern internally
        self.dynamodb.remove_user_namespace(subject, namespace)
        self.dynamodb.remove_global_namespace(namespace)

        # Read back the updated list to return
        remaining_namespaces = self.dynamodb.get_user_namespaces(subject)

        return {
            'status': 'success',
            'message': f'Namespace deleted for {subject}',
            'namespaces': remaining_namespaces,
        }

    def create_topic(
        self,
        subject: str,
        namespace: str,
        topic: str,
    ) -> dict[str, Any]:
        """Create a topic in a namespace (idempotent).

        Safe to call multiple times with the same parameters.

        Args:
            subject: User subject ID
            namespace: Namespace name
            topic: Topic name to create

        Returns:
            Dictionary with status, message, and topics list (on success)
            or status 'failure' with message
        """
        # Validate topic name
        validation_error = self.validate_name(topic)
        if validation_error:
            return validation_error

        # Check that namespace exists for this user
        user_namespaces = self.dynamodb.get_user_namespaces(subject)
        if namespace not in user_namespaces:
            return {
                'status': 'failure',
                'message': f'Namespace {namespace} not found for {subject}',
            }

        # Get existing topics for the namespace (for idempotency check)
        existing_topics = self.dynamodb.get_namespace_topics(namespace)

        # If topic already exists, return success (idempotent)
        if topic in existing_topics:
            return {
                'status': 'success',
                'message': f'Topic {topic} already exists in {namespace}',
                'topics': existing_topics,
            }

        # Atomically add topic to the list (prevents race conditions)
        self.dynamodb.add_namespace_topic(namespace, topic)

        # Read back the updated list
        # (already deduplicated by get_namespace_topics)
        all_topics = self.dynamodb.get_namespace_topics(namespace)

        return {
            'status': 'success',
            'message': f'Topic {topic} created in {namespace}',
            'topics': all_topics,
        }

    def delete_topic(
        self,
        subject: str,
        namespace: str,
        topic: str,
    ) -> dict[str, Any]:
        """Delete a topic from a namespace (idempotent).

        Safe to call multiple times with the same parameters.

        Args:
            subject: User subject ID
            namespace: Namespace name
            topic: Topic name to delete

        Returns:
            Dictionary with status, message, and remaining topics list.
            Returns success message even if topic doesn't exist.
        """
        # Check that namespace exists for this user
        user_namespaces = self.dynamodb.get_user_namespaces(subject)
        if namespace not in user_namespaces:
            return {
                'status': 'failure',
                'message': f'Namespace {namespace} not found for {subject}',
            }

        # Get existing topics for the namespace
        existing_topics = self.dynamodb.get_namespace_topics(namespace)

        # If topic doesn't exist, return success (idempotent)
        if topic not in existing_topics:
            return {
                'status': 'success',
                'message': f'Topic {topic} not found in {namespace}',
                'topics': existing_topics,
            }

        # Atomically remove topic from the set
        self.dynamodb.remove_namespace_topic(namespace, topic)

        # Read back the updated list
        remaining_topics = self.dynamodb.get_namespace_topics(namespace)

        return {
            'status': 'success',
            'message': f'Topic {topic} deleted from {namespace}',
            'topics': remaining_topics,
        }

    def list_namespace_and_topics(
        self,
        subject: str,
    ) -> dict[str, Any]:
        """List all namespaces and their topics for a user.

        Args:
            subject: User subject ID

        Returns:
            Dictionary with status, message, and a dict mapping
            namespace names to their topic lists
        """
        user_namespaces = self.dynamodb.get_user_namespaces(subject)

        namespace_topics: dict[str, list[str]] = {}
        for namespace in user_namespaces:
            topics = self.dynamodb.get_namespace_topics(namespace)
            namespace_topics[namespace] = topics

        return {
            'status': 'success',
            'message': (
                f'Found {len(user_namespaces)} namespaces for {subject}'
            ),
            'namespaces': namespace_topics,
        }


class WebService:
    """Service for managing user lifecycle with IAM, Kafka, and Namespace."""

    def __init__(
        self,
        iam_service: IAMService,
        kafka_service: KafkaService,
        namespace_service: NamespaceService,
    ) -> None:
        """Initialize WebService.

        Args:
            iam_service: IAM service instance
            kafka_service: Kafka service instance
            namespace_service: Namespace service instance
        """
        self.iam_service = iam_service
        self.kafka_service = kafka_service
        self.namespace_service = namespace_service
        self.bootstrap_servers = kafka_service.bootstrap_servers
        # Per-subject locks for atomic create_key() operations
        self._key_creation_locks: dict[str, threading.Lock] = {}
        self._locks_lock = threading.Lock()  # Protects the locks dictionary

    def create_user(self, subject: str) -> dict[str, Any]:
        """Create a user in IAM and create its default namespace.

        Args:
            subject: User subject ID

        Returns:
            Dictionary with status, message, and user/namespace information
        """
        # Create user in IAM
        iam_result = self.iam_service.create_user_and_policy(subject)
        if iam_result['status'] != 'success':
            return {
                'status': 'failure',
                'message': (
                    f'Failed to create IAM user: {iam_result["message"]}'
                ),
            }

        # Generate default namespace
        namespace = self.namespace_service.generate_default(subject)

        # Create namespace
        namespace_result = self.namespace_service.create_namespace(
            subject,
            namespace,
        )
        if namespace_result['status'] != 'success':
            # If namespace creation fails, try to clean up IAM user
            with contextlib.suppress(Exception):
                self.iam_service.delete_user_and_policy(subject)
            return {
                'status': 'failure',
                'message': (
                    f'Failed to create namespace: '
                    f'{namespace_result["message"]}'
                ),
            }

        return {
            'status': 'success',
            'message': (f'User {subject} created with namespace {namespace}'),
            'subject': subject,
            'namespace': namespace,
        }

    def delete_user(self, subject: str) -> dict[str, Any]:
        """Delete a user from IAM and its namespace.

        If the user has namespaces beyond the default one, deletion is not
        allowed. Otherwise, deletes the default namespace, cached DynamoDB
        keys, and IAM user.

        This method is atomic per subject - concurrent calls for the same
        subject are serialized using a per-subject lock.

        Args:
            subject: User subject ID

        Returns:
            Dictionary with status and message
        """
        # Get per-subject lock to ensure atomicity
        lock = self._get_subject_lock(subject)
        with lock:
            # Check if user has any namespaces
            user_namespaces = (
                self.namespace_service.dynamodb.get_user_namespaces(
                    subject,
                )
            )

            # Get default namespace
            default_namespace = self.namespace_service.generate_default(
                subject,
            )

            # Check if user has namespaces other than the default one
            other_namespaces = [
                ns for ns in user_namespaces if ns != default_namespace
            ]

            if other_namespaces:
                return {
                    'status': 'failure',
                    'message': (
                        f'Cannot delete user {subject}: '
                        f'user has {len(other_namespaces)} additional '
                        f'namespace(s). Delete namespaces first.'
                    ),
                }

            # Delete all topics under the default namespace
            topics = self.namespace_service.dynamodb.get_namespace_topics(
                default_namespace,
            )
            for topic in topics:
                # Delete Kafka topic (idempotent)
                with contextlib.suppress(Exception):
                    self.kafka_service.delete_topic(default_namespace, topic)
                # Delete topic from DynamoDB (idempotent)
                self.namespace_service.delete_topic(
                    subject,
                    default_namespace,
                    topic,
                )

            # Delete default namespace (idempotent, safe even if doesn't exist)
            self.namespace_service.delete_namespace(
                subject,
                default_namespace,
            )

            # Delete cached key from DynamoDB
            # (idempotent, safe even if doesn't exist)
            self.namespace_service.dynamodb.delete_key(subject)

            # Delete user in IAM
            iam_result = self.iam_service.delete_user_and_policy(subject)
            if iam_result['status'] != 'success':
                return {
                    'status': 'failure',
                    'message': (
                        f'Failed to delete IAM user: '
                        f'{iam_result["message"]}'
                    ),
                }

            return {
                'status': 'success',
                'message': f'User {subject} deleted',
            }

    def _get_subject_lock(self, subject: str) -> threading.Lock:
        """Get or create a lock for a specific subject.

        Args:
            subject: User subject ID

        Returns:
            Lock object for the subject
        """
        # Double-checked locking pattern for thread-safe lock creation
        if subject not in self._key_creation_locks:
            with self._locks_lock:
                if subject not in self._key_creation_locks:
                    self._key_creation_locks[subject] = threading.Lock()
        return self._key_creation_locks[subject]

    def create_key(self, subject: str) -> dict[str, Any]:
        """Create an access key for a user.

        Creates user and namespace if they don't exist, then creates
        a new access key and stores it. If a key already exists in DynamoDB,
        returns it without creating a new one (idempotent behavior).

        This method is atomic per subject - concurrent calls for the same
        subject are serialized using a per-subject lock.

        Args:
            subject: User subject ID

        Returns:
            Dictionary with status, message, and key information
            (access_key, secret_key, create_date, endpoint, fresh)
            where fresh=False if key existed in DynamoDB,
            fresh=True if newly created
        """
        # Get per-subject lock to ensure atomicity
        lock = self._get_subject_lock(subject)
        with lock:
            # Check if key already exists in DynamoDB (fast path)
            existing_key = self.namespace_service.dynamodb.get_key(subject)
            if existing_key:
                return {
                    'status': 'success',
                    'message': f'Access key already exists for {subject}',
                    'access_key': existing_key['access_key'],
                    'secret_key': existing_key['secret_key'],
                    'create_date': existing_key['create_date'],
                    'endpoint': self.bootstrap_servers or '',
                    'fresh': False,
                }

            # Ensure user and namespace exist (idempotent)
            user_result = self.create_user(subject)
            if user_result['status'] != 'success':
                return {
                    'status': 'failure',
                    'message': (
                        f'Failed to create user: {user_result["message"]}'
                    ),
                }

            # Delete existing access keys in IAM before creating new one
            self.iam_service.delete_access_keys(subject)

            # Create new access key in IAM
            iam_key_result = self.iam_service.create_access_key(subject)
            if iam_key_result['status'] != 'success':
                return {
                    'status': 'failure',
                    'message': (
                        f'Failed to create IAM access key: '
                        f'{iam_key_result["message"]}'
                    ),
                }

            # Store key in DynamoDB (overwrites existing)
            self.namespace_service.dynamodb.store_key(
                subject=subject,
                access_key=iam_key_result['access_key'],
                secret_key=iam_key_result['secret_key'],
                create_date=iam_key_result['create_date'],
            )

            return {
                'status': 'success',
                'message': f'Access key created for {subject}',
                'access_key': iam_key_result['access_key'],
                'secret_key': iam_key_result['secret_key'],
                'create_date': iam_key_result['create_date'],
                'endpoint': self.bootstrap_servers or '',
                'fresh': True,
            }

    def delete_key(self, subject: str) -> dict[str, Any]:
        """Delete an access key for a user.

        Deletes access key from both IAM and DynamoDB.

        Args:
            subject: User subject ID

        Returns:
            Dictionary with status and message
        """
        # Delete access keys in IAM (idempotent)
        self.iam_service.delete_access_keys(subject)

        # Delete key from DynamoDB (idempotent)
        self.namespace_service.dynamodb.delete_key(subject)

        return {
            'status': 'success',
            'message': f'Access key deleted for {subject}',
        }

    def create_topic(
        self,
        subject: str,
        namespace: str,
        topic: str,
    ) -> dict[str, Any]:
        """Create a topic in a namespace.

        Creates the topic in both NamespaceService (DynamoDB) and
        KafkaService (Kafka cluster).

        Args:
            subject: User subject ID
            namespace: Namespace name
            topic: Topic name to create

        Returns:
            Dictionary with status, message, and topics list
        """
        # Create topic in NamespaceService (DynamoDB)
        namespace_result = self.namespace_service.create_topic(
            subject,
            namespace,
            topic,
        )
        if namespace_result['status'] != 'success':
            return namespace_result

        # Create topic in KafkaService (Kafka cluster)
        kafka_result = self.kafka_service.create_topic(namespace, topic)
        if kafka_result and kafka_result.get('status') == 'failure':
            # If Kafka creation fails, try to clean up DynamoDB entry
            with contextlib.suppress(Exception):
                self.namespace_service.delete_topic(subject, namespace, topic)
            return {
                'status': 'failure',
                'message': (
                    f'Failed to create Kafka topic: '
                    f'{kafka_result.get("message", "Unknown error")}'
                ),
            }

        # Return namespace result (includes topics list)
        return namespace_result

    def delete_topic(
        self,
        subject: str,
        namespace: str,
        topic: str,
    ) -> dict[str, Any]:
        """Delete a topic from a namespace.

        Deletes the topic from both KafkaService (Kafka cluster) and
        NamespaceService (DynamoDB).

        Args:
            subject: User subject ID
            namespace: Namespace name
            topic: Topic name to delete

        Returns:
            Dictionary with status, message, and remaining topics list
        """
        # Delete topic from KafkaService (Kafka cluster) first
        kafka_result = self.kafka_service.delete_topic(namespace, topic)
        # Note: kafka_result can be None if Kafka is not configured,
        # which is acceptable

        # Delete topic from NamespaceService (DynamoDB)
        namespace_result = self.namespace_service.delete_topic(
            subject,
            namespace,
            topic,
        )

        # If Kafka deletion failed and Kafka is configured, return failure
        if (
            kafka_result is not None
            and kafka_result.get('status') == 'failure'
        ):
            return {
                'status': 'failure',
                'message': (
                    f'Failed to delete Kafka topic: '
                    f'{kafka_result.get("message", "Unknown error")}'
                ),
            }

        # Return namespace result (includes remaining topics list)
        return namespace_result

    def recreate_topic(
        self,
        subject: str,
        namespace: str,
        topic: str,
    ) -> dict[str, Any]:
        """Recreate a topic in a namespace.

        Calls KafkaService's recreate_topic to delete and recreate
        the Kafka topic. The topic must already exist in DynamoDB.

        Args:
            subject: User subject ID
            namespace: Namespace name
            topic: Topic name to recreate

        Returns:
            Dictionary with status and message from KafkaService
        """
        # Verify namespace exists for this user
        user_namespaces = self.namespace_service.dynamodb.get_user_namespaces(
            subject,
        )
        if namespace not in user_namespaces:
            return {
                'status': 'failure',
                'message': f'Namespace {namespace} not found for {subject}',
            }

        # Verify topic exists in DynamoDB
        existing_topics = self.namespace_service.dynamodb.get_namespace_topics(
            namespace,
        )
        if topic not in existing_topics:
            return {
                'status': 'failure',
                'message': f'Topic {topic} not found in {namespace}',
            }

        # Call KafkaService's recreate_topic
        kafka_result = self.kafka_service.recreate_topic(namespace, topic)
        if kafka_result is None:
            return {
                'status': 'failure',
                'message': 'Kafka endpoint is not configured',
            }

        return kafka_result


def main() -> None:
    """Main method to delete a specific user."""
    # Check required environment variables
    EnvironmentChecker.check_env_variables(
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY',
        'SERVER_CLIENT_ID',
        'SERVER_SECRET',
        'AWS_ACCOUNT_ID',
        'AWS_ACCOUNT_REGION',
        'MSK_CLUSTER_NAME',
    )

    # Initialize services
    region = os.getenv('AWS_ACCOUNT_REGION') or ''
    iam_service = IAMService(
        account_id=os.getenv('AWS_ACCOUNT_ID') or '',
        region=region,
        cluster_name=os.getenv('MSK_CLUSTER_NAME') or '',
    )
    kafka_service = KafkaService(
        bootstrap_servers=os.getenv('DEFAULT_SERVERS'),
        region=region,
    )
    db_service = DynamoDBService(
        region=region,
        keys_table_name=os.getenv('KEYS_TABLE_NAME', 'diaspora-keys-table'),
        users_table_name=os.getenv('USERS_TABLE_NAME', 'diaspora-users-table'),
        namespace_table_name=os.getenv(
            'NAMESPACE_TABLE_NAME',
            'diaspora-namespace-table',
        ),
    )
    namespace_service = NamespaceService(dynamodb_service=db_service)
    web_service = WebService(
        iam_service=iam_service,
        kafka_service=kafka_service,
        namespace_service=namespace_service,
    )

    # Delete the specified user
    subject = ''
    print(f'Deleting user: {subject}')
    result = web_service.delete_user(subject)
    print(json.dumps(result, indent=2, default=str))


if __name__ == '__main__':
    main()
