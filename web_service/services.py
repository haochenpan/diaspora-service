"""Service classes for AWS resource management."""

from __future__ import annotations

import contextlib
import json
import logging
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

# The admin client's own request timeout, and the time the controller may
# take to finish a topic creation or deletion before it answers. The second
# is shorter so the controller answers before the client gives up.
ADMIN_REQUEST_TIMEOUT_MS = 15_000
ADMIN_OPERATION_TIMEOUT_MS = 10_000
# Wait before retrying a create that found the topic still present
TOPIC_EXISTS_RETRY_DELAY_S = 2
# The most topics a namespace may hold when MAX_TOPICS_PER_NAMESPACE is
# unset or invalid
DEFAULT_MAX_TOPICS_PER_NAMESPACE = 50

logger = logging.getLogger(__name__)


def parse_max_topics_per_namespace(value: str | None) -> int:
    """Parse the MAX_TOPICS_PER_NAMESPACE setting.

    Args:
        value: The variable's value, or None if it is unset

    Returns:
        The value as an integer if it is a whole number of at least 1,
        otherwise DEFAULT_MAX_TOPICS_PER_NAMESPACE (logged unless unset)
    """
    if value is None:
        return DEFAULT_MAX_TOPICS_PER_NAMESPACE
    try:
        parsed = int(value)
    except ValueError:
        parsed = 0
    if parsed < 1:
        logger.warning(
            'MAX_TOPICS_PER_NAMESPACE=%r is not a whole number of at '
            'least 1; using %d',
            value,
            DEFAULT_MAX_TOPICS_PER_NAMESPACE,
        )
        return DEFAULT_MAX_TOPICS_PER_NAMESPACE
    return parsed


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
            keys: list[dict[str, Any]] = []
            with contextlib.suppress(
                self.iam.exceptions.NoSuchEntityException,
            ):
                keys = self.iam.list_access_keys(
                    UserName=subject,
                )['AccessKeyMetadata']
            for key in keys:
                # A key that is already gone doesn't stop the others
                with contextlib.suppress(
                    self.iam.exceptions.NoSuchEntityException,
                ):
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

    def delete_namespace_topics(self, namespace: str) -> None:
        """Delete a namespace's topics item from DynamoDB (idempotent).

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

    def _admin_client(self) -> KafkaAdminClient:
        """Build an admin client whose requests end within a bound."""
        return KafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers,
            security_protocol='SASL_SSL',
            sasl_mechanism='OAUTHBEARER',
            sasl_oauth_token_provider=MSKTokenProvider(self.region),
            request_timeout_ms=ADMIN_REQUEST_TIMEOUT_MS,
        )

    def create_topic(
        self,
        namespace: str,
        topic: str,
        *,
        exists_ok: bool = True,
    ) -> dict[str, Any] | None:
        """Create a Kafka topic (idempotent unless exists_ok is False).

        Args:
            namespace: Namespace name
            topic: Topic name
            exists_ok: If True, a topic that already exists counts as
                success. If False, the create is retried after a short
                wait and fails if the topic still exists after the last
                attempt, unless an earlier attempt of this call failed
                otherwise (it may have created the topic before failing).

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

        # Whether an earlier attempt failed other than with the topic
        # existing, for example a timeout after the broker created it
        failed_otherwise = False

        for attempt in range(max_retries):
            admin: KafkaAdminClient | None = None
            topic_exists = False
            try:
                admin = self._admin_client()
                topic_list = [
                    NewTopic(
                        name=kafka_topic_name,
                        num_partitions=1,
                        replication_factor=2,
                    ),
                ]
                admin.create_topics(
                    new_topics=topic_list,
                    timeout_ms=ADMIN_OPERATION_TIMEOUT_MS,
                )
                return {
                    'status': 'success',
                    'message': 'Topic created',
                }
            except TopicAlreadyExistsError as e:
                if exists_ok or failed_otherwise:
                    return {
                        'status': 'success',
                        'message': 'Topic already exists',
                    }
                last_exception = e
                topic_exists = True
            except Exception as e:
                last_exception = e
                failed_otherwise = True
            finally:
                if admin is not None:
                    with contextlib.suppress(Exception):
                        admin.close()

            if topic_exists and attempt < max_retries - 1:
                time.sleep(TOPIC_EXISTS_RETRY_DELAY_S)

        return {
            'status': 'failure',
            'message': (
                f'Failed to create Kafka topic '
                f'{kafka_topic_name} after {max_retries} '
                f'attempts: {last_exception!s}'
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

        for _ in range(max_retries):
            admin: KafkaAdminClient | None = None
            try:
                admin = self._admin_client()
                admin.delete_topics(
                    topics=[kafka_topic_name],
                    timeout_ms=ADMIN_OPERATION_TIMEOUT_MS,
                )
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
            finally:
                if admin is not None:
                    with contextlib.suppress(Exception):
                        admin.close()

        return {
            'status': 'failure',
            'message': (
                f'Failed to delete Kafka topic '
                f'{kafka_topic_name} after {max_retries} '
                f'attempts: {last_exception!s}'
            ),
        }

    def recreate_topic(
        self,
        namespace: str,
        topic: str,
    ) -> dict[str, Any] | None:
        """Recreate a Kafka topic by deleting and recreating it.

        This method calls delete_topic (idempotent) and then
        create_topic. A topic that still exists when it is created again
        (for example while its deletion is still in progress) is retried
        after a short wait, and is a failure if it persists.

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

        # Create topic; one that still exists is not a success here
        create_result = self.create_topic(namespace, topic, exists_ok=False)
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

    def __init__(
        self,
        dynamodb_service: DynamoDBService,
        max_topics_per_namespace: int = DEFAULT_MAX_TOPICS_PER_NAMESPACE,
    ) -> None:
        """Initialize namespace service.

        Args:
            dynamodb_service: DynamoDB service instance
            max_topics_per_namespace: The most topics a namespace may hold;
                a create that would go past it is refused
        """
        self.dynamodb = dynamodb_service
        self.max_topics_per_namespace = max_topics_per_namespace
        # A create that leaves the namespace with at least this many topics
        # (90% of the cap, rounded up) warns that the cap is near
        self.topic_warning_threshold = (9 * max_topics_per_namespace + 9) // 10
        # Global lock for all namespace and topic operations
        self._global_lock = threading.Lock()

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

    def create_namespace(
        self,
        subject: str,
        namespace: str,
    ) -> dict[str, Any]:
        """Create a namespace for a user (idempotent).

        Namespace names must be globally unique.
        Safe to call multiple times with the same parameters.

        This method is atomic - concurrent calls are serialized using a
        global lock.

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

        # Use global lock to ensure atomicity
        with self._global_lock:
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

        This method is atomic - concurrent calls, and topic changes, are
        serialized using a global lock.

        Args:
            subject: User subject ID
            namespace: Namespace name to delete

        Returns:
            Dictionary with status, message, and remaining namespaces list.
            Returns success message even if namespace doesn't exist.
        """
        # Use global lock so no topic change runs during the delete
        with self._global_lock:
            existing_namespaces = self.dynamodb.get_user_namespaces(subject)

            # If namespace doesn't exist, return success (idempotent)
            if namespace not in existing_namespaces:
                return {
                    'status': 'success',
                    'message': (
                        f'Namespace {namespace} not found for {subject}'
                    ),
                    'namespaces': existing_namespaces,
                }

            # Drop the namespace's topics item first, so a later owner of
            # the name does not inherit the old topic list, then remove the
            # namespace from the user's and the global lists; a failure
            # dropping the topics item leaves the namespace owned, so a
            # retry deletes it again
            self.dynamodb.delete_namespace_topics(namespace)
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

        This method is atomic - concurrent calls are serialized using a
        global lock.

        A namespace holds at most max_topics_per_namespace topics: a new
        topic in a namespace that already lists that many is refused. A
        create that leaves the namespace with at least 90% of the cap
        appends a warning to the message and returns it in a 'warning'
        field. Repeating the create of a listed topic is not counted.

        Args:
            subject: User subject ID
            namespace: Namespace name
            topic: Topic name to create

        Returns:
            Dictionary with status, message, topics list and, near the
            cap, warning (on success), or status 'failure' with message
        """
        result, _ = self.add_topic(subject, namespace, topic)
        return result

    def add_topic(
        self,
        subject: str,
        namespace: str,
        topic: str,
    ) -> tuple[dict[str, Any], bool]:
        """Create a topic in a namespace and say whether this call added it.

        Behaves as create_topic.

        Args:
            subject: User subject ID
            namespace: Namespace name
            topic: Topic name to create

        Returns:
            The create_topic result, and True only if this call added the
            topic to the namespace's topics
        """
        # Validate topic name
        validation_error = self.validate_name(topic)
        if validation_error:
            return validation_error, False

        # Use global lock to ensure atomicity
        with self._global_lock:
            # Check that namespace exists for this user
            user_namespaces = self.dynamodb.get_user_namespaces(subject)
            if namespace not in user_namespaces:
                return {
                    'status': 'failure',
                    'message': (
                        f'Namespace {namespace} not found for {subject}'
                    ),
                }, False

            # Get existing topics for the namespace (for idempotency check)
            existing_topics = self.dynamodb.get_namespace_topics(namespace)

            # If topic already exists, return success (idempotent)
            if topic in existing_topics:
                return {
                    'status': 'success',
                    'message': f'Topic {topic} already exists in {namespace}',
                    'topics': existing_topics,
                }, False

            # Refuse a new topic once the namespace holds the most allowed;
            # a namespace already past the cap keeps its topics
            cap = self.max_topics_per_namespace
            if len(existing_topics) >= cap:
                return {
                    'status': 'failure',
                    'message': (
                        f'Namespace {namespace} already holds '
                        f'{len(existing_topics)} topics, the most allowed '
                        f'is {cap}; delete a topic to create another.'
                    ),
                }, False

            # Atomically add topic to the list (prevents race conditions)
            self.dynamodb.add_namespace_topic(namespace, topic)

            # Read back the updated list
            # (already deduplicated by get_namespace_topics)
            all_topics = self.dynamodb.get_namespace_topics(namespace)

            result: dict[str, Any] = {
                'status': 'success',
                'message': f'Topic {topic} created in {namespace}',
                'topics': all_topics,
            }
            if len(all_topics) >= self.topic_warning_threshold:
                warning = (
                    f'Namespace {namespace} now holds {len(all_topics)} of '
                    f'{cap} topics; delete old topics before you reach '
                    f'{cap}.'
                )
                result['message'] += f' {warning}'
                result['warning'] = warning
            return result, True

    def delete_topic(
        self,
        subject: str,
        namespace: str,
        topic: str,
    ) -> dict[str, Any]:
        """Delete a topic from a namespace (idempotent).

        Safe to call multiple times with the same parameters.

        This method is atomic - concurrent calls are serialized using a
        global lock.

        Args:
            subject: User subject ID
            namespace: Namespace name
            topic: Topic name to delete

        Returns:
            Dictionary with status, message, and remaining topics list.
            Returns success message even if topic doesn't exist.
        """
        # Use global lock to ensure atomicity
        with self._global_lock:
            # Check that namespace exists for this user
            user_namespaces = self.dynamodb.get_user_namespaces(subject)
            if namespace not in user_namespaces:
                return {
                    'status': 'failure',
                    'message': (
                        f'Namespace {namespace} not found for {subject}'
                    ),
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
        # Global lock for all user operations
        self._global_lock = threading.Lock()

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

        This method is atomic - concurrent calls are serialized using a
        global lock.

        Args:
            subject: User subject ID

        Returns:
            Dictionary with status and message
        """
        # Use global lock to ensure atomicity
        with self._global_lock:
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

            # Delete all topics under the default namespace, only if the
            # subject owns it
            topics = (
                self.namespace_service.dynamodb.get_namespace_topics(
                    default_namespace,
                )
                if default_namespace in user_namespaces
                else []
            )
            for topic in topics:
                # Delete Kafka topic (idempotent)
                kafka_result = self.kafka_service.delete_topic(
                    default_namespace,
                    topic,
                )
                # If Kafka deletion failed, stop WITHOUT deleting from
                # DynamoDB to avoid orphaning the Kafka topic
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
                        f'Failed to delete IAM user: {iam_result["message"]}'
                    ),
                }

            return {
                'status': 'success',
                'message': f'User {subject} deleted',
            }

    def create_key(self, subject: str) -> dict[str, Any]:
        """Create an access key for a user.

        Creates user and namespace if they don't exist, then creates
        a new access key and stores it. If a key already exists in DynamoDB,
        returns it without creating a new one (idempotent behavior).

        This method is atomic - concurrent calls are serialized using a
        global lock.

        Args:
            subject: User subject ID

        Returns:
            Dictionary with status, message, and key information
            (access_key, secret_key, create_date, endpoint, fresh)
            where fresh=False if key existed in DynamoDB,
            fresh=True if newly created
        """
        # Use global lock to ensure atomicity
        with self._global_lock:
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
            delete_result = self.iam_service.delete_access_keys(subject)
            if delete_result['status'] != 'success':
                return {
                    'status': 'failure',
                    'message': (
                        f'Failed to delete existing IAM access keys: '
                        f'{delete_result["message"]}'
                    ),
                }

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
        # Delete access keys in IAM (idempotent); on failure keep the
        # cached key, since the IAM key is still valid
        iam_result = self.iam_service.delete_access_keys(subject)
        if iam_result['status'] != 'success':
            return {
                'status': 'failure',
                'message': (
                    f'Failed to delete IAM access keys: '
                    f'{iam_result["message"]}'
                ),
            }

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
        namespace_result, added = self.namespace_service.add_topic(
            subject,
            namespace,
            topic,
        )
        if namespace_result['status'] != 'success':
            return namespace_result

        # Create topic in KafkaService (Kafka cluster)
        kafka_result = self.kafka_service.create_topic(namespace, topic)
        if kafka_result and kafka_result.get('status') == 'failure':
            # If Kafka creation fails, clean up the DynamoDB entry, but only
            # one this call added: an existing entry may be a live topic
            if added:
                with contextlib.suppress(Exception):
                    self.namespace_service.delete_topic(
                        subject,
                        namespace,
                        topic,
                    )
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
        NamespaceService (DynamoDB). Kafka is called only after the
        namespace is found for the subject.

        Args:
            subject: User subject ID
            namespace: Namespace name
            topic: Topic name to delete

        Returns:
            Dictionary with status, message, and remaining topics list
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

        # Topic names never contain a dot, so the Kafka name stays
        # inside the namespace
        if '.' in topic:
            return {
                'status': 'failure',
                'message': f'Invalid topic name {topic}',
            }

        # Delete topic from KafkaService (Kafka cluster) first
        kafka_result = self.kafka_service.delete_topic(namespace, topic)
        # Note: kafka_result can be None if Kafka is not configured,
        # which is acceptable

        # If Kafka deletion failed, return failure WITHOUT deleting
        # from DynamoDB to avoid orphaning the Kafka topic
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

        # Kafka succeeded (or not configured) — safe to delete from DynamoDB
        namespace_result = self.namespace_service.delete_topic(
            subject,
            namespace,
            topic,
        )

        # A Kafka topic can lack its DynamoDB entry (for example after a
        # failed create's cleanup); its deletion is still a deletion
        if (
            kafka_result is not None
            and kafka_result.get('message') == 'Topic deleted'
            and namespace_result.get('status') == 'success'
        ):
            namespace_result['message'] = (
                f'Topic {topic} deleted from {namespace}'
            )

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
