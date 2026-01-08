"""Service classes for AWS resource management."""

from __future__ import annotations

import contextlib
import json
import time
from typing import Any

import boto3
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from kafka.admin import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.errors import UnknownTopicOrPartitionError
from kafka.sasl.oauth import AbstractTokenProvider


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
            f'{base}:topic{cluster[:-1]}/ns-{subject_suffix}.*',
        ]
        return {
            'Version': '2012-10-17',
            'Statement': [{
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
            }],
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
        self._put_item_with_table_creation(
            table_name=self.keys_table_name,
            item=item,
            create_table_func=self._create_keys_table,
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

    def put_user_namespace(
        self,
        subject: str,
        namespaces: list[str],
    ) -> None:
        """Store user namespaces in DynamoDB.

        Args:
            subject: User subject ID
            namespaces: List of namespace names
        """
        item = {
            'subject': {'S': subject},
            'namespaces': {
                'L': [{'S': ns} for ns in sorted(set(namespaces))],
            },
        }
        self._put_item_with_table_creation(
            table_name=self.users_table_name,
            item=item,
            create_table_func=self._create_users_table,
        )

    def get_user_namespaces(self, subject: str) -> list[str]:
        """Get user namespaces from DynamoDB.

        Args:
            subject: User subject ID

        Returns:
            List of namespace names, empty list if not found
        """
        with contextlib.suppress(
            self.dynamodb.exceptions.ResourceNotFoundException,
        ):
            response = self.dynamodb.get_item(
                TableName=self.users_table_name,
                Key={'subject': {'S': subject}},
            )
            if 'Item' in response and 'namespaces' in response['Item']:
                return [ns['S'] for ns in response['Item']['namespaces']['L']]
        return []

    def delete_user_namespace(self, subject: str) -> None:
        """Delete user namespace record from DynamoDB.

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

    # ========================================================================
    # Global Namespace Management
    # ========================================================================

    def put_global_namespaces(self, namespaces: set[str]) -> None:
        """Store global namespaces registry in DynamoDB.

        Args:
            namespaces: Set of namespace names
        """
        item = {
            'subject': {'S': GLOBAL_NAMESPACES_KEY},
            'namespaces': {
                'L': [{'S': ns} for ns in sorted(namespaces)],
            },
        }
        self._put_item_with_table_creation(
            table_name=self.users_table_name,
            item=item,
            create_table_func=self._create_users_table,
        )

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
                return {ns['S'] for ns in response['Item']['namespaces']['L']}
        return set()

    def delete_global_namespaces(self) -> None:
        """Delete global namespaces registry from DynamoDB."""
        with contextlib.suppress(
            self.dynamodb.exceptions.ResourceNotFoundException,
        ):
            self.dynamodb.delete_item(
                TableName=self.users_table_name,
                Key={'subject': {'S': GLOBAL_NAMESPACES_KEY}},
            )

    # ========================================================================
    # Namespace Topics Management
    # ========================================================================

    def put_namespace_topics(
        self,
        namespace: str,
        topics: list[str],
    ) -> None:
        """Store namespace topics in DynamoDB.

        Args:
            namespace: Namespace name
            topics: List of topic names
        """
        item = {
            'namespace': {'S': namespace},
            'topics': {
                'L': [{'S': t} for t in sorted(set(topics))],
            },
        }
        self._put_item_with_table_creation(
            table_name=self.namespace_table_name,
            item=item,
            create_table_func=self._create_namespace_table,
        )

    def get_namespace_topics(self, namespace: str) -> list[str]:
        """Get list of topics for a namespace from DynamoDB.

        Args:
            namespace: Namespace name

        Returns:
            List of topic names, empty list if not found
        """
        with contextlib.suppress(
            self.dynamodb.exceptions.ResourceNotFoundException,
        ):
            response = self.dynamodb.get_item(
                TableName=self.namespace_table_name,
                Key={'namespace': {'S': namespace}},
            )
            if 'Item' in response and 'topics' in response['Item']:
                return [t['S'] for t in response['Item']['topics']['L']]
        return []

    def delete_namespace_topics(self, namespace: str) -> None:
        """Delete namespace topics record from DynamoDB.

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

    def _put_item_with_table_creation(
        self,
        table_name: str,
        item: dict[str, Any],
        create_table_func: Any,
    ) -> None:
        """Put item in DynamoDB table, creating table if it doesn't exist.

        Args:
            table_name: Name of the DynamoDB table
            item: Item to put in the table
            create_table_func: Function to call to create the table
        """
        try:
            self.dynamodb.put_item(
                TableName=table_name,
                Item=item,
            )
        except self.dynamodb.exceptions.ResourceNotFoundException:
            create_table_func()
            self.dynamodb.put_item(
                TableName=table_name,
                Item=item,
            )

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

