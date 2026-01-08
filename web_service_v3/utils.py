"""Utility functions for web service v3."""

from __future__ import annotations

import contextlib
import threading
from typing import Any

from kafka.admin import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.errors import UnknownTopicOrPartitionError

from web_service_v3.responses import combine_user_creation_result
from web_service_v3.responses import combine_user_deletion_result
from web_service_v3.services import DynamoDBService
from web_service_v3.services import IAMService
from web_service_v3.services import KafkaService
from web_service_v3.services import MSKTokenProvider
from web_service_v3.services import NamespaceService

# Namespace validation constants
MIN_NAMESPACE_LENGTH = 3
MAX_NAMESPACE_LENGTH = 32

# Global namespaces tracking key
GLOBAL_NAMESPACES_KEY = '__global_namespaces__'


class AWSManagerV3:
    """Manage AWS resources for Diaspora Service V3 - simplified version."""

    def __init__(
        self,
        account_id: str,
        region: str,
        cluster_name: str,
        iam_public: str,
        keys_table_name: str | None = None,
        users_table_name: str | None = None,
        namespace_table_name: str | None = None,
    ) -> None:
        """Initialize AWSManagerV3 with AWS credentials and configuration.

        Args:
            account_id: AWS account ID
            region: AWS region
            cluster_name: MSK cluster name
            iam_public: IAM public endpoint
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
        self.lock = threading.Lock()

        # Initialize services
        self.iam_service = IAMService(
            account_id,
            region,
            cluster_name,
        )
        self.dynamodb_service = DynamoDBService(
            region,
            keys_table_name or 'diaspora-keys',
            users_table_name or 'diaspora-users',
            namespace_table_name or 'diaspora-namespaces',
        )
        self.kafka_service = KafkaService(
            iam_public,
            region,
        )
        self.namespace_service = NamespaceService(
            self.dynamodb_service,
        )

    def create_user(self, subject: str) -> dict[str, str]:
        """Create an IAM user with policy for the given subject.

        Also creates and registers a default namespace for the user.
        If the default namespace (based on subject UUID) fails, retries with
        randomly generated namespaces until one succeeds.

        Note: Users can have multiple namespaces, but only one is created
        by default. Additional namespaces can be created via
        create_namespace().
        """
        with self.lock:
            iam_result = self.iam_service.ensure_user_exists(subject)
            namespace_result = self.namespace_service.ensure_exists(subject)
            result = combine_user_creation_result(
                iam_result,
                namespace_result,
            )
            result['message'] = f'IAM user created for {subject}'
            return result

    def _ensure_namespace_removed(self, subject: str) -> dict[str, bool]:
        """Ensure all namespaces and their topics are removed for a user.

        This method does NOT use a lock - caller must hold lock.
        Deletes all topics under each namespace, then removes the namespaces.

        Args:
            subject: User subject ID

        Returns:
            dict with 'namespaces_deleted' flag indicating if any namespaces
            were deleted
        """
        user_record = self.dynamodb_service.get_user_record(subject)
        if not user_record or 'namespaces' not in user_record:
            return {'namespaces_deleted': False}

        namespaces = user_record['namespaces'].copy()
        if not namespaces:
            return {'namespaces_deleted': False}

        # For each namespace, delete all topics first, then delete namespace
        for namespace in namespaces:
            # Get all topics for this namespace
            topics = self.dynamodb_service.get_namespace_topics(namespace)

            # Delete each Kafka topic (idempotent)
            # Note: We delete Kafka topics but don't update DynamoDB here
            # because deleting the namespace will clean up the topics record.
            # This is safe because namespace deletion removes the namespace
            # record from DynamoDB, which contains the topics list.
            for topic in topics:
                self._delete_kafka_topic(namespace, topic)

            # Delete the namespace itself (this will clean up DynamoDB records)
            self.namespace_service.delete(subject, namespace)

        return {'namespaces_deleted': True}

    def delete_user(self, subject: str) -> dict[str, str]:
        """Delete the IAM user and all associated resources."""
        with self.lock:
            # 1. Delete all namespaces and their topics
            namespace_result = self._ensure_namespace_removed(subject)

            # 2. Delete IAM resources (user, policy, keys)
            iam_result = self.iam_service.delete_user(subject)
            result = combine_user_deletion_result(
                namespace_result,
                iam_result,
            )
            result['message'] = f'IAM user deleted for {subject}'
            return result

    def create_key(self, subject: str) -> dict[str, str]:
        """Create an access key for a user, creating user if needed.

        Note: This method ALWAYS deletes existing keys before creating
        a new one. Use get_key() if you want to preserve existing keys.
        """
        with self.lock:
            # Ensure user exists first (idempotent)
            self.iam_service.ensure_user_exists(subject)
            # Ensure namespace exists (idempotent)
            self.namespace_service.ensure_exists(subject)

            # Delete existing keys from IAM if there are any
            self.iam_service.delete_access_keys(subject)

            # Delete existing key from DynamoDB if any
            self.dynamodb_service.delete_key(subject)

            # Create new key
            key_data = self.iam_service.create_access_key(subject)

            # Store key in DynamoDB
            self.dynamodb_service.store_key(
                subject,
                key_data['access_key'],
                key_data['secret_key'],
                key_data['create_date'],
            )

            return {
                'status': 'success',
                'message': f'Access key created for {subject}',
                'username': subject,
                'access_key': key_data['access_key'],
                'secret_key': key_data['secret_key'],
                'create_date': key_data['create_date'],
                'endpoint': self.iam_public,
            }

    def get_key(self, subject: str) -> dict[str, Any]:
        """Get access key from DynamoDB or create if needed.

        Note: This method only creates a new key if no valid key exists.
        Existing valid keys are preserved. Use create_key() to force
        key replacement.
        """
        with self.lock:
            # Ensure user exists first (idempotent)
            self.iam_service.ensure_user_exists(subject)
            # Ensure namespace exists (idempotent)
            self.namespace_service.ensure_exists(subject)

            # Try to retrieve key from DynamoDB first
            stored_key = self.dynamodb_service.get_key(subject)
            if stored_key:
                # Verify the key still exists in IAM
                access_key_ids = self.iam_service.list_access_keys(subject)
                if stored_key['access_key'] in access_key_ids:
                    # Key exists in both DynamoDB and IAM, return it
                    result: dict[str, Any] = {
                        'status': 'success',
                        'message': f'Access key retrieved for {subject}',
                        'username': subject,
                        'access_key': stored_key['access_key'],
                        'secret_key': stored_key['secret_key'],
                        'endpoint': self.iam_public,
                        'retrieved_from_dynamodb': True,
                    }
                    if 'create_date' in stored_key:
                        result['create_date'] = stored_key['create_date']
                    return result

                # Key in DynamoDB but not in IAM, delete from DynamoDB
                self.dynamodb_service.delete_key(subject)

            # No key in DynamoDB or key invalid, create new one
            # Delete existing keys from IAM if there are any
            self.iam_service.delete_access_keys(subject)

            # Create new key
            key_data = self.iam_service.create_access_key(subject)

            # Store key in DynamoDB
            self.dynamodb_service.store_key(
                subject,
                key_data['access_key'],
                key_data['secret_key'],
                key_data['create_date'],
            )

            return {
                'status': 'success',
                'message': f'Access key retrieved/created for {subject}',
                'username': subject,
                'access_key': key_data['access_key'],
                'secret_key': key_data['secret_key'],
                'create_date': key_data['create_date'],
                'endpoint': self.iam_public,
                'retrieved_from_dynamodb': False,
            }

    def delete_key(self, subject: str) -> dict[str, str]:
        """Delete access keys for a user.

        Note: For consistency with create_key() and get_key(), this method
        ensures namespace exists, though keys can technically exist without
        a namespace.
        """
        with self.lock:
            # Check if user exists
            if not self.iam_service.user_exists(subject):
                return {
                    'status': 'error',
                    'message': f'User {subject} does not exist.',
                }

            # Ensure namespace exists (for consistency with create_key/get_key)
            # Note: This is optional since keys can exist without namespace,
            # but improves symmetry with create/get operations
            self.namespace_service.ensure_exists(subject)

            # Delete existing keys from IAM
            keys_deleted = self.iam_service.delete_access_keys(subject)

            # Delete key from DynamoDB
            self.dynamodb_service.delete_key(subject)

            return {
                'status': 'success',
                'message': f'Access keys deleted for {subject}',
                'keys_deleted': str(keys_deleted),
            }

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
        validation_result = self.namespace_service.validate(namespace)
        if validation_result:
            return {
                'status': 'failure',
                'message': validation_result['message'],
            }
        return None

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
            return self.namespace_service.create(subject, namespace)

    def delete_namespace(
        self,
        subject: str,
        namespace: str,
    ) -> dict[str, Any]:
        """Delete a namespace for a user (idempotent).

        Also deletes all topics under the namespace before deleting the
        namespace itself. This ensures no orphaned Kafka topics remain.

        Args:
            subject: User subject ID
            namespace: Namespace name to delete

        Returns:
            dict with status, message, and remaining namespaces list.
            Returns success message even if namespace doesn't exist.
        """
        with self.lock:
            # Get all topics for this namespace
            topics = self.dynamodb_service.get_namespace_topics(namespace)

            # Delete each Kafka topic first (idempotent)
            # This prevents orphaned topics when namespace is deleted
            for topic in topics:
                self._delete_kafka_topic(namespace, topic)

            # Then delete the namespace (this will clean up DynamoDB records)
            return self.namespace_service.delete(subject, namespace)

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
        result = self.kafka_service.create_topic(namespace, topic)
        if result is None:
            return None
        message = (
            result['message']
            if result['status'] == 'success'
            else result.get('error') or result['message']
        )
        return {
            'status': result['status'],
            'message': message,
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
        result = self.kafka_service.delete_topic(namespace, topic)
        if result is None:
            return None
        message = (
            result['message']
            if result['status'] == 'success'
            else result.get('error') or result['message']
        )
        return {
            'status': result['status'],
            'message': message,
        }

    def create_topic(  # noqa: PLR0911
        self,
        subject: str,
        namespace: str,
        topic: str,
    ) -> dict[str, Any]:
        """Create a topic under a namespace.

        Operation order:
        1. Validate topic name
        2. Ensure user exists
        3. Verify namespace ownership
        4. Check if topic exists (early return if exists)
        5. Update DynamoDB
        6. Create Kafka topic

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
            # 1. Validate topic name (same rules as namespace)
            validation_error = self._validate_namespace_name(topic)
            if validation_error:
                validation_error['namespace'] = namespace
                validation_error['topic'] = topic
                return validation_error

            # 2. Ensure user exists (for consistency with delete_topic)
            self.iam_service.ensure_user_exists(subject)

            # 3. Verify namespace belongs to user
            user_record = self.dynamodb_service.get_user_record(subject)
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

            # 4. Get existing topics and check if topic already exists
            existing_topics = self.dynamodb_service.get_namespace_topics(
                namespace,
            )

            if topic in existing_topics:
                # Topic already exists, create Kafka topic (idempotent)
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

            # 5. Update DynamoDB - add topic to namespace
            all_topics = sorted({*existing_topics, topic})
            self.dynamodb_service.update_namespace_topics(
                namespace,
                all_topics,
            )

            # 6. Create Kafka topic (idempotent)
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

        This operation is idempotent - it returns success even if the
        namespace or topic doesn't exist. This differs from create_topic()
        which validates existence and ownership before proceeding.

        Operation order:
        1. Ensure user exists (for consistency with create_topic)
        2. Verify namespace ownership
        3. Check if topic exists (early return if not exists)
        4. Update/Delete from DynamoDB
        5. Delete Kafka topic

        Args:
            subject: User subject ID
            namespace: Namespace name
            topic: Topic name

        Returns:
            dict with status and message.
            Returns success message even if namespace or topic doesn't exist.
        """
        with self.lock:
            # 1. Ensure user exists (for consistency with create_topic)
            self.iam_service.ensure_user_exists(subject)

            # 2. Verify namespace belongs to user
            user_record = self.dynamodb_service.get_user_record(subject)
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

            # 3. Get existing topics and check if topic exists
            existing_topics = self.dynamodb_service.get_namespace_topics(
                namespace,
            )

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

            # 4. Update/Delete from DynamoDB
            remaining_topics = [t for t in existing_topics if t != topic]
            if remaining_topics:
                self.dynamodb_service.update_namespace_topics(
                    namespace,
                    remaining_topics,
                )
            else:
                self.dynamodb_service.delete_namespace_topics(namespace)

            # 5. Delete Kafka topic (idempotent)
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
            user_record = self.dynamodb_service.get_user_record(subject)
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
                topics = self.dynamodb_service.get_namespace_topics(namespace)
                result[namespace] = topics

            return {
                'status': 'success',
                'message': f'Namespaces retrieved for {subject}',
                'namespaces': result,
            }

    def recreate_topic(
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
            user_record = self.dynamodb_service.get_user_record(subject)
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
            namespace_topics = self.dynamodb_service.get_namespace_topics(
                namespace,
            )
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
