"""Consumer lifecycle management service.

Implements Confluent Kafka REST Proxy v2 consumer semantics:
in-memory consumer instances with idle timeout reaping.
"""

from __future__ import annotations

import base64
import logging
import os
import threading
import time
import uuid
from typing import Any

from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata
from kafka.structs import TopicPartition

from web_service.services import MSKTokenProvider

logger = logging.getLogger(__name__)

# Default idle timeout: 5 minutes
DEFAULT_CONSUMER_TIMEOUT_MS = 300_000
REAPER_INTERVAL_SECONDS = 30


class ManagedConsumer:
    """Wrapper around KafkaConsumer with metadata and access tracking."""

    def __init__(
        self,
        consumer: KafkaConsumer,
        subject: str,
        namespace: str,
        group_name: str,
        instance_id: str,
        format_type: str,
    ) -> None:
        """Initialize a managed consumer.

        Args:
            consumer: The underlying KafkaConsumer
            subject: Authenticated user subject
            namespace: Namespace scope
            group_name: Consumer group name
            instance_id: Unique instance identifier
            format_type: Deserialization format (binary/json)
        """
        self.consumer = consumer
        self.subject = subject
        self.namespace = namespace
        self.group_name = group_name
        self.instance_id = instance_id
        self.format_type = format_type
        self.lock = threading.Lock()
        self.last_access = time.monotonic()

    def touch(self) -> None:
        """Update last access time."""
        self.last_access = time.monotonic()

    def is_expired(self, timeout_ms: int) -> bool:
        """Check if consumer has exceeded idle timeout."""
        elapsed_ms = (time.monotonic() - self.last_access) * 1000
        return elapsed_ms > timeout_ms


def _encode_value(
    value: bytes | None,
    format_type: str,
) -> str | None:
    """Encode a message value for the response.

    Args:
        value: Raw bytes from Kafka
        format_type: "binary" (base64) or "json" (UTF-8)

    Returns:
        Encoded string or None
    """
    if value is None:
        return None
    if format_type == 'json':
        try:
            return value.decode('utf-8')
        except UnicodeDecodeError:
            return base64.b64encode(value).decode('ascii')
    # binary format: base64 encode
    return base64.b64encode(value).decode('ascii')


class ConsumerService:
    """Manage Kafka consumer instances for REST API access."""

    def __init__(
        self,
        bootstrap_servers: str | None,
        region: str,
    ) -> None:
        """Initialize consumer service.

        Args:
            bootstrap_servers: Kafka bootstrap servers
            region: AWS region for MSK auth
        """
        self.bootstrap_servers = bootstrap_servers
        self.region = region
        self.timeout_ms = int(
            os.getenv(
                'CONSUMER_INSTANCE_TIMEOUT_MS',
                str(DEFAULT_CONSUMER_TIMEOUT_MS),
            ),
        )
        # Key: (subject, namespace, group_name, instance_id)
        self._consumers: dict[
            tuple[str, str, str, str],
            ManagedConsumer,
        ] = {}
        self._global_lock = threading.Lock()
        self._reaper_thread = threading.Thread(
            target=self._reap_expired_consumers,
            daemon=True,
        )
        self._reaper_thread.start()

    def _consumer_key(
        self,
        subject: str,
        namespace: str,
        group_name: str,
        instance_id: str,
    ) -> tuple[str, str, str, str]:
        """Build the registry key for a consumer."""
        return (subject, namespace, group_name, instance_id)

    def _kafka_group_id(
        self,
        namespace: str,
        group_name: str,
    ) -> str:
        """Generate namespace-scoped Kafka consumer group ID."""
        return f'{namespace}.{group_name}'

    def _kafka_topic_name(
        self,
        namespace: str,
        topic: str,
    ) -> str:
        """Generate full Kafka topic name."""
        return f'{namespace}.{topic}'

    def _strip_namespace(
        self,
        namespace: str,
        kafka_topic: str,
    ) -> str:
        """Strip namespace prefix from a Kafka topic name."""
        prefix = f'{namespace}.'
        if kafka_topic.startswith(prefix):
            return kafka_topic[len(prefix) :]
        return kafka_topic

    def _get_consumer(
        self,
        subject: str,
        namespace: str,
        group_name: str,
        instance_id: str,
    ) -> ManagedConsumer | None:
        """Get a managed consumer by key, or None."""
        key = self._consumer_key(
            subject,
            namespace,
            group_name,
            instance_id,
        )
        with self._global_lock:
            return self._consumers.get(key)

    # --- Consumer lifecycle ---

    def create_consumer(
        self,
        subject: str,
        namespace: str,
        group_name: str,
        name: str | None = None,
        format_type: str = 'binary',
        auto_offset_reset: str = 'latest',
        auto_commit_enable: str = 'false',
        fetch_min_bytes: str | None = None,
        consumer_request_timeout_ms: str | None = None,
    ) -> tuple[dict[str, Any], int]:
        """Create a new consumer instance.

        Returns:
            Tuple of (response_body, http_status_code)
        """
        if not self.bootstrap_servers:
            return (
                {
                    'error_code': 50002,
                    'message': 'Kafka endpoint is not configured',
                },
                500,
            )

        instance_id = name or uuid.uuid4().hex[:12]
        key = self._consumer_key(
            subject,
            namespace,
            group_name,
            instance_id,
        )
        kafka_group_id = self._kafka_group_id(namespace, group_name)

        with self._global_lock:
            if key in self._consumers:
                return (
                    {
                        'error_code': 40902,
                        'message': (
                            'Consumer instance with the specified '
                            'name already exists.'
                        ),
                    },
                    409,
                )

            kwargs: dict[str, Any] = {
                'bootstrap_servers': self.bootstrap_servers,
                'group_id': kafka_group_id,
                'security_protocol': 'SASL_SSL',
                'sasl_mechanism': 'OAUTHBEARER',
                'sasl_oauth_token_provider': MSKTokenProvider(
                    self.region,
                ),
                'auto_offset_reset': auto_offset_reset,
                'enable_auto_commit': auto_commit_enable.lower() == 'true',
            }
            if fetch_min_bytes is not None:
                kwargs['fetch_min_bytes'] = int(fetch_min_bytes)
            if consumer_request_timeout_ms is not None:
                kwargs['request_timeout_ms'] = int(
                    consumer_request_timeout_ms,
                )

            try:
                consumer = KafkaConsumer(**kwargs)
            except Exception as e:
                logger.exception('Failed to create consumer')
                return (
                    {
                        'error_code': 50002,
                        'message': f'Failed to create consumer: {e!s}',
                    },
                    500,
                )

            managed = ManagedConsumer(
                consumer=consumer,
                subject=subject,
                namespace=namespace,
                group_name=group_name,
                instance_id=instance_id,
                format_type=format_type,
            )
            self._consumers[key] = managed

        return (
            {
                'instance_id': instance_id,
                'base_uri': (
                    f'/api/v3/{namespace}/consumers'
                    f'/{group_name}/instances/{instance_id}'
                ),
            },
            200,
        )

    def delete_consumer(
        self,
        subject: str,
        namespace: str,
        group_name: str,
        instance_id: str,
    ) -> tuple[dict[str, Any] | None, int]:
        """Destroy a consumer instance.

        Returns:
            Tuple of (None, http_status_code)
        """
        key = self._consumer_key(
            subject,
            namespace,
            group_name,
            instance_id,
        )
        with self._global_lock:
            managed = self._consumers.pop(key, None)
        if managed is None:
            return (
                {
                    'error_code': 40403,
                    'message': 'Consumer instance not found',
                },
                404,
            )
        with managed.lock:
            try:
                managed.consumer.close(autocommit=False)
            except Exception:
                logger.exception(
                    'Error closing consumer %s',
                    instance_id,
                )
        return (None, 204)

    # --- Subscription ---

    def subscribe(
        self,
        subject: str,
        namespace: str,
        group_name: str,
        instance_id: str,
        topics: list[str] | None = None,
        topic_pattern: str | None = None,
    ) -> tuple[dict[str, Any] | None, int]:
        """Subscribe consumer to topics or pattern.

        Returns:
            Tuple of (response_body_or_None, http_status_code)
        """
        managed = self._get_consumer(
            subject,
            namespace,
            group_name,
            instance_id,
        )
        if managed is None:
            return (
                {
                    'error_code': 40403,
                    'message': 'Consumer instance not found',
                },
                404,
            )

        with managed.lock:
            managed.touch()
            try:
                if topics is not None:
                    kafka_topics = [
                        self._kafka_topic_name(namespace, t) for t in topics
                    ]
                    managed.consumer.subscribe(topics=kafka_topics)
                elif topic_pattern is not None:
                    # Scope pattern to namespace
                    scoped = f'{namespace}\\.{topic_pattern}'
                    managed.consumer.subscribe(pattern=scoped)
                else:
                    return (
                        {
                            'error_code': 42204,
                            'message': (
                                'Must provide topics or topic_pattern'
                            ),
                        },
                        422,
                    )
            except Exception as e:
                return (
                    {
                        'error_code': 50002,
                        'message': f'Failed to subscribe: {e!s}',
                    },
                    500,
                )
        return (None, 204)

    def get_subscription(
        self,
        subject: str,
        namespace: str,
        group_name: str,
        instance_id: str,
    ) -> tuple[dict[str, Any], int]:
        """Get current subscription.

        Returns:
            Tuple of (response_body, http_status_code)
        """
        managed = self._get_consumer(
            subject,
            namespace,
            group_name,
            instance_id,
        )
        if managed is None:
            return (
                {
                    'error_code': 40403,
                    'message': 'Consumer instance not found',
                },
                404,
            )
        with managed.lock:
            managed.touch()
            sub = managed.consumer.subscription() or set()
            user_topics = sorted(
                self._strip_namespace(namespace, t) for t in sub
            )
        return ({'topics': user_topics}, 200)

    def unsubscribe(
        self,
        subject: str,
        namespace: str,
        group_name: str,
        instance_id: str,
    ) -> tuple[dict[str, Any] | None, int]:
        """Unsubscribe consumer from all topics.

        Returns:
            Tuple of (None_or_error, http_status_code)
        """
        managed = self._get_consumer(
            subject,
            namespace,
            group_name,
            instance_id,
        )
        if managed is None:
            return (
                {
                    'error_code': 40403,
                    'message': 'Consumer instance not found',
                },
                404,
            )
        with managed.lock:
            managed.touch()
            try:
                managed.consumer.unsubscribe()
            except Exception as e:
                return (
                    {
                        'error_code': 50002,
                        'message': f'Failed to unsubscribe: {e!s}',
                    },
                    500,
                )
        return (None, 204)

    # --- Records ---

    def poll_records(
        self,
        subject: str,
        namespace: str,
        group_name: str,
        instance_id: str,
        timeout_ms: int = 1000,
        max_bytes: int | None = None,
    ) -> tuple[list[dict[str, Any]] | dict[str, Any], int]:
        """Fetch records from the consumer.

        Returns:
            Tuple of (records_array_or_error, http_status_code)
        """
        managed = self._get_consumer(
            subject,
            namespace,
            group_name,
            instance_id,
        )
        if managed is None:
            return (
                {
                    'error_code': 40403,
                    'message': 'Consumer instance not found',
                },
                404,
            )
        with managed.lock:
            managed.touch()
            try:
                raw = managed.consumer.poll(
                    timeout_ms=timeout_ms,
                    max_records=max_bytes,
                )
            except Exception as e:
                return (
                    {
                        'error_code': 50002,
                        'message': f'Failed to poll: {e!s}',
                    },
                    500,
                )
            records: list[dict[str, Any]] = []
            for tp, msgs in raw.items():
                user_topic = self._strip_namespace(
                    namespace,
                    tp.topic,
                )
                for msg in msgs:
                    records.append(
                        {
                            'topic': user_topic,
                            'key': _encode_value(
                                msg.key,
                                managed.format_type,
                            ),
                            'value': _encode_value(
                                msg.value,
                                managed.format_type,
                            ),
                            'partition': tp.partition,
                            'offset': msg.offset,
                        },
                    )
        return (records, 200)

    # --- Offsets ---

    def commit_offsets(
        self,
        subject: str,
        namespace: str,
        group_name: str,
        instance_id: str,
        offsets: list[dict[str, Any]] | None = None,
    ) -> tuple[dict[str, Any] | None, int]:
        """Commit offsets for the consumer.

        Returns:
            Tuple of (None_or_error, http_status_code)
        """
        managed = self._get_consumer(
            subject,
            namespace,
            group_name,
            instance_id,
        )
        if managed is None:
            return (
                {
                    'error_code': 40403,
                    'message': 'Consumer instance not found',
                },
                404,
            )
        with managed.lock:
            managed.touch()
            try:
                if offsets:
                    kafka_offsets = {}
                    for o in offsets:
                        tp = TopicPartition(
                            self._kafka_topic_name(
                                namespace,
                                o['topic'],
                            ),
                            o['partition'],
                        )
                        kafka_offsets[tp] = OffsetAndMetadata(
                            o['offset'],
                            None,
                        )
                    managed.consumer.commit(offsets=kafka_offsets)
                else:
                    managed.consumer.commit()
            except Exception as e:
                return (
                    {
                        'error_code': 50002,
                        'message': (f'Failed to commit offsets: {e!s}'),
                    },
                    500,
                )
        return (None, 200)

    def get_committed(
        self,
        subject: str,
        namespace: str,
        group_name: str,
        instance_id: str,
        partitions: list[dict[str, Any]],
    ) -> tuple[dict[str, Any], int]:
        """Get committed offsets for partitions.

        Returns:
            Tuple of (response_body, http_status_code)
        """
        managed = self._get_consumer(
            subject,
            namespace,
            group_name,
            instance_id,
        )
        if managed is None:
            return (
                {
                    'error_code': 40403,
                    'message': 'Consumer instance not found',
                },
                404,
            )
        with managed.lock:
            managed.touch()
            results = []
            try:
                for p in partitions:
                    tp = TopicPartition(
                        self._kafka_topic_name(
                            namespace,
                            p['topic'],
                        ),
                        p['partition'],
                    )
                    committed = managed.consumer.committed(tp)
                    results.append(
                        {
                            'topic': p['topic'],
                            'partition': p['partition'],
                            'offset': committed
                            if committed is not None
                            else -1,
                            'metadata': '',
                        },
                    )
            except Exception as e:
                return (
                    {
                        'error_code': 50002,
                        'message': (f'Failed to get committed offsets: {e!s}'),
                    },
                    500,
                )
        return ({'offsets': results}, 200)

    # --- Assignments ---

    def assign_partitions(
        self,
        subject: str,
        namespace: str,
        group_name: str,
        instance_id: str,
        partitions: list[dict[str, Any]],
    ) -> tuple[dict[str, Any] | None, int]:
        """Manually assign partitions to consumer.

        Returns:
            Tuple of (None_or_error, http_status_code)
        """
        managed = self._get_consumer(
            subject,
            namespace,
            group_name,
            instance_id,
        )
        if managed is None:
            return (
                {
                    'error_code': 40403,
                    'message': 'Consumer instance not found',
                },
                404,
            )
        with managed.lock:
            managed.touch()
            try:
                tps = [
                    TopicPartition(
                        self._kafka_topic_name(
                            namespace,
                            p['topic'],
                        ),
                        p['partition'],
                    )
                    for p in partitions
                ]
                managed.consumer.assign(tps)
            except Exception as e:
                return (
                    {
                        'error_code': 50002,
                        'message': (f'Failed to assign partitions: {e!s}'),
                    },
                    500,
                )
        return (None, 204)

    def get_assignments(
        self,
        subject: str,
        namespace: str,
        group_name: str,
        instance_id: str,
    ) -> tuple[dict[str, Any], int]:
        """Get current partition assignments.

        Returns:
            Tuple of (response_body, http_status_code)
        """
        managed = self._get_consumer(
            subject,
            namespace,
            group_name,
            instance_id,
        )
        if managed is None:
            return (
                {
                    'error_code': 40403,
                    'message': 'Consumer instance not found',
                },
                404,
            )
        with managed.lock:
            managed.touch()
            assigned = managed.consumer.assignment() or set()
            partitions = sorted(
                (
                    {
                        'topic': self._strip_namespace(
                            namespace,
                            tp.topic,
                        ),
                        'partition': tp.partition,
                    }
                    for tp in assigned
                ),
                key=lambda x: (x['topic'], x['partition']),
            )
        return ({'partitions': partitions}, 200)

    # --- Positions / Seek ---

    def seek(
        self,
        subject: str,
        namespace: str,
        group_name: str,
        instance_id: str,
        offsets: list[dict[str, Any]],
    ) -> tuple[dict[str, Any] | None, int]:
        """Seek to specific offsets.

        Returns:
            Tuple of (None_or_error, http_status_code)
        """
        managed = self._get_consumer(
            subject,
            namespace,
            group_name,
            instance_id,
        )
        if managed is None:
            return (
                {
                    'error_code': 40403,
                    'message': 'Consumer instance not found',
                },
                404,
            )
        with managed.lock:
            managed.touch()
            try:
                for o in offsets:
                    tp = TopicPartition(
                        self._kafka_topic_name(
                            namespace,
                            o['topic'],
                        ),
                        o['partition'],
                    )
                    managed.consumer.seek(tp, o['offset'])
            except Exception as e:
                return (
                    {
                        'error_code': 50002,
                        'message': f'Failed to seek: {e!s}',
                    },
                    500,
                )
        return (None, 204)

    def seek_to_beginning(
        self,
        subject: str,
        namespace: str,
        group_name: str,
        instance_id: str,
        partitions: list[dict[str, Any]],
    ) -> tuple[dict[str, Any] | None, int]:
        """Seek to beginning of partitions.

        Returns:
            Tuple of (None_or_error, http_status_code)
        """
        managed = self._get_consumer(
            subject,
            namespace,
            group_name,
            instance_id,
        )
        if managed is None:
            return (
                {
                    'error_code': 40403,
                    'message': 'Consumer instance not found',
                },
                404,
            )
        with managed.lock:
            managed.touch()
            try:
                tps = [
                    TopicPartition(
                        self._kafka_topic_name(
                            namespace,
                            p['topic'],
                        ),
                        p['partition'],
                    )
                    for p in partitions
                ]
                managed.consumer.seek_to_beginning(*tps)
            except Exception as e:
                return (
                    {
                        'error_code': 50002,
                        'message': (f'Failed to seek to beginning: {e!s}'),
                    },
                    500,
                )
        return (None, 204)

    def seek_to_end(
        self,
        subject: str,
        namespace: str,
        group_name: str,
        instance_id: str,
        partitions: list[dict[str, Any]],
    ) -> tuple[dict[str, Any] | None, int]:
        """Seek to end of partitions.

        Returns:
            Tuple of (None_or_error, http_status_code)
        """
        managed = self._get_consumer(
            subject,
            namespace,
            group_name,
            instance_id,
        )
        if managed is None:
            return (
                {
                    'error_code': 40403,
                    'message': 'Consumer instance not found',
                },
                404,
            )
        with managed.lock:
            managed.touch()
            try:
                tps = [
                    TopicPartition(
                        self._kafka_topic_name(
                            namespace,
                            p['topic'],
                        ),
                        p['partition'],
                    )
                    for p in partitions
                ]
                managed.consumer.seek_to_end(*tps)
            except Exception as e:
                return (
                    {
                        'error_code': 50002,
                        'message': (f'Failed to seek to end: {e!s}'),
                    },
                    500,
                )
        return (None, 204)

    # --- Lifecycle management ---

    def _reap_expired_consumers(self) -> None:
        """Background thread to clean up idle consumers."""
        while True:
            time.sleep(REAPER_INTERVAL_SECONDS)
            expired_keys: list[tuple[str, str, str, str]] = []
            with self._global_lock:
                for key, managed in self._consumers.items():
                    if managed.is_expired(self.timeout_ms):
                        expired_keys.append(key)
                for key in expired_keys:
                    managed = self._consumers.pop(key)
                    if managed:
                        try:
                            managed.consumer.close(
                                autocommit=False,
                            )
                        except Exception:
                            logger.exception(
                                'Error closing expired consumer',
                            )
                        logger.info(
                            'Reaped expired consumer: %s in group %s',
                            managed.instance_id,
                            managed.group_name,
                        )

    def shutdown_all(self) -> None:
        """Close all consumers. Called on application shutdown."""
        with self._global_lock:
            for _key, managed in list(self._consumers.items()):
                try:
                    managed.consumer.close(autocommit=False)
                except Exception:
                    logger.exception(
                        'Error closing consumer on shutdown',
                    )
            self._consumers.clear()
