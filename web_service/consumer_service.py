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
from collections.abc import Callable
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from kafka import KafkaConsumer
from kafka.errors import AuthorizationError
from kafka.errors import BrokerNotAvailableError
from kafka.errors import CommitFailedError
from kafka.errors import IllegalStateError
from kafka.errors import InvalidTopicError
from kafka.errors import KafkaConfigurationError
from kafka.errors import KafkaError
from kafka.errors import KafkaTimeoutError
from kafka.errors import NoBrokersAvailable
from kafka.errors import ThrottlingQuotaExceededError
from kafka.errors import UnknownTopicOrPartitionError
from kafka.structs import OffsetAndMetadata
from kafka.structs import TopicPartition

from web_service.services import MSKTokenProvider

logger = logging.getLogger(__name__)

# Default idle timeout: 5 minutes
DEFAULT_CONSUMER_TIMEOUT_MS = 300_000
REAPER_INTERVAL_SECONDS = 30
# Longest a commit, a committed-offsets lookup or a close waits on the
# cluster; without a timeout kafka-python retries until it succeeds
CONSUMER_OPERATION_TIMEOUT_MS = 10_000
# Longest a request waits for an instance that another request is using
# (a fetch holds it for up to its timeout) before it is told to retry;
# longer than the default fetch wait, so that never yields a 409
INSTANCE_LOCK_TIMEOUT_S = 10.0
# Most bytes one fetch asks the cluster for, in all and per partition.
# kafka-python's defaults (50 MiB and 1 MiB) let one fetch of a 50-topic
# subscription hold about 50 MiB, which the fetcher keeps until every
# record is returned. A batch larger than these is still returned whole,
# so a fetch of a record of up to 8 MiB holds that record.
FETCH_MAX_BYTES = 1024 * 1024
MAX_PARTITION_FETCH_BYTES = 1024 * 1024
# Instances one subject may hold, and the service in all. Each is a
# KafkaClient with its own heartbeat thread and sockets to the brokers.
MAX_INSTANCES_PER_SUBJECT = 8
MAX_INSTANCES = 100

# Error codes of the consumer routes: Confluent's, from kafka-rest's
# Errors.java and rest-utils' KafkaExceptionMapper, except 40904.
# kafka_failure notes where a Kafka error gets another code than there.
INVALID_TOPIC = 40002
NOT_AUTHORIZED = 40301
NOT_FOUND = 40401
PARTITION_NOT_FOUND = 40402
INSTANCE_NOT_FOUND = 40403
INSTANCE_EXISTS = 40902
ILLEGAL_STATE = 40903
INSTANCE_BUSY = 40904  # Octopus's own
INVALID_CONSUMER_CONFIG = 42204
TOO_MANY_REQUESTS = 42901
SERVER_ERROR = 500  # rest-utils' GenericExceptionMapper
KAFKA_ERROR = 50002
KAFKA_RETRIABLE_ERROR = 50003
BROKER_NOT_AVAILABLE = 50302

Reply = tuple[dict[str, Any], int]
# A reply that may have no body (204, or 200 to a commit)
MaybeReply = tuple[dict[str, Any] | None, int]


def error_reply(status: int, error_code: int, message: str) -> Reply:
    """An error reply's body and HTTP status."""
    return {'error_code': error_code, 'message': message}, status


def _instance_not_found() -> Reply:
    return error_reply(404, INSTANCE_NOT_FOUND, 'Consumer instance not found')


def _illegal_state(message: str) -> Reply:
    return error_reply(409, ILLEGAL_STATE, f'Illegal state: {message}')


def kafka_failure(error: Exception, action: str) -> Reply:  # noqa: PLR0911
    """The reply for a Kafka call that raised error, by its type.

    The statuses and codes follow rest-utils' KafkaExceptionMapper, with
    three changes. The client mistakes kafka-python reports as
    IllegalStateError or CommitFailedError get kafka-rest's 409 and 40903
    for an illegal state, where KafkaExceptionMapper gives a
    CommitFailedException 500 and 50002. An unknown topic or partition
    gets 40401, the code for a missing topic, where KafkaExceptionMapper
    gives 40403, which kafka-rest also uses for a missing instance. A
    failed Kafka authentication is the service's own, so it gets 500 and
    50002 rather than 401 and 40101, which on these routes means the
    caller's token failed. The
    exception's text can name brokers and internal topic names, so it is
    logged and the reply names only its type.

    Args:
        error: What the Kafka call raised
        action: What failed, completing "Failed to ...", such as "commit
            offsets"
    """
    if not isinstance(error, KafkaError):
        logger.exception('Unexpected error when trying to %s', action)
        return error_reply(
            500,
            SERVER_ERROR,
            f'Failed to {action}: internal server error',
        )
    name = type(error).__name__
    logger.warning('Failed to %s: %r', action, error)
    if isinstance(error, IllegalStateError):
        return _illegal_state(
            'an instance either subscribes to topics or is assigned '
            'partitions; unsubscribe before switching',
        )
    if isinstance(error, CommitFailedError):
        return _illegal_state(
            'the group refused the commit: it rebalanced since this '
            'instance last fetched, or this instance is assigned '
            'partitions while other members of its group subscribe; fetch '
            'records again, then commit',
        )
    if isinstance(error, AuthorizationError):
        return error_reply(
            403,
            NOT_AUTHORIZED,
            f'Failed to {action}: the service is not authorized ({name})',
        )
    if isinstance(error, (NoBrokersAvailable, BrokerNotAvailableError)):
        return error_reply(
            503,
            BROKER_NOT_AVAILABLE,
            f'Failed to {action}: the Kafka cluster is not reachable; '
            'retry shortly',
        )
    if isinstance(error, UnknownTopicOrPartitionError):
        return error_reply(404, NOT_FOUND, 'Topic not found.')
    if isinstance(error, InvalidTopicError):
        return error_reply(400, INVALID_TOPIC, 'Invalid topic name.')
    if isinstance(error, ThrottlingQuotaExceededError):
        return error_reply(
            429,
            TOO_MANY_REQUESTS,
            f'Failed to {action}: the cluster throttled the request; '
            'retry shortly',
        )
    if isinstance(error, KafkaTimeoutError) or getattr(
        error,
        'retriable',
        False,
    ):
        return error_reply(
            500,
            KAFKA_RETRIABLE_ERROR,
            f'Failed to {action}: Kafka error ({name}); retry it',
        )
    return error_reply(
        500,
        KAFKA_ERROR,
        f'Failed to {action}: Kafka error ({name})',
    )


class ManagedConsumer:
    """Wrapper around KafkaConsumer with metadata and access tracking."""

    def __init__(
        self,
        consumer: KafkaConsumer,
        subject: str,
        namespace: str,
        group_name: str,
        instance_id: str,
        *,
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
        # Set, under the lock, once the consumer is closed; a request that
        # was waiting for the lock then finds the instance gone
        self.closed = False

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
        # Keys of instances whose KafkaConsumer is being built outside the
        # global lock; a create of the same name meanwhile gets 409
        self._pending: set[tuple[str, str, str, str]] = set()
        # Set by shutdown_all; creates from then on are refused
        self._closing = False
        self.lock_timeout_s = INSTANCE_LOCK_TIMEOUT_S
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

    def _in_namespace(
        self,
        namespace: str,
        kafka_topic: str,
    ) -> bool:
        """Check that a Kafka topic name belongs to the namespace."""
        if kafka_topic.startswith(f'{namespace}.'):
            return True
        logger.warning(
            'Dropping topic %s outside namespace %s',
            kafka_topic,
            namespace,
        )
        return False

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

    @staticmethod
    def _shutting_down() -> Reply:
        """The reply to a create once shutdown has begun."""
        return error_reply(
            500,
            KAFKA_ERROR,
            'Failed to create consumer: service is shutting down',
        )

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

    @contextmanager
    def _locked(
        self,
        managed: ManagedConsumer,
    ) -> Iterator[tuple[dict[str, Any], int] | None]:
        """Hold the instance's lock, or yield the error reply if it can't.

        Waits at most lock_timeout_s for another request on the same
        instance, then yields a 409 reply; yields a 404 reply if the
        instance was closed while waiting. Yields None once the lock is
        held, and releases it on exit.
        """
        if not managed.lock.acquire(timeout=self.lock_timeout_s):
            yield error_reply(
                409,
                INSTANCE_BUSY,
                'Consumer instance is busy with another request.',
            )
            return
        try:
            if managed.closed:
                yield _instance_not_found()
            else:
                yield None
        finally:
            managed.lock.release()

    @staticmethod
    def _close(managed: ManagedConsumer, error_message: str) -> None:
        """Close the instance's consumer; the caller holds its lock."""
        managed.closed = True
        try:
            managed.consumer.close(
                autocommit=False,
                timeout_ms=CONSUMER_OPERATION_TIMEOUT_MS,
            )
        except Exception:
            logger.exception(error_message, managed.instance_id)

    def _kafka_partitions(
        self,
        namespace: str,
        partitions: list[dict[str, Any]],
    ) -> list[TopicPartition] | Reply:
        """The Kafka partitions of a request's topics and partitions.

        Every topic has one partition, 0, so any other is refused.

        Returns:
            The partitions, or the 404 reply for one other than 0
        """
        for p in partitions:
            if p['partition'] != 0:
                return error_reply(
                    404,
                    PARTITION_NOT_FOUND,
                    f'Partition {p["partition"]} of topic {p["topic"]} not '
                    'found; every topic has one partition, 0',
                )
        return [
            TopicPartition(
                self._kafka_topic_name(namespace, p['topic']),
                p['partition'],
            )
            for p in partitions
        ]

    def _unassigned(
        self,
        namespace: str,
        managed: ManagedConsumer,
        tps: list[TopicPartition],
    ) -> Reply | None:
        """The 409 reply if a partition is not assigned to the instance.

        kafka-python asserts that a partition is assigned before it seeks
        on it; Confluent answers the same mistake with 409 and 40903.
        """
        assigned = managed.consumer.assignment() or set()
        if not tps and not assigned:
            return _illegal_state(
                'no partitions are assigned to this consumer instance',
            )
        for tp in tps:
            if tp not in assigned:
                topic = self._strip_namespace(namespace, tp.topic)
                return _illegal_state(
                    f'partition {tp.partition} of topic {topic} is not '
                    'assigned to this consumer instance',
                )
        return None

    def _serve(
        self,
        key: tuple[str, str, str, str],
        action: str,
        serve: Callable[[ManagedConsumer], tuple[Any, int]],
    ) -> tuple[Any, int]:
        """Run serve on the instance under its lock, mapping its errors.

        Args:
            key: The instance's subject, namespace, group and instance id
            action: What serve does, completing "Failed to ..."
            serve: Called with the instance; returns the reply's body and
                status

        Returns:
            serve's reply, or the reply for a missing or busy instance or
            for the exception serve raised
        """
        managed = self._get_consumer(*key)
        if managed is None:
            return _instance_not_found()
        with self._locked(managed) as error:
            if error:
                return error
            managed.touch()
            try:
                return serve(managed)
            except Exception as e:
                return kafka_failure(e, action)

    # --- Consumer lifecycle ---

    def _admit(self, subject: str) -> Reply | None:
        """The 429 reply if the subject or the service has its most instances.

        The caller holds the global lock. Instances being built count.
        """
        keys = [*self._consumers, *self._pending]
        if len(keys) >= MAX_INSTANCES:
            return error_reply(
                429,
                TOO_MANY_REQUESTS,
                f'The service holds {MAX_INSTANCES} consumer instances, the '
                'most it holds; retry later',
            )
        held = sum(1 for k in keys if k[0] == subject)
        if held >= MAX_INSTANCES_PER_SUBJECT:
            return error_reply(
                429,
                TOO_MANY_REQUESTS,
                f'You hold {held} consumer instances, the most one user may; '
                'delete one before you create another',
            )
        return None

    def create_consumer(  # noqa: PLR0911
        self,
        subject: str,
        namespace: str,
        group_name: str,
        *,
        name: str | None = None,
        format_type: str = 'binary',
        auto_offset_reset: str = 'latest',
        auto_commit_enable: str = 'false',
        fetch_min_bytes: str | None = None,
        consumer_request_timeout_ms: str | None = None,
    ) -> Reply:
        """Create a new consumer instance.

        A subject may hold MAX_INSTANCES_PER_SUBJECT instances, and the
        service MAX_INSTANCES; a create past either is refused with 429.

        Returns:
            Tuple of (response_body, http_status_code)
        """
        if not self.bootstrap_servers:
            return error_reply(
                500,
                KAFKA_ERROR,
                'Kafka endpoint is not configured',
            )

        instance_id = name or uuid.uuid4().hex[:12]
        key = self._consumer_key(
            subject,
            namespace,
            group_name,
            instance_id,
        )
        kafka_group_id = self._kafka_group_id(namespace, group_name)

        # Reserve the name, then build the consumer outside the global
        # lock: its bootstrap waits on the cluster, and every request on
        # every instance takes the global lock
        with self._global_lock:
            if self._closing:
                return self._shutting_down()
            if key in self._consumers or key in self._pending:
                return error_reply(
                    409,
                    INSTANCE_EXISTS,
                    'Consumer instance with the specified name already '
                    'exists.',
                )
            if refusal := self._admit(subject):
                return refusal
            self._pending.add(key)

        managed: ManagedConsumer | None = None
        try:
            try:
                kwargs = self._consumer_config(
                    kafka_group_id,
                    auto_offset_reset=auto_offset_reset,
                    auto_commit_enable=auto_commit_enable,
                    fetch_min_bytes=fetch_min_bytes,
                    consumer_request_timeout_ms=consumer_request_timeout_ms,
                )
                consumer = KafkaConsumer(**kwargs)
            except (KafkaConfigurationError, ValueError, TypeError) as e:
                logger.warning('Invalid consumer configuration: %r', e)
                return error_reply(
                    422,
                    INVALID_CONSUMER_CONFIG,
                    'Invalid consumer configuration: check '
                    'auto.offset.reset, auto.commit.enable, fetch.min.bytes '
                    'and consumer.request.timeout.ms',
                )
            except Exception as e:
                return kafka_failure(e, 'create consumer')

            managed = ManagedConsumer(
                consumer=consumer,
                subject=subject,
                namespace=namespace,
                group_name=group_name,
                instance_id=instance_id,
                format_type=format_type,
            )
        finally:
            # Swap the reservation for the instance, or drop it if the
            # build failed or shutdown began meanwhile
            with self._global_lock:
                self._pending.discard(key)
                closing = self._closing
                if managed is not None and not closing:
                    self._consumers[key] = managed

        if closing and managed is not None:
            with managed.lock:
                self._close(managed, 'Error closing consumer %s at shutdown')
            return self._shutting_down()

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

    def _consumer_config(
        self,
        kafka_group_id: str,
        *,
        auto_offset_reset: str,
        auto_commit_enable: str,
        fetch_min_bytes: str | None,
        consumer_request_timeout_ms: str | None,
    ) -> dict[str, Any]:
        """The KafkaConsumer settings of a new instance.

        Raises:
            ValueError: If a number setting is not a whole number
        """
        kwargs: dict[str, Any] = {
            'bootstrap_servers': self.bootstrap_servers,
            'group_id': kafka_group_id,
            'security_protocol': 'SASL_SSL',
            'sasl_mechanism': 'OAUTHBEARER',
            'sasl_oauth_token_provider': MSKTokenProvider(self.region),
            'auto_offset_reset': auto_offset_reset,
            'enable_auto_commit': auto_commit_enable.lower() == 'true',
            'fetch_max_bytes': FETCH_MAX_BYTES,
            'max_partition_fetch_bytes': MAX_PARTITION_FETCH_BYTES,
        }
        if fetch_min_bytes is not None:
            kwargs['fetch_min_bytes'] = int(fetch_min_bytes)
        if consumer_request_timeout_ms is not None:
            kwargs['request_timeout_ms'] = int(consumer_request_timeout_ms)
        return kwargs

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
        managed = self._get_consumer(*key)
        if managed is None:
            return _instance_not_found()
        with self._locked(managed) as error:
            if error:
                return error
            with self._global_lock:
                if self._consumers.get(key) is managed:
                    del self._consumers[key]
            self._close(managed, 'Error closing consumer %s')
        return (None, 204)

    # --- Subscription ---

    def subscribe(
        self,
        subject: str,
        namespace: str,
        group_name: str,
        instance_id: str,
        *,
        topics: list[str] | None = None,
    ) -> tuple[dict[str, Any] | None, int]:
        """Subscribe consumer to a list of topics.

        Topic patterns are not supported.

        Returns:
            Tuple of (response_body_or_None, http_status_code)
        """
        key = (subject, namespace, group_name, instance_id)
        if self._get_consumer(*key) is None:
            return _instance_not_found()
        if not topics:
            return error_reply(
                422,
                INVALID_CONSUMER_CONFIG,
                'Must provide a non-empty list of topics',
            )
        kafka_topics = [self._kafka_topic_name(namespace, t) for t in topics]

        def serve(managed: ManagedConsumer) -> tuple[None, int]:
            managed.consumer.subscribe(topics=kafka_topics)
            return (None, 204)

        return self._serve(key, 'subscribe', serve)

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

        def serve(managed: ManagedConsumer) -> tuple[dict[str, Any], int]:
            sub = managed.consumer.subscription() or set()
            user_topics = sorted(
                self._strip_namespace(namespace, t)
                for t in sub
                if self._in_namespace(namespace, t)
            )
            return ({'topics': user_topics}, 200)

        return self._serve(
            (subject, namespace, group_name, instance_id),
            'get the subscription',
            serve,
        )

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

        def serve(managed: ManagedConsumer) -> tuple[None, int]:
            managed.consumer.unsubscribe()
            return (None, 204)

        return self._serve(
            (subject, namespace, group_name, instance_id),
            'unsubscribe',
            serve,
        )

    # --- Records ---

    def poll_records(
        self,
        subject: str,
        namespace: str,
        group_name: str,
        instance_id: str,
        *,
        timeout_ms: int = 1000,
        max_bytes: int | None = None,
    ) -> tuple[list[dict[str, Any]] | dict[str, Any], int]:
        """Fetch records from the consumer.

        Returns:
            Tuple of (records_array_or_error, http_status_code)
        """

        def serve(
            managed: ManagedConsumer,
        ) -> tuple[list[dict[str, Any]], int]:
            raw = managed.consumer.poll(
                timeout_ms=timeout_ms,
                max_records=max_bytes,
            )
            records: list[dict[str, Any]] = []
            for tp, msgs in raw.items():
                if not self._in_namespace(namespace, tp.topic):
                    continue
                user_topic = self._strip_namespace(namespace, tp.topic)
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

        return self._serve(
            (subject, namespace, group_name, instance_id),
            'fetch records',
            serve,
        )

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

        Offsets may be committed only for partitions assigned to the
        instance; without offsets, it commits every record fetched.

        Returns:
            Tuple of (None_or_error, http_status_code)
        """
        tps: list[TopicPartition] = []
        if offsets:
            found = self._kafka_partitions(namespace, offsets)
            if isinstance(found, tuple):
                return found
            tps = found

        def serve(managed: ManagedConsumer) -> MaybeReply:
            if not offsets:
                managed.consumer.commit(
                    timeout_ms=CONSUMER_OPERATION_TIMEOUT_MS,
                )
                return (None, 200)
            if refusal := self._unassigned(namespace, managed, tps):
                return refusal
            managed.consumer.commit(
                offsets={
                    tp: OffsetAndMetadata(o['offset'], '', -1)
                    for tp, o in zip(tps, offsets, strict=True)
                },
                timeout_ms=CONSUMER_OPERATION_TIMEOUT_MS,
            )
            return (None, 200)

        return self._serve(
            (subject, namespace, group_name, instance_id),
            'commit offsets',
            serve,
        )

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
        found = self._kafka_partitions(namespace, partitions)
        if isinstance(found, tuple):
            return found
        tps = found

        def serve(managed: ManagedConsumer) -> tuple[dict[str, Any], int]:
            results = []
            # One deadline for all the partitions asked for
            deadline = time.monotonic() + CONSUMER_OPERATION_TIMEOUT_MS / 1000
            for p, tp in zip(partitions, tps, strict=True):
                remaining_ms = max(
                    0,
                    int((deadline - time.monotonic()) * 1000),
                )
                committed = managed.consumer.committed(
                    tp,
                    timeout_ms=remaining_ms,
                )
                results.append(
                    {
                        'topic': p['topic'],
                        'partition': p['partition'],
                        'offset': committed if committed is not None else -1,
                        'metadata': '',
                    },
                )
            return ({'offsets': results}, 200)

        return self._serve(
            (subject, namespace, group_name, instance_id),
            'get committed offsets',
            serve,
        )

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
        found = self._kafka_partitions(namespace, partitions)
        if isinstance(found, tuple):
            return found
        tps = found

        def serve(managed: ManagedConsumer) -> tuple[None, int]:
            managed.consumer.assign(tps)
            return (None, 204)

        return self._serve(
            (subject, namespace, group_name, instance_id),
            'assign partitions',
            serve,
        )

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

        def serve(managed: ManagedConsumer) -> tuple[dict[str, Any], int]:
            assigned = managed.consumer.assignment() or set()
            partitions = sorted(
                (
                    {
                        'topic': self._strip_namespace(namespace, tp.topic),
                        'partition': tp.partition,
                    }
                    for tp in assigned
                    if self._in_namespace(namespace, tp.topic)
                ),
                key=lambda x: (x['topic'], x['partition']),
            )
            return ({'partitions': partitions}, 200)

        return self._serve(
            (subject, namespace, group_name, instance_id),
            'get assignments',
            serve,
        )

    # --- Positions / Seek ---

    def seek(
        self,
        subject: str,
        namespace: str,
        group_name: str,
        instance_id: str,
        offsets: list[dict[str, Any]],
    ) -> tuple[dict[str, Any] | None, int]:
        """Seek to specific offsets of assigned partitions.

        Returns:
            Tuple of (None_or_error, http_status_code)
        """
        found = self._kafka_partitions(namespace, offsets)
        if isinstance(found, tuple):
            return found
        tps = found

        def serve(managed: ManagedConsumer) -> MaybeReply:
            if tps and (refusal := self._unassigned(namespace, managed, tps)):
                return refusal
            for tp, o in zip(tps, offsets, strict=True):
                managed.consumer.seek(tp, o['offset'])
            return (None, 204)

        return self._serve(
            (subject, namespace, group_name, instance_id),
            'seek',
            serve,
        )

    def seek_to_beginning(
        self,
        subject: str,
        namespace: str,
        group_name: str,
        instance_id: str,
        partitions: list[dict[str, Any]],
    ) -> tuple[dict[str, Any] | None, int]:
        """Seek to beginning of partitions.

        An empty list seeks every assigned partition.

        Returns:
            Tuple of (None_or_error, http_status_code)
        """
        return self._seek_to(
            (subject, namespace, group_name, instance_id),
            partitions,
            end=False,
        )

    def seek_to_end(
        self,
        subject: str,
        namespace: str,
        group_name: str,
        instance_id: str,
        partitions: list[dict[str, Any]],
    ) -> tuple[dict[str, Any] | None, int]:
        """Seek to end of partitions.

        An empty list seeks every assigned partition.

        Returns:
            Tuple of (None_or_error, http_status_code)
        """
        return self._seek_to(
            (subject, namespace, group_name, instance_id),
            partitions,
            end=True,
        )

    def _seek_to(
        self,
        key: tuple[str, str, str, str],
        partitions: list[dict[str, Any]],
        *,
        end: bool,
    ) -> tuple[dict[str, Any] | None, int]:
        """Seek assigned partitions to their beginning or their end."""
        namespace = key[1]
        found = self._kafka_partitions(namespace, partitions)
        if isinstance(found, tuple):
            return found
        tps = found

        def serve(managed: ManagedConsumer) -> MaybeReply:
            if refusal := self._unassigned(namespace, managed, tps):
                return refusal
            if end:
                managed.consumer.seek_to_end(*tps)
            else:
                managed.consumer.seek_to_beginning(*tps)
            return (None, 204)

        return self._serve(
            key,
            'seek to end' if end else 'seek to beginning',
            serve,
        )

    # --- Lifecycle management ---

    def _reap_expired_consumers(self) -> None:
        """Background thread to clean up idle consumers."""
        while True:
            time.sleep(REAPER_INTERVAL_SECONDS)
            self._reap_once()

    def _reap_once(self) -> None:
        """Close the consumers idle for longer than the timeout.

        Each is closed under its own lock and outside the global lock. An
        instance still serving a request (a fetch longer than the idle
        timeout) is kept for a later round rather than closed mid-request.
        """
        with self._global_lock:
            candidates = [
                (key, managed)
                for key, managed in self._consumers.items()
                if managed.is_expired(self.timeout_ms)
            ]
        for key, managed in candidates:
            if not managed.lock.acquire(timeout=self.lock_timeout_s):
                continue
            try:
                # A request may have used it since the scan, or a delete
                # removed it
                with self._global_lock:
                    reap = self._consumers.get(
                        key,
                    ) is managed and managed.is_expired(self.timeout_ms)
                    if reap:
                        del self._consumers[key]
                if not reap:
                    continue
                self._close(managed, 'Error closing expired consumer %s')
                logger.info(
                    'Reaped expired consumer: %s in group %s',
                    managed.instance_id,
                    managed.group_name,
                )
            finally:
                managed.lock.release()

    def shutdown_all(self) -> None:
        """Close all consumers. Called on application shutdown.

        Each is closed under its own lock and outside the global lock; one
        still busy after lock_timeout_s is left unclosed. A create still
        building its consumer closes it instead of registering it.
        """
        with self._global_lock:
            self._closing = True
            consumers = list(self._consumers.values())
            self._consumers.clear()
        for managed in consumers:
            if not managed.lock.acquire(timeout=self.lock_timeout_s):
                logger.warning(
                    'Consumer %s busy at shutdown; not closed',
                    managed.instance_id,
                )
                continue
            try:
                self._close(managed, 'Error closing consumer %s on shutdown')
            finally:
                managed.lock.release()
