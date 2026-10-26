"""Consumer lifecycle management service.

Implements Confluent Kafka REST Proxy v2 consumer semantics:
in-memory consumer instances with idle timeout reaping.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import threading
import time
import uuid
from collections.abc import AsyncIterator
from collections.abc import Callable
from collections.abc import Iterator
from contextlib import asynccontextmanager
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

from kafka import KafkaConsumer
from kafka.client_async import KafkaClient
from kafka.errors import AuthorizationError
from kafka.errors import BrokerNotAvailableError
from kafka.errors import CommitFailedError
from kafka.errors import IllegalStateError
from kafka.errors import InvalidConfigurationError
from kafka.errors import InvalidPartitionsError
from kafka.errors import InvalidReplicationFactorError
from kafka.errors import InvalidRequestError
from kafka.errors import InvalidTopicError
from kafka.errors import KafkaError
from kafka.errors import KafkaTimeoutError
from kafka.errors import NoBrokersAvailable
from kafka.errors import PolicyViolationError
from kafka.errors import SecurityDisabledError
from kafka.errors import ThrottlingQuotaExceededError
from kafka.errors import TopicAlreadyExistsError
from kafka.errors import TopicDeletionDisabledError
from kafka.errors import UnknownError
from kafka.errors import UnknownTopicOrPartitionError
from kafka.errors import UnsupportedVersionError
from kafka.structs import OffsetAndMetadata
from kafka.structs import TopicPartition

from web_service.consumer_records import EncodedField
from web_service.consumer_records import EncodedRecord
from web_service.consumer_records import FetchReply
from web_service.consumer_records import NotText
from web_service.services import MSKTokenProvider

logger = logging.getLogger(__name__)

# Default idle timeout: 5 minutes
DEFAULT_CONSUMER_TIMEOUT_MS = 300_000
REAPER_INTERVAL_SECONDS = 30
# Time beyond the idle timeout and a reaper round that an instance may go
# without a poll before kafka-python takes it out of its group: enough for
# the reaper to wait for the instance's lock and commit
POLL_SLACK_MS = 60_000
# Longest a commit or a committed-offsets lookup waits on the cluster;
# without a timeout kafka-python retries until it succeeds
CONSUMER_OPERATION_TIMEOUT_MS = 10_000
# When an instance with auto-commit is closed (by a delete, the reaper or
# shutdown), the longest the commit of its positions waits, and then the
# longest kafka-python's close waits for its two steps together: stopping
# the heartbeat thread and leaving the group
CLOSE_COMMIT_TIMEOUT_MS = 5_000
CLOSE_TIMEOUT_MS = 4_000
# Longest shutdown waits for all the instances together, which it closes
# at once, each commit and close within what is left of it. The service's
# shutdown closes the producer at the same time, so both fit in the time
# gunicorn's graceful timeout leaves after uvicorn has waited for the
# requests in progress.
SHUTDOWN_TIMEOUT_S = 8
# Longest a request waits for an instance that another request is using
# (a fetch holds it for up to its timeout) before it is told to retry;
# longer than the default fetch wait, so that never yields a 409
INSTANCE_LOCK_TIMEOUT_S = 10.0
# Most bytes one fetch asks the cluster for, in all and per partition.
# kafka-python's defaults (50 MiB and 1 MiB) let one fetch of a 50-topic
# subscription hold about 50 MiB, which the fetcher keeps until every
# record is returned. A broker still returns a first batch larger than
# these whole, so each broker's reply can hold a record of up to 8 MiB.
FETCH_MAX_BYTES = 1024 * 1024
MAX_PARTITION_FETCH_BYTES = 1024 * 1024
# Most bytes of JSON a fetch returns. Past it, the records not returned
# are fetched again by the next fetch; a fetch's first record is always
# returned, however large. 2 MiB holds the base64 of a full 1 MiB fetch.
REPLY_MAX_BYTES = 2 * 1024 * 1024
# Instances one subject may hold, and the service in all. Each is a
# KafkaClient with its own heartbeat thread and sockets to the brokers.
# After a fetch, kafka-python keeps each broker's next reply for the next
# fetch, up to 8 MiB a broker, however long the instance sits idle, and
# pausing the instance's partitions does not drop it. 16 instances on two
# brokers can hold 256 MiB this way; see the consumer routes' memory in
# docs/architecture.md.
MAX_INSTANCES_PER_SUBJECT = 4
MAX_INSTANCES = 16
# Fetch wait when neither the fetch nor the instance names one, and the
# longest either may name
DEFAULT_FETCH_WAIT_MS = 5_000
MAX_FETCH_WAIT_MS = 30_000
# auto.offset.reset values Confluent takes, which the service takes in any
# case. kafka-python takes them in any case too, but takes a value it does
# not know as none, with only a log line, so the service checks them.
OFFSET_RESETS = ('earliest', 'latest', 'none')
# Instance names: they go in URLs, so letters, digits, dots, dashes and
# underscores, starting with a letter or digit (so never . or ..)
INSTANCE_NAME = re.compile(r'[A-Za-z0-9][A-Za-z0-9._-]{0,127}')

# Error codes of the consumer routes: Confluent's, from kafka-rest's
# Errors.java and rest-utils' KafkaExceptionMapper, except 40904.
# kafka_failure notes where a Kafka error gets another code than there.
INVALID_TOPIC = 40002
KAFKA_BAD_REQUEST = 40002
NOT_AUTHORIZED = 40301
NOT_FOUND = 40401
PARTITION_NOT_FOUND = 40402
INSTANCE_NOT_FOUND = 40403
FORMAT_MISMATCH = 40601
# KafkaRestExceptionMapper's code for a record that failed to deserialize
RECORD_NOT_DECODABLE = 40801
INSTANCE_EXISTS = 40902
ILLEGAL_STATE = 40903
INSTANCE_BUSY = 40904  # Octopus's own
INVALID_CONSUMER_CONFIG = 42204
TOO_MANY_REQUESTS = 42901
SERVER_ERROR = 500  # rest-utils' GenericExceptionMapper
# rest-utils' WebApplicationExceptionMapper: an HTTP error without a code of
# its own has its status as its code
SHUTTING_DOWN = 503
KAFKA_ERROR = 50002
KAFKA_RETRIABLE_ERROR = 50003
BROKER_NOT_AVAILABLE = 50302

Reply = tuple[dict[str, Any], int]
# A reply that may have no body (204)
MaybeReply = tuple[dict[str, Any] | None, int]

# The Kafka errors KafkaExceptionMapper answers with 400 and 40002
_BAD_REQUEST_ERRORS = (
    InvalidConfigurationError,
    InvalidPartitionsError,
    InvalidReplicationFactorError,
    InvalidRequestError,
    InvalidTopicError,
    PolicyViolationError,
    SecurityDisabledError,
    TopicAlreadyExistsError,
    TopicDeletionDisabledError,
    UnknownError,
    UnsupportedVersionError,
)


def error_reply(status: int, error_code: int, message: str) -> Reply:
    """An error reply's body and HTTP status."""
    return {'error_code': error_code, 'message': message}, status


def _instance_not_found() -> Reply:
    return error_reply(404, INSTANCE_NOT_FOUND, 'Consumer instance not found.')


def _illegal_state(message: str) -> Reply:
    return error_reply(409, ILLEGAL_STATE, f'Illegal state: {message}')


def _busy() -> Reply:
    return error_reply(
        409,
        INSTANCE_BUSY,
        'Consumer instance is busy with another request.',
    )


def _not_decodable(topic: str, partition: int, offset: int, why: str) -> Reply:
    """The reply for a fetch whose first record a json instance cannot return.

    Confluent's REST Proxy answers a record that fails to deserialize with
    408 and 40801 (KafkaRestExceptionMapper). The record is not skipped,
    so the client seeks past it or reads it with a binary instance.
    """
    return error_reply(
        408,
        RECORD_NOT_DECODABLE,
        f'The key or value of the record at offset {offset} of topic '
        f'{topic}, partition {partition}, {why}. A json instance returns '
        'only JSON and UTF-8 text: seek past the record, or read it with '
        'a binary instance.',
    )


def _invalid_config(setting: str, rule: str) -> Reply:
    """The 422 reply for a create setting that breaks its rule."""
    # Confluent's message for the same refusal starts this way
    return error_reply(
        422,
        INVALID_CONSUMER_CONFIG,
        f'Invalid consumer configuration: {setting} {rule}',
    )


def _whole_number(value: int | str | None) -> int | None:
    """Value as a whole number of at least 0, or None if it is not one."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        number = value
    else:
        try:
            number = int(str(value).strip())
        except ValueError:
            return None
    return number if number >= 0 else None


def kafka_failure(error: Exception, action: str) -> Reply:  # noqa: C901, PLR0911
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
    if isinstance(error, _BAD_REQUEST_ERRORS):
        return error_reply(
            400,
            KAFKA_BAD_REQUEST,
            f'Failed to {action}: the cluster refused the request ({name})',
        )
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


def _wake(waiter: asyncio.Future[None]) -> None:
    if not waiter.done():
        waiter.set_result(None)


def _check_settings(
    *,
    auto_offset_reset: str,
    auto_commit_enable: str,
    fetch_min_bytes: int | str | None,
    consumer_request_timeout_ms: int | str | None,
) -> Reply | tuple[str, int | None, int | None]:
    """Check a create's settings, as Confluent's REST Proxy does.

    Returns:
        auto.offset.reset in lower case, and fetch.min.bytes and
        consumer.request.timeout.ms as numbers (None when not given), or
        the 422 reply naming the first setting that breaks its rule
    """
    reset = auto_offset_reset.strip().lower()
    if reset not in OFFSET_RESETS:
        return _invalid_config(
            'auto.offset.reset',
            'must be earliest, latest or none',
        )
    if auto_commit_enable.strip().lower() not in ('true', 'false'):
        return _invalid_config('auto.commit.enable', 'must be true or false')
    numbers: list[int | None] = []
    for setting, value in (
        ('fetch.min.bytes', fetch_min_bytes),
        ('consumer.request.timeout.ms', consumer_request_timeout_ms),
    ):
        number = None if value is None else _whole_number(value)
        if value is not None and number is None:
            return _invalid_config(
                setting,
                'must be a whole number of at least 0',
            )
        numbers.append(number)
    return reset, numbers[0], numbers[1]


class InstanceLock:
    """A consumer instance's lock, for threads and for the event loop.

    acquire, release and locked work as threading.Lock's do, and any thread
    may release it. A request route waits for it on the event loop with
    acquire_async, so a request waiting for a busy instance holds no
    thread. Both kinds of waiter take the same lock, so the reaper, a
    delete and every request see one held state.
    """

    def __init__(self) -> None:
        """Make an unlocked lock."""
        self._condition = threading.Condition(threading.Lock())
        self._held = False
        self._waiters: list[
            tuple[asyncio.AbstractEventLoop, asyncio.Future[None]]
        ] = []

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        """Take the lock, waiting at most timeout seconds (-1: no limit)."""
        deadline = None if timeout < 0 else time.monotonic() + timeout
        with self._condition:
            while self._held:
                if not blocking:
                    return False
                remaining = None
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                self._condition.wait(remaining)
            self._held = True
            return True

    async def acquire_async(self, timeout: float) -> bool:
        """Take the lock, waiting on the event loop at most timeout seconds."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            with self._condition:
                if not self._held:
                    self._held = True
                    return True
                waiter: asyncio.Future[None] = loop.create_future()
                self._waiters.append((loop, waiter))
            try:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    return False
                try:
                    await asyncio.wait_for(waiter, remaining)
                except TimeoutError:
                    return False
            finally:
                with self._condition:
                    if (loop, waiter) in self._waiters:
                        self._waiters.remove((loop, waiter))

    def release(self) -> None:
        """Free the lock and wake its waiters, whichever thread they wait on.

        Raises:
            RuntimeError: If the lock is not held
        """
        with self._condition:
            if not self._held:
                raise RuntimeError('release unlocked lock')
            self._held = False
            self._condition.notify()
            waiters, self._waiters = self._waiters, []
        # Each waiter tries again; one takes the lock
        for loop, waiter in waiters:
            loop.call_soon_threadsafe(_wake, waiter)

    def locked(self) -> bool:
        """Whether the lock is held."""
        return self._held

    def __enter__(self) -> InstanceLock:
        """Take the lock, waiting as long as it takes."""
        self.acquire()
        return self

    def __exit__(self, *exc: object) -> None:
        """Free the lock."""
        self.release()


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
        default_wait_ms: int | None = None,
        auto_commit: bool = False,
    ) -> None:
        """Initialize a managed consumer.

        Args:
            consumer: The underlying KafkaConsumer
            subject: Authenticated user subject
            namespace: Namespace scope
            group_name: Consumer group name
            instance_id: Unique instance identifier
            format_type: Deserialization format (binary/json)
            default_wait_ms: How long a fetch that names no timeout waits
                for records (consumer.request.timeout.ms), or None for
                the service's default
            auto_commit: Whether the consumer commits its positions
                itself (auto.commit.enable), and so when it is closed
        """
        self.consumer = consumer
        self.subject = subject
        self.namespace = namespace
        self.group_name = group_name
        self.instance_id = instance_id
        self.format_type = format_type
        self.default_wait_ms = default_wait_ms
        self.auto_commit = auto_commit
        self.lock = InstanceLock()
        # Held by the thread that uses the Kafka consumer, which is not
        # thread-safe. The lock above is the request's; a request cancelled
        # at shutdown lets it go while its thread still polls, so a close
        # also waits for this one.
        self.calls = threading.RLock()
        self.last_access = time.monotonic()
        # Fetches that found records so far, which rotate the partition a
        # reply starts from
        self.fetches = 0
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


class FetchAttempt:
    """One poll of a fetch, which the fetch's request may abandon.

    A request cancelled while its poll runs in a thread (as uvicorn does to
    requests still open late in shutdown) never gets the reply, and the
    poll has moved the instance's positions past its records. Whichever of
    the two finishes second, the poll's thread or the abandoning request,
    seeks the instance back to the reply's first records, so a commit at
    close cannot commit records the client never got.
    """

    def __init__(self) -> None:
        """Make an attempt that is neither done nor abandoned."""
        self._lock = threading.Lock()
        self._abandoned = False
        self._reply: FetchReply | None = None

    def keep(self, reply: FetchReply) -> bool:
        """Hand the poll's reply to the request; False if it was abandoned."""
        with self._lock:
            if self._abandoned:
                return False
            self._reply = reply
            return True

    def abandon(self) -> FetchReply | None:
        """Abandon the attempt; the reply, if the poll handed it over."""
        with self._lock:
            self._abandoned = True
            reply, self._reply = self._reply, None
            return reply


# The instance a request route holds while its call runs in a thread: the
# route took the instance's lock on the event loop, so the call must not
# take it again
_HELD: ContextVar[ManagedConsumer | None] = ContextVar(
    'held_instance',
    default=None,
)


def run_holding[T](managed: Any, call: Callable[[], T]) -> T:
    """Run call as the holder of managed's lock, which the caller holds."""
    token = _HELD.set(managed)
    try:
        return call()
    finally:
        _HELD.reset(token)


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
            503,
            SHUTTING_DOWN,
            'Failed to create consumer: the service is shutting down; retry',
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
        held, and releases it on exit. A request route that already holds
        the lock (run_holding) uses it as it is.
        """
        if _HELD.get() is managed:
            yield _instance_not_found() if managed.closed else None
            return
        if not managed.lock.acquire(timeout=self.lock_timeout_s):
            yield _busy()
            return
        try:
            if managed.closed:
                yield _instance_not_found()
            else:
                yield None
        finally:
            managed.lock.release()

    def lookup(
        self,
        key: tuple[str, str, str, str],
    ) -> ManagedConsumer | None:
        """The instance of a subject, namespace, group and instance id."""
        return self._get_consumer(*key)

    @asynccontextmanager
    async def hold(
        self,
        key: tuple[str, str, str, str],
        *,
        wait: bool = True,
    ) -> AsyncIterator[ManagedConsumer | Reply]:
        """Hold an instance's lock on the event loop, for a request route.

        Yields the instance once its lock is held, and releases the lock on
        exit. Yields the 404 reply if there is no such instance or it was
        closed meanwhile, and the 409 reply if another request holds it:
        after lock_timeout_s, or at once if wait is false. Waiting holds no
        thread. The route runs its calls with run_holding.
        """
        managed = self._get_consumer(*key)
        if managed is None:
            yield _instance_not_found()
            return
        if wait:
            held = await managed.lock.acquire_async(self.lock_timeout_s)
        else:
            held = managed.lock.acquire(blocking=False)
        if not held:
            yield _busy()
            return
        try:
            yield _instance_not_found() if managed.closed else managed
        finally:
            managed.lock.release()

    @staticmethod
    def _close(
        managed: ManagedConsumer,
        error_message: str,
        *,
        deadline: float | None = None,
    ) -> None:
        """Close the instance's consumer; the caller holds its lock.

        As in Confluent's REST Proxy, an instance with auto.commit.enable
        first commits its positions, the next record of each partition its
        fetches returned. The commit waits at most CLOSE_COMMIT_TIMEOUT_MS,
        and a failed commit is logged and the close goes on. An instance
        without auto-commit commits nothing. Both wait first for a thread
        still using the consumer, at most CLOSE_COMMIT_TIMEOUT_MS; if it
        is still busy then, the consumer is left open. A deadline
        (time.monotonic()) shortens every wait to the time left before it.
        """
        managed.closed = True

        def within(timeout_ms: int) -> int:
            if deadline is None:
                return timeout_ms
            left_ms = int((deadline - time.monotonic()) * 1000)
            return max(0, min(timeout_ms, left_ms))

        if not managed.calls.acquire(
            timeout=within(CLOSE_COMMIT_TIMEOUT_MS) / 1000,
        ):
            logger.warning(
                'Consumer %s still in use by a thread; not committed or '
                'closed',
                managed.instance_id,
            )
            return
        try:
            if managed.auto_commit:
                try:
                    managed.consumer.commit(
                        timeout_ms=within(CLOSE_COMMIT_TIMEOUT_MS),
                    )
                except Exception as e:
                    logger.warning(
                        'Failed to commit the positions of consumer %s '
                        'before closing it: %r',
                        managed.instance_id,
                        e,
                    )
            try:
                # kafka-python waits up to timeout_ms for each of its two
                # steps
                managed.consumer.close(
                    autocommit=False,
                    timeout_ms=within(CLOSE_TIMEOUT_MS) // 2,
                )
            except Exception:
                logger.exception(error_message, managed.instance_id)
        finally:
            managed.calls.release()

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
            with managed.calls:
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
        fetch_min_bytes: int | str | None = None,
        consumer_request_timeout_ms: int | str | None = None,
    ) -> Reply:
        """Create a new consumer instance.

        A subject may hold MAX_INSTANCES_PER_SUBJECT instances, and the
        service MAX_INSTANCES; a create past either is refused with 429.
        A name must match INSTANCE_NAME, and one is made if none is given.
        consumer.request.timeout.ms is, as in Confluent's REST Proxy, how
        long a fetch that names no timeout waits for records; it is not
        passed to the Kafka consumer.

        Returns:
            The instance's id and 200, or an error reply and its status;
            the route adds the instance's base_uri
        """
        if not self.bootstrap_servers:
            return error_reply(
                500,
                KAFKA_ERROR,
                'Kafka endpoint is not configured',
            )
        settings = _check_settings(
            auto_offset_reset=auto_offset_reset,
            auto_commit_enable=auto_commit_enable,
            fetch_min_bytes=fetch_min_bytes,
            consumer_request_timeout_ms=consumer_request_timeout_ms,
        )
        if isinstance(settings[0], dict):
            return settings
        auto_offset_reset, min_bytes, wait_ms = settings
        if name is not None and not INSTANCE_NAME.fullmatch(name):
            return _invalid_config(
                'name',
                'must be 1 to 128 letters, digits, dots, dashes or '
                'underscores, starting with a letter or digit',
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
            consumer = self._build(
                kafka_group_id,
                auto_offset_reset=auto_offset_reset,
                auto_commit_enable=auto_commit_enable,
                fetch_min_bytes=min_bytes,
            )
            if isinstance(consumer, tuple):
                return consumer
            managed = ManagedConsumer(
                consumer=consumer,
                subject=subject,
                namespace=namespace,
                group_name=group_name,
                instance_id=instance_id,
                format_type=format_type,
                default_wait_ms=wait_ms,
                auto_commit=auto_commit_enable.strip().lower() == 'true',
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

        return ({'instance_id': instance_id}, 200)

    def _build(
        self,
        kafka_group_id: str,
        *,
        auto_offset_reset: str,
        auto_commit_enable: str,
        fetch_min_bytes: int | None,
    ) -> KafkaConsumer | Reply:
        """Build an instance's KafkaConsumer, or the reply if that fails.

        KafkaConsumer builds its KafkaClient, which starts connecting,
        before it finishes checking its settings; if it then raises, the
        client is closed here, since nothing else holds it.
        """
        clients: list[KafkaClient] = []

        def client(**config: Any) -> KafkaClient:
            built = KafkaClient(**config)
            clients.append(built)
            return built

        kwargs = self._consumer_config(
            kafka_group_id,
            auto_offset_reset=auto_offset_reset,
            auto_commit_enable=auto_commit_enable,
            fetch_min_bytes=fetch_min_bytes,
        )
        kwargs['kafka_client'] = client
        try:
            return KafkaConsumer(**kwargs)
        except Exception as e:
            for built in clients:
                try:
                    built.close()
                except Exception:
                    logger.exception('Error closing a half-built consumer')
            return kafka_failure(e, 'create consumer')

    def _consumer_config(
        self,
        kafka_group_id: str,
        *,
        auto_offset_reset: str,
        auto_commit_enable: str,
        fetch_min_bytes: int | None,
    ) -> dict[str, Any]:
        """The KafkaConsumer settings of a new instance."""
        kwargs: dict[str, Any] = {
            'bootstrap_servers': self.bootstrap_servers,
            'group_id': kafka_group_id,
            'security_protocol': 'SASL_SSL',
            'sasl_mechanism': 'OAUTHBEARER',
            'sasl_oauth_token_provider': MSKTokenProvider(self.region),
            'auto_offset_reset': auto_offset_reset,
            'enable_auto_commit': (
                auto_commit_enable.strip().lower() == 'true'
            ),
            'fetch_max_bytes': FETCH_MAX_BYTES,
            'max_partition_fetch_bytes': MAX_PARTITION_FETCH_BYTES,
            # kafka-python's default (300 s, the default idle timeout) has
            # the heartbeat thread leave the group before the reaper closes
            # an idle instance, so the commit at close would fail. Longer,
            # the instance stays in its group until the reaper closes it.
            'max_poll_interval_ms': (
                self.timeout_ms
                + REAPER_INTERVAL_SECONDS * 1000
                + POLL_SLACK_MS
            ),
        }
        if fetch_min_bytes is not None:
            kwargs['fetch_min_bytes'] = fetch_min_bytes
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

    def fetch_records(
        self,
        subject: str,
        namespace: str,
        group_name: str,
        instance_id: str,
        *,
        timeout_ms: int,
        max_bytes: int | None = None,
        attempt: FetchAttempt | None = None,
    ) -> tuple[FetchReply | dict[str, Any], int]:
        """Fetch records, as the reply that returns them.

        The reply holds the records in the order kafka-python returned
        them, up to REPLY_MAX_BYTES of JSON and, as in Confluent's REST
        Proxy, fewer than max_bytes bytes of keys and values. The first
        record is always held, however large. Each partition whose records
        are not all held is sought back to the first record not held, so
        the next fetch returns it.

        Args:
            subject: The instance's subject
            namespace: The instance's namespace
            group_name: The instance's group
            instance_id: The instance's id
            timeout_ms: How long to wait for records
            max_bytes: Bytes of keys and values the records must stay
                under, or None for no limit but REPLY_MAX_BYTES
            attempt: The request's attempt; if the request abandoned it,
                the records are sought back to

        Returns:
            The reply and 200, or an error reply and its status: 408 if a
            json instance's poll found records and the first it could
            return is one whose key or value is neither JSON nor UTF-8
        """

        def serve(
            managed: ManagedConsumer,
        ) -> tuple[FetchReply | dict[str, Any], int]:
            raw = managed.consumer.poll(timeout_ms=timeout_ms)
            reply = self._reply(namespace, managed, raw, max_bytes)
            if attempt is not None and not attempt.keep(reply):
                self.unfetch(managed, reply)
            if not reply.records and reply.undecodable:
                return _not_decodable(*reply.undecodable)
            return (reply, 200)

        return self._serve(
            (subject, namespace, group_name, instance_id),
            'fetch records',
            serve,
        )

    def _reply(
        self,
        namespace: str,
        managed: ManagedConsumer,
        raw: dict[TopicPartition, list[Any]],
        max_bytes: int | None = None,
    ) -> FetchReply:
        """The reply of fetched records, rewinding the ones it leaves out.

        The partitions take turns to come first, one fetch after another,
        so a partition whose records fill every reply cannot keep the
        others out. Within a partition the records keep their order. A
        record a json instance cannot return ends its partition's part of
        the reply, and the partition is sought back to it.
        """
        records: list[EncodedRecord] = []
        starts: dict[TopicPartition, int] = {}
        size = 2
        payload = 0
        full = False
        undecodable: tuple[str, int, int, str] | None = None
        rewind: dict[TopicPartition, int] = {}
        order = sorted(
            (tp for tp in raw if self._in_namespace(namespace, tp.topic)),
            key=lambda tp: (tp.topic, tp.partition),
        )
        if order:
            turn = managed.fetches % len(order)
            order = order[turn:] + order[:turn]
            # Only replies with records take a turn, so an empty poll does
            # not skip a partition's turn
            managed.fetches += 1
        for tp in order:
            topic = self._strip_namespace(namespace, tp.topic)
            for msg in raw[tp]:
                if full:
                    rewind[tp] = msg.offset
                    break
                try:
                    record = self._record(
                        topic,
                        tp.partition,
                        msg,
                        managed.format_type,
                    )
                except NotText as e:
                    logger.warning(
                        'Instance %s cannot return offset %d of %s, whose '
                        'key or value %s',
                        managed.instance_id,
                        msg.offset,
                        tp.topic,
                        e,
                    )
                    rewind[tp] = msg.offset
                    if undecodable is None:
                        undecodable = (topic, tp.partition, msg.offset, str(e))
                    break
                grown = size + record.size + (1 if records else 0)
                taken = payload + record.payload_size
                if records and (
                    grown > REPLY_MAX_BYTES
                    or (max_bytes is not None and taken >= max_bytes)
                ):
                    rewind[tp] = msg.offset
                    full = True
                    break
                records.append(record)
                starts.setdefault(tp, msg.offset)
                size = grown
                payload = taken
        for tp, offset in rewind.items():
            managed.consumer.seek(tp, offset)
        return FetchReply(records, starts, undecodable)

    @staticmethod
    def _record(
        topic: str,
        partition: int,
        msg: Any,
        format_type: str,
    ) -> EncodedRecord:
        """A fetched record as the reply holds it.

        Raises:
            NotText: In the json format, if its key or value is neither
                JSON nor UTF-8 text
        """
        return EncodedRecord(
            topic=topic,
            key=EncodedField.of(msg.key, format_type),
            value=EncodedField.of(msg.value, format_type),
            partition=partition,
            offset=msg.offset,
            headers=tuple(msg.headers or ()),
        )

    @staticmethod
    def unfetch(managed: ManagedConsumer, reply: FetchReply) -> None:
        """Seek back to a reply's first records, which the client never got.

        The caller holds the instance's lock. Nothing commits the
        positions the reply moved past before this: kafka-python commits,
        auto-commit included, only inside poll(), commit(), assign(),
        unsubscribe() and close(), and the service calls each of them
        under the lock.
        """
        for tp, offset in reply.starts.items():
            try:
                managed.consumer.seek(tp, offset)
            except Exception:
                logger.exception(
                    'Error seeking %s back to %d for instance %s',
                    tp,
                    offset,
                    managed.instance_id,
                )

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
        """Fetch records, as the dictionaries the reply's JSON holds.

        Args:
            subject: The instance's subject
            namespace: The instance's namespace
            group_name: The instance's group
            instance_id: The instance's id
            timeout_ms: How long to wait for records
            max_bytes: Bytes of keys and values the records must stay
                under; None or a value of 0 or less sets no limit

        Returns:
            Tuple of (records_array_or_error, http_status_code)
        """
        reply, status = self.fetch_records(
            subject,
            namespace,
            group_name,
            instance_id,
            timeout_ms=timeout_ms,
            max_bytes=max_bytes if max_bytes and max_bytes > 0 else None,
        )
        if not isinstance(reply, FetchReply):
            return reply, status
        return [r.as_dict() for r in reply.records], status

    # --- Offsets ---

    def commit_offsets(
        self,
        subject: str,
        namespace: str,
        group_name: str,
        instance_id: str,
        offsets: list[dict[str, Any]] | None = None,
    ) -> tuple[list[Any] | dict[str, Any], int]:
        """Commit offsets for the consumer.

        Offsets may be committed only for partitions assigned to the
        instance; without offsets, it commits every record fetched. As in
        Confluent's REST Proxy, an offset is that of the last record
        processed, so the group's committed offset, the next record to
        read, is one more; a commit of every record fetched commits the
        positions, which are already the next records to read. Each offset
        may carry metadata, which the group stores with it.

        Returns:
            [] and 200, as Confluent's REST Proxy answers a commit, or an
            error reply and its status
        """
        tps: list[TopicPartition] = []
        if offsets:
            found = self._kafka_partitions(namespace, offsets)
            if isinstance(found, tuple):
                return found
            tps = found

        def serve(
            managed: ManagedConsumer,
        ) -> tuple[list[Any] | dict[str, Any], int]:
            if not offsets:
                managed.consumer.commit(
                    timeout_ms=CONSUMER_OPERATION_TIMEOUT_MS,
                )
                return ([], 200)
            if refusal := self._unassigned(namespace, managed, tps):
                return refusal
            managed.consumer.commit(
                offsets={
                    tp: OffsetAndMetadata(
                        o['offset'] + 1,
                        o.get('metadata') or '',
                        -1,
                    )
                    for tp, o in zip(tps, offsets, strict=True)
                },
                timeout_ms=CONSUMER_OPERATION_TIMEOUT_MS,
            )
            return ([], 200)

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

        As in Confluent's REST Proxy, a partition the group has never
        committed is left out of the reply.

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
                    metadata=True,
                    timeout_ms=remaining_ms,
                )
                if committed is None:
                    continue
                results.append(
                    {
                        'topic': p['topic'],
                        'partition': p['partition'],
                        'offset': committed.offset,
                        'metadata': committed.metadata or '',
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
        instance whose lock a request holds (a fetch longer than the idle
        timeout, or a request waiting for its turn) is kept for a later
        round rather than closed mid-request.
        """
        with self._global_lock:
            candidates = [
                (key, managed)
                for key, managed in self._consumers.items()
                if managed.is_expired(self.timeout_ms)
            ]
        for key, managed in candidates:
            # One serving or waiting for a request is not idle
            if not managed.lock.acquire(blocking=False):
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

        The instances are closed at once, each on a thread of its own,
        under its own lock and outside the global lock; those with
        auto-commit commit their positions first. Shutdown waits at most
        SHUTDOWN_TIMEOUT_S for them all: an instance still busy, or still
        committing or closing, by then is left as it is. A create still
        building its consumer closes it instead of registering it.
        """
        with self._global_lock:
            self._closing = True
            consumers = list(self._consumers.values())
            self._consumers.clear()
        deadline = time.monotonic() + SHUTDOWN_TIMEOUT_S
        closers = [
            threading.Thread(
                target=self._close_at_shutdown,
                args=(managed, deadline),
                name=f'close-consumer-{managed.instance_id}',
                daemon=True,
            )
            for managed in consumers
        ]
        for closer in closers:
            closer.start()
        for closer in closers:
            closer.join(max(0.0, deadline - time.monotonic()))
        if left := sum(closer.is_alive() for closer in closers):
            logger.warning(
                '%d consumers not closed within %d s of shutdown',
                left,
                SHUTDOWN_TIMEOUT_S,
            )

    def _close_at_shutdown(
        self,
        managed: ManagedConsumer,
        deadline: float,
    ) -> None:
        """Close one instance for shutdown_all, by the deadline."""
        wait = min(self.lock_timeout_s, deadline - time.monotonic())
        if not managed.lock.acquire(timeout=max(0.0, wait)):
            logger.warning(
                'Consumer %s busy at shutdown; not closed',
                managed.instance_id,
            )
            return
        try:
            self._close(
                managed,
                'Error closing consumer %s on shutdown',
                deadline=deadline,
            )
        finally:
            managed.lock.release()
