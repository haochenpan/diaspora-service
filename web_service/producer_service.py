"""Produce records to Kafka for the REST API.

Implements the produce requests of Confluent's REST API v3 with the
BINARY, JSON and STRING formats: one shared KafkaProducer per process,
built on first use, and replies only once the cluster acknowledges each
record.

A request goes through three stages, each bounded so that produce
requests cannot exhaust the process's memory or the threads that serve
every other route:

1. Admission (ProduceAdmission): at most PRODUCE_MAX_REQUESTS requests and
   PRODUCE_MAX_BODY_BYTES of declared body in flight; the rest are refused
   at once.
2. Parsing (prepare_record, prepare_batch): one body at a time, after a
   cheap estimate of what parsing it would cost in memory.
3. Sending (ProducerService.send): within the deadline the request took
   when it arrived.
"""

from __future__ import annotations

import base64
import binascii
import json
import logging
import re
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError
from kafka.errors import MessageSizeTooLargeError
from kafka.errors import RecordListTooLargeError
from kafka.producer.future import FutureRecordMetadata
from kafka.record.default_records import DefaultRecordBatchBuilder

from web_service.services import MSKTokenProvider

logger = logging.getLogger(__name__)

MIB = 1024 * 1024
# Largest record: the cluster's message.max.bytes, which counts the
# record batch Kafka wraps it in (about 100 bytes for one record)
MAX_RECORD_BYTES = 8 * MIB
# Largest request body the produce routes read
MAX_BODY_BYTES = 16 * MIB
MAX_BATCH_ENTRIES = 500
MAX_ENTRY_ID_LENGTH = 80
# Longest a request takes from its arrival to its reply, give or take the
# token and namespace checks
SEND_TIMEOUT_S = 30
# Longest send() blocks waiting for topic metadata; no send starts with
# less than this left of the request's time
PRODUCER_MAX_BLOCK_MS = 10_000
# Longest shutdown waits for records still in flight
PRODUCER_CLOSE_TIMEOUT_S = 10
# How long produce requests fail at once after the producer could not be
# built, before the next request tries again
PRODUCER_BUILD_RETRY_S = 5

# Requests in the produce path at once; each holds a thread of its own
# limiter while it waits for the cluster
PRODUCE_MAX_REQUESTS = 8
# Declared body bytes in the produce path at once (a body without a
# Content-Length counts as MAX_BODY_BYTES)
PRODUCE_MAX_BODY_BYTES = 48 * MIB
# Most a body may cost in memory to parse, by estimate_parse_cost
MAX_PARSE_COST = 192 * MIB
# Seconds a refused client is told to wait
RETRY_AFTER_S = 1

# Error codes of the produce routes; the Confluent ones are noted
BAD_TOPIC_NAME = 40002  # Confluent: invalid topic
BAD_BODY = 40003
MULTIPLE_VALUES = 40004
BAD_RECORD = 40005
TOPIC_NOT_FOUND = 40401  # Confluent
PARTITION_NOT_FOUND = 40402  # Confluent
BODY_TOO_LARGE = 41301
RECORD_TOO_LARGE = 41302
PARSE_TOO_COSTLY = 41303
UNSUPPORTED_MEDIA_TYPE = 41501
INVALID_PAYLOAD = 42206  # Confluent
INVALID_BATCH = 42208  # Confluent
TOO_MANY_REQUESTS = 42901  # Confluent
KAFKA_ERROR = 50002  # Confluent
KAFKA_RETRIABLE_ERROR = 50003  # Confluent
BROKER_NOT_AVAILABLE = 50302  # Confluent

_FORMATS = ('BINARY', 'JSON', 'STRING')
_SCHEMA_FORMATS = frozenset({'AVRO', 'JSONSCHEMA', 'PROTOBUF'})
_SCHEMA_FIELDS = frozenset(
    {
        'schema',
        'schema_id',
        'schema_version',
        'subject',
        'subject_name_strategy',
    },
)
_DATA_FIELDS = frozenset({'type', 'data'})
_RECORD_FIELDS = frozenset(
    {'partition_id', 'headers', 'key', 'value', 'timestamp'},
)
_HEADER_FIELDS = frozenset({'name', 'value'})
_ENTRY_ID = re.compile(r'[0-9a-zA-Z_-]+')
_WHITESPACE = re.compile(r'[ \t\n\r]*')
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_ONE_MS = timedelta(milliseconds=1)
_NO_SCHEMA_REGISTRY = 'Octopus runs no schema registry'

# Bytes of memory that parsing costs per byte of the body that opens a
# container, delimits a string or separates two values: an upper bound of
# CPython's objects (a one-key dict is 184 bytes, a short string about
# 50, a float 24, plus the 8-byte pointer to each)
_CONTAINER_COST = 160
_QUOTE_COST = 32
_COMMA_COST = 40


class RecordError(Exception):
    """A request or record the routes refuse, with its reply."""

    def __init__(self, status: int, error_code: int, message: str) -> None:
        """Initialize with the HTTP status, error code and message."""
        super().__init__(message)
        self.status = status
        self.error_code = error_code
        self.message = message

    def body(self) -> dict[str, Any]:
        """The error reply's body."""
        return {'error_code': self.error_code, 'message': self.message}


def _bad_record(message: str) -> RecordError:
    return RecordError(400, BAD_RECORD, message)


def _too_large(size: int) -> RecordError:
    return RecordError(
        413,
        RECORD_TOO_LARGE,
        f'The record is at least {size} bytes; the most a record may be '
        f'is {MAX_RECORD_BYTES} bytes, counting its key, value, headers '
        "and about 100 bytes of Kafka's overhead",
    )


@dataclass(frozen=True)
class OutgoingRecord:
    """A record checked and encoded, ready to send."""

    key: bytes | None
    value: bytes | None
    headers: list[tuple[str, bytes]]
    timestamp_ms: int | None


# --- Admission ---


class ProduceAdmission:
    """Bound the produce requests in flight, by count and by body size.

    A request that would pass either bound is refused at once rather than
    queued, so a burst cannot hold memory or threads while it waits.
    """

    def __init__(
        self,
        max_requests: int = PRODUCE_MAX_REQUESTS,
        max_body_bytes: int = PRODUCE_MAX_BODY_BYTES,
    ) -> None:
        """Initialize with the bounds."""
        self.max_requests = max_requests
        self.max_body_bytes = max_body_bytes
        self.requests = 0
        self.body_bytes = 0
        self._lock = threading.Lock()

    def acquire(self, body_bytes: int) -> RecordError | None:
        """Admit a request declaring body_bytes, or return its refusal."""
        with self._lock:
            if self.requests >= self.max_requests:
                return RecordError(
                    429,
                    TOO_MANY_REQUESTS,
                    f'The service is serving {self.max_requests} produce '
                    'requests, the most it serves at once; retry shortly',
                )
            if self.body_bytes + body_bytes > self.max_body_bytes:
                return RecordError(
                    429,
                    TOO_MANY_REQUESTS,
                    'The service is reading or sending as many produce '
                    'bytes as it can hold; retry shortly',
                )
            self.requests += 1
            self.body_bytes += body_bytes
            return None

    def release(self, body_bytes: int) -> None:
        """Release a request admitted with body_bytes."""
        with self._lock:
            self.requests -= 1
            self.body_bytes -= body_bytes


# --- Parsing ---


def estimate_parse_cost(raw: bytes | bytearray) -> int:
    """An upper bound of the memory parsing raw and encoding it costs.

    Counts the bytes that make Python objects when parsed (container
    openings, quotes, commas), plus the decoded text and the compact
    re-encoding of JSON data. The counts run at memchr speed and include
    bytes inside strings, so the bound is loose for text-heavy bodies.
    """
    width = 1 if raw.isascii() else 4
    return (
        (2 * width + 1) * len(raw)
        + _CONTAINER_COST * (raw.count(b'{') + raw.count(b'['))
        + _QUOTE_COST * raw.count(b'"')
        + _COMMA_COST * raw.count(b',')
    )


def _no_constant(name: str) -> Any:
    raise ValueError(f'{name} is not valid JSON')


def parse_body(raw: bytes | bytearray, *, empty: RecordError) -> Any:
    """Parse a request body that must hold exactly one JSON object.

    A bytearray body is emptied once decoded, so that only the text and
    the parsed object take memory while parsing.

    Raises:
        RecordError: empty for an empty body, 40003 if the body is not
            one JSON object, 40004 if it holds more than one JSON value,
            41303 if parsing it would cost too much memory
    """
    if estimate_parse_cost(raw) > MAX_PARSE_COST:
        raise RecordError(
            413,
            PARSE_TOO_COSTLY,
            'The body holds too many JSON values to parse within the '
            "service's memory; send fewer or larger values, or the data "
            'as BINARY',
        )
    try:
        text = raw.decode('utf-8')
    except UnicodeDecodeError:
        raise RecordError(
            400,
            BAD_BODY,
            'The request body is not UTF-8 text',
        ) from None
    if isinstance(raw, bytearray):
        raw.clear()
    start = _WHITESPACE.match(text).end()  # type: ignore[union-attr]
    if start == len(text):
        raise empty
    decoder = json.JSONDecoder(parse_constant=_no_constant)
    try:
        body, end = decoder.raw_decode(text, start)
    except (ValueError, RecursionError) as e:
        raise RecordError(
            400,
            BAD_BODY,
            f'The request body is not valid JSON: {e!s}',
        ) from None
    rest = _WHITESPACE.match(text, end).end()  # type: ignore[union-attr]
    if rest < len(text):
        try:
            decoder.raw_decode(text, rest)
        except (ValueError, RecursionError):
            raise RecordError(
                400,
                BAD_BODY,
                'The request body is not valid JSON: extra data after '
                'the first value',
            ) from None
        raise RecordError(
            400,
            MULTIPLE_VALUES,
            'The request body holds more than one JSON value; send one '
            'record per request, or several records to records:batch',
        )
    if not isinstance(body, dict):
        raise RecordError(
            400,
            BAD_BODY,
            'The request body must be a JSON object',
        )
    return body


def _check_partition(partition_id: Any) -> None:
    if partition_id is None:
        return
    if type(partition_id) is not int:
        raise _bad_record('partition_id must be an integer')
    if partition_id != 0:
        raise RecordError(
            404,
            PARTITION_NOT_FOUND,
            f'Partition {partition_id} not found; every topic has one '
            'partition, 0',
        )


def _data_type(field: str, spec: dict[str, Any]) -> str:
    """Check the fields of a key or value and return its type."""
    if schema_fields := sorted(set(spec) & _SCHEMA_FIELDS):
        raise RecordError(
            422,
            INVALID_PAYLOAD,
            f'{field} names {", ".join(schema_fields)}; '
            f'{_NO_SCHEMA_REGISTRY}, so {field} takes only type and data',
        )
    if unknown := sorted(set(spec) - _DATA_FIELDS):
        raise _bad_record(
            f'{field} has unknown field(s) {", ".join(unknown)}; it takes '
            'only type and data',
        )
    data_type = spec.get('type')
    if data_type is None:
        raise RecordError(
            422,
            INVALID_PAYLOAD,
            f'{field} has no type; {_NO_SCHEMA_REGISTRY} to infer one, so '
            'give type BINARY, JSON or STRING',
        )
    if isinstance(data_type, str) and data_type in _SCHEMA_FORMATS:
        raise RecordError(
            422,
            INVALID_PAYLOAD,
            f'{field} type {data_type} needs a schema registry; '
            f'{_NO_SCHEMA_REGISTRY}, so use BINARY, JSON or STRING',
        )
    if data_type not in _FORMATS:
        raise _bad_record(
            f'{field} type must be BINARY, JSON or STRING, not {data_type!r}',
        )
    return data_type


def _decode_base64(data: str, what: str) -> bytes:
    # The decoded length, known before decoding (base64 holds 3 bytes
    # in every 4 characters)
    size = len(data) // 4 * 3
    if size > MAX_RECORD_BYTES:
        raise _too_large(size - 2)
    try:
        return base64.b64decode(data, validate=True)
    except (binascii.Error, ValueError):
        raise _bad_record(f'{what} is not valid base64') from None


def _encode_utf8(text: str, what: str) -> bytes:
    # Every character takes at least one byte
    if len(text) > MAX_RECORD_BYTES:
        raise _too_large(len(text))
    try:
        return text.encode('utf-8')
    except UnicodeEncodeError:
        raise _bad_record(f'{what} cannot be written as UTF-8') from None


def _encode_data(field: str, spec: Any) -> bytes | None:
    """Encode a record's key or value; None when it has no data."""
    if spec is None:
        return None
    if not isinstance(spec, dict):
        raise _bad_record(f'{field} must be an object with type and data')
    data_type = _data_type(field, spec)
    data = spec.get('data')
    if data is None:
        return None
    if data_type == 'JSON':
        try:
            text = json.dumps(
                data,
                ensure_ascii=False,
                separators=(',', ':'),
                allow_nan=False,
            )
        except (ValueError, RecursionError):
            raise _bad_record(
                f'{field} data cannot be written as JSON',
            ) from None
        return _encode_utf8(text, f'{field} data')
    if not isinstance(data, str):
        raise _bad_record(f'{field} data of type {data_type} must be a string')
    if data_type == 'BINARY':
        return _decode_base64(data, f'{field} data of type BINARY')
    return _encode_utf8(data, f'{field} data')


def _encode_header(i: int, header: Any) -> tuple[str, bytes]:
    """Encode one header as a (name, value) pair.

    kafka-python cannot write a header with a null value, so a null value
    is written as an empty one.
    """
    if not isinstance(header, dict):
        raise _bad_record(f'Header {i} must be an object with a name')
    if unknown := sorted(set(header) - _HEADER_FIELDS):
        raise _bad_record(
            f'Header {i} has unknown field(s) {", ".join(unknown)}; it '
            'takes only name and value',
        )
    name = header.get('name')
    if not isinstance(name, str):
        raise _bad_record(f'Header {i} needs a string name')
    _encode_utf8(name, f'Header {i} name')
    value = header.get('value')
    if value is None:
        return name, b''
    if not isinstance(value, str):
        raise _bad_record(f'Header {i} value must be a base64 string or null')
    return name, _decode_base64(value, f'Header {i} value')


def _encode_headers(headers: Any) -> list[tuple[str, bytes]]:
    """Encode a record's headers as (name, value) pairs."""
    if headers is None:
        return []
    if not isinstance(headers, list):
        raise _bad_record('headers must be a list of {name, value} objects')
    return [_encode_header(i, header) for i, header in enumerate(headers)]


def _parse_timestamp(timestamp: Any) -> int | None:
    """Milliseconds since the epoch of an ISO-8601 timestamp, or None."""
    if timestamp is None:
        return None
    example = 'for example 2026-10-26T15:04:05.123Z'
    if not isinstance(timestamp, str):
        raise _bad_record(f'timestamp must be an ISO-8601 string, {example}')
    text = timestamp
    if text[-1:] in ('Z', 'z'):
        text = text[:-1] + '+00:00'
    try:
        moment = datetime.fromisoformat(text)
    except ValueError:
        raise _bad_record(
            f'timestamp {timestamp!r} is not ISO-8601, {example}',
        ) from None
    if moment.utcoffset() is None:
        raise _bad_record(
            f'timestamp {timestamp!r} has no UTC offset, {example}',
        )
    ms = (moment - _EPOCH) // _ONE_MS
    if ms < 0:
        raise _bad_record(f'timestamp {timestamp!r} is before 1970')
    return ms


def record_size(record: OutgoingRecord) -> int:
    """The record's size as Kafka counts it, in a batch of its own."""
    return DefaultRecordBatchBuilder.estimate_size_in_bytes(
        record.key,
        record.value,
        record.headers,
    )


def build_record(
    entry: dict[str, Any],
    *,
    extra_fields: frozenset[str] = frozenset(),
) -> OutgoingRecord:
    """Check a record of a produce request and encode it.

    Args:
        entry: The record as the request gives it
        extra_fields: Fields allowed besides the record's own (a batch
            entry's id)

    Raises:
        RecordError: The status, error code and message for a record the
            routes refuse
    """
    if unknown := sorted(set(entry) - _RECORD_FIELDS - extra_fields):
        raise _bad_record(
            f'Unknown record field(s) {", ".join(unknown)}; a record takes '
            'partition_id, headers, key, value and timestamp',
        )
    _check_partition(entry.get('partition_id'))
    key = _encode_data('key', entry.get('key'))
    value = _encode_data('value', entry.get('value'))
    headers = _encode_headers(entry.get('headers'))
    timestamp_ms = _parse_timestamp(entry.get('timestamp'))
    if key is None and value is None:
        raise _bad_record(
            'A record needs a key or a value with data: kafka-python, '
            'which the service sends with, cannot send a record that has '
            'neither',
        )
    record = OutgoingRecord(
        key=key,
        value=value,
        headers=headers,
        timestamp_ms=timestamp_ms,
    )
    if (size := record_size(record)) > MAX_RECORD_BYTES:
        raise _too_large(size)
    return record


def prepare_record(raw: bytes | bytearray) -> OutgoingRecord:
    """Parse and encode the one record a request body holds.

    Raises:
        RecordError: For a body or record the route refuses
    """
    return build_record(
        parse_body(
            raw,
            empty=RecordError(
                422,
                INVALID_PAYLOAD,
                'Request body is empty. Data is required.',
            ),
        ),
    )


def _batch_error(message: str) -> RecordError:
    return RecordError(422, INVALID_BATCH, message)


def _entry_ids(entries: list[Any]) -> list[str]:
    """Check each entry's id, distinct within the batch, and return them."""
    seen: set[str] = set()
    for i, entry in enumerate(entries):
        if not isinstance(entry, dict) or 'id' not in entry:
            raise _batch_error(f'Entry {i} has no id')
        entry_id = entry['id']
        if (
            not isinstance(entry_id, str)
            or not (1 <= len(entry_id) <= MAX_ENTRY_ID_LENGTH)
            or not _ENTRY_ID.fullmatch(entry_id)
        ):
            raise _batch_error(
                f'Entry {i} id must be 1 to {MAX_ENTRY_ID_LENGTH} letters, '
                'digits, dashes or underscores',
            )
        if entry_id in seen:
            raise _batch_error(
                f'Entry id {entry_id!r} appears more than once',
            )
        seen.add(entry_id)
    return [entry['id'] for entry in entries]


def prepare_batch(
    raw: bytes | bytearray,
) -> list[tuple[str, OutgoingRecord | RecordError]]:
    """Parse a batch body and encode each entry, or its refusal.

    The parsed body is dropped before returning: only the encoded records
    are held while they are sent.

    Raises:
        RecordError: For a body the batch route refuses as a whole
    """
    body = parse_body(
        raw,
        empty=_batch_error('Request body is empty. Data is required.'),
    )
    if unknown := sorted(set(body) - {'entries'}):
        raise _batch_error(
            f'Unknown field(s) {", ".join(unknown)}; a batch takes only '
            'entries',
        )
    entries = body.pop('entries', None)
    if not isinstance(entries, list) or not entries:
        raise _batch_error(
            f'entries must be a list of 1 to {MAX_BATCH_ENTRIES} records',
        )
    if len(entries) > MAX_BATCH_ENTRIES:
        raise _batch_error(
            f'The batch has {len(entries)} entries; the most allowed is '
            f'{MAX_BATCH_ENTRIES}',
        )
    ids = _entry_ids(entries)
    prepared: list[tuple[str, OutgoingRecord | RecordError]] = []
    for i, entry_id in enumerate(ids):
        entry, entries[i] = entries[i], None
        try:
            prepared.append(
                (
                    entry_id,
                    build_record(entry, extra_fields=frozenset({'id'})),
                ),
            )
        except RecordError as e:
            prepared.append((entry_id, e))
    return prepared


def format_timestamp(timestamp_ms: int) -> str:
    """Format epoch milliseconds as ISO-8601 UTC."""
    moment = _EPOCH + timestamp_ms * _ONE_MS
    return moment.isoformat(timespec='milliseconds').replace('+00:00', 'Z')


def record_reply(
    topic: str,
    result: dict[str, Any] | RecordError,
) -> tuple[dict[str, Any], int]:
    """The reply to the single-record route."""
    if isinstance(result, RecordError):
        return result.body(), result.status
    return {'topic_name': topic, **result}, 200


def batch_reply(
    topic: str,
    prepared: list[tuple[str, OutgoingRecord | RecordError]],
    results: list[dict[str, Any] | RecordError],
) -> tuple[dict[str, Any], int]:
    """The 207 reply to the batch route.

    Args:
        topic: The topic, as the request names it
        prepared: Each entry's id and record or refusal, in order
        results: The send result of each entry that had a record, in order
    """
    sent = iter(results)
    successes: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for entry_id, record in prepared:
        result = record if isinstance(record, RecordError) else next(sent)
        if isinstance(result, RecordError):
            failures.append({'id': entry_id, **result.body()})
        else:
            successes.append({'id': entry_id, 'topic_name': topic, **result})
    return {'successes': successes, 'failures': failures}, 207


# --- Sending ---


def _kafka_failure(error: Exception, *, sent: bool) -> RecordError:
    """The reply for a record the producer failed to write.

    The exception's text names brokers, so it is logged, not returned.
    """
    if isinstance(
        error,
        (MessageSizeTooLargeError, RecordListTooLargeError),
    ):
        return RecordError(
            413,
            RECORD_TOO_LARGE,
            'The cluster refused the record as too large',
        )
    logger.warning('Failed to produce a record: %r', error)
    if isinstance(error, KafkaTimeoutError):
        message = (
            'The cluster did not acknowledge the record in time; it may '
            'still be written'
            if sent
            else 'The cluster did not answer in time; the record was not sent'
        )
        return RecordError(500, KAFKA_RETRIABLE_ERROR, message)
    name = type(error).__name__
    if getattr(error, 'retriable', False):
        return RecordError(
            500,
            KAFKA_RETRIABLE_ERROR,
            f'Kafka failed to write the record ({name}); retry it',
        )
    return RecordError(
        500,
        KAFKA_ERROR,
        f'Kafka failed to write the record ({name})',
    )


def _out_of_time() -> RecordError:
    return RecordError(
        500,
        KAFKA_RETRIABLE_ERROR,
        "Not sent: too little of the request's "
        f'{SEND_TIMEOUT_S} s was left to send it; retry it',
    )


class ProducerService:
    """Produce records to Kafka for REST API requests."""

    def __init__(
        self,
        bootstrap_servers: str | None,
        region: str,
    ) -> None:
        """Initialize the producer service; the producer is built on use.

        Args:
            bootstrap_servers: Kafka bootstrap servers
            region: AWS region for MSK auth
        """
        self.bootstrap_servers = bootstrap_servers
        self.region = region
        self._producer: KafkaProducer | None = None
        self._lock = threading.Lock()
        # Set by close; produce requests from then on are refused
        self._closing = False
        # Until when requests fail at once after a failed build
        self._build_failed_until = 0.0

    @staticmethod
    def kafka_topic_name(namespace: str, topic: str) -> str:
        """The Kafka topic of a namespace's topic."""
        return f'{namespace}.{topic}'

    def _build(self) -> KafkaProducer:
        """Build the producer; the caller holds the lock."""
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            security_protocol='SASL_SSL',
            sasl_mechanism='OAUTHBEARER',
            sasl_oauth_token_provider=MSKTokenProvider(self.region),
            acks='all',
            # Retries keep the batch's order on one connection
            max_in_flight_requests_per_connection=1,
            delivery_timeout_ms=SEND_TIMEOUT_S * 1000,
            request_timeout_ms=SEND_TIMEOUT_S * 1000,
            max_block_ms=PRODUCER_MAX_BLOCK_MS,
            max_request_size=MAX_RECORD_BYTES,
            # Topics are made by the topic routes, never by a produce to a
            # missing one
            allow_auto_create_topics=False,
        )

    def _get_producer(self, deadline: float) -> KafkaProducer:
        """The process's producer, built on first use.

        Waits for another request's build only while a send could still
        start before the deadline.

        Raises:
            RecordError: If Kafka is not configured, the service is
                shutting down, or the producer cannot be built
        """
        wait = deadline - time.monotonic() - PRODUCER_MAX_BLOCK_MS / 1000
        if not self._lock.acquire(timeout=max(0.0, wait)):
            raise _out_of_time()
        try:
            if self._closing:
                raise RecordError(
                    500,
                    KAFKA_ERROR,
                    'Failed to produce: service is shutting down',
                )
            if self._producer is not None:
                return self._producer
            if not self.bootstrap_servers:
                raise RecordError(
                    500,
                    KAFKA_ERROR,
                    'Kafka endpoint is not configured',
                )
            unreachable = RecordError(
                503,
                BROKER_NOT_AVAILABLE,
                'The Kafka cluster is not reachable; retry shortly',
            )
            if time.monotonic() < self._build_failed_until:
                raise unreachable
            try:
                self._producer = self._build()
            except Exception:
                logger.exception('Failed to create producer')
                self._build_failed_until = (
                    time.monotonic() + PRODUCER_BUILD_RETRY_S
                )
                raise unreachable from None
            return self._producer
        finally:
            self._lock.release()

    def send(
        self,
        kafka_topic: str,
        records: list[OutgoingRecord],
        deadline: float,
    ) -> list[dict[str, Any] | RecordError]:
        """Send records and wait for each acknowledgement.

        No send starts with less than PRODUCER_MAX_BLOCK_MS left before
        the deadline, and no wait passes it. Returns, in order, each
        record's partition, offset and timestamp or its error.
        """
        if not records:
            return []
        try:
            producer = self._get_producer(deadline)
        except RecordError as e:
            return [e] * len(records)
        sent: list[FutureRecordMetadata | RecordError] = []
        for record in records:
            remaining = deadline - time.monotonic()
            if remaining < PRODUCER_MAX_BLOCK_MS / 1000:
                sent.append(_out_of_time())
                continue
            try:
                sent.append(
                    producer.send(
                        kafka_topic,
                        key=record.key,
                        value=record.value,
                        headers=record.headers,
                        partition=0,
                        timestamp_ms=record.timestamp_ms,
                    ),
                )
            except Exception as e:
                sent.append(_kafka_failure(e, sent=False))
        results: list[dict[str, Any] | RecordError] = []
        for future in sent:
            if isinstance(future, RecordError):
                results.append(future)
                continue
            try:
                metadata = future.get(
                    timeout=max(0.0, deadline - time.monotonic()),
                )
            except Exception as e:
                results.append(_kafka_failure(e, sent=True))
                continue
            results.append(
                {
                    'partition_id': metadata.partition,
                    'offset': metadata.offset,
                    'timestamp': format_timestamp(metadata.timestamp),
                },
            )
        return results

    def close(self) -> None:
        """Close the producer, waiting at most PRODUCER_CLOSE_TIMEOUT_S.

        Called on application shutdown; produce requests from then on are
        refused.
        """
        with self._lock:
            self._closing = True
            producer, self._producer = self._producer, None
        if producer is None:
            return
        try:
            producer.close(timeout=PRODUCER_CLOSE_TIMEOUT_S)
        except Exception:
            logger.exception('Error closing producer on shutdown')
