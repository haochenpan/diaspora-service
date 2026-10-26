"""Produce records to Kafka for the REST API.

Implements the produce requests of Confluent's REST API v3 with the
BINARY, JSON and STRING formats: one shared KafkaProducer per process,
built on first use, and replies only once the cluster acknowledges each
record.
"""

from __future__ import annotations

import base64
import binascii
import json
import logging
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

from web_service.services import MSKTokenProvider

logger = logging.getLogger(__name__)

MIB = 1024 * 1024
# Largest record once decoded (key, value and headers): the cluster's
# message.max.bytes
MAX_RECORD_BYTES = 8 * MIB
# Largest request body the produce routes read
MAX_BODY_BYTES = 16 * MIB
MAX_BATCH_ENTRIES = 500
MAX_ENTRY_ID_LENGTH = 80
# Longest a request waits for the cluster to acknowledge its records
SEND_TIMEOUT_S = 30
# Longest send() blocks waiting for topic metadata
PRODUCER_MAX_BLOCK_MS = 10_000
# Room above the record limit for the record and batch overhead
PRODUCER_MAX_REQUEST_SIZE = MAX_RECORD_BYTES + MIB
# Longest shutdown waits for records still in flight
PRODUCER_CLOSE_TIMEOUT_S = 10

# Error codes of the produce routes
BAD_BODY = 40001
MULTIPLE_VALUES = 40002
BAD_RECORD = 40003
NEEDS_SCHEMA_REGISTRY = 40004
BAD_PARTITION = 40005
BAD_TOPIC_NAME = 40006
BAD_ENTRIES = 40007
BAD_ENTRY_ID = 40008
BODY_TOO_LARGE = 41301
RECORD_TOO_LARGE = 41302
KAFKA_FAILURE = 50002

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
_JSON_WHITESPACE = ' \t\n\r'
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_ONE_MS = timedelta(milliseconds=1)


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


@dataclass(frozen=True)
class OutgoingRecord:
    """A record checked and encoded, ready to send."""

    key: bytes | None
    value: bytes | None
    headers: list[tuple[str, bytes]]
    timestamp_ms: int | None


def _no_constant(name: str) -> Any:
    raise ValueError(f'{name} is not valid JSON')


def parse_body(raw: bytes) -> dict[str, Any]:
    """Parse a request body that must hold exactly one JSON object.

    Raises:
        RecordError: 40001 if the body is not one JSON object, 40002 if it
            holds more than one JSON value
    """
    try:
        text = raw.decode('utf-8')
    except UnicodeDecodeError:
        raise RecordError(
            400,
            BAD_BODY,
            'The request body is not UTF-8 text',
        ) from None
    decoder = json.JSONDecoder(parse_constant=_no_constant)
    start = len(text) - len(text.lstrip(_JSON_WHITESPACE))
    if start == len(text):
        raise RecordError(400, BAD_BODY, 'The request body is empty')
    try:
        body, end = decoder.raw_decode(text, start)
    except (ValueError, RecursionError) as e:
        raise RecordError(
            400,
            BAD_BODY,
            f'The request body is not valid JSON: {e!s}',
        ) from None
    rest = len(text) - len(text[end:].lstrip(_JSON_WHITESPACE))
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
    if type(partition_id) is not int or partition_id != 0:
        raise RecordError(
            400,
            BAD_PARTITION,
            f'Partition {partition_id!r} does not exist; every topic has '
            'one partition, 0',
        )


def _data_type(field: str, spec: dict[str, Any]) -> str:
    """Check the fields of a key or value and return its type."""
    if schema_fields := sorted(set(spec) & _SCHEMA_FIELDS):
        raise RecordError(
            400,
            NEEDS_SCHEMA_REGISTRY,
            f'{field} names {", ".join(schema_fields)}; Octopus runs no '
            f'schema registry, so {field} takes only type and data',
        )
    if unknown := sorted(set(spec) - _DATA_FIELDS):
        raise _bad_record(
            f'{field} has unknown field(s) {", ".join(unknown)}; it takes '
            'only type and data',
        )
    data_type = spec.get('type')
    if data_type is None:
        raise RecordError(
            400,
            NEEDS_SCHEMA_REGISTRY,
            f'{field} has no type; Octopus runs no schema registry to '
            'infer one, so give type BINARY, JSON or STRING',
        )
    if isinstance(data_type, str) and data_type in _SCHEMA_FORMATS:
        raise RecordError(
            400,
            NEEDS_SCHEMA_REGISTRY,
            f'{field} type {data_type} needs a schema registry, which '
            'Octopus does not run; use BINARY, JSON or STRING',
        )
    if data_type not in _FORMATS:
        raise _bad_record(
            f'{field} type must be BINARY, JSON or STRING, not {data_type!r}',
        )
    return data_type


def _decode_base64(data: str, what: str) -> bytes:
    try:
        return base64.b64decode(data, validate=True)
    except (binascii.Error, ValueError):
        raise _bad_record(f'{what} is not valid base64') from None


def _encode_utf8(text: str, what: str) -> bytes:
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
        raise _bad_record('A record needs a key or a value with data')
    size = len(key or b'') + len(value or b'')
    size += sum(len(name.encode('utf-8')) + len(v) for name, v in headers)
    if size > MAX_RECORD_BYTES:
        raise RecordError(
            413,
            RECORD_TOO_LARGE,
            f'The record is {size} bytes once decoded; the most a record '
            f'may hold (key, value and headers) is {MAX_RECORD_BYTES}',
        )
    return OutgoingRecord(
        key=key,
        value=value,
        headers=headers,
        timestamp_ms=timestamp_ms,
    )


def parse_batch(raw: bytes) -> list[dict[str, Any]]:
    """Parse a batch request body and check its entries' ids.

    Raises:
        RecordError: For a body the batch route refuses as a whole
    """
    body = parse_body(raw)
    if unknown := sorted(set(body) - {'entries'}):
        raise RecordError(
            400,
            BAD_ENTRIES,
            f'Unknown field(s) {", ".join(unknown)}; a batch takes only '
            'entries',
        )
    entries = body.get('entries')
    if not isinstance(entries, list) or not entries:
        raise RecordError(
            400,
            BAD_ENTRIES,
            f'entries must be a list of 1 to {MAX_BATCH_ENTRIES} records',
        )
    if len(entries) > MAX_BATCH_ENTRIES:
        raise RecordError(
            400,
            BAD_ENTRIES,
            f'The batch has {len(entries)} entries; the most allowed is '
            f'{MAX_BATCH_ENTRIES}',
        )
    seen: set[str] = set()
    for i, entry in enumerate(entries):
        if not isinstance(entry, dict) or 'id' not in entry:
            raise RecordError(
                400,
                BAD_ENTRY_ID,
                f'Entry {i} has no id',
            )
        entry_id = entry['id']
        if not isinstance(entry_id, str) or not (
            1 <= len(entry_id) <= MAX_ENTRY_ID_LENGTH
        ):
            raise RecordError(
                400,
                BAD_ENTRY_ID,
                f'Entry {i} id must be a string of 1 to '
                f'{MAX_ENTRY_ID_LENGTH} characters',
            )
        if entry_id in seen:
            raise RecordError(
                400,
                BAD_ENTRY_ID,
                f'Entry id {entry_id!r} appears more than once',
            )
        seen.add(entry_id)
    return entries


def format_timestamp(timestamp_ms: int) -> str:
    """Format epoch milliseconds as ISO-8601 UTC."""
    moment = _EPOCH + timestamp_ms * _ONE_MS
    return moment.isoformat(timespec='milliseconds').replace('+00:00', 'Z')


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

    @staticmethod
    def _kafka_topic_name(namespace: str, topic: str) -> str:
        return f'{namespace}.{topic}'

    def _get_producer(self) -> KafkaProducer:
        """The process's producer, built on first use.

        Raises:
            RecordError: If Kafka is not configured, the service is
                shutting down, or the producer cannot be built
        """
        with self._lock:
            if self._closing:
                raise RecordError(
                    500,
                    KAFKA_FAILURE,
                    'Failed to produce: service is shutting down',
                )
            if self._producer is not None:
                return self._producer
            if not self.bootstrap_servers:
                raise RecordError(
                    500,
                    KAFKA_FAILURE,
                    'Kafka endpoint is not configured',
                )
            try:
                self._producer = KafkaProducer(
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
                    max_request_size=PRODUCER_MAX_REQUEST_SIZE,
                    # Topics are made by the topic routes, never by a
                    # produce to a missing one
                    allow_auto_create_topics=False,
                )
            except Exception as e:
                logger.exception('Failed to create producer')
                raise RecordError(
                    500,
                    KAFKA_FAILURE,
                    f'Failed to produce: {e!s}',
                ) from None
            return self._producer

    def _send_all(
        self,
        kafka_topic: str,
        records: list[OutgoingRecord],
    ) -> list[dict[str, Any] | RecordError]:
        """Send records and wait for each acknowledgement.

        All the records share one deadline of SEND_TIMEOUT_S. Returns, in
        order, each record's partition, offset and timestamp or its error.
        """
        try:
            producer = self._get_producer()
        except RecordError as e:
            return [e] * len(records)
        deadline = time.monotonic() + SEND_TIMEOUT_S
        sent: list[FutureRecordMetadata | RecordError] = []
        for record in records:
            if time.monotonic() >= deadline:
                sent.append(
                    RecordError(
                        500,
                        KAFKA_FAILURE,
                        'Failed to produce: not sent, the request ran out '
                        f'of its {SEND_TIMEOUT_S} s',
                    ),
                )
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
                sent.append(self._send_error(e, waiting=False))
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
                results.append(self._send_error(e, waiting=True))
                continue
            results.append(
                {
                    'partition_id': metadata.partition,
                    'offset': metadata.offset,
                    'timestamp': format_timestamp(metadata.timestamp),
                },
            )
        return results

    @staticmethod
    def _send_error(error: Exception, *, waiting: bool) -> RecordError:
        """The error for a record the producer failed to write."""
        if isinstance(
            error,
            (MessageSizeTooLargeError, RecordListTooLargeError),
        ):
            return RecordError(
                413,
                RECORD_TOO_LARGE,
                f'The cluster refused the record as too large: {error!s}',
            )
        if isinstance(error, KafkaTimeoutError):
            if waiting:
                message = (
                    f'Failed to produce: no acknowledgement from the '
                    f'cluster within {SEND_TIMEOUT_S} s; the record may '
                    'still be written'
                )
            else:
                message = f'Failed to produce: not sent: {error!s}'
            return RecordError(500, KAFKA_FAILURE, message)
        logger.warning('Failed to produce a record', exc_info=error)
        return RecordError(500, KAFKA_FAILURE, f'Failed to produce: {error!s}')

    def produce_record(
        self,
        namespace: str,
        topic: str,
        raw: bytes,
    ) -> tuple[dict[str, Any], int]:
        """Produce the one record a request body holds.

        Returns:
            Tuple of (response_body, http_status_code)
        """
        try:
            record = build_record(parse_body(raw))
        except RecordError as e:
            return e.body(), e.status
        (result,) = self._send_all(
            self._kafka_topic_name(namespace, topic),
            [record],
        )
        if isinstance(result, RecordError):
            return result.body(), result.status
        return {'topic_name': topic, **result}, 200

    def produce_batch(
        self,
        namespace: str,
        topic: str,
        raw: bytes,
    ) -> tuple[dict[str, Any], int]:
        """Produce the entries of a batch request body.

        A body refused as a whole answers 400; otherwise the reply is 207
        with each entry in successes or failures.

        Returns:
            Tuple of (response_body, http_status_code)
        """
        try:
            entries = parse_batch(raw)
        except RecordError as e:
            return e.body(), e.status
        ids: list[str] = []
        records: list[OutgoingRecord] = []
        failures: list[dict[str, Any]] = []
        for entry in entries:
            try:
                records.append(
                    build_record(entry, extra_fields=frozenset({'id'})),
                )
                ids.append(entry['id'])
            except RecordError as e:
                failures.append({'id': entry['id'], **e.body()})
        successes: list[dict[str, Any]] = []
        results = (
            self._send_all(
                self._kafka_topic_name(namespace, topic),
                records,
            )
            if records
            else []
        )
        for entry_id, result in zip(ids, results):
            if isinstance(result, RecordError):
                failures.append({'id': entry_id, **result.body()})
            else:
                successes.append(
                    {'id': entry_id, 'topic_name': topic, **result},
                )
        order = {entry['id']: i for i, entry in enumerate(entries)}
        failures.sort(key=lambda f: order[f['id']])
        return {'successes': successes, 'failures': failures}, 207

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
