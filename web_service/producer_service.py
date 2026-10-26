"""Produce records to Kafka for the REST API.

Implements the produce requests of Confluent's REST API v3 with the
BINARY, JSON and STRING formats: one shared KafkaProducer per process,
built on first use, and replies only once the cluster acknowledges each
record.

A request goes through three stages, each bounded so that produce
requests cannot exhaust the process's memory or the threads that serve
every other route:

1. Admission (ProduceAdmission): the bytes a request has sent, and then
   its records, count against PRODUCE_MAX_BODY_BYTES; once its body is
   read, it takes one of PRODUCE_MAX_REQUESTS places. Each subject has a
   share of both. A request past a bound is refused at once, and one
   that could not be served when it arrives before its body is sent.
2. Parsing (prepare_record, prepare_batch): one body at a time, after a
   cheap estimate of what parsing it would cost in memory.
3. Sending (ProducerService.send): within SEND_TIMEOUT_S of the parse.
"""

from __future__ import annotations

import base64
import binascii
import json
import logging
import math
import re
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from datetime import UTC
from typing import Any

from kafka import KafkaProducer
from kafka.errors import AuthorizationError
from kafka.errors import KafkaTimeoutError
from kafka.errors import MessageSizeTooLargeError
from kafka.errors import RecordListTooLargeError
from kafka.errors import UnknownTopicOrPartitionError
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
# Longest a request's records take to be sent and acknowledged, from the
# moment its body is read and parsed
SEND_TIMEOUT_S = 30
# Longest an admitted request may take to send its body: 16 MiB in 20 s
# needs about 7 Mbit/s, and a stalled upload holds its admission no longer
READ_TIMEOUT_S = 20
# Longest send() blocks waiting for topic metadata; no send starts with
# less than this left of the request's time
PRODUCER_MAX_BLOCK_MS = 10_000
# Longest shutdown waits for records still in flight: the consumer
# instances' shutdown takes as long, at the same time, within the time
# gunicorn leaves after uvicorn has waited for open requests
PRODUCER_CLOSE_TIMEOUT_S = 8
# How long produce requests fail at once after the producer could not be
# built, before the next request tries again
PRODUCER_BUILD_RETRY_S = 5
# How long a topic the cluster answers for without listing it may stay
# missing before it counts as not found; a topic just created can take a
# moment to reach every broker's metadata
TOPIC_GRACE_S = 2
# Answers from the cluster without the topic before it counts as not found;
# the first can come from a request sent before the topic was asked for
TOPIC_ANSWERS = 2

# Requests past their body's read at once, parsing or sending; each holds
# a thread of its own limiter while it waits for the cluster
PRODUCE_MAX_REQUESTS = 8
# Bytes the produce path holds at once: the bytes each body has sent so
# far, whatever its Content-Length, and then its encoded records
PRODUCE_MAX_BODY_BYTES = 48 * MIB
# One subject's share, so one user cannot take them all: its requests at
# any stage, and its bytes
PRODUCE_SUBJECT_MAX_REQUESTS = 2
PRODUCE_SUBJECT_MAX_BODY_BYTES = 16 * MIB
# Most a body may cost in memory to parse, by estimate_parse_cost
MAX_PARSE_COST = 192 * MIB
# Seconds a refused client is told to wait
RETRY_AFTER_S = 1

# Error codes of the produce routes; the Confluent ones are noted
BAD_TOPIC_NAME = 40002  # Confluent: invalid topic
BAD_BODY = 40003
MULTIPLE_VALUES = 40004
BAD_RECORD = 40005
TOPIC_AUTHORIZATION_FAILED = 40301  # Confluent
TOPIC_NOT_FOUND = 40401  # Confluent
PARTITION_NOT_FOUND = 40402  # Confluent
BODY_TIMEOUT = 40801
BODY_TOO_LARGE = 41301
RECORD_TOO_LARGE = 41302
PARSE_TOO_COSTLY = 41303
RECORDS_TOO_LARGE = 41304
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
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_ONE_MS = timedelta(milliseconds=1)
_NO_SCHEMA_REGISTRY = 'Octopus runs no schema registry'

# Bytes of memory that parsing costs per byte of the body that opens a
# container, delimits a string or separates two values: an upper bound of
# CPython's objects (a one-key dict is 184 bytes, a short string about
# 50, a float 24, plus the 8-byte pointer to each)
_CONTAINER_COST = 160
_QUOTE_COST = 32
_COMMA_COST = 40
# Bytes for each key of an object, counted by its colon: the dict entry,
# its share of the hash table (which can be three times the entries while
# it grows) and the decoder's memo of the key
_COLON_COST = 48
# Memory any parse can take beyond what the body's bytes predict: lists
# and tables over-allocate as they grow, which tells most in a small body
_FIXED_COST = 4 * 1024 * 1024
# Most characters json.dumps adds to a number with an exponent (1e15 is
# written 1000000000000000.0) or a decimal point (a 17-digit decimal can
# gain an exponent)
_EXPONENT_GROWTH = 14
_DECIMAL_GROWTH = 3
# Signs of characters past Latin-1 (2 bytes each in a str) and past the
# Basic Multilingual Plane (4 bytes), as UTF-8 lead bytes or \u escapes
_WIDE_UTF8 = re.compile(rb'[\xc4-\xef]')
_ASTRAL_UTF8 = re.compile(rb'[\xf0-\xf4]')
_WIDE_ESCAPE = re.compile(rb'\\u(?!00)')
_ASTRAL_ESCAPE = re.compile(rb'\\u[dD][89abAB]')


class RecordError(Exception):
    """A request or record the routes refuse, with its reply."""

    def __init__(
        self,
        status: int,
        error_code: int,
        message: str,
        *,
        retry_after_s: int | None = None,
    ) -> None:
        """Initialize with the HTTP status, error code and message.

        retry_after_s, if given, is sent as the reply's Retry-After.
        """
        super().__init__(message)
        self.status = status
        self.error_code = error_code
        self.message = message
        self.retry_after_s = retry_after_s

    def body(self) -> dict[str, Any]:
        """The error reply's body."""
        return {'error_code': self.error_code, 'message': self.message}


def without_traceback(error: RecordError) -> RecordError:
    """The error, freed of the frames that raised it.

    Those frames can hold a body's parsed data and encoded records, which
    would otherwise live as long as the error.
    """
    error.__traceback__ = None
    error.__context__ = None
    return error


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
    """Bound the produce requests in flight, by count and by bytes held.

    A request holds only the bytes it has sent (and then its records), so
    a stalled upload holds no more than it sent. It takes one of the
    service's places only once its body is read, so stalled uploads hold
    none: a place is for parsing and sending. Each subject has a share of
    both bounds. A request past a bound is refused at once rather than
    queued, so a burst cannot hold memory or threads while it waits.
    """

    def __init__(
        self,
        max_requests: int = PRODUCE_MAX_REQUESTS,
        max_body_bytes: int = PRODUCE_MAX_BODY_BYTES,
        subject_max_requests: int = PRODUCE_SUBJECT_MAX_REQUESTS,
        subject_max_body_bytes: int = PRODUCE_SUBJECT_MAX_BODY_BYTES,
    ) -> None:
        """Initialize with the bounds."""
        self.max_requests = max_requests
        self.max_body_bytes = max_body_bytes
        self.subject_max_requests = subject_max_requests
        self.subject_max_body_bytes = subject_max_body_bytes
        # Requests past their read, and bytes held, by all subjects
        self.requests = 0
        self.body_bytes = 0
        # Requests at any stage and bytes held, by subject
        self._subjects: dict[str, list[int]] = {}
        self._lock = threading.Lock()

    def admit(
        self,
        subject: str,
        declared: int | None = None,
    ) -> Reservation:
        """Admit a request of subject to send its body, holding nothing.

        A request that could not be served now is refused before its body
        is sent: every place is taken, or its declared length is more than
        the bytes free. Nothing is reserved for it, so a stalled upload
        holds nothing; these checks race with other requests, so the read
        and the place taken after it check again.

        Raises:
            RecordError: 429 if the subject has its most requests, every
                place is taken, or the declared length does not fit
        """
        with self._lock:
            held = self._subjects.get(subject, [0, 0])
            if held[0] >= self.subject_max_requests:
                raise _busy(
                    f'You have {held[0]} produce requests in flight, the '
                    'most one user may',
                )
            self._check_place()
            if declared:
                self._check_bytes(held, declared)
            self._subjects.setdefault(subject, held)[0] += 1
        return Reservation(self, subject)

    def _check_place(self) -> None:
        if self.requests >= self.max_requests:
            raise _busy(
                f'The service is serving {self.max_requests} produce '
                'requests, the most it serves at once',
            )

    def _check_bytes(self, held: list[int], more: int) -> None:
        if self.body_bytes + more > self.max_body_bytes:
            raise _busy(
                'The service holds as many produce bytes as it can',
            )
        if held[1] + more > self.subject_max_body_bytes:
            raise _busy(
                f'Your produce requests in flight hold {held[1]} bytes; '
                f'one user may hold {self.subject_max_body_bytes}',
            )

    def _grow(self, subject: str, more: int) -> None:
        with self._lock:
            held = self._subjects[subject]
            self._check_bytes(held, more)
            self.body_bytes += more
            held[1] += more

    def _start(self) -> None:
        with self._lock:
            self._check_place()
            self.requests += 1

    def _release(
        self,
        subject: str,
        body_bytes: int,
        *,
        started: bool,
    ) -> None:
        with self._lock:
            self.requests -= started
            self.body_bytes -= body_bytes
            held = self._subjects[subject]
            held[0] -= 1
            held[1] -= body_bytes
            if held[0] == 0:
                del self._subjects[subject]


class Reservation:
    """One admitted request's share of the produce bounds."""

    def __init__(self, admission: ProduceAdmission, subject: str) -> None:
        """Initialize holding no bytes and no place."""
        self._admission = admission
        self._subject = subject
        self.body_bytes = 0
        self.started = False
        self._released = False

    def grow_to(self, body_bytes: int) -> None:
        """Hold at least body_bytes.

        Raises:
            RecordError: 429 if the bounds cannot hold them
        """
        if body_bytes > self.body_bytes:
            self._admission._grow(self._subject, body_bytes - self.body_bytes)
            self.body_bytes = body_bytes

    def start(self) -> None:
        """Take one of the service's places, once the body is read.

        Raises:
            RecordError: 429 if every place is taken
        """
        if not self.started:
            self._admission._start()
            self.started = True

    def release(self) -> None:
        """Release the request's share; later calls do nothing."""
        if not self._released:
            self._released = True
            self._admission._release(
                self._subject,
                self.body_bytes,
                started=self.started,
            )


def _busy(reason: str) -> RecordError:
    return RecordError(
        429,
        TOO_MANY_REQUESTS,
        f'{reason}; retry shortly',
        retry_after_s=RETRY_AFTER_S,
    )


# --- Parsing ---


def char_width(raw: bytes | bytearray) -> int:
    r"""Bytes per character of the widest str that raw can decode to.

    CPython stores a str in 1, 2 or 4 bytes per character, by its widest
    character; a \u escape in an ASCII body can make a wide one.
    """
    ascii_only = raw.isascii()
    if ascii_only and b'\\u' not in raw:
        return 1
    if _ASTRAL_ESCAPE.search(raw) or (
        not ascii_only and _ASTRAL_UTF8.search(raw)
    ):
        return 4
    if _WIDE_ESCAPE.search(raw) or (not ascii_only and _WIDE_UTF8.search(raw)):
        return 2
    return 1


def estimate_parse_cost(raw: bytes | bytearray) -> int:
    """An upper bound of the memory parsing raw and encoding it costs.

    Counts the decoded text, the strings and other objects that parsing
    makes (from the bytes that open a container, delimit a string or
    separate two values), and the compact re-encoding of JSON data, which
    json.dumps builds as pieces, joins and then encodes to UTF-8. The
    re-encoding can be longer than the body: a float is written in full
    (1e15 becomes 1000000000000000.0). The counts run at memchr speed and
    include bytes inside strings, so the bound is loose for text.
    """
    width = char_width(raw)
    length = len(raw)
    commas = raw.count(b',')
    # Each number ends at a comma or a closing bracket, so there are no
    # more numbers than those; a long base64 string has none
    numbers = commas + raw.count(b']') + raw.count(b'}')
    encoded = (
        length
        + _EXPONENT_GROWTH * min(numbers, raw.count(b'e') + raw.count(b'E'))
        + _DECIMAL_GROWTH * min(numbers, raw.count(b'.'))
    )
    return (
        _FIXED_COST
        + width * length
        # The pieces, the joined text and its UTF-8, a byte a character
        # to spare
        + (2 * width + 2) * encoded
        + _CONTAINER_COST * (raw.count(b'{') + raw.count(b'['))
        + _QUOTE_COST * raw.count(b'"')
        + _COMMA_COST * commas
        + _COLON_COST * raw.count(b':')
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
        except ValueError, RecursionError:
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
    except binascii.Error, ValueError:
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
        except ValueError, RecursionError:
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
    total = 0
    for i, entry_id in enumerate(ids):
        entry, entries[i] = entries[i], None
        try:
            record = build_record(entry, extra_fields=frozenset({'id'}))
        except RecordError as e:
            prepared.append((entry_id, without_traceback(e)))
            continue
        # JSON data can be written longer than it was sent, so the records
        # are bounded as well as the body
        total += encoded_size(record)
        if total > MAX_BODY_BYTES:
            raise RecordError(
                413,
                RECORDS_TOO_LARGE,
                f'The records total over {MAX_BODY_BYTES} bytes once '
                'encoded, the most a request may send; JSON numbers are '
                'written in full (1e15 as 1000000000000000.0)',
            )
        prepared.append((entry_id, record))
    return prepared


def encoded_size(record: OutgoingRecord) -> int:
    """The bytes a record's key, value and headers hold."""
    return (
        len(record.key or b'')
        + len(record.value or b'')
        + sum(len(name) + len(value) for name, value in record.headers)
    )


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
    if isinstance(error, AuthorizationError):
        return _not_authorized()
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
        f'Not sent: too little of the {SEND_TIMEOUT_S} s the request has '
        'to send its records was left; retry it',
    )


def _not_authorized() -> RecordError:
    return RecordError(
        403,
        TOPIC_AUTHORIZATION_FAILED,
        'The service is not authorized to write to the topic',
    )


def _unreachable(retry_in_s: float) -> RecordError:
    return RecordError(
        503,
        BROKER_NOT_AVAILABLE,
        'The Kafka cluster is not reachable; retry shortly',
        retry_after_s=max(1, math.ceil(retry_in_s)),
    )


def _topic_not_found() -> RecordError:
    return RecordError(404, TOPIC_NOT_FOUND, 'Topic not found.')


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
            if (left := self._build_failed_until - time.monotonic()) > 0:
                raise _unreachable(left)
            try:
                self._producer = self._build()
            except Exception:
                logger.exception('Failed to create producer')
                self._build_failed_until = (
                    time.monotonic() + PRODUCER_BUILD_RETRY_S
                )
                raise _unreachable(PRODUCER_BUILD_RETRY_S) from None
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
        if refusal := self._find_topic(producer, kafka_topic):
            return [refusal] * len(records)
        # A record still unacknowledged at the deadline stays in
        # kafka-python's accumulator until its own delivery timeout, which
        # runs from its send() below, so for up to the time this request
        # took to reach that send after its admission is released. The
        # produce path's memory bound does not count such records; it
        # matters only while the cluster stalls with requests in flight.
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

    @staticmethod
    def _find_topic(
        producer: KafkaProducer,
        kafka_topic: str,
    ) -> RecordError | None:
        """Refuse a topic the cluster's metadata does not list.

        For such a topic, kafka-python's send() waits PRODUCER_MAX_BLOCK_MS
        and then fails as if the cluster were slow: it logs the broker's
        error but keeps no record of it. So this waits with the producer's
        own metadata instead, and answers as soon as the cluster has
        answered TOPIC_ANSWERS times without the topic and TOPIC_GRACE_S
        have passed.

        Returns:
            403 if the cluster refuses the topic, 404 if it does not list
            it, 500 if it does not answer, None if the topic is listed
        """
        # kafka-python's own, as its send() uses them; absent from another
        # version, which then leaves the wait to send()
        metadata = getattr(producer, '_metadata', None)
        sender = getattr(producer, '_sender', None)
        if metadata is None or sender is None:
            return None
        if metadata.partitions_for_topic(kafka_topic) is not None:
            return None
        sender.add_topic(kafka_topic)
        start = time.monotonic()
        end = start + PRODUCER_MAX_BLOCK_MS / 1000
        answers = 0
        while (left := end - time.monotonic()) > 0:
            answered = threading.Event()
            update = metadata.request_update()
            update.add_both(lambda event, _: event.set(), answered)
            sender.wakeup()
            answered.wait(left)
            if metadata.partitions_for_topic(kafka_topic) is not None:
                return None
            if kafka_topic in metadata.unauthorized_topics:
                logger.warning('Not authorized for topic %s', kafka_topic)
                return _not_authorized()
            # A response about this topic alone fails the update with the
            # topic's error; one about several succeeds without it
            if update.is_done and (
                update.succeeded()
                or isinstance(update.exception, UnknownTopicOrPartitionError)
            ):
                answers += 1
                if (
                    answers >= TOPIC_ANSWERS
                    and time.monotonic() - start >= TOPIC_GRACE_S
                ):
                    logger.warning('Topic %s not in Kafka', kafka_topic)
                    return _topic_not_found()
        return _kafka_failure(KafkaTimeoutError('no metadata'), sent=False)

    def close(self) -> None:
        """Close the producer, waiting about PRODUCER_CLOSE_TIMEOUT_S.

        Called on application shutdown; produce requests from then on are
        refused.
        """
        with self._lock:
            self._closing = True
            producer, self._producer = self._producer, None
        if producer is None:
            return
        try:
            # kafka-python's close waits up to its timeout for the records
            # still unsent and then as long again for its network thread.
            # Its flush waits up to the timeout for each unsent batch in
            # turn, so with several the close can take longer.
            producer.close(timeout=PRODUCER_CLOSE_TIMEOUT_S / 2)
        except Exception:
            logger.exception('Error closing producer on shutdown')
