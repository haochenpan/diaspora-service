"""Unit tests for ProducerService and the produce request parsing."""

from __future__ import annotations

import base64
import io
import json
import threading
import time
import tracemalloc
from collections.abc import Callable
from collections.abc import Iterator
from typing import Any
from unittest.mock import patch

import pytest
from kafka import KafkaProducer
from kafka.cluster import ClusterMetadata
from kafka.errors import KafkaTimeoutError
from kafka.errors import LeaderNotAvailableError
from kafka.errors import MessageSizeTooLargeError
from kafka.errors import NotEnoughReplicasError
from kafka.errors import TopicAuthorizationFailedError
from kafka.errors import UnknownTopicOrPartitionError
from kafka.errors import UnsupportedForMessageFormatError
from kafka.future import Future as KafkaFuture
from kafka.producer.future import RecordMetadata
from kafka.producer.sender import Sender
from kafka.protocol.metadata import MetadataResponse
from kafka.structs import TopicPartition

from web_service.producer_service import batch_reply
from web_service.producer_service import build_record
from web_service.producer_service import estimate_parse_cost
from web_service.producer_service import format_timestamp
from web_service.producer_service import MAX_BATCH_ENTRIES
from web_service.producer_service import MAX_BODY_BYTES
from web_service.producer_service import MAX_PARSE_COST
from web_service.producer_service import MAX_RECORD_BYTES
from web_service.producer_service import OutgoingRecord
from web_service.producer_service import parse_body
from web_service.producer_service import prepare_batch
from web_service.producer_service import prepare_record
from web_service.producer_service import ProduceAdmission
from web_service.producer_service import PRODUCER_BUILD_RETRY_S
from web_service.producer_service import PRODUCER_CLOSE_TIMEOUT_S
from web_service.producer_service import PRODUCER_MAX_BLOCK_MS
from web_service.producer_service import ProducerService
from web_service.producer_service import record_reply
from web_service.producer_service import record_size
from web_service.producer_service import RecordError
from web_service.producer_service import RETRY_AFTER_S
from web_service.producer_service import SEND_TIMEOUT_S

NAMESPACE = 'ns-test123'
TOPIC = 't1'
KAFKA_TOPIC = f'{NAMESPACE}.{TOPIC}'
# 2026-10-26T12:00:00.000Z
NOW_MS = 1_793_016_000_000
_VALUE = {'type': 'STRING', 'data': 'v'}
_EMPTY = RecordError(422, 42206, 'empty')
# A broker address as kafka-python's errors name it
_BROKER = 'b-1.cluster.kafka.us-east-1.amazonaws.com:9098'


# ============================================================================
# A stand-in for KafkaProducer
# ============================================================================


class FakeFuture:
    """A send's future that resolves to metadata or raises an error."""

    def __init__(
        self,
        metadata: RecordMetadata,
        error: Exception | None,
        gate: Callable[[float | None], None] | None,
    ) -> None:
        self.metadata = metadata
        self.error = error
        self.gate = gate
        self.timeout: float | None = None

    def get(self, timeout: float | None = None) -> RecordMetadata:
        self.timeout = timeout
        if self.gate is not None:
            self.gate(timeout)
        if self.error is not None:
            raise self.error
        return self.metadata


class FakeMetadata:
    """The parts of kafka-python's ClusterMetadata the producer reads.

    Lists every topic but those in missing. While answering, each update
    succeeds 10 ms after it is requested; otherwise none completes.
    """

    def __init__(self) -> None:
        self.missing: set[str] = set()
        self.unauthorized_topics: set[str] = set()
        self.answering = True
        self.updates = 0

    def partitions_for_topic(self, topic: str) -> set[int] | None:
        return None if topic in self.missing else {0}

    def request_update(self) -> KafkaFuture:
        self.updates += 1
        future = KafkaFuture()
        if self.answering:
            threading.Timer(0.01, future.success, (self,)).start()
        return future


class FakeSender:
    def __init__(self) -> None:
        self.topics: list[str] = []

    def add_topic(self, topic: str) -> None:
        self.topics.append(topic)

    def wakeup(self) -> None:
        pass


class FakeProducer:
    """Records each send; sends and acknowledgements can be made to fail."""

    def __init__(self) -> None:
        self._metadata = FakeMetadata()
        self._sender = FakeSender()
        self.config: dict[str, Any] = {}
        self.builds = 0
        self.sent: list[dict[str, Any]] = []
        self.futures: list[FakeFuture] = []
        # Errors by send index: raised by send(), or by the future's get()
        self.send_errors: dict[int, Exception] = {}
        self.get_errors: dict[int, Exception] = {}
        # Called by every future's get() before it returns
        self.gate: Callable[[float | None], None] | None = None
        self.close_calls: list[float | None] = []

    def build(self, **config: Any) -> FakeProducer:
        self.config = config
        self.builds += 1
        return self

    def send(self, topic: str, **kwargs: Any) -> FakeFuture:
        index = len(self.sent)
        self.sent.append({'topic': topic, **kwargs})
        if index in self.send_errors:
            raise self.send_errors[index]
        timestamp = kwargs.get('timestamp_ms')
        metadata = RecordMetadata(
            topic,
            0,
            TopicPartition(topic, 0),
            100 + index,
            NOW_MS if timestamp is None else timestamp,
            None,
            -1,
            -1,
            -1,
        )
        future = FakeFuture(metadata, self.get_errors.get(index), self.gate)
        self.futures.append(future)
        return future

    def close(self, timeout: float | None = None) -> None:
        self.close_calls.append(timeout)


@pytest.fixture
def fake_producer() -> Iterator[FakeProducer]:
    """Patch KafkaProducer so each build returns one FakeProducer."""
    fake = FakeProducer()
    with patch(
        'web_service.producer_service.KafkaProducer',
        side_effect=fake.build,
    ):
        yield fake


@pytest.fixture
def producer_service() -> ProducerService:
    return ProducerService(
        bootstrap_servers='localhost:9092',
        region='us-east-1',
    )


def _raw(body: Any) -> bytes:
    return json.dumps(body).encode()


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


def _refusal(call: Callable[[], Any]) -> RecordError:
    with pytest.raises(RecordError) as info:
        call()
    return info.value


def _record(value: bytes = b'v') -> OutgoingRecord:
    return OutgoingRecord(key=None, value=value, headers=[], timestamp_ms=None)


def _deadline() -> float:
    return time.monotonic() + SEND_TIMEOUT_S


# ============================================================================
# parse_body
# ============================================================================

# Nested deeper than the JSON parser allows
_DEEP = b'{"value": ' + b'[' * 1_000_000 + b']' * 1_000_000 + b'}'


def test_parse_body_one_object() -> None:
    raw = b' \n{"value": null}\r\n\t'
    assert parse_body(raw, empty=_EMPTY) == {'value': None}


def test_parse_body_empties_a_bytearray() -> None:
    raw = bytearray(b'{"value": null}')
    assert parse_body(raw, empty=_EMPTY) == {'value': None}
    assert raw == bytearray()


@pytest.mark.parametrize('raw', (b'', b'  \n\t'))
def test_parse_body_empty_refused(raw: bytes) -> None:
    assert _refusal(lambda: parse_body(raw, empty=_EMPTY)) is _EMPTY


@pytest.mark.parametrize(
    'raw',
    (
        b'{"value": {"type": "STRING", "data": "a"}} {"value": null}',
        b'{}{}',
        b'{} []',
        b'{} 1',
    ),
)
def test_parse_body_more_than_one_value_refused(raw: bytes) -> None:
    err = _refusal(lambda: parse_body(raw, empty=_EMPTY))
    assert (err.status, err.error_code) == (400, 40004)
    assert 'more than one JSON value' in err.message
    assert 'records:batch' in err.message


@pytest.mark.parametrize(
    ('raw', 'reason'),
    (
        (b'{"value": ', 'not valid JSON'),
        (b'{} x', 'not valid JSON'),
        (b'\xff\xfe{}', 'not UTF-8'),
        (b'{"value": NaN}', 'not valid JSON'),
        (b'{"value": Infinity}', 'not valid JSON'),
        (b'[1]', 'must be a JSON object'),
        (b'"text"', 'must be a JSON object'),
        (_DEEP, 'not valid JSON'),
    ),
    ids=(
        'truncated',
        'trailing-garbage',
        'not-utf8',
        'nan',
        'infinity',
        'array',
        'string',
        'deep-nesting',
    ),
)
def test_parse_body_invalid_refused(raw: bytes, reason: str) -> None:
    err = _refusal(lambda: parse_body(raw, empty=_EMPTY))
    assert (err.status, err.error_code) == (400, 40003)
    assert reason in err.message


def test_parse_body_deep_nesting_refused_in_a_thread() -> None:
    """The threadpool's smaller stacks refuse it too, without crashing."""
    errors: list[RecordError] = []

    def parse() -> None:
        try:
            parse_body(_DEEP, empty=_EMPTY)
        except RecordError as e:
            errors.append(e)

    thread = threading.Thread(target=parse)
    thread.start()
    thread.join()
    assert [e.error_code for e in errors] == [40003]


# ============================================================================
# estimate_parse_cost
# ============================================================================


def _flood(unit: bytes, size: int) -> bytes:
    head = b'{"value":{"type":"JSON","data":['
    tail = b'0]}}'
    return head + unit * ((size - len(head) - len(tail)) // len(unit)) + tail


@pytest.mark.parametrize(
    'unit',
    (
        b'{},',
        b'[],',
        b'[[[]]],',
        b'{"a":1},',
        b'[1],',
        b'"ab",',
        '"€€",'.encode(),
        b'0.1,',
        b'1000,',
        b'"abcdefghij",',
        b'{"a":0.5,"b":"cd"},',
        # Written longer than they are sent
        b'1e15,',
        b'41654541853973785.0,',
        # Wide strings from escapes in an ASCII body
        b'"\\ud83d\\ude00' + b'a' * 1000 + b'",',
        b'"\\u4e2d' + b'a' * 1000 + b'",',
        '"\U0001f600",'.encode(),
    ),
)
def test_parse_cost_bounds_parsing_and_encoding(unit: bytes) -> None:
    """The estimate is at least what parsing and encoding allocate."""
    raw = _flood(unit, 1 * 1024 * 1024)
    tracemalloc.start()
    try:
        prepare_record(raw)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert estimate_parse_cost(raw) >= peak


@pytest.mark.parametrize('value', (b'1', b'1e15', b'"v"'))
def test_parse_cost_of_one_object_with_unique_keys(value: bytes) -> None:
    """Each key costs its entry, its hash table share and its memo."""
    keys = b','.join(b'"%d":%s' % (i, value) for i in range(100_000))
    raw = b'{"value":{"type":"JSON","data":{' + keys + b'}}}'
    tracemalloc.start()
    try:
        prepare_record(raw)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert estimate_parse_cost(raw) >= peak


@pytest.mark.parametrize(
    'escape',
    (b'\\ud83d\\ude00', b'\\u4e2d'),
)
def test_parse_cost_of_one_long_string_made_wide_by_an_escape(
    escape: bytes,
) -> None:
    """One escape widens the whole string, and its re-encoding."""
    size = 1024 * 1024
    raw = b'{"value":{"type":"JSON","data":["' + escape + b'a' * size + b'"]}}'
    tracemalloc.start()
    try:
        prepare_record(raw)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert estimate_parse_cost(raw) >= peak


@pytest.mark.parametrize('unit', (b'{},', b'[],'))
def test_parse_cost_refuses_a_16_mib_flood_unparsed(unit: bytes) -> None:
    raw = _flood(unit, MAX_BODY_BYTES)
    with patch('web_service.producer_service.json.JSONDecoder') as decoder:
        err = _refusal(lambda: prepare_record(raw))
    assert (err.status, err.error_code) == (413, 41303)
    decoder.assert_not_called()


def test_parse_cost_of_a_full_base64_body_is_low() -> None:
    raw = _raw(
        {
            'value': {
                'type': 'BINARY',
                'data': _b64(b'x' * (MAX_BODY_BYTES // 2)),
            },
        },
    )
    assert estimate_parse_cost(raw) < MAX_PARSE_COST // 2


# ============================================================================
# build_record - key and value formats
# ============================================================================


def test_build_record_json_is_compact_utf8() -> None:
    record = build_record(
        {
            'key': {'type': 'JSON', 'data': 'k'},
            'value': {'type': 'JSON', 'data': {'a': [1, 2.5], 'é': None}},
        },
    )
    assert record.key == b'"k"'
    assert record.value == '{"a":[1,2.5],"é":null}'.encode()


@pytest.mark.parametrize(
    'data',
    (0, False, [], {}, 'text', 12345678901234567890),
)
def test_build_record_json_any_value(data: Any) -> None:
    record = build_record({'value': {'type': 'JSON', 'data': data}})
    assert record.value is not None
    assert json.loads(record.value) == data


def test_build_record_binary_is_decoded() -> None:
    payload = bytes(range(256))
    record = build_record({'value': {'type': 'BINARY', 'data': _b64(payload)}})
    assert record.value == payload


def test_build_record_string_is_utf8() -> None:
    record = build_record({'value': {'type': 'STRING', 'data': 'héllo'}})
    assert record.value == 'héllo'.encode()


@pytest.mark.parametrize('data_type', ('BINARY', 'JSON', 'STRING'))
def test_build_record_null_or_absent_data_is_null(data_type: str) -> None:
    absent = build_record({'key': {'type': data_type}, 'value': _VALUE})
    assert absent.key is None
    null = build_record(
        {'key': _VALUE, 'value': {'type': data_type, 'data': None}},
    )
    assert null.value is None


def test_build_record_key_only_and_value_only() -> None:
    key_only = build_record({'key': {'type': 'STRING', 'data': 'k'}})
    assert (key_only.key, key_only.value) == (b'k', None)
    value_only = build_record({'value': {'type': 'STRING', 'data': 'v'}})
    assert (value_only.key, value_only.value) == (None, b'v')


@pytest.mark.parametrize(
    'entry',
    ({}, {'key': None, 'value': None}, {'value': {'type': 'JSON'}}),
    ids=('empty', 'nulls', 'no-data'),
)
def test_build_record_without_key_or_value_refused(
    entry: dict[str, Any],
) -> None:
    err = _refusal(lambda: build_record(entry))
    assert (err.status, err.error_code) == (400, 40005)
    assert 'needs a key or a value' in err.message
    assert 'kafka-python' in err.message


@pytest.mark.parametrize('data_type', ('AVRO', 'PROTOBUF', 'JSONSCHEMA'))
@pytest.mark.parametrize('field', ('key', 'value'))
def test_build_record_schema_format_refused(
    field: str,
    data_type: str,
) -> None:
    err = _refusal(
        lambda: build_record({field: {'type': data_type, 'data': {}}}),
    )
    assert (err.status, err.error_code) == (422, 42206)
    assert f'{field} type {data_type} needs a schema registry' in err.message
    assert 'Octopus runs no schema registry' in err.message


@pytest.mark.parametrize(
    'schema_field',
    (
        'schema',
        'schema_id',
        'schema_version',
        'subject',
        'subject_name_strategy',
    ),
)
def test_build_record_schema_field_refused(schema_field: str) -> None:
    err = _refusal(
        lambda: build_record(
            {'value': {'type': 'JSON', 'data': 1, schema_field: 1}},
        ),
    )
    assert (err.status, err.error_code) == (422, 42206)
    assert schema_field in err.message
    assert 'no schema registry' in err.message


@pytest.mark.parametrize('field', ('key', 'value'))
@pytest.mark.parametrize(
    'spec',
    ({'data': 'x'}, {'type': None, 'data': 'x'}),
    ids=('missing', 'null'),
)
def test_build_record_untyped_refused(
    field: str,
    spec: dict[str, Any],
) -> None:
    err = _refusal(lambda: build_record({field: spec}))
    assert (err.status, err.error_code) == (422, 42206)
    assert f'{field} has no type' in err.message
    assert 'no schema registry' in err.message


@pytest.mark.parametrize(
    ('spec', 'reason'),
    (
        (
            {'type': 'json', 'data': 1},
            "must be BINARY, JSON or STRING, not 'json'",
        ),
        ({'type': ['JSON'], 'data': 1}, 'must be BINARY, JSON or STRING'),
        ({'type': 'XML', 'data': '<a/>'}, 'must be BINARY, JSON or STRING'),
        ({'type': 'BINARY', 'data': 'not base64!'}, 'not valid base64'),
        ({'type': 'BINARY', 'data': 'YQ'}, 'not valid base64'),
        ({'type': 'BINARY', 'data': 5}, 'must be a string'),
        ({'type': 'STRING', 'data': {'a': 1}}, 'must be a string'),
        ({'type': 'STRING', 'data': '\ud800'}, 'cannot be written as UTF-8'),
        ({'type': 'JSON', 'data': '\ud800'}, 'cannot be written as UTF-8'),
        (
            {'type': 'JSON', 'data': 1, 'format': 'x'},
            'unknown field(s) format',
        ),
        ('JSON', 'must be an object with type and data'),
    ),
    ids=(
        'lowercase-type',
        'list-type',
        'unknown-type',
        'bad-base64',
        'unpadded-base64',
        'binary-not-string',
        'string-not-string',
        'string-lone-surrogate',
        'json-lone-surrogate',
        'unknown-field',
        'not-object',
    ),
)
def test_build_record_invalid_data_refused(spec: Any, reason: str) -> None:
    err = _refusal(lambda: build_record({'value': spec}))
    assert (err.status, err.error_code) == (400, 40005)
    assert reason in err.message


# ============================================================================
# build_record - partition, fields, headers, timestamp, size
# ============================================================================


@pytest.mark.parametrize('partition_id', (0, None))
def test_build_record_partition_zero_or_absent(partition_id: Any) -> None:
    build_record({'partition_id': partition_id, 'value': _VALUE})


@pytest.mark.parametrize('partition_id', (1, -1, 7))
def test_build_record_other_partition_not_found(partition_id: int) -> None:
    err = _refusal(
        lambda: build_record({'partition_id': partition_id, 'value': _VALUE}),
    )
    assert (err.status, err.error_code) == (404, 40402)
    assert f'Partition {partition_id} not found' in err.message


@pytest.mark.parametrize('partition_id', ('0', 0.0, True, [0]))
def test_build_record_non_integer_partition_refused(
    partition_id: Any,
) -> None:
    err = _refusal(
        lambda: build_record({'partition_id': partition_id, 'value': _VALUE}),
    )
    assert (err.status, err.error_code) == (400, 40005)
    assert 'partition_id must be an integer' in err.message


def test_build_record_unknown_field_refused() -> None:
    err = _refusal(lambda: build_record({'value': _VALUE, 'topic': 'x'}))
    assert (err.status, err.error_code) == (400, 40005)
    assert 'Unknown record field(s) topic' in err.message


def test_build_record_id_only_where_allowed() -> None:
    entry = {'id': '1', 'value': _VALUE}
    assert build_record(entry, extra_fields=frozenset({'id'})).value == b'v'
    err = _refusal(lambda: build_record(entry))
    assert 'Unknown record field(s) id' in err.message


def test_build_record_headers_encoded() -> None:
    record = build_record(
        {
            'value': _VALUE,
            'headers': [
                {'name': 'trace', 'value': _b64(b'\x00\x01')},
                {'name': 'empty', 'value': ''},
                {'name': 'null', 'value': None},
                {'name': 'absent'},
                {'name': 'trace', 'value': _b64(b'again')},
            ],
        },
    )
    assert record.headers == [
        ('trace', b'\x00\x01'),
        ('empty', b''),
        ('null', b''),
        ('absent', b''),
        ('trace', b'again'),
    ]


def test_build_record_no_headers() -> None:
    assert build_record({'value': _VALUE}).headers == []
    assert build_record({'value': _VALUE, 'headers': None}).headers == []
    assert build_record({'value': _VALUE, 'headers': []}).headers == []


@pytest.mark.parametrize(
    ('headers', 'reason'),
    (
        ({'name': 'a'}, 'headers must be a list'),
        (['a'], 'Header 0 must be an object'),
        ([{'value': _b64(b'x')}], 'Header 0 needs a string name'),
        ([{'name': 3}], 'Header 0 needs a string name'),
        ([{'name': '\ud800'}], 'Header 0 name cannot be written as UTF-8'),
        ([{'name': 'a', 'value': 'not base64!'}], 'not valid base64'),
        ([{'name': 'a', 'value': 1}], 'base64 string or null'),
        ([{'name': 'a', 'key': 'b'}], 'unknown field(s) key'),
    ),
)
def test_build_record_invalid_headers_refused(
    headers: Any,
    reason: str,
) -> None:
    err = _refusal(lambda: build_record({'value': _VALUE, 'headers': headers}))
    assert (err.status, err.error_code) == (400, 40005)
    assert reason in err.message


@pytest.mark.parametrize(
    ('timestamp', 'expected_ms'),
    (
        ('2026-10-26T12:00:00Z', NOW_MS),
        ('2026-10-26T12:00:00.123Z', NOW_MS + 123),
        ('2026-10-26T12:00:00.123456z', NOW_MS + 123),
        ('2026-10-26T12:00:00+00:00', NOW_MS),
        ('2026-10-26T07:00:00-05:00', NOW_MS),
        ('2026-10-26T14:00:00.5+02:00', NOW_MS + 500),
        ('1970-01-01T00:00:00Z', 0),
        (None, None),
    ),
)
def test_build_record_timestamp(
    timestamp: str | None,
    expected_ms: int | None,
) -> None:
    record = build_record({'value': _VALUE, 'timestamp': timestamp})
    assert record.timestamp_ms == expected_ms


@pytest.mark.parametrize(
    ('timestamp', 'reason'),
    (
        ('2026-10-26T12:00:00', 'has no UTC offset'),
        ('2026-10-26', 'has no UTC offset'),
        ('yesterday', 'is not ISO-8601'),
        ('', 'is not ISO-8601'),
        ('1969-12-31T23:59:59Z', 'is before 1970'),
        (1_793_016_000_000, 'must be an ISO-8601 string'),
    ),
)
def test_build_record_invalid_timestamp_refused(
    timestamp: Any,
    reason: str,
) -> None:
    err = _refusal(
        lambda: build_record({'value': _VALUE, 'timestamp': timestamp}),
    )
    assert (err.status, err.error_code) == (400, 40005)
    assert reason in err.message


def _sized_value(total: int) -> dict[str, Any]:
    """A BINARY value whose record, with key 'k', is total bytes to Kafka."""
    overhead = record_size(
        OutgoingRecord(key=b'k', value=b'', headers=[], timestamp_ms=None),
    )
    size = total - overhead
    # A larger value can take an extra byte to encode its length
    while (
        record_size(
            OutgoingRecord(
                key=b'k',
                value=b'x' * size,
                headers=[],
                timestamp_ms=None,
            ),
        )
        > total
    ):
        size -= 1
    return {'type': 'BINARY', 'data': _b64(b'x' * size)}


def test_build_record_at_size_limit_accepted() -> None:
    record = build_record(
        {
            'key': {'type': 'STRING', 'data': 'k'},
            'value': _sized_value(MAX_RECORD_BYTES),
        },
    )
    assert record_size(record) == MAX_RECORD_BYTES


def test_build_record_8_mib_value_refused_for_kafka_overhead() -> None:
    """An 8 MiB value is over the limit once Kafka's overhead is counted."""
    err = _refusal(
        lambda: build_record(
            {
                'value': {
                    'type': 'BINARY',
                    'data': _b64(b'x' * MAX_RECORD_BYTES),
                },
            },
        ),
    )
    assert (err.status, err.error_code) == (413, 41302)
    assert 'about 100 bytes' in err.message


@pytest.mark.parametrize(
    'entry',
    (
        {
            'key': {'type': 'STRING', 'data': 'k'},
            'value': _sized_value(MAX_RECORD_BYTES + 1),
        },
        {
            'key': {'type': 'STRING', 'data': 'k' * (MAX_RECORD_BYTES // 2)},
            'value': {'type': 'STRING', 'data': 'v' * (MAX_RECORD_BYTES // 2)},
            'headers': [{'name': 'h'}],
        },
    ),
    ids=('value', 'key-value-and-headers'),
)
def test_build_record_over_size_limit_refused(entry: dict[str, Any]) -> None:
    err = _refusal(lambda: build_record(entry))
    assert (err.status, err.error_code) == (413, 41302)
    assert str(MAX_RECORD_BYTES) in err.message


@pytest.mark.parametrize('data_type', ('BINARY', 'STRING'))
def test_build_record_oversized_data_refused_before_decoding(
    data_type: str,
) -> None:
    data = 'QUJD' * (MAX_RECORD_BYTES // 3 + 1)
    with (
        patch('web_service.producer_service.base64.b64decode') as decode,
        patch(
            'web_service.producer_service.DefaultRecordBatchBuilder',
        ) as size,
    ):
        err = _refusal(
            lambda: build_record({'value': {'type': data_type, 'data': data}}),
        )
    assert err.error_code == 41302
    decode.assert_not_called()
    size.estimate_size_in_bytes.assert_not_called()


# ============================================================================
# prepare_record and prepare_batch
# ============================================================================


def test_prepare_record_empty_body() -> None:
    err = _refusal(lambda: prepare_record(b' '))
    assert (err.status, err.error_code) == (422, 42206)
    assert err.message == 'Request body is empty. Data is required.'


def _entries(count: int) -> list[dict[str, Any]]:
    return [{'id': str(i), 'value': _VALUE} for i in range(count)]


@pytest.mark.parametrize('count', (1, MAX_BATCH_ENTRIES))
def test_prepare_batch_accepts_1_to_500(count: int) -> None:
    prepared = prepare_batch(_raw({'entries': _entries(count)}))
    assert [entry_id for entry_id, _ in prepared] == [
        str(i) for i in range(count)
    ]
    assert all(isinstance(r, OutgoingRecord) for _, r in prepared)


def test_prepare_batch_keeps_each_entry_refusal() -> None:
    prepared = prepare_batch(
        _raw(
            {
                'entries': [
                    {'id': 'a', 'value': _VALUE},
                    {'id': 'b', 'value': {'type': 'AVRO', 'data': {}}},
                    {'id': 'c', 'partition_id': 3, 'value': _VALUE},
                ],
            },
        ),
    )
    assert [entry_id for entry_id, _ in prepared] == ['a', 'b', 'c']
    a, b, c = (r for _, r in prepared)
    assert isinstance(a, OutgoingRecord)
    assert isinstance(b, RecordError)
    assert b.error_code == 42206
    assert isinstance(c, RecordError)
    assert c.error_code == 40402


@pytest.mark.parametrize(
    ('raw', 'reason'),
    (
        (b'', 'Request body is empty. Data is required.'),
        (_raw({}), 'entries must be a list of 1 to 500'),
        (_raw({'entries': []}), 'entries must be a list of 1 to 500'),
        (_raw({'entries': {'id': '1'}}), 'entries must be a list of 1 to 500'),
        (
            _raw({'entries': _entries(501)}),
            'has 501 entries; the most allowed is 500',
        ),
        (_raw({'entries': _entries(1), 'extra': 1}), 'Unknown field(s) extra'),
    ),
    ids=(
        'empty',
        'missing',
        'no-entries',
        'not-list',
        'over-500',
        'unknown-field',
    ),
)
def test_prepare_batch_refused(raw: bytes, reason: str) -> None:
    err = _refusal(lambda: prepare_batch(raw))
    assert (err.status, err.error_code) == (422, 42208)
    assert reason in err.message


@pytest.mark.parametrize(
    ('entries', 'reason'),
    (
        ([{'value': _VALUE}], 'Entry 0 has no id'),
        ([{'id': '1', 'value': _VALUE}, 'x'], 'Entry 1 has no id'),
        ([{'id': 1, 'value': _VALUE}], 'Entry 0 id must be 1 to 80'),
        ([{'id': None, 'value': _VALUE}], 'Entry 0 id must be 1 to 80'),
        ([{'id': '', 'value': _VALUE}], 'Entry 0 id must be 1 to 80'),
        ([{'id': 'x' * 81, 'value': _VALUE}], 'Entry 0 id must be 1 to 80'),
        ([{'id': 'a b', 'value': _VALUE}], 'letters, digits, dashes'),
        ([{'id': 'é', 'value': _VALUE}], 'letters, digits, dashes'),
        (
            [
                {'id': 'a', 'value': _VALUE},
                {'id': 'b', 'value': _VALUE},
                {'id': 'a', 'value': _VALUE},
            ],
            "Entry id 'a' appears more than once",
        ),
    ),
    ids=(
        'missing',
        'not-object',
        'not-string',
        'null',
        'empty',
        'too-long',
        'space',
        'non-ascii',
        'duplicate',
    ),
)
def test_prepare_batch_bad_id_refused(
    entries: list[Any],
    reason: str,
) -> None:
    err = _refusal(lambda: prepare_batch(_raw({'entries': entries})))
    assert (err.status, err.error_code) == (422, 42208)
    assert reason in err.message


def test_prepare_batch_id_of_80_accepted() -> None:
    entry_id = 'aZ09_-' * 13 + 'xy'
    assert len(entry_id) == 80
    prepared = prepare_batch(
        _raw({'entries': [{'id': entry_id, 'value': _VALUE}]}),
    )
    assert prepared[0][0] == entry_id


def test_prepare_batch_more_than_one_value_refused() -> None:
    raw = _raw({'entries': _entries(1)}) * 2
    err = _refusal(lambda: prepare_batch(raw))
    assert (err.status, err.error_code) == (400, 40004)


def test_prepare_batch_records_over_16_mib_encoded_refused() -> None:
    """Numbers written in full can make the records outgrow the body."""
    floats = b'[' + b'1e15,' * 999 + b'1e15]'
    entry = b'{"id":"%d","value":{"type":"JSON","data":' + floats + b'}}'
    raw = b'{"entries":[' + b','.join(entry % i for i in range(10)) + b']}'
    one = len(b'1000000000000000.0,') * 1000 - 1 + 2
    # The body fits, the records do not
    with patch(
        'web_service.producer_service.MAX_BODY_BYTES',
        one * 9 + 10,
    ):
        assert len(raw) < one * 9
        err = _refusal(lambda: prepare_batch(raw))
        assert (err.status, err.error_code) == (413, 41304)
        assert len(prepare_batch(raw[: raw.index(b',{"id":"9"')] + b']}')) == 9


# ============================================================================
# Replies
# ============================================================================


@pytest.mark.parametrize(
    ('ms', 'text'),
    (
        (NOW_MS, '2026-10-26T12:00:00.000Z'),
        (NOW_MS + 7, '2026-10-26T12:00:00.007Z'),
        (0, '1970-01-01T00:00:00.000Z'),
    ),
)
def test_format_timestamp(ms: int, text: str) -> None:
    assert format_timestamp(ms) == text


def test_record_reply() -> None:
    result = {'partition_id': 0, 'offset': 3, 'timestamp': 'T'}
    assert record_reply(TOPIC, result) == (
        {
            'topic_name': TOPIC,
            'partition_id': 0,
            'offset': 3,
            'timestamp': 'T',
        },
        200,
    )
    err = RecordError(500, 50003, 'later')
    assert record_reply(TOPIC, err) == (
        {'error_code': 50003, 'message': 'later'},
        500,
    )


def test_batch_reply_keeps_entry_order() -> None:
    ok = {'partition_id': 0, 'offset': 1, 'timestamp': 'T'}
    refused = RecordError(422, 42206, 'schema')
    failed = RecordError(500, 50003, 'later')
    prepared: list[tuple[str, OutgoingRecord | RecordError]] = [
        ('a', _record()),
        ('b', refused),
        ('c', _record()),
        ('d', _record()),
    ]
    body, status = batch_reply(TOPIC, prepared, [ok, failed, ok])
    assert status == 207
    assert [s['id'] for s in body['successes']] == ['a', 'd']
    assert body['successes'][0] == {'id': 'a', 'topic_name': TOPIC, **ok}
    assert body['failures'] == [
        {'id': 'b', 'error_code': 42206, 'message': 'schema'},
        {'id': 'c', 'error_code': 50003, 'message': 'later'},
    ]


# ============================================================================
# ProduceAdmission
# ============================================================================


def _refused(call: Callable[[], Any]) -> RecordError:
    with pytest.raises(RecordError) as caught:
        call()
    assert (caught.value.status, caught.value.error_code) == (429, 42901)
    assert caught.value.retry_after_s == RETRY_AFTER_S
    return caught.value


def test_admission_bounds_requests_past_their_read() -> None:
    """Places are taken once a body is read, not on admission."""
    admission = ProduceAdmission(max_requests=2, max_body_bytes=100)
    reading = [admission.admit(f'reader-{i}') for i in range(5)]
    first = admission.admit('a')
    second = admission.admit('b')
    late = admission.admit('c')
    first.start()
    second.start()
    # Admitted while places were free, it finds none once read
    _refused(late.start)
    assert admission.requests == 2
    first.release()
    late.start()
    for reservation in reading:
        reservation.release()
    assert admission.requests == 2


def test_admission_bounds_body_bytes() -> None:
    admission = ProduceAdmission(
        max_requests=10,
        max_body_bytes=100,
        subject_max_body_bytes=100,
    )
    first = admission.admit('a')
    first.grow_to(60)
    second = admission.admit('b')
    _refused(lambda: second.grow_to(41))
    second.grow_to(40)
    first.release()
    second.release()
    assert (admission.requests, admission.body_bytes) == (0, 0)


def test_admission_bounds_each_subject() -> None:
    admission = ProduceAdmission(
        max_requests=10,
        max_body_bytes=100,
        subject_max_requests=2,
        subject_max_body_bytes=50,
    )
    admission.admit('a')
    admission.admit('a')
    # Requests still sending their body count against the share
    assert 'one user' in _refused(lambda: admission.admit('a')).message
    # Another subject is not held back, up to its own share of bytes
    other = admission.admit('b')
    other.grow_to(50)
    _refused(lambda: other.grow_to(51))


def test_admission_refuses_what_it_could_not_serve() -> None:
    """Admission checks the places and declared bytes, reserving none."""
    admission = ProduceAdmission(
        max_requests=1,
        max_body_bytes=100,
        subject_max_body_bytes=60,
    )
    first = admission.admit('a', 50)
    assert (admission.requests, admission.body_bytes) == (0, 0)
    first.grow_to(50)
    assert 'one user' in _refused(lambda: admission.admit('a', 11)).message
    assert 'service' in _refused(lambda: admission.admit('b', 51)).message
    second = admission.admit('b', 50)
    second.start()
    assert 'at once' in _refused(lambda: admission.admit('c')).message
    # A refused subject leaves nothing behind
    assert set(admission._subjects) == {'a', 'b'}
    first.release()
    second.release()
    assert admission._subjects == {}


def test_reservation_grows_and_releases_once() -> None:
    admission = ProduceAdmission(
        max_requests=10,
        max_body_bytes=100,
        subject_max_body_bytes=60,
    )
    reservation = admission.admit('a')
    assert (reservation.body_bytes, admission.body_bytes) == (0, 0)
    reservation.grow_to(40)
    reservation.grow_to(30)  # never shrinks
    assert (reservation.body_bytes, admission.body_bytes) == (40, 40)
    _refused(lambda: reservation.grow_to(61))
    other = admission.admit('b')
    other.grow_to(50)
    _refused(lambda: reservation.grow_to(51))
    reservation.start()
    reservation.start()  # one place only
    assert admission.requests == 1
    reservation.release()
    reservation.release()  # once only
    other.release()
    assert (admission.requests, admission.body_bytes) == (0, 0)
    assert admission._subjects == {}


# ============================================================================
# ProducerService - the producer
# ============================================================================


def test_producer_built_on_first_use_only(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
) -> None:
    assert fake_producer.builds == 0
    producer_service.send(KAFKA_TOPIC, [_record()], _deadline())
    producer_service.send(KAFKA_TOPIC, [_record()], _deadline())
    assert fake_producer.builds == 1
    assert len(fake_producer.sent) == 2


def test_producer_config(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
) -> None:
    producer_service.send(KAFKA_TOPIC, [_record()], _deadline())
    config = fake_producer.config
    assert config['bootstrap_servers'] == 'localhost:9092'
    assert config['security_protocol'] == 'SASL_SSL'
    assert config['sasl_mechanism'] == 'OAUTHBEARER'
    assert config['sasl_oauth_token_provider'].region == 'us-east-1'
    assert config['acks'] == 'all'
    assert config['max_request_size'] == 8_388_608
    assert config['max_block_ms'] == PRODUCER_MAX_BLOCK_MS
    assert config['delivery_timeout_ms'] == SEND_TIMEOUT_S * 1000
    assert config['max_in_flight_requests_per_connection'] == 1
    assert config['allow_auto_create_topics'] is False
    assert not config.get('enable_idempotence', False)


def test_producer_config_accepted_by_kafka_python(
    producer_service: ProducerService,
) -> None:
    """kafka-python takes every setting (it asserts on unknown ones)."""
    from kafka import KafkaProducer

    captured: dict[str, Any] = {}

    def build(**config: Any) -> FakeProducer:
        captured.update(config)
        return FakeProducer()

    with patch(
        'web_service.producer_service.KafkaProducer',
        side_effect=build,
    ):
        producer_service.send(KAFKA_TOPIC, [_record()], _deadline())
    unknown = set(captured) - set(KafkaProducer.DEFAULT_CONFIG)
    assert not unknown
    assert (
        captured['delivery_timeout_ms']
        >= captured['request_timeout_ms']
        + KafkaProducer.DEFAULT_CONFIG['linger_ms']
    )


def test_producer_built_once_under_concurrency(
    producer_service: ProducerService,
) -> None:
    fake = FakeProducer()
    started = threading.Barrier(8)

    def slow_build(**config: Any) -> FakeProducer:
        # Long enough for the other threads to reach the lock
        time.sleep(0.05)
        fake.builds += 1
        return fake

    def produce() -> None:
        started.wait()
        producer_service.send(KAFKA_TOPIC, [_record()], _deadline())

    with patch(
        'web_service.producer_service.KafkaProducer',
        side_effect=slow_build,
    ):
        threads = [threading.Thread(target=produce) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    assert fake.builds == 1
    assert len(fake.sent) == 8


def test_producer_not_configured() -> None:
    service = ProducerService(bootstrap_servers=None, region='us-east-1')
    with patch('web_service.producer_service.KafkaProducer') as cls:
        (result,) = service.send(KAFKA_TOPIC, [_record()], _deadline())
    assert isinstance(result, RecordError)
    assert result.body() == {
        'error_code': 50002,
        'message': 'Kafka endpoint is not configured',
    }
    cls.assert_not_called()


def test_failed_build_fails_fast_then_retries(
    producer_service: ProducerService,
) -> None:
    fake = FakeProducer()
    builds: list[Any] = [
        KafkaTimeoutError(f'Unable to bootstrap from {_BROKER}'),
        fake,
    ]
    clock = [1000.0]

    def build(**config: Any) -> FakeProducer:
        result = builds.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    with (
        patch('web_service.producer_service.KafkaProducer', side_effect=build),
        patch(
            'web_service.producer_service.time.monotonic',
            lambda: clock[0],
        ),
    ):
        deadline = clock[0] + SEND_TIMEOUT_S
        (first,) = producer_service.send(KAFKA_TOPIC, [_record()], deadline)
        # Within the retry window: no build, the same reply
        clock[0] += PRODUCER_BUILD_RETRY_S - 1
        deadline = clock[0] + SEND_TIMEOUT_S
        (second,) = producer_service.send(KAFKA_TOPIC, [_record()], deadline)
        assert len(builds) == 1
        clock[0] += 2
        deadline = clock[0] + SEND_TIMEOUT_S
        (third,) = producer_service.send(KAFKA_TOPIC, [_record()], deadline)
    for result in (first, second):
        assert isinstance(result, RecordError)
        assert (result.status, result.error_code) == (503, 50302)
        assert _BROKER not in result.message
    # Retry-After is what is left of the retry window
    assert isinstance(first, RecordError)
    assert isinstance(second, RecordError)
    assert first.retry_after_s == PRODUCER_BUILD_RETRY_S
    assert second.retry_after_s == 1
    assert not isinstance(third, RecordError)
    assert builds == []


# ============================================================================
# ProducerService - topics Kafka does not list
# ============================================================================


@pytest.fixture
def short_grace() -> Iterator[None]:
    with (
        patch('web_service.producer_service.TOPIC_GRACE_S', 0.1),
        patch('web_service.producer_service.PRODUCER_MAX_BLOCK_MS', 500),
    ):
        yield


@pytest.mark.usefixtures('short_grace')
def test_topic_missing_from_kafka_not_found_quickly(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
) -> None:
    """Not found once the cluster has answered without it, unsent."""
    fake_producer._metadata.missing.add(KAFKA_TOPIC)
    start = time.monotonic()
    results = producer_service.send(
        KAFKA_TOPIC,
        [_record(), _record()],
        _deadline(),
    )
    elapsed = time.monotonic() - start
    for result in results:
        assert isinstance(result, RecordError)
        assert (result.status, result.error_code) == (404, 40401)
        assert result.message == 'Topic not found.'
    assert 0.1 <= elapsed < 0.4
    assert fake_producer._metadata.updates >= 2
    assert fake_producer._sender.topics == [KAFKA_TOPIC]
    assert fake_producer.sent == []


class _AnsweringSender(FakeSender):
    """Answers each metadata update with one response, 10 ms later.

    Runs kafka-python's own ClusterMetadata, so the producer reads its
    answers as it would a broker's.
    """

    def __init__(
        self,
        metadata: ClusterMetadata,
        topics: list[tuple[int, str]],
    ) -> None:
        super().__init__()
        self.metadata = metadata
        response = MetadataResponse[1]
        topic_class = response.MetadataResponseTopic
        partition = topic_class.MetadataResponsePartition(
            error_code=0,
            partition_index=0,
            leader_id=0,
            replica_nodes=[0],
            isr_nodes=[0],
        )
        built = response(
            brokers=[
                response.MetadataResponseBroker(
                    node_id=0,
                    host='broker',
                    port=9098,
                    rack=None,
                ),
            ],
            controller_id=0,
            topics=[
                topic_class(
                    error_code=error,
                    name=topic,
                    is_internal=False,
                    partitions=[] if error else [partition],
                )
                for error, topic in topics
            ],
        )
        # Decoded from its bytes, as a broker's response is
        self.response = response.decode(io.BytesIO(built.encode()))

    def add_topic(self, topic: str) -> None:
        super().add_topic(topic)
        # As kafka-python's sender does, before its next metadata request
        self.metadata.add_topic(topic)

    def wakeup(self) -> None:
        threading.Timer(
            0.01,
            self.metadata.update_metadata,
            (self.response,),
        ).start()


_UNKNOWN = UnknownTopicOrPartitionError.errno
_LEADER_NOT_AVAILABLE = LeaderNotAvailableError.errno
_NOT_AUTHORIZED = TopicAuthorizationFailedError.errno


@pytest.mark.usefixtures('short_grace')
@pytest.mark.parametrize(
    ('topics', 'status', 'code'),
    (
        # The response names only this topic, and the update fails
        ([(_UNKNOWN, KAFKA_TOPIC)], 404, 40401),
        # It names others too, and succeeds without this one
        ([(_UNKNOWN, KAFKA_TOPIC), (0, 'ns-other.t')], 404, 40401),
        ([(0, 'ns-other.t')], 404, 40401),
        ([(_NOT_AUTHORIZED, KAFKA_TOPIC)], 403, 40301),
        # A topic being created is not taken to be missing
        ([(_LEADER_NOT_AVAILABLE, KAFKA_TOPIC)], 500, 50003),
    ),
    ids=(
        'unknown',
        'unknown-among-others',
        'unlisted',
        'unauthorized',
        'leader',
    ),
)
def test_topic_lookup_with_kafka_python_metadata(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
    *,
    topics: list[tuple[int, str]],
    status: int,
    code: int,
) -> None:
    metadata = ClusterMetadata()
    fake_producer._metadata = metadata
    fake_producer._sender = _AnsweringSender(metadata, topics)
    (result,) = producer_service.send(KAFKA_TOPIC, [_record()], _deadline())
    assert isinstance(result, RecordError)
    assert (result.status, result.error_code) == (status, code)
    assert fake_producer.sent == []


@pytest.mark.usefixtures('short_grace')
def test_topic_listed_by_kafka_python_metadata_is_sent(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
) -> None:
    metadata = ClusterMetadata()
    fake_producer._metadata = metadata
    fake_producer._sender = _AnsweringSender(
        metadata,
        [(0, KAFKA_TOPIC)],
    )
    (result,) = producer_service.send(KAFKA_TOPIC, [_record()], _deadline())
    assert not isinstance(result, RecordError)
    assert len(fake_producer.sent) == 1


@pytest.fixture
def real_producer(
    producer_service: ProducerService,
) -> Iterator[KafkaProducer]:
    """A real KafkaProducer with the service's settings, never connected.

    The api_version spares it the version probe, and its sender thread is
    never started, so it opens no connection.
    """
    built: list[KafkaProducer] = []

    def build(**config: Any) -> KafkaProducer:
        built.append(KafkaProducer(**config, api_version=(2, 8, 0)))
        return built[0]

    with (
        patch.object(Sender, 'start'),
        patch('web_service.producer_service.KafkaProducer', side_effect=build),
    ):
        producer_service._get_producer(_deadline())
    producer = built[0]
    yield producer
    # close() would join the sender thread, which never started
    producer._unregister_cleanup()
    producer._sender._client.close()
    producer._closed = True


@pytest.mark.usefixtures('short_grace')
def test_topic_lookup_runs_on_kafka_python_internals(
    producer_service: ProducerService,
    real_producer: KafkaProducer,
) -> None:
    """Fails if a kafka-python upgrade drops what the topic lookup uses.

    The lookup reads the producer's _metadata and _sender, as its own
    send() does; here they answer the lookup as a broker would.
    """
    metadata = real_producer._metadata
    sender = real_producer._sender
    assert isinstance(metadata, ClusterMetadata)
    assert isinstance(sender, Sender)
    assert metadata.unauthorized_topics == set()
    response = _AnsweringSender(metadata, [(_UNKNOWN, KAFKA_TOPIC)]).response
    answer = sender.wakeup

    def wakeup() -> None:
        answer()
        threading.Timer(0.01, metadata.update_metadata, (response,)).start()

    with patch.object(sender, 'wakeup', wakeup):
        refusal = ProducerService._find_topic(
            real_producer,
            KAFKA_TOPIC,
        )
    assert refusal is not None
    assert (refusal.status, refusal.error_code) == (404, 40401)


@pytest.mark.usefixtures('short_grace')
def test_topic_that_appears_is_sent(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
) -> None:
    """A topic just created reaches the metadata within the grace."""
    metadata = fake_producer._metadata
    metadata.missing.add(KAFKA_TOPIC)
    threading.Timer(0.05, metadata.missing.clear).start()
    (result,) = producer_service.send(KAFKA_TOPIC, [_record()], _deadline())
    assert not isinstance(result, RecordError)
    assert len(fake_producer.sent) == 1


@pytest.mark.usefixtures('short_grace')
def test_topic_not_authorized(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
) -> None:
    metadata = fake_producer._metadata
    metadata.missing.add(KAFKA_TOPIC)
    metadata.unauthorized_topics.add(KAFKA_TOPIC)
    (result,) = producer_service.send(KAFKA_TOPIC, [_record()], _deadline())
    assert isinstance(result, RecordError)
    assert (result.status, result.error_code) == (403, 40301)
    assert fake_producer.sent == []


@pytest.mark.usefixtures('short_grace')
def test_topic_lookup_without_an_answer(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
) -> None:
    """A cluster that does not answer is not taken to lack the topic."""
    metadata = fake_producer._metadata
    metadata.missing.add(KAFKA_TOPIC)
    metadata.answering = False
    start = time.monotonic()
    (result,) = producer_service.send(KAFKA_TOPIC, [_record()], _deadline())
    assert isinstance(result, RecordError)
    assert (result.status, result.error_code) == (500, 50003)
    assert 'not sent' in result.message
    assert 0.5 <= time.monotonic() - start < 1
    assert fake_producer.sent == []


def test_build_lock_wait_bounded_by_deadline(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
) -> None:
    """A request waits for another's build only while a send could start."""
    deadline = time.monotonic() + PRODUCER_MAX_BLOCK_MS / 1000 + 0.2
    with producer_service._lock:
        start = time.monotonic()
        (result,) = producer_service.send(KAFKA_TOPIC, [_record()], deadline)
        waited = time.monotonic() - start
    assert isinstance(result, RecordError)
    assert result.error_code == 50003
    assert 'too little' in result.message
    assert waited < 1
    assert fake_producer.builds == 0


def test_no_send_starts_with_less_than_max_block_left(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
) -> None:
    producer_service.send(KAFKA_TOPIC, [_record()], _deadline())
    deadline = time.monotonic() + PRODUCER_MAX_BLOCK_MS / 1000 - 0.5
    (result,) = producer_service.send(KAFKA_TOPIC, [_record()], deadline)
    assert isinstance(result, RecordError)
    assert (result.status, result.error_code) == (500, 50003)
    assert len(fake_producer.sent) == 1


def test_send_waits_until_the_deadline_only(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
) -> None:
    deadline = time.monotonic() + 20
    producer_service.send(KAFKA_TOPIC, [_record()], deadline)
    timeout = fake_producer.futures[0].timeout
    assert timeout is not None
    assert 19 < timeout <= 20


def test_send_shares_one_deadline(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
) -> None:
    """Records left once too little time remains fail without a send."""
    clock = [1000.0]
    real_send = fake_producer.send

    def slow_send(topic: str, **kwargs: Any) -> FakeFuture:
        # Each send takes 8 s of the 30 s
        clock[0] += 8
        return real_send(topic, **kwargs)

    fake_producer.send = slow_send  # type: ignore[method-assign]
    with patch(
        'web_service.producer_service.time.monotonic',
        lambda: clock[0],
    ):
        results = producer_service.send(
            KAFKA_TOPIC,
            [_record()] * 4,
            clock[0] + SEND_TIMEOUT_S,
        )
    # Sends at 0, 8 and 16 s leave 6 s, under the 10 s a send may block
    assert len(fake_producer.sent) == 3
    assert isinstance(results[3], RecordError)
    assert 'too little' in results[3].message
    # The waits share what is left of the 30 s
    assert [f.timeout for f in fake_producer.futures] == [6.0, 6.0, 6.0]


@pytest.mark.parametrize(
    ('error', 'waiting', 'status', 'code', 'reason'),
    (
        (
            KafkaTimeoutError(f'Timeout after waiting for 30 secs. {_BROKER}'),
            True,
            500,
            50003,
            'did not acknowledge the record in time; it may still be written',
        ),
        (
            KafkaTimeoutError(f'Failed to update metadata. {_BROKER}'),
            False,
            500,
            50003,
            'the record was not sent',
        ),
        (
            NotEnoughReplicasError(_BROKER),
            True,
            500,
            50003,
            '(NotEnoughReplicasError); retry it',
        ),
        (
            UnsupportedForMessageFormatError(_BROKER),
            True,
            500,
            50002,
            '(UnsupportedForMessageFormatError)',
        ),
        (RuntimeError(_BROKER), False, 500, 50002, '(RuntimeError)'),
        (
            MessageSizeTooLargeError(_BROKER),
            True,
            413,
            41302,
            'refused the record as too large',
        ),
        (
            TopicAuthorizationFailedError(_BROKER),
            True,
            403,
            40301,
            'not authorized to write to the topic',
        ),
    ),
    ids=(
        'ack-timeout',
        'metadata-timeout',
        'retriable',
        'not-retriable',
        'send-raises',
        'broker-size',
        'not-authorized',
    ),
)
def test_send_kafka_failure(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
    *,
    error: Exception,
    waiting: bool,
    status: int,
    code: int,
    reason: str,
) -> None:
    errors = fake_producer.get_errors if waiting else fake_producer.send_errors
    errors[0] = error
    (result,) = producer_service.send(KAFKA_TOPIC, [_record()], _deadline())
    assert isinstance(result, RecordError)
    assert (result.status, result.error_code) == (status, code)
    assert reason in result.message
    # The exception's text, which names brokers, is not returned
    assert _BROKER not in result.message


def test_send_success_and_partial_failure(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
) -> None:
    fake_producer.get_errors[1] = NotEnoughReplicasError()
    records = [
        OutgoingRecord(
            key=b'k',
            value=b'{"a":1}',
            headers=[('h', b'hv')],
            timestamp_ms=NOW_MS + 250,
        ),
        _record(),
        _record(b'w'),
    ]
    first, second, third = producer_service.send(
        KAFKA_TOPIC,
        records,
        _deadline(),
    )
    assert first == {
        'partition_id': 0,
        'offset': 100,
        'timestamp': '2026-10-26T12:00:00.250Z',
    }
    assert isinstance(second, RecordError)
    assert third == {
        'partition_id': 0,
        'offset': 102,
        'timestamp': '2026-10-26T12:00:00.000Z',
    }
    assert fake_producer.sent[0] == {
        'topic': KAFKA_TOPIC,
        'key': b'k',
        'value': b'{"a":1}',
        'headers': [('h', b'hv')],
        'partition': 0,
        'timestamp_ms': NOW_MS + 250,
    }


def test_send_in_order_before_waiting(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
) -> None:
    sent_before_first_wait: list[int] = []
    fake_producer.gate = lambda _timeout: sent_before_first_wait.append(
        len(fake_producer.sent),
    )
    records = [_record(str(i).encode()) for i in range(5)]
    producer_service.send(KAFKA_TOPIC, records, _deadline())
    assert [s['value'] for s in fake_producer.sent] == [
        str(i).encode() for i in range(5)
    ]
    assert sent_before_first_wait[0] == 5


def test_send_nothing_builds_no_producer(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
) -> None:
    assert producer_service.send(KAFKA_TOPIC, [], _deadline()) == []
    assert fake_producer.builds == 0


# ============================================================================
# ProducerService - close
# ============================================================================


def test_close_closes_producer_with_timeout(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
) -> None:
    producer_service.send(KAFKA_TOPIC, [_record()], _deadline())
    producer_service.close()
    assert fake_producer.close_calls == [PRODUCER_CLOSE_TIMEOUT_S / 2]


def test_close_without_producer_builds_none(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
) -> None:
    producer_service.close()
    assert fake_producer.builds == 0
    assert fake_producer.close_calls == []


def test_send_after_close_refused(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
) -> None:
    producer_service.close()
    (result,) = producer_service.send(KAFKA_TOPIC, [_record()], _deadline())
    assert isinstance(result, RecordError)
    assert result.body() == {
        'error_code': 50002,
        'message': 'Failed to produce: service is shutting down',
    }
    assert fake_producer.builds == 0


def test_close_error_is_logged_not_raised(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
) -> None:
    producer_service.send(KAFKA_TOPIC, [_record()], _deadline())

    def fail(timeout: float | None = None) -> None:
        raise RuntimeError('close failed')

    fake_producer.close = fail  # type: ignore[method-assign]
    producer_service.close()
