"""Unit tests for ProducerService and the produce request parsing."""

from __future__ import annotations

import base64
import json
import threading
import time
from collections.abc import Callable
from collections.abc import Iterator
from typing import Any
from unittest.mock import patch

import pytest
from kafka.errors import KafkaTimeoutError
from kafka.errors import MessageSizeTooLargeError
from kafka.errors import NoBrokersAvailable
from kafka.errors import NotEnoughReplicasError
from kafka.producer.future import RecordMetadata
from kafka.structs import TopicPartition

from web_service.producer_service import build_record
from web_service.producer_service import format_timestamp
from web_service.producer_service import MAX_BATCH_ENTRIES
from web_service.producer_service import MAX_RECORD_BYTES
from web_service.producer_service import parse_batch
from web_service.producer_service import parse_body
from web_service.producer_service import PRODUCER_CLOSE_TIMEOUT_S
from web_service.producer_service import ProducerService
from web_service.producer_service import RecordError
from web_service.producer_service import SEND_TIMEOUT_S

NAMESPACE = 'ns-test123'
TOPIC = 't1'
KAFKA_TOPIC = f'{NAMESPACE}.{TOPIC}'
# 2026-10-26T12:00:00.000Z
NOW_MS = 1_793_016_000_000
_VALUE = {'type': 'STRING', 'data': 'v'}


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


class FakeProducer:
    """Records each send; sends and acknowledgements can be made to fail."""

    def __init__(self) -> None:
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


# ============================================================================
# parse_body
# ============================================================================

# Nested deeper than the JSON parser allows
_DEEP = b'{"value": ' + b'[' * 1_000_000 + b']' * 1_000_000 + b'}'


def test_parse_body_one_object() -> None:
    assert parse_body(b' \n{"value": null}\r\n\t') == {'value': None}


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
    err = _refusal(lambda: parse_body(raw))
    assert (err.status, err.error_code) == (400, 40002)
    assert 'more than one JSON value' in err.message
    assert 'records:batch' in err.message


@pytest.mark.parametrize(
    ('raw', 'reason'),
    (
        (b'', 'empty'),
        (b'  \n', 'empty'),
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
        'empty',
        'whitespace',
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
    err = _refusal(lambda: parse_body(raw))
    assert (err.status, err.error_code) == (400, 40001)
    assert reason in err.message


def test_parse_body_deep_nesting_refused_in_a_thread() -> None:
    """The threadpool's smaller stacks refuse it too, without crashing."""
    errors: list[RecordError] = []

    def parse() -> None:
        try:
            parse_body(_DEEP)
        except RecordError as e:
            errors.append(e)

    thread = threading.Thread(target=parse)
    thread.start()
    thread.join()
    assert [e.error_code for e in errors] == [40001]


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
    assert (err.status, err.error_code) == (400, 40003)
    assert 'needs a key or a value' in err.message


@pytest.mark.parametrize('data_type', ('AVRO', 'PROTOBUF', 'JSONSCHEMA'))
@pytest.mark.parametrize('field', ('key', 'value'))
def test_build_record_schema_format_refused(
    field: str,
    data_type: str,
) -> None:
    err = _refusal(
        lambda: build_record({field: {'type': data_type, 'data': {}}}),
    )
    assert (err.status, err.error_code) == (400, 40004)
    assert f'{field} type {data_type} needs a schema registry' in err.message
    assert 'Octopus does not run' in err.message


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
    assert (err.status, err.error_code) == (400, 40004)
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
    assert (err.status, err.error_code) == (400, 40004)
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
    assert (err.status, err.error_code) == (400, 40003)
    assert reason in err.message


# ============================================================================
# build_record - partition, fields, headers, timestamp, size
# ============================================================================


@pytest.mark.parametrize('partition_id', (0, None))
def test_build_record_partition_zero_or_absent(partition_id: Any) -> None:
    build_record({'partition_id': partition_id, 'value': _VALUE})


@pytest.mark.parametrize('partition_id', (1, -1, 7, '0', 0.0, True, [0]))
def test_build_record_other_partition_refused(partition_id: Any) -> None:
    err = _refusal(
        lambda: build_record({'partition_id': partition_id, 'value': _VALUE}),
    )
    assert (err.status, err.error_code) == (400, 40005)
    assert 'every topic has one partition, 0' in err.message


def test_build_record_unknown_field_refused() -> None:
    err = _refusal(lambda: build_record({'value': _VALUE, 'topic': 'x'}))
    assert (err.status, err.error_code) == (400, 40003)
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
    assert (err.status, err.error_code) == (400, 40003)
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
    assert (err.status, err.error_code) == (400, 40003)
    assert reason in err.message


def test_build_record_at_size_limit_accepted() -> None:
    record = build_record(
        {
            'key': {'type': 'STRING', 'data': 'k'},
            'value': {
                'type': 'BINARY',
                'data': _b64(b'x' * (MAX_RECORD_BYTES - 1 - 5)),
            },
            'headers': [{'name': 'h', 'value': _b64(b'1234')}],
        },
    )
    assert record.value is not None


@pytest.mark.parametrize(
    'entry',
    (
        {
            'value': {
                'type': 'BINARY',
                'data': _b64(b'x' * (MAX_RECORD_BYTES + 1)),
            },
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


# ============================================================================
# parse_batch
# ============================================================================


def _entries(count: int) -> list[dict[str, Any]]:
    return [{'id': str(i), 'value': _VALUE} for i in range(count)]


@pytest.mark.parametrize('count', (1, MAX_BATCH_ENTRIES))
def test_parse_batch_accepts_1_to_500(count: int) -> None:
    assert len(parse_batch(_raw({'entries': _entries(count)}))) == count


@pytest.mark.parametrize(
    ('body', 'reason'),
    (
        ({}, 'entries must be a list of 1 to 500'),
        ({'entries': []}, 'entries must be a list of 1 to 500'),
        ({'entries': {'id': '1'}}, 'entries must be a list of 1 to 500'),
        (
            {'entries': _entries(501)},
            'has 501 entries; the most allowed is 500',
        ),
        ({'entries': _entries(1), 'extra': 1}, 'Unknown field(s) extra'),
    ),
    ids=('missing', 'empty', 'not-list', 'over-500', 'unknown-field'),
)
def test_parse_batch_entries_refused(body: Any, reason: str) -> None:
    err = _refusal(lambda: parse_batch(_raw(body)))
    assert (err.status, err.error_code) == (400, 40007)
    assert reason in err.message


@pytest.mark.parametrize(
    ('entries', 'reason'),
    (
        ([{'value': _VALUE}], 'Entry 0 has no id'),
        ([{'id': '1', 'value': _VALUE}, 'x'], 'Entry 1 has no id'),
        (
            [{'id': 1, 'value': _VALUE}],
            'Entry 0 id must be a string of 1 to 80',
        ),
        ([{'id': None, 'value': _VALUE}], 'Entry 0 id must be a string'),
        ([{'id': '', 'value': _VALUE}], 'Entry 0 id must be a string'),
        ([{'id': 'x' * 81, 'value': _VALUE}], 'Entry 0 id must be a string'),
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
        'duplicate',
    ),
)
def test_parse_batch_bad_id_refused(
    entries: list[Any],
    reason: str,
) -> None:
    err = _refusal(lambda: parse_batch(_raw({'entries': entries})))
    assert (err.status, err.error_code) == (400, 40008)
    assert reason in err.message


def test_parse_batch_id_of_80_accepted() -> None:
    entries = [{'id': 'x' * 80, 'value': _VALUE}]
    assert parse_batch(_raw({'entries': entries})) == entries


def test_parse_batch_more_than_one_value_refused() -> None:
    raw = _raw({'entries': _entries(1)}) * 2
    err = _refusal(lambda: parse_batch(raw))
    assert (err.status, err.error_code) == (400, 40002)


# ============================================================================
# format_timestamp
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


# ============================================================================
# ProducerService - the producer
# ============================================================================


def test_producer_built_on_first_use_only(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
) -> None:
    assert fake_producer.builds == 0
    raw = _raw({'value': _VALUE})
    producer_service.produce_record(NAMESPACE, TOPIC, raw)
    producer_service.produce_record(NAMESPACE, TOPIC, raw)
    assert fake_producer.builds == 1
    assert len(fake_producer.sent) == 2


def test_producer_config(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
) -> None:
    producer_service.produce_record(NAMESPACE, TOPIC, _raw({'value': _VALUE}))
    config = fake_producer.config
    assert config['bootstrap_servers'] == 'localhost:9092'
    assert config['security_protocol'] == 'SASL_SSL'
    assert config['sasl_mechanism'] == 'OAUTHBEARER'
    assert config['sasl_oauth_token_provider'].region == 'us-east-1'
    assert config['acks'] == 'all'
    assert config['max_request_size'] > MAX_RECORD_BYTES
    assert config['max_block_ms'] <= SEND_TIMEOUT_S * 1000
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
        producer_service.produce_record(
            NAMESPACE,
            TOPIC,
            _raw({'value': _VALUE}),
        )
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
        producer_service.produce_record(
            NAMESPACE,
            TOPIC,
            _raw({'value': _VALUE}),
        )

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
        body, status = service.produce_record(
            NAMESPACE,
            TOPIC,
            _raw({'value': _VALUE}),
        )
    assert status == 500
    assert body == {
        'error_code': 50002,
        'message': 'Kafka endpoint is not configured',
    }
    cls.assert_not_called()


def test_producer_build_failure_then_retried(
    producer_service: ProducerService,
) -> None:
    fake = FakeProducer()
    builds = iter([NoBrokersAvailable(), fake])

    def build(**config: Any) -> FakeProducer:
        result = next(builds)
        if isinstance(result, Exception):
            raise result
        return result

    raw = _raw({'value': _VALUE})
    with patch(
        'web_service.producer_service.KafkaProducer',
        side_effect=build,
    ):
        body, status = producer_service.produce_record(NAMESPACE, TOPIC, raw)
        assert status == 500
        assert body['error_code'] == 50002
        assert body['message'].startswith('Failed to produce: ')
        body, status = producer_service.produce_record(NAMESPACE, TOPIC, raw)
    assert status == 200


def test_invalid_record_does_not_build_producer(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
) -> None:
    body, status = producer_service.produce_record(
        NAMESPACE,
        TOPIC,
        _raw({'value': {'type': 'AVRO', 'data': {}}}),
    )
    assert status == 400
    assert body['error_code'] == 40004
    assert fake_producer.builds == 0


# ============================================================================
# ProducerService - produce_record
# ============================================================================


def test_produce_record_success_shape(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
) -> None:
    body, status = producer_service.produce_record(
        NAMESPACE,
        TOPIC,
        _raw(
            {
                'partition_id': 0,
                'key': {'type': 'STRING', 'data': 'k'},
                'value': {'type': 'JSON', 'data': {'a': 1}},
                'headers': [{'name': 'h', 'value': _b64(b'hv')}],
            },
        ),
    )
    assert status == 200
    assert body == {
        'topic_name': TOPIC,
        'partition_id': 0,
        'offset': 100,
        'timestamp': '2026-10-26T12:00:00.000Z',
    }
    assert fake_producer.sent == [
        {
            'topic': KAFKA_TOPIC,
            'key': b'k',
            'value': b'{"a":1}',
            'headers': [('h', b'hv')],
            'partition': 0,
            'timestamp_ms': None,
        },
    ]


def test_produce_record_timestamp_sent_and_returned(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
) -> None:
    body, status = producer_service.produce_record(
        NAMESPACE,
        TOPIC,
        _raw({'value': _VALUE, 'timestamp': '2026-10-26T12:00:00.250Z'}),
    )
    assert status == 200
    assert fake_producer.sent[0]['timestamp_ms'] == NOW_MS + 250
    assert body['timestamp'] == '2026-10-26T12:00:00.250Z'


def test_produce_record_waits_at_most_send_timeout(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
) -> None:
    producer_service.produce_record(NAMESPACE, TOPIC, _raw({'value': _VALUE}))
    timeout = fake_producer.futures[0].timeout
    assert timeout is not None
    assert 0 < timeout <= SEND_TIMEOUT_S


@pytest.mark.parametrize(
    ('error', 'waiting', 'status', 'code', 'reason'),
    (
        (
            KafkaTimeoutError('Timeout after waiting for 30 secs.'),
            True,
            500,
            50002,
            (
                'no acknowledgement from the cluster within 30 s; the '
                'record may still be written'
            ),
        ),
        (
            KafkaTimeoutError('Failed to update metadata after 10.0 secs.'),
            False,
            500,
            50002,
            'not sent: ',
        ),
        (NotEnoughReplicasError(), True, 500, 50002, 'Failed to produce: '),
        (RuntimeError('boom'), False, 500, 50002, 'Failed to produce: boom'),
        (
            MessageSizeTooLargeError(),
            True,
            413,
            41302,
            'refused the record as too large',
        ),
    ),
    ids=(
        'ack-timeout',
        'metadata-timeout',
        'broker-error',
        'send-raises',
        'broker-size',
    ),
)
def test_produce_record_kafka_failure(
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
    body, got = producer_service.produce_record(
        NAMESPACE,
        TOPIC,
        _raw({'value': _VALUE}),
    )
    assert got == status
    assert body['error_code'] == code
    assert reason in body['message']


# ============================================================================
# ProducerService - produce_batch
# ============================================================================


def test_produce_batch_success_shape(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
) -> None:
    body, status = producer_service.produce_batch(
        NAMESPACE,
        TOPIC,
        _raw(
            {
                'entries': [
                    {'id': 'a', 'value': _VALUE},
                    {
                        'id': 'b',
                        'value': _VALUE,
                        'timestamp': '2026-10-26T12:00:01Z',
                    },
                ],
            },
        ),
    )
    assert status == 207
    assert body == {
        'successes': [
            {
                'id': 'a',
                'topic_name': TOPIC,
                'partition_id': 0,
                'offset': 100,
                'timestamp': '2026-10-26T12:00:00.000Z',
            },
            {
                'id': 'b',
                'topic_name': TOPIC,
                'partition_id': 0,
                'offset': 101,
                'timestamp': '2026-10-26T12:00:01.000Z',
            },
        ],
        'failures': [],
    }
    assert [s['topic'] for s in fake_producer.sent] == [KAFKA_TOPIC] * 2


def test_produce_batch_partial_failure(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
) -> None:
    """One entry's send fails and one is invalid; the others are written."""
    fake_producer.get_errors[1] = NotEnoughReplicasError()
    entries = [
        {'id': 'a', 'value': _VALUE},
        {'id': 'b', 'value': {'type': 'AVRO', 'data': {}}},
        {'id': 'c', 'value': _VALUE},
        {
            'id': 'd',
            'value': {
                'type': 'BINARY',
                'data': _b64(b'x' * (MAX_RECORD_BYTES + 1)),
            },
        },
        {'id': 'e', 'value': _VALUE},
    ]
    body, status = producer_service.produce_batch(
        NAMESPACE,
        TOPIC,
        _raw({'entries': entries}),
    )
    assert status == 207
    assert [s['id'] for s in body['successes']] == ['a', 'e']
    assert [s['offset'] for s in body['successes']] == [100, 102]
    failures = {f['id']: f for f in body['failures']}
    assert [f['id'] for f in body['failures']] == ['b', 'c', 'd']
    assert failures['b']['error_code'] == 40004
    assert failures['c']['error_code'] == 50002
    assert failures['d']['error_code'] == 41302
    assert set(failures['c']) == {'id', 'error_code', 'message'}
    # The invalid entries were never sent
    assert len(fake_producer.sent) == 3


def test_produce_batch_sends_in_order_before_waiting(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
) -> None:
    sent_before_first_wait: list[int] = []
    fake_producer.gate = lambda _timeout: sent_before_first_wait.append(
        len(fake_producer.sent),
    )
    entries = [
        {'id': str(i), 'value': {'type': 'STRING', 'data': str(i)}}
        for i in range(5)
    ]
    producer_service.produce_batch(
        NAMESPACE,
        TOPIC,
        _raw({'entries': entries}),
    )
    assert [s['value'] for s in fake_producer.sent] == [
        str(i).encode() for i in range(5)
    ]
    assert sent_before_first_wait[0] == 5


def test_produce_batch_shares_one_deadline(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
) -> None:
    """Entries left once the deadline passes fail without being sent."""
    clock = [1000.0]

    def monotonic() -> float:
        return clock[0]

    real_send = fake_producer.send

    def slow_send(topic: str, **kwargs: Any) -> FakeFuture:
        # Each send takes 12 s of the 30 s
        clock[0] += 12
        return real_send(topic, **kwargs)

    fake_producer.send = slow_send  # type: ignore[method-assign]
    entries = [{'id': str(i), 'value': _VALUE} for i in range(4)]
    with patch('web_service.producer_service.time.monotonic', monotonic):
        body, status = producer_service.produce_batch(
            NAMESPACE,
            TOPIC,
            _raw({'entries': entries}),
        )
    assert status == 207
    # Sends at 0, 12 and 24 s; the fourth would start at 36 s
    assert len(fake_producer.sent) == 3
    assert [f['id'] for f in body['failures']] == ['3']
    assert 'ran out of its 30 s' in body['failures'][0]['message']
    # The waits share what is left of the 30 s: none, at 36 s
    assert [f.timeout for f in fake_producer.futures] == [0.0, 0.0, 0.0]


def test_produce_batch_all_invalid_builds_no_producer(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
) -> None:
    body, status = producer_service.produce_batch(
        NAMESPACE,
        TOPIC,
        _raw({'entries': [{'id': 'a', 'value': {'data': 'x'}}]}),
    )
    assert status == 207
    assert body['successes'] == []
    assert body['failures'][0]['error_code'] == 40004
    assert fake_producer.builds == 0


def test_produce_batch_producer_unavailable_fails_each_entry() -> None:
    service = ProducerService(bootstrap_servers=None, region='us-east-1')
    body, status = service.produce_batch(
        NAMESPACE,
        TOPIC,
        _raw({'entries': _entries(2)}),
    )
    assert status == 207
    assert body['successes'] == []
    assert body['failures'] == [
        {
            'id': str(i),
            'error_code': 50002,
            'message': 'Kafka endpoint is not configured',
        }
        for i in range(2)
    ]


def test_produce_batch_refused_as_a_whole(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
) -> None:
    body, status = producer_service.produce_batch(
        NAMESPACE,
        TOPIC,
        _raw({'entries': [{'id': 'a', 'value': _VALUE}, {'value': _VALUE}]}),
    )
    assert status == 400
    assert body == {'error_code': 40008, 'message': 'Entry 1 has no id'}
    assert fake_producer.sent == []


# ============================================================================
# ProducerService - close
# ============================================================================


def test_close_closes_producer_with_timeout(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
) -> None:
    producer_service.produce_record(NAMESPACE, TOPIC, _raw({'value': _VALUE}))
    producer_service.close()
    assert fake_producer.close_calls == [PRODUCER_CLOSE_TIMEOUT_S]


def test_close_without_producer_builds_none(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
) -> None:
    producer_service.close()
    assert fake_producer.builds == 0
    assert fake_producer.close_calls == []


def test_produce_after_close_refused(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
) -> None:
    producer_service.close()
    body, status = producer_service.produce_record(
        NAMESPACE,
        TOPIC,
        _raw({'value': _VALUE}),
    )
    assert status == 500
    assert body == {
        'error_code': 50002,
        'message': 'Failed to produce: service is shutting down',
    }
    assert fake_producer.builds == 0


def test_close_error_is_logged_not_raised(
    producer_service: ProducerService,
    fake_producer: FakeProducer,
) -> None:
    producer_service.produce_record(NAMESPACE, TOPIC, _raw({'value': _VALUE}))

    def fail(timeout: float | None = None) -> None:
        raise RuntimeError('close failed')

    fake_producer.close = fail  # type: ignore[method-assign]
    producer_service.close()
