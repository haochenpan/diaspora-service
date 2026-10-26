"""Unit tests for the produce routes of the FastAPI app (main.py).

The Kafka producer is replaced by a fake; nothing here uses the network.
"""

from __future__ import annotations

import asyncio
import base64
import json
import threading
import time
from collections.abc import Iterator
from typing import Any
from unittest.mock import MagicMock
from unittest.mock import patch

import anyio
import httpx
import pytest

from tests.producer_service_test import FakeProducer
from web_service.main import DiasporaService
from web_service.producer_service import MAX_BODY_BYTES
from web_service.producer_service import MAX_RECORD_BYTES
from web_service.producer_service import PRODUCE_MAX_BODY_BYTES
from web_service.producer_service import PRODUCE_MAX_REQUESTS
from web_service.producer_service import PRODUCE_SUBJECT_MAX_REQUESTS
from web_service.producer_service import ProduceAdmission
from web_service.producer_service import PRODUCER_BUILD_RETRY_S
from web_service.producer_service import PRODUCER_CLOSE_TIMEOUT_S
from web_service.producer_service import PRODUCER_MAX_BLOCK_MS
from web_service.producer_service import RETRY_AFTER_S

SUBJECT = '123e4567-e89b-12d3-a456-426614174000'
HEADERS = {'subject': SUBJECT, 'authorization': 'Bearer ' + 'a' * 50}
NAMESPACE = 'ns-test123'
TOPIC = 't1'
RECORDS = f'/api/v3/{NAMESPACE}/topics/{TOPIC}/records'
BATCH = f'/api/v3/{NAMESPACE}/topics/{TOPIC}/records:batch'
VALUE = {'type': 'STRING', 'data': 'v'}
AUTH_ERROR = {'status': 'error', 'reason': 'Token is inactive.'}


@pytest.fixture
def fake_producer() -> Iterator[FakeProducer]:
    fake = FakeProducer()
    with patch(
        'web_service.producer_service.KafkaProducer',
        side_effect=fake.build,
    ):
        yield fake


@pytest.fixture
def service(fake_producer: FakeProducer) -> DiasporaService:
    """A DiasporaService whose auth, AWS and consumer services are mocks.

    Its token check passes and it owns NAMESPACE, which holds TOPIC.
    """
    auth = MagicMock()
    auth.validate_access_token.return_value = None
    web_service = MagicMock()
    ns_db = web_service.namespace_service.dynamodb
    ns_db.get_user_namespaces.return_value = [NAMESPACE]
    ns_db.get_namespace_topics.return_value = [TOPIC]
    with (
        patch('web_service.main.AuthManager', return_value=auth),
        patch('web_service.main.IAMService'),
        patch('web_service.main.KafkaService'),
        patch('web_service.main.DynamoDBService'),
        patch('web_service.main.NamespaceService'),
        patch('web_service.main.WebService', return_value=web_service),
        patch('web_service.main.ConsumerService'),
    ):
        service = DiasporaService()
    # The fake producer is built whatever the servers; any name will do
    service.producer_service.bootstrap_servers = 'broker.invalid:9098'
    return service


def _client(service: DiasporaService) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=service.app),
        base_url='http://test',
    )


async def _post(
    service: DiasporaService,
    path: str,
    body: Any = None,
    *,
    content: bytes | None = None,
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    if headers is None:
        headers = HEADERS
        if content is not None:
            headers = {**HEADERS, 'content-type': 'application/json'}
    async with _client(service) as client:
        return await client.post(
            path,
            json=body if content is None else None,
            content=content,
            headers=headers,
        )


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


def _ns_db(service: DiasporaService) -> MagicMock:
    return service.web_service.namespace_service.dynamodb  # type: ignore[return-value]


# ============================================================================
# Success
# ============================================================================


async def test_produce_record_success(
    service: DiasporaService,
    fake_producer: FakeProducer,
) -> None:
    response = await _post(
        service,
        RECORDS,
        {
            'key': {'type': 'BINARY', 'data': _b64(b'\x00k')},
            'value': {'type': 'JSON', 'data': {'step': 7}},
            'headers': [{'name': 'trace', 'value': _b64(b'abc')}],
            'timestamp': '2026-10-26T12:00:00.500Z',
        },
    )
    assert response.status_code == 200
    assert response.json() == {
        'topic_name': TOPIC,
        'partition_id': 0,
        'offset': 100,
        'timestamp': '2026-10-26T12:00:00.500Z',
    }
    assert fake_producer.sent == [
        {
            'topic': f'{NAMESPACE}.{TOPIC}',
            'key': b'\x00k',
            'value': b'{"step":7}',
            'headers': [('trace', b'abc')],
            'partition': 0,
            'timestamp_ms': 1_793_016_000_500,
        },
    ]


async def test_produce_batch_success(
    service: DiasporaService,
    fake_producer: FakeProducer,
) -> None:
    response = await _post(
        service,
        BATCH,
        {
            'entries': [
                {'id': 'a', 'value': VALUE},
                {'id': 'b', 'partition_id': 0, 'value': VALUE},
            ],
        },
    )
    assert response.status_code == 207
    assert response.json() == {
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
                'timestamp': '2026-10-26T12:00:00.000Z',
            },
        ],
        'failures': [],
    }


async def test_produce_batch_partial_failure(
    service: DiasporaService,
    fake_producer: FakeProducer,
) -> None:
    from kafka.errors import NotLeaderForPartitionError

    fake_producer.get_errors[1] = NotLeaderForPartitionError()
    big = {'type': 'BINARY', 'data': _b64(b'x' * (MAX_RECORD_BYTES + 1))}
    response = await _post(
        service,
        BATCH,
        {
            'entries': [
                {'id': 'a', 'value': VALUE},
                {'id': 'b', 'value': VALUE},
                {'id': 'big', 'value': big},
                {'id': 'p1', 'partition_id': 1, 'value': VALUE},
                {'id': 'c', 'value': VALUE},
            ],
        },
    )
    assert response.status_code == 207
    body = response.json()
    assert [s['id'] for s in body['successes']] == ['a', 'c']
    assert [(f['id'], f['error_code']) for f in body['failures']] == [
        ('b', 50003),
        ('big', 41302),
        ('p1', 40402),
    ]


# ============================================================================
# Refusals of the request
# ============================================================================


@pytest.mark.parametrize(
    ('content', 'status', 'code'),
    (
        (
            b'{"value": {"type": "STRING", "data": "a"}}\n{"value": null}',
            400,
            40004,
        ),
        (b'not json', 400, 40003),
        (b'[]', 400, 40003),
        (b'', 422, 42206),
    ),
    ids=('two-values', 'not-json', 'not-object', 'empty'),
)
async def test_produce_record_body_refused(
    service: DiasporaService,
    fake_producer: FakeProducer,
    content: bytes,
    status: int,
    code: int,
) -> None:
    response = await _post(service, RECORDS, content=content)
    assert response.status_code == status
    assert response.json()['error_code'] == code
    assert fake_producer.sent == []


@pytest.mark.parametrize(
    ('record', 'status', 'code'),
    (
        ({'value': {'type': 'AVRO', 'data': {}}}, 422, 42206),
        ({'value': {'type': 'PROTOBUF', 'data': {}}}, 422, 42206),
        ({'value': {'type': 'JSONSCHEMA', 'data': {}}}, 422, 42206),
        ({'value': {'type': 'JSON', 'data': 1, 'schema_id': 3}}, 422, 42206),
        ({'key': {'type': 'JSON', 'data': 1, 'subject': 's'}}, 422, 42206),
        ({'value': {'data': 'x'}}, 422, 42206),
        ({'partition_id': 1, 'value': VALUE}, 404, 40402),
        ({'value': {'type': 'BINARY', 'data': '***'}}, 400, 40005),
        ({'value': VALUE, 'timestamp': 'noon'}, 400, 40005),
    ),
    ids=(
        'avro',
        'protobuf',
        'jsonschema',
        'schema-id',
        'subject',
        'untyped',
        'partition',
        'bad-base64',
        'bad-timestamp',
    ),
)
async def test_produce_record_refused(
    service: DiasporaService,
    fake_producer: FakeProducer,
    record: dict[str, Any],
    status: int,
    code: int,
) -> None:
    response = await _post(service, RECORDS, record)
    assert response.status_code == status
    body = response.json()
    assert set(body) == {'error_code', 'message'}
    assert body['error_code'] == code
    if code == 42206:
        assert 'schema registry' in body['message']
    assert fake_producer.sent == []


async def test_produce_record_over_8_mib_refused(
    service: DiasporaService,
    fake_producer: FakeProducer,
) -> None:
    response = await _post(
        service,
        RECORDS,
        {
            'value': {
                'type': 'BINARY',
                'data': _b64(b'x' * (MAX_RECORD_BYTES + 1)),
            },
        },
    )
    assert response.status_code == 413
    assert response.json()['error_code'] == 41302
    assert fake_producer.sent == []


@pytest.mark.parametrize(
    'body',
    (
        {'entries': [{'id': 'a', 'value': VALUE}] * 2},
        {'entries': [{'value': VALUE}]},
        {'entries': [{'id': 'x' * 81, 'value': VALUE}]},
        {'entries': [{'id': 'a/b', 'value': VALUE}]},
        {'entries': [{'id': str(i), 'value': VALUE} for i in range(501)]},
        {'entries': []},
    ),
    ids=(
        'duplicate-id',
        'missing-id',
        'long-id',
        'bad-id',
        'over-500',
        'empty',
    ),
)
async def test_produce_batch_refused_as_a_whole(
    service: DiasporaService,
    fake_producer: FakeProducer,
    body: dict[str, Any],
) -> None:
    response = await _post(service, BATCH, body)
    assert response.status_code == 422
    assert response.json()['error_code'] == 42208
    assert fake_producer.sent == []


async def test_produce_batch_two_values_refused(
    service: DiasporaService,
    fake_producer: FakeProducer,
) -> None:
    one = json.dumps({'entries': [{'id': 'a', 'value': VALUE}]}).encode()
    response = await _post(service, BATCH, content=one + one)
    assert response.status_code == 400
    assert response.json()['error_code'] == 40004


@pytest.mark.parametrize('path', (RECORDS, BATCH))
async def test_produce_kafka_failure_reply(
    service: DiasporaService,
    fake_producer: FakeProducer,
    path: str,
) -> None:
    from kafka.errors import KafkaTimeoutError

    fake_producer.get_errors[0] = KafkaTimeoutError()
    record: dict[str, Any] = {'value': VALUE}
    body = {'entries': [{'id': 'a', **record}]} if path == BATCH else record
    response = await _post(service, path, body)
    if path == RECORDS:
        assert response.status_code == 500
        reply = response.json()
    else:
        assert response.status_code == 207
        assert response.json()['successes'] == []
        (reply,) = response.json()['failures']
        assert reply.pop('id') == 'a'
    assert reply['error_code'] == 50003
    assert 'may still be written' in reply['message']


# ============================================================================
# Token, namespace and topic checks, as on the consumer routes
# ============================================================================

CONSUMER_SUBSCRIBE = (
    f'/api/v3/{NAMESPACE}/consumers/g/instances/i/subscription',
    {'topics': [TOPIC]},
)
PRODUCE_ROUTES = (
    (RECORDS, {'value': VALUE}),
    (BATCH, {'entries': [{'id': 'a', 'value': VALUE}]}),
)


async def _reply(
    service: DiasporaService,
    path: str,
    body: Any,
) -> tuple[int, Any]:
    response = await _post(service, path, body)
    return response.status_code, response.json()


@pytest.mark.parametrize(('path', 'body'), PRODUCE_ROUTES)
async def test_token_failure_keeps_the_produce_reply(
    service: DiasporaService,
    fake_producer: FakeProducer,
    path: str,
    body: Any,
) -> None:
    """The produce routes keep their 401; the consumer routes add a code."""
    service.auth.validate_access_token.return_value = AUTH_ERROR
    consumer = await _reply(service, *CONSUMER_SUBSCRIBE)
    produce = await _reply(service, path, body)
    assert produce == (401, AUTH_ERROR)
    assert consumer == (
        401,
        {
            'error_code': 40101,
            'message': 'Authentication failed: Token is inactive.',
        },
    )
    service.auth.validate_access_token.assert_called_with(
        SUBJECT,
        HEADERS['authorization'],
    )
    _ns_db(service).get_user_namespaces.assert_not_called()
    assert fake_producer.builds == 0


@pytest.mark.parametrize(('path', 'body'), PRODUCE_ROUTES)
async def test_namespace_failure_matches_consumer_routes(
    service: DiasporaService,
    fake_producer: FakeProducer,
    path: str,
    body: Any,
) -> None:
    _ns_db(service).get_user_namespaces.return_value = ['ns-other']
    consumer = await _reply(service, *CONSUMER_SUBSCRIBE)
    produce = await _reply(service, path, body)
    assert produce == consumer
    assert produce == (
        404,
        {
            'error_code': 40401,
            'message': f'Namespace {NAMESPACE} not found for user',
        },
    )
    _ns_db(service).get_user_namespaces.assert_called_with(SUBJECT)
    _ns_db(service).get_namespace_topics.assert_not_called()
    assert fake_producer.builds == 0


@pytest.mark.parametrize(('path', 'body'), PRODUCE_ROUTES)
async def test_topic_failure_matches_consumer_routes(
    service: DiasporaService,
    fake_producer: FakeProducer,
    path: str,
    body: Any,
) -> None:
    _ns_db(service).get_namespace_topics.return_value = ['other']
    consumer = await _reply(service, *CONSUMER_SUBSCRIBE)
    produce = await _reply(service, path, body)
    assert produce == consumer
    assert produce == (
        404,
        {
            'error_code': 40401,
            'message': f'Topics not found in namespace {NAMESPACE}: {TOPIC}',
        },
    )
    _ns_db(service).get_namespace_topics.assert_called_with(NAMESPACE)
    assert fake_producer.builds == 0


@pytest.mark.parametrize('suffix', ('records', 'records:batch'))
async def test_dot_in_topic_refused(
    service: DiasporaService,
    fake_producer: FakeProducer,
    suffix: str,
) -> None:
    _ns_db(service).get_namespace_topics.return_value = ['a.b']
    response = await _post(
        service,
        f'/api/v3/{NAMESPACE}/topics/a.b/{suffix}',
        {'value': VALUE},
    )
    assert response.status_code == 400
    assert response.json() == {
        'error_code': 40002,
        'message': 'Invalid topic name a.b: topic names never contain a dot',
    }
    _ns_db(service).get_namespace_topics.assert_not_called()
    assert fake_producer.builds == 0


async def test_dot_checked_after_namespace(
    service: DiasporaService,
) -> None:
    _ns_db(service).get_user_namespaces.return_value = []
    response = await _post(
        service,
        f'/api/v3/{NAMESPACE}/topics/a.b/records',
        {'value': VALUE},
    )
    assert response.status_code == 404
    assert response.json()['error_code'] == 40401


# ============================================================================
# Body size limits
# ============================================================================


async def _call_asgi(
    service: DiasporaService,
    path: str,
    chunks: Iterator[bytes],
    *,
    content_length: int | None,
    pause_s: float = 0,
) -> tuple[int, Any, int]:
    """Send a request straight to the app, a chunk per receive().

    Waits pause_s before each chunk after the first. Returns the status,
    the JSON reply and how many chunks were read.
    """
    headers = [
        (b'subject', SUBJECT.encode()),
        (b'authorization', HEADERS['authorization'].encode()),
        (b'content-type', b'application/json'),
    ]
    if content_length is not None:
        headers.append((b'content-length', str(content_length).encode()))
    scope = {
        'type': 'http',
        'asgi': {'version': '3.0'},
        'http_version': '1.1',
        'method': 'POST',
        'scheme': 'http',
        'path': path,
        'raw_path': path.encode(),
        'query_string': b'',
        'root_path': '',
        'headers': headers,
        'client': ('127.0.0.1', 1234),
        'server': ('test', 80),
    }
    read = 0
    pending = list(chunks)

    async def receive() -> dict[str, Any]:
        nonlocal read
        if not pending:
            # Starlette waits for a disconnect once the reply is sent
            await asyncio.sleep(0)
            return {'type': 'http.disconnect'}
        if read:
            await asyncio.sleep(pause_s)
        read += 1
        chunk = pending.pop(0)
        return {
            'type': 'http.request',
            'body': chunk,
            'more_body': bool(pending),
        }

    messages: list[dict[str, Any]] = []

    async def send(message: dict[str, Any]) -> None:
        messages.append(message)

    await service.app(scope, receive, send)
    status = next(
        m['status'] for m in messages if m['type'] == 'http.response.start'
    )
    body = b''.join(
        m.get('body', b'')
        for m in messages
        if m['type'] == 'http.response.body'
    )
    return status, json.loads(body), read


@pytest.mark.parametrize('path', (RECORDS, BATCH))
async def test_content_length_over_limit_refused_unread(
    service: DiasporaService,
    fake_producer: FakeProducer,
    path: str,
) -> None:
    """A declared length over 16 MiB is refused before any check or read."""
    status, body, read = await _call_asgi(
        service,
        path,
        iter([b'{}']),
        content_length=MAX_BODY_BYTES + 1,
    )
    assert status == 413
    assert body['error_code'] == 41301
    assert read == 0
    service.auth.validate_access_token.assert_not_called()


@pytest.mark.parametrize('path', (RECORDS, BATCH))
async def test_streamed_body_over_limit_refused(
    service: DiasporaService,
    fake_producer: FakeProducer,
    path: str,
) -> None:
    """Without Content-Length, reading stops once 16 MiB is passed."""
    mib = b' ' * (1024 * 1024)
    status, body, read = await _call_asgi(
        service,
        path,
        iter([mib] * 40),
        content_length=None,
    )
    assert status == 413
    assert body['error_code'] == 41301
    # 16 chunks reach the limit; the 17th passes it
    assert read == 17
    assert fake_producer.sent == []


async def test_streamed_body_at_limit_read(
    service: DiasporaService,
    fake_producer: FakeProducer,
) -> None:
    record = json.dumps({'value': VALUE}).encode()
    padding = b' ' * (MAX_BODY_BYTES - len(record))
    status, body, _ = await _call_asgi(
        service,
        RECORDS,
        iter([record, padding]),
        content_length=None,
    )
    assert status == 200
    assert body['offset'] == 100


async def test_content_length_at_limit_read(
    service: DiasporaService,
    fake_producer: FakeProducer,
) -> None:
    record = json.dumps({'value': VALUE}).encode()
    padding = b' ' * (MAX_BODY_BYTES - len(record))
    status, _, read = await _call_asgi(
        service,
        RECORDS,
        iter([record, padding]),
        content_length=MAX_BODY_BYTES,
    )
    assert status == 200
    assert read == 2


# ============================================================================
# Blocking work runs off the event loop
# ============================================================================


def _blocking() -> tuple[Any, threading.Event, threading.Event]:
    """A gate that blocks until released, and its two events."""
    entered = threading.Event()
    release = threading.Event()

    def gate(*_args: Any) -> Any:
        entered.set()
        gate.release_seen = release.wait(timeout=2)

    return gate, entered, release


async def _served_while_blocked(
    service: DiasporaService,
    path: str,
    body: Any,
    entered: threading.Event,
    release: threading.Event,
) -> httpx.Response:
    """Send a request that blocks; check the loop serves another meanwhile."""
    async with _client(service) as client:
        slow = asyncio.create_task(
            client.post(path, json=body, headers=HEADERS),
        )
        assert await asyncio.to_thread(entered.wait, 2)
        fast = await client.get('/')
        assert fast.status_code == 200
        release.set()
        return await slow


@pytest.mark.parametrize(('path', 'body'), PRODUCE_ROUTES)
async def test_waiting_for_the_cluster_leaves_loop_free(
    service: DiasporaService,
    fake_producer: FakeProducer,
    path: str,
    body: Any,
) -> None:
    gate, entered, release = _blocking()
    fake_producer.gate = gate
    response = await _served_while_blocked(
        service,
        path,
        body,
        entered,
        release,
    )
    assert gate.release_seen, 'waiting for the ack held the event loop'
    assert response.status_code in (200, 207)


@pytest.mark.parametrize(('path', 'body'), PRODUCE_ROUTES)
async def test_access_checks_leave_loop_free(
    service: DiasporaService,
    path: str,
    body: Any,
) -> None:
    gate, entered, release = _blocking()

    def slow_check(*_args: Any) -> None:
        gate()

    service.auth.validate_access_token.side_effect = slow_check
    response = await _served_while_blocked(
        service,
        path,
        body,
        entered,
        release,
    )
    assert gate.release_seen, 'the token check held the event loop'
    assert response.status_code in (200, 207)


async def test_building_the_producer_leaves_loop_free(
    service: DiasporaService,
    fake_producer: FakeProducer,
) -> None:
    gate, entered, release = _blocking()

    def slow_build(**config: Any) -> FakeProducer:
        gate()
        return fake_producer.build(**config)

    with patch(
        'web_service.producer_service.KafkaProducer',
        side_effect=slow_build,
    ):
        response = await _served_while_blocked(
            service,
            RECORDS,
            {'value': VALUE},
            entered,
            release,
        )
    assert gate.release_seen, 'building the producer held the event loop'
    assert response.status_code == 200


# ============================================================================
# Content-Type
# ============================================================================


@pytest.mark.parametrize(
    'content_type',
    (
        None,
        'text/plain',
        'application/x-www-form-urlencoded',
        'application/json; charset=latin-1',
        'application/jsonl',
    ),
)
@pytest.mark.parametrize('path', (RECORDS, BATCH))
async def test_body_not_json_refused(
    service: DiasporaService,
    fake_producer: FakeProducer,
    path: str,
    content_type: str | None,
) -> None:
    headers = dict(HEADERS)
    if content_type is not None:
        headers['content-type'] = content_type
    response = await _post(
        service,
        path,
        content=json.dumps({'value': VALUE}).encode(),
        headers=headers,
    )
    assert response.status_code == 415
    assert response.json()['error_code'] == 41501
    service.auth.validate_access_token.assert_not_called()


@pytest.mark.parametrize(
    'content_type',
    (
        'application/json',
        'Application/JSON',
        'application/json; charset=UTF-8',
        'application/json;charset="utf-8"',
    ),
)
async def test_json_content_types_accepted(
    service: DiasporaService,
    fake_producer: FakeProducer,
    content_type: str,
) -> None:
    response = await _post(
        service,
        RECORDS,
        content=json.dumps({'value': VALUE}).encode(),
        headers={**HEADERS, 'content-type': content_type},
    )
    assert response.status_code == 200


# ============================================================================
# Admission, parsing one body at a time, and the threads that send
# ============================================================================


class _Gate:
    """Blocks each acknowledgement wait until released; counts them."""

    def __init__(self) -> None:
        self.waiting = 0
        self.release = threading.Event()
        self._lock = threading.Lock()

    def __call__(self, _timeout: float | None) -> None:
        with self._lock:
            self.waiting += 1
        assert self.release.wait(timeout=5)

    async def until_waiting(self, count: int) -> None:
        for _ in range(500):
            if self.waiting >= count:
                return
            await asyncio.sleep(0.01)
        raise AssertionError(f'{self.waiting} of {count} sends waiting')


def _headers(subject: str) -> dict[str, str]:
    return {**HEADERS, 'subject': subject}


class _Stalled:
    """A request whose body stops after its first chunks.

    The client then goes away when told to.
    """

    def __init__(
        self,
        service: DiasporaService,
        chunks: list[bytes],
        *,
        content_length: int | None,
        subject: str = SUBJECT,
    ) -> None:
        self.messages: list[dict[str, Any]] = []
        self.gone = asyncio.Event()
        headers = [
            (b'subject', subject.encode()),
            (b'authorization', HEADERS['authorization'].encode()),
            (b'content-type', b'application/json'),
        ]
        if content_length is not None:
            headers.append((b'content-length', str(content_length).encode()))
        pending = list(chunks)

        async def receive() -> dict[str, Any]:
            if pending:
                return {
                    'type': 'http.request',
                    'body': pending.pop(0),
                    'more_body': True,
                }
            await self.gone.wait()
            return {'type': 'http.disconnect'}

        async def send(message: dict[str, Any]) -> None:
            self.messages.append(message)

        scope = {
            'type': 'http',
            'asgi': {'version': '3.0'},
            'http_version': '1.1',
            'method': 'POST',
            'scheme': 'http',
            'path': RECORDS,
            'raw_path': RECORDS.encode(),
            'query_string': b'',
            'root_path': '',
            'headers': headers,
            'client': ('127.0.0.1', 1234),
            'server': ('test', 80),
        }
        self.task = asyncio.create_task(service.app(scope, receive, send))

    async def reply(self) -> tuple[int, dict[str, str], Any]:
        """The status, headers and JSON body, once sent."""
        await asyncio.wait_for(self.task, 5)
        start = next(
            m for m in self.messages if m['type'] == 'http.response.start'
        )
        body = b''.join(
            m.get('body', b'')
            for m in self.messages
            if m['type'] == 'http.response.body'
        )
        headers = {k.decode(): v.decode() for k, v in start['headers']}
        return start['status'], headers, json.loads(body) if body else None

    async def leave(self) -> None:
        self.gone.set()
        await asyncio.wait_for(self.task, 5)


async def _until(condition: Any) -> None:
    for _ in range(500):
        if condition():
            return
        await asyncio.sleep(0.01)
    raise AssertionError('condition not met')


def _admitted(service: DiasporaService) -> tuple[int, int]:
    admission = service.produce_admission
    return admission.requests, admission.body_bytes


async def test_requests_past_the_limit_get_429(
    service: DiasporaService,
    fake_producer: FakeProducer,
) -> None:
    gate = _Gate()
    fake_producer.gate = gate
    async with _client(service) as client:
        held = [
            asyncio.create_task(
                client.post(
                    RECORDS,
                    json={'value': VALUE},
                    headers=_headers(f'user-{i}'),
                ),
            )
            for i in range(PRODUCE_MAX_REQUESTS)
        ]
        await gate.until_waiting(PRODUCE_MAX_REQUESTS)
        refused = await client.post(
            BATCH,
            json={'entries': [{'id': 'a', 'value': VALUE}]},
            headers=_headers('user-new'),
        )
        gate.release.set()
        replies = await asyncio.gather(*held)
    assert refused.status_code == 429
    assert refused.headers['retry-after'] == str(RETRY_AFTER_S)
    assert refused.json()['error_code'] == 42901
    assert 'at once' in refused.json()['message']
    assert [r.status_code for r in replies] == [200] * PRODUCE_MAX_REQUESTS
    assert len(fake_producer.sent) == PRODUCE_MAX_REQUESTS
    assert _admitted(service) == (0, 0)


async def test_one_user_past_its_share_gets_429(
    service: DiasporaService,
    fake_producer: FakeProducer,
) -> None:
    gate = _Gate()
    fake_producer.gate = gate
    async with _client(service) as client:
        held = [
            asyncio.create_task(
                client.post(RECORDS, json={'value': VALUE}, headers=HEADERS),
            )
            for _ in range(PRODUCE_SUBJECT_MAX_REQUESTS)
        ]
        await gate.until_waiting(PRODUCE_SUBJECT_MAX_REQUESTS)
        refused = await client.post(
            RECORDS,
            json={'value': VALUE},
            headers=HEADERS,
        )
        # Another user is still served
        other = asyncio.create_task(
            client.post(
                RECORDS,
                json={'value': VALUE},
                headers=_headers('user-other'),
            ),
        )
        await gate.until_waiting(PRODUCE_SUBJECT_MAX_REQUESTS + 1)
        gate.release.set()
        replies = await asyncio.gather(*held, other)
    assert refused.status_code == 429
    assert refused.json()['error_code'] == 42901
    assert 'one user' in refused.json()['message']
    assert [r.status_code for r in replies] == [200] * 3


async def test_stalled_bodies_hold_only_what_they_sent(
    service: DiasporaService,
    fake_producer: FakeProducer,
) -> None:
    """Declared lengths reserve nothing, and reads take no place."""
    stalled = [
        _Stalled(
            service,
            [b'{'],
            content_length=MAX_BODY_BYTES,
            subject=f'user-{i}',
        )
        for i in range(PRODUCE_MAX_REQUESTS + 1)
    ]
    count = PRODUCE_MAX_REQUESTS + 1
    await _until(lambda: service.produce_admission.body_bytes == count)
    assert service.produce_admission.requests == 0
    served = await _post(
        service,
        RECORDS,
        {'value': VALUE},
        headers=_headers('user-new'),
    )
    assert served.status_code == 200
    # A client that goes away mid-body releases what it held
    for request in stalled:
        await request.leave()
    assert _admitted(service) == (0, 0)


async def test_bodies_past_the_byte_budget_get_429(
    service: DiasporaService,
    fake_producer: FakeProducer,
) -> None:
    """Bytes sent hold the budget until the clients go away."""
    count = PRODUCE_MAX_BODY_BYTES // MAX_BODY_BYTES
    sent = b' ' * (MAX_BODY_BYTES - 1)
    stalled = [
        _Stalled(
            service,
            [sent],
            content_length=MAX_BODY_BYTES,
            subject=f'user-{i}',
        )
        for i in range(count)
    ]
    held = count * len(sent)
    await _until(lambda: service.produce_admission.body_bytes == held)
    # A declared length that does not fit is refused before the read, and
    # an undeclared one once its bytes do not fit
    for content_length, chunks_read in ((10, 0), (None, 1)):
        status, body, read = await _call_asgi(
            service,
            RECORDS,
            iter([b' ' * 10]),
            content_length=content_length,
        )
        assert (status, body['error_code']) == (429, 42901)
        assert 'bytes' in body['message']
        assert read == chunks_read
    for request in stalled:
        await request.leave()
    assert _admitted(service) == (0, 0)
    served = await _post(service, RECORDS, {'value': VALUE})
    assert served.status_code == 200


async def test_requests_past_the_limit_get_429_before_their_body(
    service: DiasporaService,
    fake_producer: FakeProducer,
) -> None:
    """With every place taken, a new body is refused unread."""
    gate = _Gate()
    fake_producer.gate = gate
    async with _client(service) as client:
        held = [
            asyncio.create_task(
                client.post(
                    RECORDS,
                    json={'value': VALUE},
                    headers=_headers(f'user-{i}'),
                ),
            )
            for i in range(PRODUCE_MAX_REQUESTS)
        ]
        await gate.until_waiting(PRODUCE_MAX_REQUESTS)
        for content_length in (40, None):
            status, body, read = await _call_asgi(
                service,
                RECORDS,
                iter([b'{"value":{"type":"STRING","data":"x"}}']),
                content_length=content_length,
            )
            assert (status, body['error_code']) == (429, 42901)
            assert 'at once' in body['message']
            assert read == 0
        gate.release.set()
        replies = await asyncio.gather(*held)
    assert [r.status_code for r in replies] == [200] * PRODUCE_MAX_REQUESTS
    assert _admitted(service) == (0, 0)


async def test_declared_body_past_the_share_gets_429_before_it_is_read(
    service: DiasporaService,
    fake_producer: FakeProducer,
) -> None:
    service.produce_admission = ProduceAdmission(subject_max_body_bytes=1500)
    stalled = _Stalled(service, [b' ' * 1000], content_length=1000)
    await _until(lambda: _admitted(service) == (0, 1000))
    status, body, read = await _call_asgi(
        service,
        RECORDS,
        iter([b' ' * 600]),
        content_length=600,
    )
    assert (status, body['error_code']) == (429, 42901)
    assert 'one user may hold' in body['message']
    assert read == 0
    await stalled.leave()
    assert _admitted(service) == (0, 0)


@pytest.mark.parametrize('content_length', (None, 5000))
async def test_body_reserves_bytes_as_they_arrive(
    service: DiasporaService,
    fake_producer: FakeProducer,
    content_length: int | None,
) -> None:
    stalled = _Stalled(
        service,
        [b' ' * 1000, b' ' * 500],
        content_length=content_length,
    )
    await _until(lambda: _admitted(service) == (0, 1500))
    await stalled.leave()
    assert _admitted(service) == (0, 0)


async def test_chunked_body_past_the_share_mid_read_gets_429(
    service: DiasporaService,
    fake_producer: FakeProducer,
) -> None:
    service.produce_admission = ProduceAdmission(subject_max_body_bytes=1500)
    status, body, read = await _call_asgi(
        service,
        RECORDS,
        iter([b' ' * 1000] * 3),
        content_length=None,
    )
    assert (status, body['error_code']) == (429, 42901)
    assert read == 2
    assert _admitted(service) == (0, 0)


async def test_slow_body_gets_408(
    service: DiasporaService,
    fake_producer: FakeProducer,
) -> None:
    with patch('web_service.main.READ_TIMEOUT_S', 0.2):
        stalled = _Stalled(service, [b'{'], content_length=40)
        status, headers, body = await stalled.reply()
    assert (status, body['error_code']) == (408, 40801)
    assert headers['connection'] == 'close'
    assert _admitted(service) == (0, 0)
    assert fake_producer.sent == []


async def test_records_reserve_their_encoded_bytes(
    service: DiasporaService,
    fake_producer: FakeProducer,
) -> None:
    """JSON numbers written in full hold more than the body did."""
    service.produce_admission = ProduceAdmission(subject_max_body_bytes=2000)
    body = {'value': {'type': 'JSON', 'data': [1e15] * 120}}
    raw = json.dumps(body, separators=(',', ':')).replace(
        '1000000000000000.0',
        '1e15',
    )
    assert len(raw) < 1000
    response = await _post(service, RECORDS, content=raw.encode())
    assert (response.status_code, response.json()['error_code']) == (
        429,
        42901,
    )
    assert fake_producer.sent == []
    assert _admitted(service) == (0, 0)


async def test_admission_released_on_an_unexpected_error(
    service: DiasporaService,
    fake_producer: FakeProducer,
) -> None:
    with patch(
        'web_service.main.prepare_record',
        side_effect=RuntimeError('bug'),
    ):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(
                app=service.app,
                raise_app_exceptions=False,
            ),
            base_url='http://test',
        ) as client:
            response = await client.post(
                RECORDS,
                json={'value': VALUE},
                headers=HEADERS,
            )
    assert response.status_code == 500
    assert _admitted(service) == (0, 0)


async def test_admission_released_on_refusal(
    service: DiasporaService,
    fake_producer: FakeProducer,
) -> None:
    for body in ({'value': {'type': 'AVRO', 'data': {}}}, {'value': VALUE}):
        await _post(service, RECORDS, body)
    await _post(service, RECORDS, content=b'x' * 10)
    assert _admitted(service) == (0, 0)


async def test_bodies_parsed_one_at_a_time(
    service: DiasporaService,
    fake_producer: FakeProducer,
) -> None:
    from web_service import producer_service

    active = 0
    most = 0
    lock = threading.Lock()

    def slow_prepare(raw: Any) -> Any:
        nonlocal active, most
        with lock:
            active += 1
            most = max(most, active)
        time.sleep(0.05)
        with lock:
            active -= 1
        return producer_service.prepare_record(raw)

    with patch('web_service.main.prepare_record', slow_prepare):
        async with _client(service) as client:
            replies = await asyncio.gather(
                *(
                    client.post(
                        RECORDS,
                        json={'value': VALUE},
                        headers=_headers(f'user-{i}'),
                    )
                    for i in range(4)
                ),
            )
    assert [r.status_code for r in replies] == [200] * 4
    assert most == 1


async def test_sends_leave_the_shared_threadpool_free(
    service: DiasporaService,
    fake_producer: FakeProducer,
) -> None:
    """Produce requests waiting on the cluster hold none of its threads.

    The shared threadpool is cut to 2 threads; with 8 sends waiting, a
    management route is still served.
    """
    shared = anyio.to_thread.current_default_thread_limiter()
    tokens = shared.total_tokens
    shared.total_tokens = 2
    gate = _Gate()
    fake_producer.gate = gate
    service.web_service.namespace_service.list_namespace_and_topics.return_value = {  # noqa: E501
        'status': 'success',
    }
    try:
        async with _client(service) as client:
            held = [
                asyncio.create_task(
                    client.post(
                        RECORDS,
                        json={'value': VALUE},
                        headers=_headers(f'user-{i}'),
                    ),
                )
                for i in range(PRODUCE_MAX_REQUESTS)
            ]
            await gate.until_waiting(PRODUCE_MAX_REQUESTS)
            with anyio.fail_after(2):
                other = await client.get('/api/v3/namespace', headers=HEADERS)
            gate.release.set()
            await asyncio.gather(*held)
    finally:
        shared.total_tokens = tokens
    assert other.status_code == 200
    assert other.json() == {'status': 'success'}


async def test_upload_time_does_not_count_against_the_send(
    service: DiasporaService,
    fake_producer: FakeProducer,
) -> None:
    """The records' time starts once the body is read."""
    record = json.dumps({'value': VALUE}).encode()
    with patch(
        'web_service.main.SEND_TIMEOUT_S',
        PRODUCER_MAX_BLOCK_MS / 1000 + 0.3,
    ):
        status, body, _ = await _call_asgi(
            service,
            RECORDS,
            iter([record[:5], record[5:]]),
            content_length=len(record),
            pause_s=0.5,
        )
    assert status == 200
    assert body['offset'] == 100


async def test_unreachable_cluster_gets_503(
    service: DiasporaService,
) -> None:
    from kafka.errors import KafkaTimeoutError

    broker = 'b-1.cluster.kafka.us-east-1.amazonaws.com:9098'
    with patch(
        'web_service.producer_service.KafkaProducer',
        side_effect=KafkaTimeoutError(f'Unable to bootstrap from {broker}'),
    ) as build:
        first = await _post(service, RECORDS, {'value': VALUE})
        second = await _post(service, RECORDS, {'value': VALUE})
    for response in (first, second):
        assert response.status_code == 503
        assert response.json()['error_code'] == 50302
        assert broker not in response.text
    # The time left before the next build
    assert first.headers['retry-after'] == str(PRODUCER_BUILD_RETRY_S)
    assert 1 <= int(second.headers['retry-after']) <= PRODUCER_BUILD_RETRY_S
    # The second request fails at once instead of building again
    assert build.call_count == 1


# ============================================================================
# Shutdown
# ============================================================================


async def test_shutdown_closes_producer(
    service: DiasporaService,
    fake_producer: FakeProducer,
) -> None:
    async with service.app.router.lifespan_context(service.app):
        response = await _post(service, RECORDS, {'value': VALUE})
        assert response.status_code == 200
        assert fake_producer.close_calls == []
    assert fake_producer.close_calls == [PRODUCER_CLOSE_TIMEOUT_S / 2]
    service.consumer_service.shutdown_all.assert_called_once_with()


async def test_shutdown_without_produce_builds_no_producer(
    service: DiasporaService,
    fake_producer: FakeProducer,
) -> None:
    async with service.app.router.lifespan_context(service.app):
        pass
    assert fake_producer.builds == 0


# ============================================================================
# OpenAPI
# ============================================================================


def test_openapi_documents_produce_routes(service: DiasporaService) -> None:
    paths = service.app.openapi()['paths']
    single = paths['/api/v3/{namespace}/topics/{topic}/records']['post']
    batch = paths['/api/v3/{namespace}/topics/{topic}/records:batch']['post']
    for route in (single, batch):
        assert route['tags'] == ['Producer']
        schema = route['requestBody']['content']['application/json']['schema']
        assert schema['additionalProperties'] is False
    common = {'400', '401', '404', '413', '415', '422', '429'}
    assert common | {'200', '500', '503'} <= set(single['responses'])
    assert common | {'207'} <= set(batch['responses'])
    assert '200' not in batch['responses']
    assert 'schema registry' in single['description']
    assert 'write a record twice' in single['description']
    for code in (
        '40002',
        '40003',
        '40004',
        '40005',
        '40401',
        '40402',
        '41301',
        '41302',
        '41303',
        '41501',
        '42206',
        '42901',
        '50002',
        '50003',
        '50302',
    ):
        assert code in single['description']
    assert '42208' in batch['description']
    batch_schema = batch['requestBody']['content']['application/json']
    entry = batch_schema['schema']['properties']['entries']['items']
    assert entry['properties']['id']['pattern'] == '^[0-9a-zA-Z_-]+$'
