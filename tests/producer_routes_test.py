"""Unit tests for the produce routes of the FastAPI app (main.py).

The Kafka producer is replaced by a fake; nothing here uses the network.
"""

from __future__ import annotations

import asyncio
import base64
import json
import threading
from collections.abc import Iterator
from typing import Any
from unittest.mock import MagicMock
from unittest.mock import patch

import httpx
import pytest

from tests.producer_service_test import FakeProducer
from web_service.main import DiasporaService
from web_service.producer_service import MAX_BODY_BYTES
from web_service.producer_service import MAX_RECORD_BYTES
from web_service.producer_service import PRODUCER_CLOSE_TIMEOUT_S

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
    async with _client(service) as client:
        return await client.post(
            path,
            json=body if content is None else None,
            content=content,
            headers=HEADERS if headers is None else headers,
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
        ('b', 50002),
        ('big', 41302),
        ('p1', 40005),
    ]


# ============================================================================
# Refusals of the request
# ============================================================================


@pytest.mark.parametrize(
    ('content', 'code'),
    (
        (
            b'{"value": {"type": "STRING", "data": "a"}}\n{"value": null}',
            40002,
        ),
        (b'not json', 40001),
        (b'[]', 40001),
        (b'', 40001),
    ),
    ids=('two-values', 'not-json', 'not-object', 'empty'),
)
async def test_produce_record_body_refused(
    service: DiasporaService,
    fake_producer: FakeProducer,
    content: bytes,
    code: int,
) -> None:
    response = await _post(service, RECORDS, content=content)
    assert response.status_code == 400
    assert response.json()['error_code'] == code
    assert fake_producer.sent == []


@pytest.mark.parametrize(
    ('record', 'code'),
    (
        ({'value': {'type': 'AVRO', 'data': {}}}, 40004),
        ({'value': {'type': 'PROTOBUF', 'data': {}}}, 40004),
        ({'value': {'type': 'JSONSCHEMA', 'data': {}}}, 40004),
        ({'value': {'type': 'JSON', 'data': 1, 'schema_id': 3}}, 40004),
        ({'key': {'type': 'JSON', 'data': 1, 'subject': 's'}}, 40004),
        ({'value': {'data': 'x'}}, 40004),
        ({'partition_id': 1, 'value': VALUE}, 40005),
        ({'value': {'type': 'BINARY', 'data': '***'}}, 40003),
        ({'value': VALUE, 'timestamp': 'noon'}, 40003),
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
    code: int,
) -> None:
    response = await _post(service, RECORDS, record)
    assert response.status_code == 400
    body = response.json()
    assert set(body) == {'error_code', 'message'}
    assert body['error_code'] == code
    if code == 40004:
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
    ('body', 'code'),
    (
        ({'entries': [{'id': 'a', 'value': VALUE}] * 2}, 40008),
        ({'entries': [{'value': VALUE}]}, 40008),
        ({'entries': [{'id': 'x' * 81, 'value': VALUE}]}, 40008),
        (
            {'entries': [{'id': str(i), 'value': VALUE} for i in range(501)]},
            40007,
        ),
        ({'entries': []}, 40007),
    ),
    ids=('duplicate-id', 'missing-id', 'long-id', 'over-500', 'empty'),
)
async def test_produce_batch_refused_as_a_whole(
    service: DiasporaService,
    fake_producer: FakeProducer,
    body: dict[str, Any],
    code: int,
) -> None:
    response = await _post(service, BATCH, body)
    assert response.status_code == 400
    assert response.json()['error_code'] == code
    assert fake_producer.sent == []


async def test_produce_batch_two_values_refused(
    service: DiasporaService,
    fake_producer: FakeProducer,
) -> None:
    one = json.dumps({'entries': [{'id': 'a', 'value': VALUE}]}).encode()
    response = await _post(service, BATCH, content=one + one)
    assert response.status_code == 400
    assert response.json()['error_code'] == 40002


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
    assert reply['error_code'] == 50002
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
async def test_token_failure_matches_consumer_routes(
    service: DiasporaService,
    fake_producer: FakeProducer,
    path: str,
    body: Any,
) -> None:
    service.auth.validate_access_token.return_value = AUTH_ERROR
    consumer = await _reply(service, *CONSUMER_SUBSCRIBE)
    produce = await _reply(service, path, body)
    assert produce == consumer == (401, AUTH_ERROR)
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
        'error_code': 40006,
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
) -> tuple[int, Any, int]:
    """Send a request straight to the app, a chunk per receive().

    Returns the status, the JSON reply and how many chunks were read.
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
    assert fake_producer.close_calls == [PRODUCER_CLOSE_TIMEOUT_S]
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
    assert {'200', '400', '401', '404', '413', '500'} <= set(
        single['responses'],
    )
    assert {'207', '400', '401', '404', '413'} <= set(batch['responses'])
    assert '200' not in batch['responses']
    assert 'schema registry' in single['description']
    for code in ('40001', '40002', '40003', '40004', '40005', '40006'):
        assert code in single['description']
    for code in ('40007', '40008', '41301'):
        assert code in batch['description']
