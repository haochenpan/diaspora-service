"""Unit tests for the consumer routes' limits, errors and formats.

The consumer instances are real kafka-python KafkaConsumers that never
connect: api_version spares them the version probe, and nothing here
polls them. kafka-python's own state checks and errors therefore run as
they do in the service. Nothing here uses the network.
"""

from __future__ import annotations

import asyncio
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
from fastapi.testclient import TestClient
from kafka import KafkaConsumer
from kafka.errors import BrokerNotAvailableError
from kafka.errors import CommitFailedError
from kafka.errors import CoordinatorNotAvailableError
from kafka.errors import GroupAuthorizationFailedError
from kafka.errors import IllegalStateError
from kafka.errors import InvalidTopicError
from kafka.errors import KafkaError
from kafka.errors import KafkaTimeoutError
from kafka.errors import NoBrokersAvailable
from kafka.errors import SaslAuthenticationFailedError
from kafka.errors import ThrottlingQuotaExceededError
from kafka.errors import TopicAuthorizationFailedError
from kafka.errors import UnknownTopicOrPartitionError
from kafka.protocol.commit import OffsetCommitRequest
from kafka.structs import TopicPartition

from web_service import main
from web_service.consumer_routes import MAX_BODY_BYTES
from web_service.consumer_service import FETCH_MAX_BYTES
from web_service.consumer_service import kafka_failure
from web_service.consumer_service import MAX_INSTANCES
from web_service.consumer_service import MAX_INSTANCES_PER_SUBJECT
from web_service.consumer_service import MAX_PARTITION_FETCH_BYTES
from web_service.main import DiasporaService

SUBJECT = '123e4567-e89b-12d3-a456-426614174000'
HEADERS = {'subject': SUBJECT, 'authorization': 'Bearer ' + 'a' * 50}
NAMESPACE = 'ns-test123'
TOPICS = ['t1', 't2']
GROUP = 'g'
BASE = f'/api/v3/{NAMESPACE}/consumers'
TOKEN_INACTIVE = {'status': 'error', 'reason': 'Token is inactive.'}


def _build(**config: Any) -> KafkaConsumer:
    """A real KafkaConsumer with the service's settings, never connected."""
    return KafkaConsumer(**config, api_version=(2, 8, 0))


@pytest.fixture
def built() -> Iterator[list[dict[str, Any]]]:
    """Build real consumers; yields the settings each was built with."""
    configs: list[dict[str, Any]] = []

    def build(**config: Any) -> KafkaConsumer:
        configs.append(config)
        return _build(**config)

    with patch(
        'web_service.consumer_service.KafkaConsumer',
        side_effect=build,
    ):
        yield configs


@pytest.fixture
def service(built: list[dict[str, Any]]) -> Iterator[DiasporaService]:
    """A DiasporaService with a real ConsumerService and mocked AWS.

    The token check passes, and the subject owns NAMESPACE, which holds
    TOPICS. The reaper thread never starts.
    """
    auth = MagicMock()
    auth.validate_access_token.return_value = None
    web_service = MagicMock()
    ns_db = web_service.namespace_service.dynamodb
    ns_db.get_user_namespaces.return_value = [NAMESPACE]
    ns_db.get_namespace_topics.return_value = TOPICS
    with (
        patch('web_service.main.AuthManager', return_value=auth),
        patch('web_service.main.IAMService'),
        patch('web_service.main.KafkaService'),
        patch('web_service.main.DynamoDBService'),
        patch('web_service.main.NamespaceService'),
        patch('web_service.main.WebService', return_value=web_service),
        patch('web_service.consumer_service.threading.Thread'),
    ):
        svc = DiasporaService()
    svc.consumer_service.bootstrap_servers = 'broker.invalid:9098'
    yield svc
    svc.consumer_service.shutdown_all()


@pytest.fixture
def client(service: DiasporaService) -> TestClient:
    return TestClient(service.app)


def _create(client: TestClient, name: str = 'i1', **body: Any) -> Any:
    return client.post(
        f'{BASE}/{GROUP}',
        json={'name': name, **body},
        headers=HEADERS,
    )


def _inst(name: str = 'i1') -> str:
    return f'{BASE}/{GROUP}/instances/{name}'


def _send(
    client: TestClient,
    method: str,
    path: str,
    body: Any = None,
) -> Any:
    return client.request(
        method,
        path,
        content=None if body is None else json.dumps(body),
        headers={**HEADERS, 'content-type': 'application/json'},
    )


def _consumer(service: DiasporaService, name: str = 'i1') -> KafkaConsumer:
    key = (SUBJECT, NAMESPACE, GROUP, name)
    return service.consumer_service._consumers[key].consumer


def _kafka_tp(topic: str = 't1') -> TopicPartition:
    return TopicPartition(f'{NAMESPACE}.{topic}', 0)


# ============================================================================
# Limits
# ============================================================================


def test_instances_are_built_with_the_fetch_limits(
    client: TestClient,
    built: list[dict[str, Any]],
) -> None:
    assert _create(client).status_code == 200
    assert built[0]['fetch_max_bytes'] == FETCH_MAX_BYTES
    assert built[0]['max_partition_fetch_bytes'] == MAX_PARTITION_FETCH_BYTES


def test_a_subject_may_hold_eight_instances(client: TestClient) -> None:
    for i in range(MAX_INSTANCES_PER_SUBJECT):
        assert _create(client, f'i{i}').status_code == 200
    response = _create(client, 'one-more')
    assert response.status_code == 429
    assert response.json()['error_code'] == 42901
    # Deleting one frees its place
    assert client.delete(_inst('i0'), headers=HEADERS).status_code == 204
    assert _create(client, 'one-more').status_code == 200


def test_the_service_holds_at_most_100_instances(
    service: DiasporaService,
) -> None:
    consumers = service.consumer_service
    created = 0
    subject = 0
    while created < MAX_INSTANCES:
        _, status = consumers.create_consumer(
            f'subject-{subject}',
            NAMESPACE,
            GROUP,
            name=f'i{created}',
        )
        assert status == 200
        created += 1
        subject += created % MAX_INSTANCES_PER_SUBJECT == 0
    body, status = consumers.create_consumer('another', NAMESPACE, GROUP)
    assert status == 429
    assert body['error_code'] == 42901
    assert 'The service holds 100 consumer instances' in body['message']


def test_an_instance_being_built_counts_against_the_limit(
    service: DiasporaService,
) -> None:
    consumers = service.consumer_service
    for i in range(MAX_INSTANCES_PER_SUBJECT - 1):
        consumers.create_consumer(SUBJECT, NAMESPACE, GROUP, name=f'i{i}')
    building = threading.Event()
    release = threading.Event()

    def slow_build(**config: Any) -> KafkaConsumer:
        building.set()
        release.wait(5)
        return _build(**config)

    results: list[Any] = []
    with patch(
        'web_service.consumer_service.KafkaConsumer',
        side_effect=slow_build,
    ):
        thread = threading.Thread(
            target=lambda: results.append(
                consumers.create_consumer(
                    SUBJECT,
                    NAMESPACE,
                    GROUP,
                    name='slow',
                ),
            ),
        )
        thread.start()
        assert building.wait(5)
        body, status = consumers.create_consumer(
            SUBJECT,
            NAMESPACE,
            GROUP,
            name='late',
        )
        release.set()
        thread.join(5)
    assert (status, body['error_code']) == (429, 42901)
    assert results[0][1] == 200


def test_subscription_lists_at_most_the_namespace_cap(
    service: DiasporaService,
    client: TestClient,
) -> None:
    _create(client)
    ns_db = service.web_service.namespace_service.dynamodb
    topics = [f't{i}' for i in range(service.max_topics + 1)]
    ns_db.get_namespace_topics.return_value = topics
    response = _send(
        client,
        'POST',
        f'{_inst()}/subscription',
        {'topics': topics},
    )
    assert response.status_code == 422
    assert response.json()['error_code'] == 422
    assert '51 topics listed' in response.json()['message']
    ns_db.get_namespace_topics.assert_not_called()
    response = _send(
        client,
        'POST',
        f'{_inst()}/subscription',
        {'topics': topics[:-1]},
    )
    assert response.status_code == 204


@pytest.mark.parametrize(
    ('method', 'suffix', 'field', 'item'),
    (
        ('POST', 'assignments', 'partitions', {'topic': 't1', 'partition': 0}),
        (
            'POST',
            'positions',
            'offsets',
            {'topic': 't1', 'partition': 0, 'offset': 0},
        ),
        (
            'POST',
            'positions/beginning',
            'partitions',
            {'topic': 't1', 'partition': 0},
        ),
        (
            'POST',
            'positions/end',
            'partitions',
            {'topic': 't1', 'partition': 0},
        ),
        (
            'POST',
            'offsets',
            'offsets',
            {'topic': 't1', 'partition': 0, 'offset': 0},
        ),
        ('GET', 'offsets', 'partitions', {'topic': 't1', 'partition': 0}),
    ),
)
def test_partition_lists_are_bounded(
    service: DiasporaService,
    client: TestClient,
    *,
    method: str,
    suffix: str,
    field: str,
    item: dict[str, Any],
) -> None:
    _create(client)
    items = [item] * (service.max_topics + 1)
    response = _send(client, method, f'{_inst()}/{suffix}', {field: items})
    assert response.status_code == 422
    assert response.json() == {
        'error_code': 422,
        'message': (
            f'51 {field} listed; a request may list at most 50, the most '
            'topics a namespace holds'
        ),
    }


def test_list_bound_follows_the_configured_cap(
    service: DiasporaService,
    client: TestClient,
) -> None:
    service.max_topics = 2
    _create(client)
    response = _send(
        client,
        'POST',
        f'{_inst()}/assignments',
        {'partitions': [{'topic': 't1', 'partition': 0}] * 3},
    )
    assert response.status_code == 422


def test_body_over_the_limit_is_refused_unread(client: TestClient) -> None:
    body = json.dumps({'topics': ['t' * 100] * 1000}).encode()
    assert len(body) > MAX_BODY_BYTES
    response = client.post(
        f'{_inst()}/subscription',
        content=body,
        headers={**HEADERS, 'content-type': 'application/json'},
    )
    assert response.status_code == 413
    assert response.json()['error_code'] == 41301


def test_chunked_body_over_the_limit_is_refused(client: TestClient) -> None:
    chunk = b' ' * 16384

    def chunks() -> Iterator[bytes]:
        for _ in range(MAX_BODY_BYTES // len(chunk) + 1):
            yield chunk

    response = client.post(
        f'{_inst()}/subscription',
        content=chunks(),
        headers={**HEADERS, 'content-type': 'application/json'},
    )
    assert response.status_code == 413
    assert response.json()['error_code'] == 41301


async def test_lifespan_sets_the_shared_threadpool(
    service: DiasporaService,
) -> None:
    limiter = anyio.to_thread.current_default_thread_limiter()
    limiter.total_tokens = 7
    with patch.object(service.consumer_service, 'shutdown_all'):
        async with service._lifespan(service.app):
            assert limiter.total_tokens == main.SHARED_THREADS


async def test_fetches_run_on_their_own_limiter(
    service: DiasporaService,
) -> None:
    """At most FETCH_MAX_CONCURRENT fetches poll at once; others wait."""
    running = 0
    most = 0
    lock = threading.Lock()
    turns = threading.Semaphore(0)

    def poll(**kwargs: Any) -> tuple[list[Any], int]:
        nonlocal running, most
        with lock:
            running += 1
            most = max(most, running)
        turns.acquire(timeout=5)
        with lock:
            running -= 1
        return [], 200

    count = main.FETCH_MAX_CONCURRENT + 3
    transport = httpx.ASGITransport(app=service.app)
    with patch.object(
        service.consumer_service,
        'poll_records',
        side_effect=poll,
    ):
        async with httpx.AsyncClient(
            transport=transport,
            base_url='http://test',
        ) as client:
            fetches = [
                asyncio.create_task(
                    client.get(f'{_inst()}/records', headers=HEADERS),
                )
                for _ in range(count)
            ]
            deadline = time.monotonic() + 5
            while running < main.FETCH_MAX_CONCURRENT and (
                time.monotonic() < deadline
            ):
                await asyncio.sleep(0.01)
            await asyncio.sleep(0.2)
            assert running == main.FETCH_MAX_CONCURRENT
            # The shared pool still serves other routes meanwhile
            other = await client.get(
                f'{_inst("none")}/assignments',
                headers=HEADERS,
            )
            assert other.status_code == 404
            for _ in range(count):
                turns.release()
            responses = await asyncio.gather(*fetches)
    assert most == main.FETCH_MAX_CONCURRENT
    assert [r.status_code for r in responses] == [200] * count


# ============================================================================
# Errors: shape and client mistakes
# ============================================================================


def test_validation_failure_is_confluent_shaped(client: TestClient) -> None:
    _create(client)
    response = _send(
        client,
        'POST',
        f'{_inst()}/positions',
        {'offsets': [{'topic': 't1', 'partition': 0, 'offset': -1}]},
    )
    assert response.status_code == 422
    assert response.json() == {
        'error_code': 422,
        'message': (
            'body.offsets.0.offset: Input should be greater than or equal to 0'
        ),
    }


def test_missing_field_is_confluent_shaped(client: TestClient) -> None:
    response = _send(client, 'POST', f'{_inst()}/assignments', {})
    assert response.status_code == 422
    assert response.json() == {
        'error_code': 422,
        'message': 'body.partitions: Field required',
    }


@pytest.mark.parametrize(
    ('method', 'suffix'),
    (('POST', 'assignments'), ('GET', 'offsets')),
)
def test_body_not_json_is_400(
    client: TestClient,
    method: str,
    suffix: str,
) -> None:
    response = client.request(
        method,
        f'{_inst()}/{suffix}',
        content=b'{"partitions": [',
        headers={**HEADERS, 'content-type': 'application/json'},
    )
    assert response.status_code == 400
    assert response.json() == {
        'error_code': 40003,
        'message': 'The body is not valid JSON',
    }


def test_token_failure_has_a_code(
    service: DiasporaService,
    client: TestClient,
) -> None:
    service.auth.validate_access_token.return_value = TOKEN_INACTIVE
    response = _create(client)
    assert response.status_code == 401
    assert response.json() == {
        'error_code': 40101,
        'message': 'Authentication failed: Token is inactive.',
    }


def test_token_audience_reason_is_not_echoed(
    service: DiasporaService,
    client: TestClient,
) -> None:
    service.auth.validate_access_token.return_value = {
        'status': 'error',
        'reason': 'Not in the audience set. introspected = [x] '
        'server_client_id = secret-id',
    }
    response = client.get(f'{_inst()}/records', headers=HEADERS)
    assert response.status_code == 401
    assert response.json() == {
        'error_code': 40101,
        'message': 'Authentication failed: Token is not for this service.',
    }


def test_unhandled_error_hides_its_text(
    service: DiasporaService,
    client: TestClient,
) -> None:
    with patch.object(
        service.consumer_service,
        'get_assignments',
        side_effect=RuntimeError('secret broker b-1.internal'),
    ):
        response = client.get(f'{_inst()}/assignments', headers=HEADERS)
    assert response.status_code == 500
    assert response.json() == {
        'error_code': 500,
        'message': 'Internal server error',
    }


def test_other_routes_keep_fastapi_replies(client: TestClient) -> None:
    """The produce and topic routes keep FastAPI's validation reply."""
    response = client.post(f'/api/v3/{NAMESPACE}/topics/t1/records')
    assert response.status_code == 422
    assert 'detail' in response.json()
    response = client.post(f'/api/v3/{NAMESPACE}/t1')
    assert response.status_code == 422
    assert 'detail' in response.json()


def test_instance_not_found_is_404(client: TestClient) -> None:
    response = client.get(f'{_inst("nope")}/subscription', headers=HEADERS)
    assert response.status_code == 404
    assert response.json() == {
        'error_code': 40403,
        'message': 'Consumer instance not found',
    }


def test_subscribe_after_assign_is_409(client: TestClient) -> None:
    _create(client)
    _send(
        client,
        'POST',
        f'{_inst()}/assignments',
        {'partitions': [{'topic': 't1', 'partition': 0}]},
    )
    response = _send(
        client,
        'POST',
        f'{_inst()}/subscription',
        {'topics': ['t1']},
    )
    assert response.status_code == 409
    assert response.json()['error_code'] == 40903
    assert response.json()['message'].startswith('Illegal state: ')


def test_assign_after_subscribe_is_409(client: TestClient) -> None:
    _create(client)
    _send(client, 'POST', f'{_inst()}/subscription', {'topics': ['t1']})
    response = _send(
        client,
        'POST',
        f'{_inst()}/assignments',
        {'partitions': [{'topic': 't1', 'partition': 0}]},
    )
    assert response.status_code == 409
    assert response.json()['error_code'] == 40903


def test_illegal_state_comes_from_kafka_python(
    service: DiasporaService,
    client: TestClient,
) -> None:
    """The 409 above is kafka-python's own IllegalStateError."""
    _create(client)
    consumer = _consumer(service)
    consumer.assign([_kafka_tp()])
    with pytest.raises(IllegalStateError):
        consumer.subscribe([_kafka_tp().topic])


@pytest.mark.parametrize(
    ('suffix', 'body'),
    (
        (
            'positions',
            {'offsets': [{'topic': 't2', 'partition': 0, 'offset': 3}]},
        ),
        (
            'positions/beginning',
            {'partitions': [{'topic': 't2', 'partition': 0}]},
        ),
        ('positions/end', {'partitions': [{'topic': 't2', 'partition': 0}]}),
        (
            'offsets',
            {'offsets': [{'topic': 't2', 'partition': 0, 'offset': 3}]},
        ),
    ),
)
def test_unassigned_partition_is_409(
    client: TestClient,
    suffix: str,
    body: dict[str, Any],
) -> None:
    _create(client)
    _send(
        client,
        'POST',
        f'{_inst()}/assignments',
        {'partitions': [{'topic': 't1', 'partition': 0}]},
    )
    response = _send(client, 'POST', f'{_inst()}/{suffix}', body)
    assert response.status_code == 409
    assert response.json() == {
        'error_code': 40903,
        'message': (
            'Illegal state: partition 0 of topic t2 is not assigned to '
            'this consumer instance'
        ),
    }


def test_seek_on_an_unassigned_partition_fails_in_kafka_python(
    service: DiasporaService,
    client: TestClient,
) -> None:
    """kafka-python only asserts, which the 409 above saves the caller."""
    _create(client)
    with pytest.raises(AssertionError):
        _consumer(service).seek(_kafka_tp('t2'), 3)


def test_seek_to_with_no_partitions_needs_an_assignment(
    client: TestClient,
) -> None:
    _create(client)
    response = _send(
        client,
        'POST',
        f'{_inst()}/positions/beginning',
        {'partitions': []},
    )
    assert response.status_code == 409
    assert response.json()['error_code'] == 40903


def test_seek_on_an_assigned_partition(
    service: DiasporaService,
    client: TestClient,
) -> None:
    _create(client)
    _send(
        client,
        'POST',
        f'{_inst()}/assignments',
        {'partitions': [{'topic': 't1', 'partition': 0}]},
    )
    response = _send(
        client,
        'POST',
        f'{_inst()}/positions',
        {'offsets': [{'topic': 't1', 'partition': 0, 'offset': 7}]},
    )
    assert response.status_code == 204
    subscription = _consumer(service)._subscription
    assert subscription.assignment[_kafka_tp()].position.offset == 7


@pytest.mark.parametrize(
    ('method', 'suffix', 'body'),
    (
        (
            'POST',
            'assignments',
            {'partitions': [{'topic': 't1', 'partition': 1}]},
        ),
        (
            'POST',
            'positions',
            {'offsets': [{'topic': 't1', 'partition': 1, 'offset': 0}]},
        ),
        (
            'POST',
            'offsets',
            {'offsets': [{'topic': 't1', 'partition': 1, 'offset': 0}]},
        ),
        ('GET', 'offsets', {'partitions': [{'topic': 't1', 'partition': 1}]}),
    ),
)
def test_partition_other_than_0_is_404(
    client: TestClient,
    method: str,
    suffix: str,
    body: dict[str, Any],
) -> None:
    _create(client)
    response = _send(client, method, f'{_inst()}/{suffix}', body)
    assert response.status_code == 404
    assert response.json()['error_code'] == 40402


def test_commit_sends_offsets_kafka_python_can_encode(
    service: DiasporaService,
    client: TestClient,
) -> None:
    """The offsets committed encode in kafka-python's OffsetCommitRequest.

    MSK's Kafka 3.8 takes version 7, the highest kafka-python sends, which
    needs an integer leader epoch and a string metadata.
    """
    _create(client)
    _send(
        client,
        'POST',
        f'{_inst()}/assignments',
        {'partitions': [{'topic': 't1', 'partition': 0}]},
    )
    consumer = _consumer(service)
    with patch.object(
        consumer._coordinator,
        'commit_offsets_sync',
    ) as commit:
        response = _send(
            client,
            'POST',
            f'{_inst()}/offsets',
            {'offsets': [{'topic': 't1', 'partition': 0, 'offset': 5}]},
        )
    assert response.status_code == 200
    ((offsets,), _) = commit.call_args
    request = OffsetCommitRequest[7](
        'group',
        1,
        'member',
        None,
        [
            (
                tp.topic,
                [(tp.partition, o.offset, o.leader_epoch, o.metadata)],
            )
            for tp, o in offsets.items()
        ],
    )
    assert request.encode()
    assert offsets[_kafka_tp()].offset == 5


def test_commit_failed_is_409(
    service: DiasporaService,
    client: TestClient,
) -> None:
    _create(client)
    _send(client, 'POST', f'{_inst()}/subscription', {'topics': ['t1']})
    with patch.object(
        _consumer(service)._coordinator,
        'commit_offsets_sync',
        side_effect=CommitFailedError('UnknownMemberIdError'),
    ):
        response = client.post(f'{_inst()}/offsets', headers=HEADERS)
    assert response.status_code == 409
    assert response.json()['error_code'] == 40903
    assert 'UnknownMemberId' not in response.json()['message']


def test_kafka_error_text_is_not_echoed(
    service: DiasporaService,
    client: TestClient,
) -> None:
    _create(client)
    _send(client, 'POST', f'{_inst()}/subscription', {'topics': ['t1']})
    with patch.object(
        _consumer(service)._coordinator,
        'commit_offsets_sync',
        side_effect=KafkaTimeoutError('b-2.cluster.internal:9098 timed out'),
    ):
        response = client.post(f'{_inst()}/offsets', headers=HEADERS)
    assert response.status_code == 500
    assert response.json() == {
        'error_code': 50003,
        'message': (
            'Failed to commit offsets: Kafka error (KafkaTimeoutError); '
            'retry it'
        ),
    }


@pytest.mark.parametrize(
    ('error', 'status', 'code'),
    (
        (IllegalStateError('x'), 409, 40903),
        (CommitFailedError('x'), 409, 40903),
        (TopicAuthorizationFailedError('x'), 403, 40301),
        (GroupAuthorizationFailedError('x'), 403, 40301),
        (NoBrokersAvailable('x'), 503, 50302),
        (BrokerNotAvailableError('x'), 503, 50302),
        (UnknownTopicOrPartitionError('x'), 404, 40401),
        (InvalidTopicError('x'), 400, 40002),
        (ThrottlingQuotaExceededError('x'), 429, 42901),
        (KafkaTimeoutError('x'), 500, 50003),
        (CoordinatorNotAvailableError('x'), 500, 50003),
        (SaslAuthenticationFailedError('x'), 500, 50002),
        (KafkaError('x'), 500, 50002),
        (AssertionError('x'), 500, 500),
        (ValueError('x'), 500, 500),
    ),
)
def test_kafka_failure_mapping(
    error: Exception,
    status: int,
    code: int,
) -> None:
    error = type(error)('secret text from the cluster')
    try:
        raise error
    except Exception as e:
        body, got = kafka_failure(e, 'do it')
    assert (got, body['error_code']) == (status, code)
    assert 'secret' not in body['message']


def test_broker_unreachable_at_create_is_503(
    service: DiasporaService,
) -> None:
    with patch(
        'web_service.consumer_service.KafkaConsumer',
        side_effect=NoBrokersAvailable(),
    ):
        body, status = service.consumer_service.create_consumer(
            SUBJECT,
            NAMESPACE,
            GROUP,
        )
    assert (status, body['error_code']) == (503, 50302)


@pytest.mark.parametrize(
    'body',
    (
        {'fetch.min.bytes': 'many'},
        # kafka-python refuses a request timeout under its fetch wait
        {'consumer.request.timeout.ms': '100'},
    ),
)
def test_invalid_consumer_config_is_422(
    client: TestClient,
    body: dict[str, Any],
) -> None:
    response = _create(client, **body)
    assert response.status_code == 422
    assert response.json()['error_code'] == 42204
    # The name is free again
    assert _create(client).status_code == 200


# ============================================================================
# Formats
# ============================================================================


@pytest.mark.parametrize('fmt', ('avro', 'jsonschema', 'protobuf', 'AVRO'))
def test_schema_formats_are_refused(
    client: TestClient,
    built: list[dict[str, Any]],
    fmt: str,
) -> None:
    response = _create(client, format=fmt)
    assert response.status_code == 422
    assert response.json() == {
        'error_code': 422,
        'message': (
            f'body.format: format {fmt} needs a schema registry; Octopus '
            'runs no schema registry, so use binary or json'
        ),
    }
    assert built == []


def test_unknown_format_is_refused(client: TestClient) -> None:
    response = _create(client, format='xml')
    assert response.status_code == 422
    assert response.json() == {
        'error_code': 422,
        'message': 'body.format: Invalid format type xml; use binary or json',
    }


@pytest.mark.parametrize(
    ('fmt', 'stored'),
    (('json', 'json'), ('JSON', 'json'), ('Binary', 'binary')),
)
def test_binary_and_json_are_accepted_in_any_case(
    service: DiasporaService,
    client: TestClient,
    fmt: str,
    stored: str,
) -> None:
    assert _create(client, format=fmt).status_code == 200
    key = (SUBJECT, NAMESPACE, GROUP, 'i1')
    assert service.consumer_service._consumers[key].format_type == stored
