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
from contextlib import contextmanager
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import anyio
import httpx
import pytest
from fastapi.testclient import TestClient
from kafka import KafkaConsumer
from kafka.client_async import KafkaClient
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
from kafka.errors import UnsupportedVersionError
from kafka.future import Future
from kafka.protocol.commit import OffsetCommitResponse
from kafka.structs import OffsetAndMetadata
from kafka.structs import TopicPartition
from starlette.responses import JSONResponse

from web_service import main
from web_service.consumer_records import FetchReply
from web_service.consumer_routes import MAX_BODY_BYTES
from web_service.consumer_service import FETCH_MAX_BYTES
from web_service.consumer_service import InstanceLock
from web_service.consumer_service import kafka_failure
from web_service.consumer_service import MAX_INSTANCES
from web_service.consumer_service import MAX_INSTANCES_PER_SUBJECT
from web_service.consumer_service import MAX_PARTITION_FETCH_BYTES
from web_service.consumer_service import REPLY_MAX_BYTES
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


def test_a_subject_may_hold_four_instances(client: TestClient) -> None:
    for i in range(MAX_INSTANCES_PER_SUBJECT):
        assert _create(client, f'i{i}').status_code == 200
    response = _create(client, 'one-more')
    assert response.status_code == 429
    assert response.json()['error_code'] == 42901
    # Deleting one frees its place
    assert client.delete(_inst('i0'), headers=HEADERS).status_code == 204
    assert _create(client, 'one-more').status_code == 200


def test_the_service_holds_at_most_16_instances(
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
    assert 'The service holds 16 consumer instances' in body['message']


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
    _create(client)
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
        'message': 'Consumer instance not found.',
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
    ('body', 'message'),
    (
        (
            {'auto.offset.reset': 'smallest'},
            'auto.offset.reset must be earliest, latest or none',
        ),
        (
            {'auto.offset.reset': 'largest'},
            'auto.offset.reset must be earliest, latest or none',
        ),
        (
            {'auto.commit.enable': 'yes'},
            'auto.commit.enable must be true or false',
        ),
        (
            {'fetch.min.bytes': 'many'},
            'fetch.min.bytes must be a whole number of at least 0',
        ),
        (
            {'fetch.min.bytes': -1},
            'fetch.min.bytes must be a whole number of at least 0',
        ),
        (
            {'consumer.request.timeout.ms': '-5'},
            'consumer.request.timeout.ms must be a whole number of at least 0',
        ),
        (
            {'consumer.request.timeout.ms': 'soon'},
            'consumer.request.timeout.ms must be a whole number of at least 0',
        ),
    ),
)
def test_invalid_consumer_config_is_422(
    client: TestClient,
    built: list[dict[str, Any]],
    body: dict[str, Any],
    message: str,
) -> None:
    """Each setting is checked as Confluent checks it, before any build."""
    response = _create(client, **body)
    assert response.status_code == 422
    assert response.json() == {
        'error_code': 42204,
        'message': f'Invalid consumer configuration: {message}',
    }
    assert built == []
    # The name is free again
    assert _create(client).status_code == 200


@pytest.mark.parametrize(
    'body',
    (
        {'auto.offset.reset': 'none', 'auto.commit.enable': 'TRUE'},
        {'fetch.min.bytes': 1024, 'consumer.request.timeout.ms': 2000},
        {'fetch.min.bytes': '1024', 'consumer.request.timeout.ms': '2000'},
    ),
)
def test_valid_consumer_config_is_accepted(
    client: TestClient,
    body: dict[str, Any],
) -> None:
    assert _create(client, **body).status_code == 200


@pytest.mark.parametrize(
    ('given', 'passed'),
    (('Earliest', 'earliest'), ('LATEST', 'latest'), (' none ', 'none')),
)
def test_offset_reset_is_taken_in_any_case(
    client: TestClient,
    built: list[dict[str, Any]],
    given: str,
    passed: str,
) -> None:
    assert _create(client, **{'auto.offset.reset': given}).status_code == 200
    assert built[-1]['auto_offset_reset'] == passed


def test_half_built_consumer_is_closed(
    client: TestClient,
    built: list[dict[str, Any]],
) -> None:
    """A KafkaConsumer that raises after building its client closes it."""
    closed: list[Any] = []
    real_close = KafkaClient.close

    def close(self: KafkaClient, *args: Any, **kwargs: Any) -> None:
        closed.append(self)
        real_close(self, *args, **kwargs)

    with (
        patch.object(KafkaClient, 'close', close),
        patch(
            'kafka.consumer.group.ConsumerCoordinator',
            side_effect=RuntimeError('late failure'),
        ),
    ):
        response = _create(client)
    assert response.status_code == 500
    assert len(built) == 1
    assert len(closed) == 1
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
            'body.format: avro, jsonschema and protobuf need a schema '
            'registry, which Octopus does not run; use binary or json'
        ),
    }
    assert built == []


def test_unknown_format_is_refused(client: TestClient) -> None:
    response = _create(client, format='xml')
    assert response.status_code == 422
    assert response.json() == {
        'error_code': 422,
        'message': 'body.format: Invalid format type; use binary or json',
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


# ============================================================================
# Fetches: fairness, waits and reply size
# ============================================================================


def _subject(i: int) -> str:
    return f'00000000-0000-0000-0000-{i:012d}'


def _headers(subject: str) -> dict[str, str]:
    return {**HEADERS, 'subject': subject}


def _records(subject: str, name: str, query: str = '') -> str:
    return f'{BASE}/{GROUP}/instances/{name}/records{query}'


class _Blocked:
    """A fetch_records that blocks until released, counting its callers."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.running = 0
        self.most = 0
        self.calls: list[dict[str, Any]] = []
        self.release = threading.Event()

    def __call__(self, *args: Any, **kwargs: Any) -> tuple[FetchReply, int]:
        with self.lock:
            self.running += 1
            self.most = max(self.most, self.running)
            self.calls.append(kwargs)
        self.release.wait(timeout=10)
        with self.lock:
            self.running -= 1
        return FetchReply([]), 200

    async def until_running(self, count: int) -> None:
        deadline = time.monotonic() + 5
        while self.running < count and time.monotonic() < deadline:
            await asyncio.sleep(0.01)
        assert self.running == count


def _instances(service: DiasporaService, subjects: int) -> None:
    """Two instances, a and b, for each of the first subjects."""
    for i in range(subjects):
        for name in ('a', 'b'):
            _, status = service.consumer_service.create_consumer(
                _subject(i),
                NAMESPACE,
                GROUP,
                name=name,
            )
            assert status == 200


def _client(service: DiasporaService) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=service.app),
        base_url='http://test',
        timeout=30,
    )


async def test_fetches_take_one_of_the_slots(service: DiasporaService) -> None:
    """At most FETCH_MAX_CONCURRENT fetches run; the others wait a turn."""
    slots = main.FETCH_MAX_CONCURRENT
    count = slots + 3
    _instances(service, subjects=(count + 1) // 2)
    blocked = _Blocked()
    with patch.object(
        service.consumer_service,
        'fetch_records',
        side_effect=blocked,
    ):
        async with _client(service) as client:
            fetches = [
                asyncio.create_task(
                    client.get(
                        _records(_subject(i // 2), 'ab'[i % 2]),
                        params={'timeout': 10000},
                        headers=_headers(_subject(i // 2)),
                    ),
                )
                for i in range(count)
            ]
            await blocked.until_running(slots)
            await asyncio.sleep(0.2)
            assert blocked.running == slots
            # The shared pool still serves other routes meanwhile
            other = await client.get(
                f'{_inst("none")}/assignments',
                headers=HEADERS,
            )
            assert other.status_code == 404
            blocked.release.set()
            responses = await asyncio.gather(*fetches)
    assert blocked.most == slots
    assert [r.status_code for r in responses] == [200] * count


async def test_a_subject_may_run_two_fetches(service: DiasporaService) -> None:
    """A third fetch by one subject is refused at once, without a poll."""
    _instances(service, subjects=1)
    service.consumer_service.create_consumer(
        _subject(0),
        NAMESPACE,
        GROUP,
        name='c',
    )
    blocked = _Blocked()
    headers = _headers(_subject(0))
    with patch.object(
        service.consumer_service,
        'fetch_records',
        side_effect=blocked,
    ):
        async with _client(service) as client:
            running = [
                asyncio.create_task(
                    client.get(_records(_subject(0), name), headers=headers),
                )
                for name in ('a', 'b')
            ]
            await blocked.until_running(main.FETCH_MAX_PER_SUBJECT)
            started = time.monotonic()
            third = await client.get(
                _records(_subject(0), 'c'),
                headers=headers,
            )
            assert time.monotonic() - started < 1
            # Another subject is not held back
            other = asyncio.create_task(
                client.get(
                    _records(_subject(1), 'a'),
                    headers=_headers(_subject(1)),
                ),
            )
            blocked.release.set()
            await asyncio.gather(*running)
    assert third.status_code == 429
    assert third.headers['retry-after'] == '1'
    assert third.json() == {
        'error_code': 42901,
        'message': (
            'You have 2 fetches in progress, the most one user may; retry '
            'when one finishes'
        ),
    }
    assert len(blocked.calls) == 2
    assert (await other).status_code == 404


async def test_a_slot_wait_is_bounded_by_the_timeout(
    service: DiasporaService,
) -> None:
    """A fetch that finds no slot within its timeout gets 429, unpolled."""
    slots = main.FETCH_MAX_CONCURRENT
    _instances(service, subjects=slots // 2 + 1)
    blocked = _Blocked()
    with patch.object(
        service.consumer_service,
        'fetch_records',
        side_effect=blocked,
    ):
        async with _client(service) as client:
            hogs = [
                asyncio.create_task(
                    client.get(
                        _records(_subject(i // 2), 'ab'[i % 2]),
                        headers=_headers(_subject(i // 2)),
                    ),
                )
                for i in range(slots)
            ]
            await blocked.until_running(slots)
            late = _subject(slots // 2)
            started = time.monotonic()
            refused = await client.get(
                _records(late, 'a'),
                params={'timeout': 300},
                headers=_headers(late),
            )
            waited = time.monotonic() - started
            blocked.release.set()
            await asyncio.gather(*hogs)
    assert refused.status_code == 429
    assert refused.headers['retry-after'] == '1'
    assert refused.json()['error_code'] == 42901
    assert 0.25 < waited < 1.5
    assert len(blocked.calls) == slots


async def test_the_slot_wait_comes_off_the_poll(
    service: DiasporaService,
) -> None:
    """A fetch that waited for a slot polls only for what is left."""
    slots = main.FETCH_MAX_CONCURRENT
    _instances(service, subjects=slots // 2 + 1)
    blocked = _Blocked()
    with patch.object(
        service.consumer_service,
        'fetch_records',
        side_effect=blocked,
    ):
        async with _client(service) as client:
            hogs = [
                asyncio.create_task(
                    client.get(
                        _records(_subject(i // 2), 'ab'[i % 2]),
                        headers=_headers(_subject(i // 2)),
                    ),
                )
                for i in range(slots)
            ]
            await blocked.until_running(slots)
            late = _subject(slots // 2)
            waiting = asyncio.create_task(
                client.get(
                    _records(late, 'a'),
                    params={'timeout': 3000},
                    headers=_headers(late),
                ),
            )
            await asyncio.sleep(1)
            blocked.release.set()
            await asyncio.gather(*hogs)
            assert (await waiting).status_code == 200
    late_call = blocked.calls[-1]
    assert 1000 < 3000 - late_call['timeout_ms'] < 2000


async def test_a_busy_or_missing_instance_is_answered_at_once(
    service: DiasporaService,
) -> None:
    """Neither waits for the instance's lock nor for a slot."""
    _instances(service, subjects=1)
    managed = service.consumer_service._consumers[
        (_subject(0), NAMESPACE, GROUP, 'a')
    ]
    managed.lock.acquire()
    try:
        async with _client(service) as client:
            started = time.monotonic()
            busy = await client.get(
                _records(_subject(0), 'a'),
                headers=_headers(_subject(0)),
            )
            missing = await client.get(
                _records(_subject(0), 'none'),
                headers=_headers(_subject(0)),
            )
            assert time.monotonic() - started < 1
    finally:
        managed.lock.release()
    assert (busy.status_code, busy.json()['error_code']) == (409, 40904)
    assert (missing.status_code, missing.json()['error_code']) == (404, 40403)


async def test_a_fetch_whose_client_left_does_not_poll(
    service: DiasporaService,
) -> None:
    _instances(service, subjects=1)
    request = MagicMock()
    request.is_disconnected = AsyncMock(return_value=True)
    with patch.object(service.consumer_service, 'fetch_records') as fetch:
        response = await service.fetch_consumer_records(
            NAMESPACE,
            GROUP,
            'a',
            request=request,
            subject=_subject(0),
            token=HEADERS['authorization'],
            timeout=None,
            max_bytes=None,
        )
    fetch.assert_not_called()
    assert response.status_code == 400
    # The instance and the slot are free again
    key = (_subject(0), NAMESPACE, GROUP, 'a')
    assert not service.consumer_service._consumers[key].lock.locked()
    assert service.fetch_slots.borrowed_tokens == 0
    assert service.fetching == {}


@pytest.mark.parametrize(
    ('query', 'created', 'wait_ms', 'max_records'),
    (
        ('', {}, 5000, None),
        ('?timeout=-1', {}, 5000, None),
        ('?timeout=0', {}, 5000, None),
        ('?timeout=-1', {'consumer.request.timeout.ms': 1200}, 1200, None),
        ('', {'consumer.request.timeout.ms': '90000'}, 30000, None),
        # The instance's wait does not cap a fetch's own timeout
        ('?timeout=4000', {'consumer.request.timeout.ms': 1200}, 4000, None),
        ('?max_bytes=0', {}, 5000, None),
        ('?max_bytes=-1', {}, 5000, None),
        ('?max_bytes=7', {}, 5000, 7),
    ),
)
def test_fetch_waits_and_record_counts(
    service: DiasporaService,
    client: TestClient,
    *,
    query: str,
    created: dict[str, Any],
    wait_ms: int,
    max_records: int | None,
) -> None:
    """Values Confluent takes as its defaults do here too, rather than 500."""
    assert _create(client, **created).status_code == 200
    _send(
        client,
        'POST',
        f'{_inst()}/assignments',
        {'partitions': [{'topic': 't1', 'partition': 0}]},
    )
    with patch.object(_consumer(service), 'poll', return_value={}) as poll:
        response = client.get(f'{_inst()}/records{query}', headers=HEADERS)
    assert response.status_code == 200
    assert response.json() == []
    kwargs = poll.call_args.kwargs
    assert wait_ms - 500 < kwargs['timeout_ms'] <= wait_ms
    assert kwargs['max_records'] == max_records


async def test_requests_waiting_for_an_instance_hold_no_thread(
    service: DiasporaService,
) -> None:
    """Requests queued on a busy instance wait on the event loop."""
    _instances(service, subjects=2)
    busy = service.consumer_service._consumers[
        (_subject(0), NAMESPACE, GROUP, 'a')
    ]
    busy.lock.acquire()
    threads = anyio.to_thread.current_default_thread_limiter()
    async with _client(service) as client:
        waiting = [
            asyncio.create_task(
                client.get(
                    f'{BASE}/{GROUP}/instances/a/assignments',
                    headers=_headers(_subject(0)),
                ),
            )
            for _ in range(60)
        ]
        await asyncio.sleep(0.5)
        assert threads.borrowed_tokens == 0
        started = time.monotonic()
        other = await client.get(
            f'{BASE}/{GROUP}/instances/a/assignments',
            headers=_headers(_subject(1)),
        )
        assert time.monotonic() - started < 1
        assert other.status_code == 200
        busy.lock.release()
        replies = await asyncio.gather(*waiting)
    assert [r.status_code for r in replies] == [200] * 60


async def test_instance_lock_wakes_a_loop_waiter_from_a_thread() -> None:
    lock = InstanceLock()
    assert lock.acquire(blocking=False)
    threading.Timer(0.05, lock.release).start()
    started = time.monotonic()
    assert await lock.acquire_async(5)
    assert time.monotonic() - started < 1
    assert not await lock.acquire_async(0.05)
    lock.release()
    assert lock.acquire(timeout=0.1)
    lock.release()


def test_reaper_skips_an_instance_a_request_holds(
    service: DiasporaService,
) -> None:
    consumers = service.consumer_service
    _instances(service, subjects=1)
    key = (_subject(0), NAMESPACE, GROUP, 'a')
    managed = consumers._consumers[key]
    managed.last_access = time.monotonic() - 600
    managed.lock.acquire()
    try:
        started = time.monotonic()
        consumers._reap_once()
        # It does not wait for the lock either
        assert time.monotonic() - started < 1
        assert key in consumers._consumers
    finally:
        managed.lock.release()
    consumers._reap_once()
    assert key not in consumers._consumers


def _fetched(
    *values: bytes | None,
    topic: str = 't1',
) -> dict[TopicPartition, list[Any]]:
    record = MagicMock
    return {
        _kafka_tp(topic): [
            record(key=None, value=v, offset=i) for i, v in enumerate(values)
        ],
    }


@pytest.mark.parametrize('fmt', ('binary', 'json'))
@pytest.mark.parametrize(
    'value',
    (
        b'plain text',
        b'\x00\x01\x1f quotes " and \\ \n\t\r\x08\x0c',
        'h\u00e9llo \u2028 \U0001f600 \u0000'.encode(),
        b'\x80\xff not utf-8',
        b'',
        None,
        bytes(range(256)) * 300,
        ('\u00e9' * 40000 + '\U0001f600').encode(),
    ),
)
def test_reply_is_what_jsonresponse_writes(
    service: DiasporaService,
    client: TestClient,
    fmt: str,
    value: bytes | None,
) -> None:
    """The reply's bytes and size match JSONResponse for the same records."""
    _create(client, format=fmt)
    managed = service.consumer_service._consumers[
        (SUBJECT, NAMESPACE, GROUP, 'i1')
    ]
    reply = service.consumer_service._reply(
        NAMESPACE,
        managed,
        _fetched(value, b'second', topic='t1'),
    )
    expected = JSONResponse([r.as_dict() for r in reply.records]).body
    assert reply.body() == expected
    assert reply.size == len(expected)
    assert b''.join(reply.chunks()) == expected


def test_reply_stops_at_its_cap_and_rewinds(
    service: DiasporaService,
    client: TestClient,
) -> None:
    """Past REPLY_MAX_BYTES the records left out are fetched again."""
    _create(client)
    _send(
        client,
        'POST',
        f'{_inst()}/assignments',
        {
            'partitions': [
                {'topic': 't1', 'partition': 0},
                {'topic': 't2', 'partition': 0},
            ],
        },
    )
    managed = service.consumer_service._consumers[
        (SUBJECT, NAMESPACE, GROUP, 'i1')
    ]
    # Each record is about 0.7 MiB as base64
    big = b'x' * (512 * 1024)
    raw = {**_fetched(big, big, big, big), **_fetched(big, topic='t2')}
    with patch.object(managed.consumer, 'seek') as seek:
        reply = service.consumer_service._reply(NAMESPACE, managed, raw)
    assert [(r.topic, r.offset) for r in reply.records] == [
        ('t1', 0),
        ('t1', 1),
    ]
    assert reply.size <= REPLY_MAX_BYTES
    assert sorted(c.args for c in seek.call_args_list) == [
        (_kafka_tp('t1'), 2),
        (_kafka_tp('t2'), 0),
    ]


def test_a_large_first_record_is_returned_alone(
    service: DiasporaService,
    client: TestClient,
) -> None:
    """The first record goes out however large, so a fetch always moves on."""
    _create(client, format='json')
    _send(
        client,
        'POST',
        f'{_inst()}/assignments',
        {'partitions': [{'topic': 't1', 'partition': 0}]},
    )
    # Control characters take six bytes each as JSON
    value = b'\x00' * (1024 * 1024)
    raw = _fetched(value, b'next')
    with (
        patch.object(_consumer(service), 'poll', return_value=raw),
        patch.object(_consumer(service), 'seek') as seek,
    ):
        response = client.get(f'{_inst()}/records', headers=HEADERS)
    assert response.status_code == 200
    # Written a piece at a time, so it has no length
    assert 'content-length' not in response.headers
    assert response.json() == [
        {
            'topic': 't1',
            'key': None,
            'value': '\x00' * (1024 * 1024),
            'partition': 0,
            'offset': 0,
        },
    ]
    assert len(response.content) > REPLY_MAX_BYTES
    seek.assert_called_once_with(_kafka_tp(), 1)


# ============================================================================
# Commits, through kafka-python's own request builder
# ============================================================================


@pytest.fixture
def kafka38(built: list[dict[str, Any]]) -> Iterator[None]:
    """Consumers that take the broker for Kafka 3.8, as MSK's is."""
    with patch(
        'web_service.consumer_service.KafkaConsumer',
        side_effect=lambda **config: KafkaConsumer(
            **config,
            api_version=(3, 8, 0),
        ),
    ):
        yield


@contextmanager
def _commits(service: DiasporaService) -> Iterator[list[Any]]:
    """Capture the OffsetCommit requests the consumer sends, as encoded.

    Only the network is stubbed: the coordinator is known and ready, and
    the client encodes each request, as BrokerConnection.send does, and
    answers it with success.
    """
    consumer = _consumer(service)
    coordinator, kafka_client = consumer._coordinator, consumer._client
    sent: list[Any] = []

    def send(node_id: int, request: Any, **kwargs: Any) -> Future:
        request.encode()
        sent.append(request)
        topics = [
            (topic, [(p[0], 0) for p in parts])
            for topic, parts in request.topics
        ]
        return Future().success(
            OffsetCommitResponse[request.API_VERSION](0, topics),
        )

    with (
        patch.object(
            coordinator,
            'ensure_coordinator_ready',
            return_value=True,
        ),
        patch.object(coordinator, 'coordinator_unknown', return_value=False),
        patch.object(coordinator, 'coordinator', return_value=0),
        patch.object(kafka_client, 'ready', return_value=True),
        patch.object(kafka_client, 'send', side_effect=send),
        patch.object(kafka_client, 'poll', return_value=[]),
    ):
        yield sent


def test_commit_of_an_offset_commits_the_next(
    service: DiasporaService,
    client: TestClient,
    kafka38: None,
) -> None:
    """As Confluent's REST Proxy does, offset N commits N + 1."""
    _create(client)
    _send(
        client,
        'POST',
        f'{_inst()}/assignments',
        {'partitions': [{'topic': 't1', 'partition': 0}]},
    )
    with _commits(service) as sent:
        response = _send(
            client,
            'POST',
            f'{_inst()}/offsets',
            {'offsets': [{'topic': 't1', 'partition': 0, 'offset': 5}]},
        )
    assert response.status_code == 200
    (request,) = sent
    assert request.API_VERSION == 7
    assert request.topics == [(f'{NAMESPACE}.t1', [(0, 6, -1, '')])]


def test_commit_of_everything_commits_the_positions(
    service: DiasporaService,
    client: TestClient,
    kafka38: None,
) -> None:
    """A commit without offsets commits the next records, not one more."""
    _create(client)
    _send(
        client,
        'POST',
        f'{_inst()}/assignments',
        {'partitions': [{'topic': 't1', 'partition': 0}]},
    )
    _send(
        client,
        'POST',
        f'{_inst()}/positions',
        {'offsets': [{'topic': 't1', 'partition': 0, 'offset': 6}]},
    )
    with _commits(service) as sent:
        response = client.post(f'{_inst()}/offsets', headers=HEADERS)
    assert response.status_code == 200
    (request,) = sent
    assert request.topics == [(f'{NAMESPACE}.t1', [(0, 6, -1, '')])]


def test_committed_offset_is_the_next_record(
    service: DiasporaService,
    client: TestClient,
) -> None:
    """After a commit of offset 5, GET offsets answers 6, as Confluent's."""
    _create(client)
    consumer = _consumer(service)
    with patch.object(
        consumer._coordinator,
        'fetch_committed_offsets',
        return_value={_kafka_tp(): OffsetAndMetadata(6, '', -1)},
    ):
        response = _send(
            client,
            'GET',
            f'{_inst()}/offsets',
            {'partitions': [{'topic': 't1', 'partition': 0}]},
        )
    assert response.status_code == 200
    assert response.json()['offsets'][0]['offset'] == 6


def test_unsupported_version_is_400(client: TestClient) -> None:
    body, status = kafka_failure(UnsupportedVersionError(), 'commit offsets')
    assert (status, body['error_code']) == (400, 40002)
