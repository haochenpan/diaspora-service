"""Unit tests for the consumer routes' limits, errors and formats.

The consumer instances are real kafka-python KafkaConsumers that never
connect: api_version spares them the version probe, and nothing here
polls them. kafka-python's own state checks and errors therefore run as
they do in the service. Nothing here uses the network.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from types import SimpleNamespace
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
from web_service.consumer_records import CHUNK_BYTES
from web_service.consumer_records import EncodedField
from web_service.consumer_records import EncodedRecord
from web_service.consumer_records import FetchReply
from web_service.consumer_records import PIECE_BYTES
from web_service.consumer_routes import MAX_BODY_BYTES
from web_service.consumer_routes import negotiate_records
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
# The v2 routes take the caller's default namespace
V2 = '/api/v2/consumers'
V2_NAMESPACE = 'ns-426614174000'
JSON_RECORDS = 'application/vnd.kafka.json.v2+json'
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

    The token check passes, and the subject owns NAMESPACE and its
    default namespace, V2_NAMESPACE, which hold TOPICS. The reaper thread
    never starts.
    """
    auth = MagicMock()
    auth.validate_access_token.return_value = None
    web_service = MagicMock()
    ns_db = web_service.namespace_service.dynamodb
    ns_db.get_user_namespaces.return_value = [NAMESPACE, V2_NAMESPACE]
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


async def test_shutdown_closes_consumers_and_producer_at_once(
    service: DiasporaService,
) -> None:
    """Neither close waits for the other, so the whole is the longer one."""
    both = threading.Barrier(2, timeout=5)
    with (
        patch.object(
            service.consumer_service,
            'shutdown_all',
            side_effect=both.wait,
        ),
        patch.object(
            service.producer_service,
            'close',
            side_effect=both.wait,
        ),
    ):
        async with service._lifespan(service.app):
            pass
    assert not both.broken


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


def _one_record() -> FetchReply:
    """A reply of one small record, which ends a fetch at its first poll."""
    return FetchReply(
        [
            EncodedRecord(
                topic='t1',
                key=EncodedField.of(None, 'binary'),
                value=EncodedField.of(b'x', 'binary'),
                partition=0,
                offset=0,
            ),
        ],
    )


@pytest.fixture
def long_slices(monkeypatch: pytest.MonkeyPatch) -> None:
    """Let a fetch poll for its whole wait in one slice."""
    monkeypatch.setattr(main, 'FETCH_SLICE_MS', 60_000)


class _Blocked:
    """A fetch_records that blocks until released, counting its callers.

    Each call returns one record, so each fetch polls once.
    """

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
        return _one_record(), 200

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


def _client(
    service: DiasporaService,
    *,
    raise_app_exceptions: bool = True,
) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(
            app=service.app,
            raise_app_exceptions=raise_app_exceptions,
        ),
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
                _records(late, 'b'),
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
    long_slices: None,
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
                    _records(late, 'b'),
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
    request.headers = {}
    request.is_disconnected = AsyncMock(return_value=True)
    with patch.object(service.consumer_service, 'fetch_records') as fetch:
        response = await service.fetch_consumer_records(
            GROUP,
            'a',
            request=request,
            namespace=NAMESPACE,
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
    ('query', 'created', 'wait_ms'),
    (
        ('', {}, 5000),
        ('?timeout=-1', {}, 5000),
        ('?timeout=0', {}, 5000),
        ('?timeout=-1', {'consumer.request.timeout.ms': 1200}, 1200),
        ('', {'consumer.request.timeout.ms': '90000'}, 30000),
        # The instance's wait does not cap a fetch's own timeout
        ('?timeout=4000', {'consumer.request.timeout.ms': 1200}, 4000),
        ('?max_bytes=0', {}, 5000),
        ('?max_bytes=-1', {}, 5000),
        ('?max_bytes=7', {}, 5000),
    ),
)
def test_fetch_waits_and_byte_limits(
    service: DiasporaService,
    client: TestClient,
    long_slices: None,
    *,
    query: str,
    created: dict[str, Any],
    wait_ms: int,
) -> None:
    """Values Confluent takes as its defaults do here too, rather than 500."""
    assert _create(client, **created).status_code == 200
    _send(
        client,
        'POST',
        f'{_inst()}/assignments',
        {'partitions': [{'topic': 't1', 'partition': 0}]},
    )
    with patch.object(
        _consumer(service),
        'poll',
        return_value=_fetched(b'x'),
    ) as poll:
        response = client.get(f'{_inst()}/records{query}', headers=HEADERS)
    assert response.status_code == 200
    assert len(response.json()) == 1
    kwargs = poll.call_args.kwargs
    assert wait_ms - 500 < kwargs['timeout_ms'] <= wait_ms
    # max_bytes limits the reply, not the poll
    assert 'max_records' not in kwargs


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
    return {
        _kafka_tp(topic): [
            SimpleNamespace(key=None, value=v, offset=i, headers=[])
            for i, v in enumerate(values)
        ],
    }


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
    value: bytes | None,
) -> None:
    """The reply's bytes and size match JSONResponse for the same records."""
    _create(client, format='binary')
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


def test_a_large_header_is_written_a_piece_at_a_time() -> None:
    """A header value of 8 MiB is never encoded whole."""
    value = bytes(range(256)) * (32 * 1024)
    record = EncodedRecord(
        topic='t1',
        key=EncodedField.of(None, 'binary'),
        value=EncodedField.of(b'v', 'binary'),
        partition=0,
        offset=0,
        headers=(('big', value), ('none', None), ('', b'')),
    )
    reply = FetchReply([record])
    with patch(
        'web_service.consumer_records.base64.b64encode',
        wraps=base64.b64encode,
    ) as encode:
        chunks = list(reply.chunks())
    assert max(len(c) for c in chunks) < CHUNK_BYTES + 128 * 1024
    assert max(len(c.args[0]) for c in encode.call_args_list) <= PIECE_BYTES
    body = b''.join(chunks)
    assert reply.size == len(body)
    assert json.loads(body) == [record.as_dict()]
    assert json.loads(body)[0]['headers'] == [
        {'name': 'big', 'value': base64.b64encode(value).decode()},
        {'name': 'none', 'value': None},
        {'name': '', 'value': ''},
    ]


@pytest.mark.parametrize(
    'value',
    (
        b'{"msg": "h\xc3\xa9llo", "n": [1, 2.5e3, null, true]}',
        b' "quotes \\" and \\u0000" \n',
        b'[{"a": {}}, [], -0]',
        b'12345678901234567890123',
        '"\u2028 \U0001f600"'.encode(),
        None,
    ),
)
def test_a_json_reply_holds_each_value_as_produced(
    service: DiasporaService,
    client: TestClient,
    value: bytes | None,
) -> None:
    """A json instance's reply holds each value's JSON as it was produced."""
    _create(client, format='json')
    managed = service.consumer_service._consumers[
        (SUBJECT, NAMESPACE, GROUP, 'i1')
    ]
    reply = service.consumer_service._reply(
        NAMESPACE,
        managed,
        _fetched(value, b'"second"', topic='t1'),
    )
    body = reply.body()
    if value is not None:
        assert value in body
    assert json.loads(body) == [r.as_dict() for r in reply.records]
    assert [r['value'] for r in json.loads(body)] == [
        None if value is None else json.loads(value),
        'second',
    ]
    assert reply.size == len(body)
    assert b''.join(reply.chunks()) == body


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
    # A JSON string of escaped control characters, six bytes each
    value = b'"' + b'\\u0000' * 360_000 + b'"'
    raw = _fetched(value, b'"next"')
    with (
        patch.object(_consumer(service), 'poll', return_value=raw),
        patch.object(_consumer(service), 'seek') as seek,
    ):
        response = client.get(
            f'{_inst()}/records',
            headers={**HEADERS, 'accept': JSON_RECORDS},
        )
    assert response.status_code == 200
    # Written a piece at a time, so it has no length
    assert 'content-length' not in response.headers
    assert response.json() == [
        {
            'topic': 't1',
            'key': None,
            'value': '\x00' * 360_000,
            'partition': 0,
            'offset': 0,
            'headers': [],
        },
    ]
    assert len(response.content) > REPLY_MAX_BYTES
    seek.assert_called_once_with(_kafka_tp(), 1)


# ============================================================================
# Fetches: slices, releases and records the client never got
# ============================================================================


class _Log:
    """A consumer over in-memory partitions, with kafka-python's positions.

    poll returns records from each partition's position, in the order of
    logs, and moves the positions past them; seek sets a position. A poll
    that finds nothing waits out its timeout, and returns records that
    arrive meanwhile.
    """

    def __init__(self, logs: dict[TopicPartition, list[bytes]]) -> None:
        self.logs = logs
        self.pos = dict.fromkeys(logs, 0)
        self.polls: list[int] = []

    def poll(
        self,
        timeout_ms: int = 0,
        max_records: int | None = None,
    ) -> dict[TopicPartition, list[Any]]:
        self.polls.append(timeout_ms)
        deadline = time.monotonic() + timeout_ms / 1000
        while True:
            taken = self._take(max_records or 500)
            if taken or time.monotonic() >= deadline:
                return taken
            time.sleep(0.01)

    def _take(self, most: int) -> dict[TopicPartition, list[Any]]:
        taken: dict[TopicPartition, list[Any]] = {}
        for tp, log in self.logs.items():
            start = self.pos[tp]
            values = log[start : start + most]
            if values:
                taken[tp] = [
                    SimpleNamespace(
                        key=None,
                        value=v,
                        offset=start + i,
                        headers=[],
                    )
                    for i, v in enumerate(values)
                ]
                self.pos[tp] += len(values)
                most -= len(values)
            if most <= 0:
                break
        return taken

    def seek(self, tp: TopicPartition, offset: int) -> None:
        self.pos[tp] = offset

    def assignment(self) -> set[TopicPartition]:
        return set(self.logs)

    def close(self, **kwargs: Any) -> None:
        pass


def _log(
    service: DiasporaService,
    name: str,
    *values: bytes,
    subject: str = SUBJECT,
) -> _Log:
    """Make an instance's consumer a _Log of partition t1, holding values."""
    key = (subject, NAMESPACE, GROUP, name)
    consumers = service.consumer_service
    if key not in consumers._consumers:
        _, status = consumers.create_consumer(
            subject,
            NAMESPACE,
            GROUP,
            name=name,
        )
        assert status == 200
    managed = consumers._consumers[key]
    managed.consumer.close(autocommit=False)
    log = _Log({_kafka_tp(): list(values)})
    managed.consumer = log
    return log


def _assert_released(service: DiasporaService, name: str = 'i1') -> None:
    """The instance, the subject's count and every slot are free."""
    managed = service.consumer_service._consumers[
        (SUBJECT, NAMESPACE, GROUP, name)
    ]
    assert not managed.lock.locked()
    assert service.fetching == {}
    assert service.fetch_slots.borrowed_tokens == 0


def _connected() -> MagicMock:
    request = MagicMock()
    request.headers = {}
    request.is_disconnected = AsyncMock(return_value=False)
    return request


async def _fetch(
    service: DiasporaService,
    request: MagicMock,
    name: str = 'i1',
) -> Any:
    """Call the fetch route as FastAPI would, returning its response."""
    return await service.fetch_consumer_records(
        GROUP,
        name,
        request=request,
        namespace=NAMESPACE,
        subject=SUBJECT,
        token=HEADERS['authorization'],
        timeout=1000,
        max_bytes=None,
    )


async def test_a_failed_encode_releases_the_fetch(
    service: DiasporaService,
) -> None:
    """A reply that fails to encode frees the instance and the slot."""
    log = _log(service, 'i1', b'r0', b'r1')
    async with _client(service, raise_app_exceptions=False) as client:
        with patch.object(
            FetchReply,
            'body',
            side_effect=MemoryError('encoding the reply'),
        ):
            failed = await client.get(_inst() + '/records', headers=HEADERS)
        assert failed.status_code == 500
        assert failed.json() == {
            'error_code': 500,
            'message': 'Internal server error',
        }
        _assert_released(service)
        # The records it polled were never sent, so they come again
        assert log.pos[_kafka_tp()] == 0
        again = await client.get(_inst() + '/records', headers=HEADERS)
        assert [r['offset'] for r in again.json()] == [0, 1]
        deleted = await client.delete(_inst(), headers=HEADERS)
    assert deleted.status_code == 204


async def test_quiet_long_polls_leave_slots_for_fetches_with_records(
    service: DiasporaService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two users' quiet long polls on 2 instances each hold no slot idle."""
    monkeypatch.setattr(main, 'FETCH_SLICE_MS', 200)
    for i in range(2):
        for name in ('a', 'b'):
            _log(service, name, subject=_subject(i))
    _log(service, 'a', b'r0', b'r1', b'r2', subject=_subject(2))
    async with _client(service) as client:

        async def get(i: int, name: str, timeout: int) -> tuple[Any, float]:
            started = time.monotonic()
            response = await client.get(
                _records(_subject(i), name),
                params={'timeout': timeout},
                headers=_headers(_subject(i)),
            )
            return response, time.monotonic() - started

        polls = [
            asyncio.create_task(get(i, name, 2000))
            for i in range(2)
            for name in ('a', 'b')
        ]
        await asyncio.sleep(0.3)
        ready, took = await get(2, 'a', 1000)
        assert ready.status_code == 200
        assert len(ready.json()) == 3
        # About one slice: it waits at most for a long poll's slice to end
        assert took < 0.6
        quiet = await asyncio.gather(*polls)
    for response, waited in quiet:
        assert response.status_code == 200
        assert response.json() == []
        assert 1.8 < waited < 2.6


async def test_a_long_poll_returns_records_that_arrive_mid_wait(
    service: DiasporaService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(main, 'FETCH_SLICE_MS', 200)
    log = _log(service, 'i1')
    async with _client(service) as client:
        started = time.monotonic()
        polling = asyncio.create_task(
            client.get(
                _inst() + '/records',
                params={'timeout': 3000},
                headers=HEADERS,
            ),
        )
        await asyncio.sleep(0.5)
        log.logs[_kafka_tp()].extend([b'late-0', b'late-1'])
        response = await polling
        took = time.monotonic() - started
    assert [r['offset'] for r in response.json()] == [0, 1]
    assert took < 1.2
    # Each slice polled for at most a slice
    assert max(log.polls) <= 200


async def test_records_polled_for_a_client_that_left_are_fetched_again(
    service: DiasporaService,
) -> None:
    """A client that leaves while its fetch polls loses no records."""
    log = _log(service, 'i1', b'r0', b'r1')
    request = MagicMock()
    request.headers = {}
    # Connected before the poll, gone after it
    request.is_disconnected = AsyncMock(side_effect=[False, True])
    response = await _fetch(service, request)
    assert response.status_code == 400
    assert log.polls
    assert log.pos[_kafka_tp()] == 0
    _assert_released(service)
    async with _client(service) as client:
        again = await client.get(_inst() + '/records', headers=HEADERS)
    assert [r['offset'] for r in again.json()] == [0, 1]
    assert log.pos[_kafka_tp()] == 2


class _SlowLog:
    """A consumer whose poll takes records at once and returns them late.

    Like kafka-python's, its positions move when the poll takes the
    records, before poll returns; commit records the positions.
    """

    def __init__(self, count: int) -> None:
        self.pos = 0
        self.count = count
        self.committed: list[int] = []
        self.taken = threading.Event()
        self.release = threading.Event()

    def poll(self, timeout_ms: int) -> dict[TopicPartition, list[Any]]:
        start, self.pos = self.pos, self.count
        self.taken.set()
        self.release.wait(5)
        return {
            _kafka_tp(): [
                SimpleNamespace(key=None, value=b'r', offset=i, headers=[])
                for i in range(start, self.count)
            ],
        }

    def seek(self, tp: TopicPartition, offset: int) -> None:
        self.pos = offset

    def commit(self, timeout_ms: int) -> None:
        self.committed.append(self.pos)

    def close(self, autocommit: bool, timeout_ms: int) -> None:
        pass


async def test_shutdown_commits_no_records_of_a_cancelled_fetch(
    service: DiasporaService,
) -> None:
    """A fetch cancelled while it polls commits nothing it did not send.

    uvicorn cancels requests still open late in shutdown, then runs the
    service's shutdown at once, while the cancelled fetch's poll still
    runs in its thread.
    """
    consumers = service.consumer_service
    _, status = consumers.create_consumer(
        SUBJECT,
        NAMESPACE,
        GROUP,
        name='i1',
        auto_commit_enable='true',
    )
    assert status == 200
    managed = consumers._consumers[(SUBJECT, NAMESPACE, GROUP, 'i1')]
    managed.consumer.close(autocommit=False, timeout_ms=0)
    log = _SlowLog(3)
    managed.consumer = log
    fetching = asyncio.create_task(_fetch(service, _connected()))
    assert await asyncio.to_thread(log.taken.wait, 5)
    fetching.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await fetching
    # The poll still runs; shutdown starts, and the poll ends after
    closing = asyncio.create_task(asyncio.to_thread(consumers.shutdown_all))
    await asyncio.sleep(0.2)
    log.release.set()
    await closing
    assert log.committed == [0]


async def test_a_streamed_reply_the_client_left_is_fetched_again(
    service: DiasporaService,
) -> None:
    """The client leaves after the first chunk of a reply over the cap."""
    # About 2.7 MiB as base64, so it is written a piece at a time
    log = _log(service, 'i1', b'x' * (2 * 1024 * 1024), b'small')
    response = await _fetch(service, _connected())
    sent: list[dict[str, Any]] = []
    first_chunk = asyncio.Event()

    async def receive() -> dict[str, Any]:
        await first_chunk.wait()
        return {'type': 'http.disconnect'}

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)
        if message['type'] == 'http.response.body':
            first_chunk.set()
            await asyncio.sleep(0)

    await response({'type': 'http'}, receive, send)
    # The reply never ended
    assert all(m.get('more_body', True) for m in sent[1:])
    assert log.pos[_kafka_tp()] == 0
    _assert_released(service)
    # A reply handed whole to the server counts as received
    async with _client(service) as client:
        again = await client.get(_inst() + '/records', headers=HEADERS)
    assert [r['offset'] for r in again.json()] == [0]
    assert log.pos[_kafka_tp()] == 1


async def test_a_reply_past_the_send_limit_is_skipped(
    service: DiasporaService,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A client too slow for a reply would get it again on every retry."""
    monkeypatch.setattr(main, 'FETCH_SEND_TIMEOUT_S', 0.2)
    log = _log(service, 'i1', b'r0', b'r1')
    response = await _fetch(service, _connected())
    stalled = asyncio.Event()

    async def receive() -> dict[str, Any]:
        await stalled.wait()
        return {'type': 'http.disconnect'}

    async def send(message: dict[str, Any]) -> None:
        # The client reads nothing, so the send never finishes
        await stalled.wait()

    started = time.monotonic()
    with caplog.at_level('WARNING', logger='web_service.main'):
        await response({'type': 'http'}, receive, send)
    assert time.monotonic() - started < 1
    # The next fetch starts after the reply
    assert log.pos[_kafka_tp()] == 2
    _assert_released(service)
    (warning,) = [r for r in caplog.records if r.name == 'web_service.main']
    assert warning.levelname == 'WARNING'
    message = warning.getMessage()
    assert f'{NAMESPACE}/{GROUP}/i1' in message
    assert f'{NAMESPACE}.t1[0] offset 0' in message
    assert 'r0' not in message


async def test_a_reply_out_of_time_before_its_send_is_fetched_again(
    service: DiasporaService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A reply whose time runs out while it is encoded was never sent."""
    monkeypatch.setattr(main, 'FETCH_SEND_TIMEOUT_S', 0.2)
    log = _log(service, 'i1', b'r0', b'r1')
    encode = FetchReply.body

    def slow_body(reply: FetchReply) -> bytes:
        time.sleep(0.5)
        return encode(reply)

    response = await _fetch(service, _connected())
    sent: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        await asyncio.Event().wait()
        return {'type': 'http.disconnect'}

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    with patch.object(FetchReply, 'body', slow_body):
        await response({'type': 'http'}, receive, send)
    assert sent[0]['status'] == 500
    assert json.loads(sent[1]['body']) == {
        'error_code': 500,
        'message': 'Internal server error',
    }
    # Nothing of the reply was sent, so its records come again
    assert log.pos[_kafka_tp()] == 0
    _assert_released(service)


async def test_fetch_slots_go_to_the_longest_waiter() -> None:
    """A slot given back and asked for again at once waits its turn."""
    slots = main.FetchSlots(2)
    assert await slots.take(0)
    assert await slots.take(0)
    waiting = asyncio.create_task(slots.take(5))
    await asyncio.sleep(0)
    slots.give_back()
    # The fetch that gave it back asks again before the waiter has run
    again = asyncio.create_task(slots.take(0.2))
    assert await waiting
    assert not await again
    assert slots.borrowed_tokens == 2
    # A wait that times out leaves no trace
    assert not await slots.take(0.05)
    slots.give_back()
    slots.give_back()
    assert slots.borrowed_tokens == 0
    assert await slots.take(0)


async def test_quiet_long_polls_never_hold_more_slots_than_there_are(
    service: DiasporaService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Long polls give their slots back and take them again many times.

    One poll more than there are slots, so each slot given back has one
    fetch waiting for it, which the fetch that gave it back must not pass.
    """
    monkeypatch.setattr(main, 'FETCH_SLICE_MS', 50)
    count = main.FETCH_MAX_CONCURRENT + 1
    fetches = [(_subject(i // 2), 'ab'[i % 2]) for i in range(count)]
    for subject, name in fetches:
        _log(service, name, subject=subject)
    most = 0
    done = asyncio.Event()

    async def watch() -> None:
        nonlocal most
        while not done.is_set():
            most = max(most, service.fetch_slots.borrowed_tokens)
            await asyncio.sleep(0)

    async with _client(service) as client:
        watcher = asyncio.create_task(watch())
        polls = await asyncio.gather(
            *(
                client.get(
                    _records(subject, name),
                    params={'timeout': 1000},
                    headers=_headers(subject),
                )
                for subject, name in fetches
            ),
        )
        done.set()
        await watcher
    assert [p.status_code for p in polls] == [200] * count
    assert most == main.FETCH_MAX_CONCURRENT
    assert service.fetch_slots.borrowed_tokens == 0


def test_partitions_take_turns_to_start_a_reply(
    service: DiasporaService,
) -> None:
    """A partition whose records fill each reply cannot keep others out."""
    consumers = service.consumer_service
    log = _log(service, 'i1')
    t1, t2 = _kafka_tp('t1'), _kafka_tp('t2')
    # Each t1 record is about 1.3 MiB as base64, so a reply holds one
    log.logs = {t1: [b'a' * (1024 * 1024)] * 20, t2: [b'b'] * 5}
    log.pos = {t1: 0, t2: 0}
    got: dict[str, list[int]] = {'t1': [], 't2': []}
    # A poll that finds nothing takes no partition's turn
    managed = consumers._consumers[(SUBJECT, NAMESPACE, GROUP, 'i1')]
    consumers._reply(NAMESPACE, managed, {})
    assert managed.fetches == 0
    for _ in range(2):
        reply, status = consumers.fetch_records(
            SUBJECT,
            NAMESPACE,
            GROUP,
            'i1',
            timeout_ms=0,
        )
        assert status == 200
        assert isinstance(reply, FetchReply)
        for record in reply.records:
            got[record.topic].append(record.offset)
    assert got == {'t1': [0, 1], 't2': [0, 1, 2, 3, 4]}
    assert log.pos == {t1: 2, t2: 5}


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
    # As in Confluent's REST Proxy, an empty list of offsets
    assert (response.status_code, response.json()) == (200, [])
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


# ============================================================================
# The v2 paths and their deprecated aliases
# ============================================================================


def _v2_inst(name: str = 'i1') -> str:
    return f'{V2}/{GROUP}/instances/{name}'


def _alias_inst(name: str = 'i1') -> str:
    return f'/api/v3/{V2_NAMESPACE}/consumers/{GROUP}/instances/{name}'


def test_a_v2_create_returns_the_v2_url(client: TestClient) -> None:
    response = client.post(
        f'{V2}/{GROUP}',
        json={'name': 'i1', 'format': 'binary'},
        headers=HEADERS,
    )
    assert response.status_code == 200
    assert response.headers['content-type'] == 'application/vnd.kafka.v2+json'
    assert 'deprecation' not in response.headers
    assert response.json() == {
        'instance_id': 'i1',
        'base_uri': f'http://testserver{_v2_inst()}',
    }


def test_an_alias_create_returns_the_alias_url(client: TestClient) -> None:
    response = client.post(
        f'/api/v3/{V2_NAMESPACE}/consumers/{GROUP}',
        json={'name': 'i1'},
        headers=HEADERS,
    )
    assert response.status_code == 200
    assert response.headers['deprecation'] == '@1792972800'
    assert response.json()['base_uri'] == f'http://testserver{_alias_inst()}'


def test_an_instance_is_the_same_through_either_prefix(
    service: DiasporaService,
    client: TestClient,
) -> None:
    """An instance made through v2 is used through the alias, and back."""
    created = client.post(
        f'{V2}/{GROUP}',
        json={'name': 'i1'},
        headers=HEADERS,
    )
    assert created.status_code == 200
    subscribed = _send(
        client,
        'POST',
        f'{_alias_inst()}/subscription',
        {'topics': ['t1']},
    )
    assert subscribed.status_code == 204
    assert subscribed.headers['deprecation'] == '@1792972800'
    got = client.get(f'{_v2_inst()}/subscription', headers=HEADERS)
    assert got.json() == {'topics': ['t1']}
    assert list(service.consumer_service._consumers) == [
        (SUBJECT, V2_NAMESPACE, GROUP, 'i1'),
    ]
    assert client.delete(_alias_inst(), headers=HEADERS).status_code == 204
    gone = client.get(f'{_v2_inst()}/subscription', headers=HEADERS)
    assert (gone.status_code, gone.json()['error_code']) == (404, 40403)


def test_v2_takes_the_callers_default_namespace(
    service: DiasporaService,
    client: TestClient,
) -> None:
    """A subject without its default namespace cannot use the v2 paths."""
    ns_db = service.web_service.namespace_service.dynamodb
    ns_db.get_user_namespaces.return_value = [NAMESPACE]
    response = client.post(
        f'{V2}/{GROUP}',
        json={'name': 'i1'},
        headers=HEADERS,
    )
    assert response.status_code == 404
    assert response.json() == {
        'error_code': 40401,
        'message': f'Namespace {V2_NAMESPACE} not found for user',
    }
    assert service.consumer_service._consumers == {}


def test_openapi_marks_the_aliases_deprecated(client: TestClient) -> None:
    paths = client.get('/openapi.json').json()['paths']
    v2 = {p: ops for p, ops in paths.items() if p.startswith(V2)}
    aliases = {
        p: ops
        for p, ops in paths.items()
        if p.startswith('/api/v3/{namespace}/consumers')
    }
    assert sum(len(ops) for ops in v2.values()) == 13
    assert sum(len(ops) for ops in aliases.values()) == 13
    for ops in v2.values():
        assert not any(op.get('deprecated') for op in ops.values())
    for ops in aliases.values():
        assert all(op.get('deprecated') for op in ops.values())


@pytest.mark.parametrize(
    ('headers', 'base'),
    (
        ({'x-forwarded-proto': 'https'}, 'https://testserver'),
        (
            {
                'x-forwarded-proto': 'https, http',
                'x-forwarded-host': 'api.example.org',
            },
            'https://api.example.org',
        ),
        (
            {'x-forwarded-host': 'api.example.org:8443'},
            'http://api.example.org:8443',
        ),
        ({'x-forwarded-proto': 'javascript'}, 'http://testserver'),
        ({'x-forwarded-host': 'evil.example/path'}, 'http://testserver'),
        ({'x-forwarded-host': 'user@evil.example'}, 'http://testserver'),
        ({'x-forwarded-host': ''}, 'http://testserver'),
        ({'host': 'service.example'}, 'http://service.example'),
    ),
)
def test_base_uri_honours_valid_forwarded_headers(
    client: TestClient,
    headers: dict[str, str],
    base: str,
) -> None:
    response = client.post(
        f'{V2}/{GROUP}',
        json={'name': 'i1'},
        headers={**HEADERS, **headers},
    )
    assert response.json()['base_uri'] == f'{base}{_v2_inst()}'


def test_a_create_without_a_body_takes_the_defaults(
    service: DiasporaService,
    client: TestClient,
    built: list[dict[str, Any]],
) -> None:
    response = client.post(f'{V2}/{GROUP}', headers=HEADERS)
    assert response.status_code == 200
    (managed,) = service.consumer_service._consumers.values()
    assert response.json()['instance_id'] == managed.instance_id
    assert managed.format_type == 'binary'
    assert built[0]['auto_offset_reset'] == 'latest'


def test_a_create_takes_the_deprecated_id_as_its_name(
    client: TestClient,
) -> None:
    response = client.post(
        f'{V2}/{GROUP}',
        json={'id': 'by-id', 'name': 'by-name'},
        headers=HEADERS,
    )
    assert response.json()['instance_id'] == 'by-id'


def test_an_invalid_name_is_refused(client: TestClient) -> None:
    response = client.post(
        f'{V2}/{GROUP}',
        json={'name': 'a/b'},
        headers=HEADERS,
    )
    assert response.status_code == 422
    assert response.json()['error_code'] == 42204


@pytest.mark.parametrize(
    'content_type',
    (
        'application/vnd.kafka.v2+json',
        'application/vnd.kafka.json.v2+json; charset=utf-8',
        'application/json',
    ),
)
def test_requests_of_the_v2_types_are_accepted(
    client: TestClient,
    content_type: str,
) -> None:
    response = client.post(
        f'{V2}/{GROUP}',
        content=json.dumps({'name': 'i1'}),
        headers={**HEADERS, 'content-type': content_type},
    )
    assert response.status_code == 200


@pytest.mark.parametrize(
    'content_type',
    ('text/plain', 'application/x-www-form-urlencoded', 'application/xml'),
)
def test_requests_of_other_types_are_refused_with_415(
    service: DiasporaService,
    client: TestClient,
    content_type: str,
) -> None:
    response = client.post(
        f'{V2}/{GROUP}',
        content=json.dumps({'name': 'i1'}),
        headers={**HEADERS, 'content-type': content_type},
    )
    assert response.status_code == 415
    assert response.headers['content-type'] == 'application/vnd.kafka.v2+json'
    assert response.json() == {
        'error_code': 415,
        'message': 'HTTP 415 Unsupported Media Type',
    }
    assert service.consumer_service._consumers == {}


def test_errors_have_the_v2_type(client: TestClient) -> None:
    response = client.get(f'{_v2_inst("none")}/subscription', headers=HEADERS)
    assert response.status_code == 404
    assert response.headers['content-type'] == 'application/vnd.kafka.v2+json'


@pytest.mark.parametrize(
    ('accept', 'chosen'),
    (
        (None, ('binary', 'application/vnd.kafka.binary.v2+json')),
        ('*/*', ('binary', 'application/vnd.kafka.binary.v2+json')),
        ('application/*', ('binary', 'application/vnd.kafka.binary.v2+json')),
        (
            'application/vnd.kafka.v2+json',
            ('binary', 'application/vnd.kafka.v2+json'),
        ),
        (
            'application/vnd.kafka.json.v2+json',
            ('json', 'application/vnd.kafka.json.v2+json'),
        ),
        (
            (
                'application/vnd.kafka.binary.v2+json;q=0.5, '
                'application/vnd.kafka.json.v2+json'
            ),
            ('json', 'application/vnd.kafka.json.v2+json'),
        ),
        (
            'application/vnd.kafka.json.v2+json, */*',
            ('binary', 'application/vnd.kafka.binary.v2+json'),
        ),
        (
            'application/vnd.kafka.binary.v2+json;q=0, */*',
            ('binary', 'application/vnd.kafka.v2+json'),
        ),
        (
            'application/vnd.kafka.avro.v2+json',
            ('avro', 'application/vnd.kafka.avro.v2+json'),
        ),
        ('application/json', None),
        ('text/html', None),
        ('application/vnd.kafka.json.v2+json;q=0', None),
    ),
)
def test_records_negotiate_as_jersey_does(
    accept: str | None,
    chosen: tuple[str, str] | None,
) -> None:
    assert negotiate_records(accept) == chosen


def _assigned_v2(client: TestClient, fmt: str, *partitions: str) -> None:
    created = client.post(
        f'{V2}/{GROUP}',
        json={'name': 'i1', 'format': fmt},
        headers=HEADERS,
    )
    assert created.status_code == 200
    _send(
        client,
        'POST',
        f'{_v2_inst()}/assignments',
        {'partitions': [{'topic': t, 'partition': 0} for t in partitions]},
    )


def _v2_fetched(
    *values: bytes | None,
    topic: str = 't1',
    headers: list[tuple[str, bytes | None]] | None = None,
) -> dict[TopicPartition, list[Any]]:
    return {
        TopicPartition(f'{V2_NAMESPACE}.{topic}', 0): [
            SimpleNamespace(key=None, value=v, offset=i, headers=headers or [])
            for i, v in enumerate(values)
        ],
    }


def _v2_consumer(service: DiasporaService) -> KafkaConsumer:
    key = (SUBJECT, V2_NAMESPACE, GROUP, 'i1')
    return service.consumer_service._consumers[key].consumer


@pytest.mark.parametrize(
    ('fmt', 'accept', 'status', 'code'),
    (
        ('json', None, 406, 40601),
        ('json', '*/*', 406, 40601),
        ('binary', JSON_RECORDS, 406, 40601),
        ('json', 'application/json', 406, 406),
        ('binary', 'application/json', 406, 406),
    ),
)
def test_a_fetch_in_another_format_is_refused(
    service: DiasporaService,
    client: TestClient,
    *,
    fmt: str,
    accept: str | None,
    status: int,
    code: int,
) -> None:
    _assigned_v2(client, fmt, 't1')
    with patch.object(_v2_consumer(service), 'poll') as poll:
        response = client.get(
            f'{_v2_inst()}/records',
            headers=HEADERS
            if accept is None
            else {**HEADERS, 'accept': accept},
        )
    poll.assert_not_called()
    assert response.status_code == status
    assert response.json()['error_code'] == code
    # The instance is free again
    again = client.get(f'{_v2_inst()}/subscription', headers=HEADERS)
    assert again.status_code == 200


def test_a_json_instance_returns_text_as_a_string(
    service: DiasporaService,
    client: TestClient,
) -> None:
    """A STRING-typed value is text, not JSON, and comes back as a string."""
    _assigned_v2(client, 'json', 't1')
    raw = _v2_fetched(b'job done', b'"job done"', b'{"a": 1} trailing')
    with patch.object(_v2_consumer(service), 'poll', return_value=raw):
        response = client.get(
            f'{_v2_inst()}/records',
            headers={**HEADERS, 'accept': JSON_RECORDS},
        )
    assert response.status_code == 200
    # A client cannot tell "job done" produced as JSON from text
    assert [r['value'] for r in response.json()] == [
        'job done',
        'job done',
        '{"a": 1} trailing',
    ]


def test_a_json_fetch_has_the_json_type(
    service: DiasporaService,
    client: TestClient,
) -> None:
    _assigned_v2(client, 'json', 't1')
    with patch.object(
        _v2_consumer(service),
        'poll',
        return_value=_v2_fetched(b'{"a": 1}'),
    ):
        response = client.get(
            f'{_v2_inst()}/records',
            headers={**HEADERS, 'accept': JSON_RECORDS},
        )
    assert response.status_code == 200
    assert response.headers['content-type'] == JSON_RECORDS
    assert response.json() == [
        {
            'topic': 't1',
            'key': None,
            'value': {'a': 1},
            'partition': 0,
            'offset': 0,
            'headers': [],
        },
    ]


def test_records_carry_their_headers(
    service: DiasporaService,
    client: TestClient,
) -> None:
    _assigned_v2(client, 'binary', 't1')
    raw = _v2_fetched(b'v', headers=[('trace', b'\x00\x01'), ('empty', None)])
    with patch.object(_v2_consumer(service), 'poll', return_value=raw):
        response = client.get(f'{_v2_inst()}/records', headers=HEADERS)
    assert response.headers['content-type'] == (
        'application/vnd.kafka.binary.v2+json'
    )
    assert response.json()[0]['headers'] == [
        {'name': 'trace', 'value': 'AAE='},
        {'name': 'empty', 'value': None},
    ]


@pytest.mark.parametrize(
    ('max_bytes', 'returned'),
    (
        # As in Confluent's REST Proxy, the keys and values stay under it
        (25, [0, 1]),
        (21, [0, 1]),
        (20, [0]),
        # The first record is returned however large
        (5, [0]),
        (0, [0, 1, 2, 3]),
        (-1, [0, 1, 2, 3]),
    ),
)
def test_max_bytes_counts_the_bytes_of_keys_and_values(
    service: DiasporaService,
    client: TestClient,
    max_bytes: int,
    returned: list[int],
) -> None:
    _assigned_v2(client, 'binary', 't1')
    consumer = _v2_consumer(service)
    with (
        patch.object(
            consumer,
            'poll',
            return_value=_v2_fetched(*[b'x' * 10] * 4),
        ),
        patch.object(consumer, 'seek') as seek,
    ):
        response = client.get(
            f'{_v2_inst()}/records',
            params={'max_bytes': max_bytes},
            headers=HEADERS,
        )
    assert [r['offset'] for r in response.json()] == returned
    if len(returned) < 4:
        seek.assert_called_once_with(
            TopicPartition(f'{V2_NAMESPACE}.t1', 0),
            len(returned),
        )
    else:
        seek.assert_not_called()


def test_a_first_record_that_is_not_utf8_is_408(
    service: DiasporaService,
    client: TestClient,
) -> None:
    _assigned_v2(client, 'json', 't1')
    consumer = _v2_consumer(service)
    with (
        patch.object(
            consumer,
            'poll',
            return_value=_v2_fetched(b'\xff\xfe', b'{}'),
        ),
        patch.object(consumer, 'seek') as seek,
    ):
        response = client.get(
            f'{_v2_inst()}/records',
            headers={**HEADERS, 'accept': JSON_RECORDS},
        )
    assert response.status_code == 408
    assert response.json() == {
        'error_code': 40801,
        'message': (
            'The key or value of the record at offset 0 of topic t1, '
            'partition 0, is not UTF-8 text. A json instance returns only '
            'JSON and UTF-8 text: seek past the record, or read it with a '
            'binary instance.'
        ),
    }
    # The next fetch meets it again rather than losing it
    seek.assert_called_once_with(TopicPartition(f'{V2_NAMESPACE}.t1', 0), 0)


def test_a_record_that_is_not_utf8_ends_only_its_partition(
    service: DiasporaService,
    client: TestClient,
) -> None:
    """Records before it, and other partitions' records, are returned."""
    _assigned_v2(client, 'json', 't1', 't2')
    consumer = _v2_consumer(service)
    raw = {
        **_v2_fetched(b'1', b'\xff', b'3', topic='t1'),
        **_v2_fetched(b'"a"', b'"b"', topic='t2'),
    }
    with (
        patch.object(consumer, 'poll', return_value=raw),
        patch.object(consumer, 'seek') as seek,
    ):
        response = client.get(
            f'{_v2_inst()}/records',
            headers={**HEADERS, 'accept': JSON_RECORDS},
        )
    assert response.status_code == 200
    assert sorted((r['topic'], r['value']) for r in response.json()) == [
        ('t1', 1),
        ('t2', 'a'),
        ('t2', 'b'),
    ]
    seek.assert_called_once_with(TopicPartition(f'{V2_NAMESPACE}.t1', 0), 1)


def test_get_offsets_without_partitions_is_40402(client: TestClient) -> None:
    """As in Confluent's REST Proxy, no partitions is none found."""
    client.post(f'{V2}/{GROUP}', json={'name': 'i1'}, headers=HEADERS)
    for body in (None, 'null'):
        response = client.request(
            'GET',
            f'{_v2_inst()}/offsets',
            content=body,
            headers=HEADERS,
        )
        assert response.status_code == 404
        assert response.json() == {
            'error_code': 40402,
            'message': 'Partition not found.',
        }
