"""Live tests of the consumer routes against the real cluster.

Each test makes a user with a temporary topic, produces records through
the produce route, and reads them through the v2 consumer routes, which
take the user's default namespace. One test uses the deprecated aliases
under /api/v3/{namespace}/consumers. The token check is replaced by a
pass, since CI has no user token; the unit tests cover it. The namespace,
the topic, the consumer groups, DynamoDB and Kafka are real.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import os
import time
from collections.abc import AsyncIterator
from collections.abc import Iterator
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest

pytestmark = pytest.mark.integration

REQUIRED_VARS = (
    'AWS_ACCESS_KEY_ID',
    'AWS_SECRET_ACCESS_KEY',
    'SERVER_CLIENT_ID',
    'SERVER_SECRET',
    'AWS_ACCOUNT_ID',
    'AWS_ACCOUNT_REGION',
    'MSK_CLUSTER_NAME',
    'DEFAULT_SERVERS',
)
V2 = '/api/v2/consumers'
BINARY = 'application/vnd.kafka.binary.v2+json'
JSON = 'application/vnd.kafka.json.v2+json'


@pytest.fixture
def live_service(monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    """A DiasporaService on the test tables and the real cluster."""
    missing = [var for var in REQUIRED_VARS if not os.getenv(var)]
    if missing:
        pytest.skip(
            f'Missing required environment variables: {", ".join(missing)}',
        )
    monkeypatch.setenv('KEYS_TABLE_NAME', 'test-diaspora-keys-table')
    monkeypatch.setenv('USERS_TABLE_NAME', 'test-diaspora-users-table')
    monkeypatch.setenv(
        'NAMESPACE_TABLE_NAME',
        'test-diaspora-namespace-table',
    )
    from web_service.main import DiasporaService

    service = DiasporaService()
    service.auth = MagicMock()
    service.auth.validate_access_token.return_value = None
    yield service
    service.consumer_service.shutdown_all()
    service.producer_service.close()


@pytest.fixture
def live_topic(
    live_service: Any,
    random_subject: str,
) -> Iterator[tuple[str, str, str]]:
    """A user with a temporary topic; deleted with the user afterwards."""
    subject = random_subject
    web_service = live_service.web_service
    try:
        created = web_service.create_user(subject)
        assert created['status'] == 'success', created
        namespace = created['namespace']
        topic = f'test-topic-{os.urandom(4).hex()}'
        result = web_service.create_topic(subject, namespace, topic)
        assert result['status'] == 'success', result
        print(f'\n  Created topic: {namespace}.{topic}')
        yield subject, namespace, topic
    finally:
        with contextlib.suppress(Exception):
            web_service.delete_user(subject)


@pytest.fixture
async def client(live_service: Any) -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=live_service.app),
        base_url='http://test',
        timeout=60,
    ) as client:
        yield client


def _headers(subject: str) -> dict[str, str]:
    return {'subject': subject, 'authorization': 'Bearer ' + 'a' * 50}


def _group() -> str:
    return f'grp-{os.urandom(4).hex()}'


async def _produce(
    client: httpx.AsyncClient,
    subject: str,
    namespace: str,
    topic: str,
    values: list[dict[str, Any]],
) -> None:
    """Produce one record per value through the produce route."""
    for value in values:
        response = await client.post(
            f'/api/v3/{namespace}/topics/{topic}/records',
            json={
                'value': value,
                'headers': [
                    {
                        'name': 'trace',
                        'value': base64.b64encode(b'\x00\x01').decode(),
                    },
                ],
            },
            headers=_headers(subject),
        )
        assert response.status_code == 200, response.text


async def _create(
    client: httpx.AsyncClient,
    subject: str,
    group: str,
    name: str,
    **settings: str,
) -> httpx.Response:
    response = await client.post(
        f'{V2}/{group}',
        json={'name': name, **settings},
        headers=_headers(subject),
    )
    print(f'  Create: {response.status_code} {response.text}')
    return response


async def _fetch_all(
    client: httpx.AsyncClient,
    subject: str,
    instance: str,
    count: int,
    accept: str = BINARY,
) -> list[dict[str, Any]]:
    """Fetch until count records have come, or a minute has passed."""
    records: list[dict[str, Any]] = []
    deadline = time.monotonic() + 60
    while len(records) < count and time.monotonic() < deadline:
        response = await client.get(
            f'{instance}/records',
            params={'timeout': 5000},
            headers={**_headers(subject), 'accept': accept},
        )
        assert response.status_code == 200, response.text
        assert response.headers['content-type'] == accept
        records.extend(response.json())
    return records


async def test_create_conflict_and_delete(
    client: httpx.AsyncClient,
    live_topic: tuple[str, str, str],
) -> None:
    subject, _, _ = live_topic
    group = _group()
    created = await _create(client, subject, group, 'inst')
    assert created.status_code == 200
    instance = f'{V2}/{group}/instances/inst'
    assert created.json() == {
        'instance_id': 'inst',
        'base_uri': f'http://test{instance}',
    }
    again = await _create(client, subject, group, 'inst')
    assert (again.status_code, again.json()['error_code']) == (409, 40902)
    deleted = await client.delete(instance, headers=_headers(subject))
    assert deleted.status_code == 204
    gone = await client.delete(instance, headers=_headers(subject))
    assert (gone.status_code, gone.json()['error_code']) == (404, 40403)


async def test_subscribe_poll_and_unsubscribe(
    client: httpx.AsyncClient,
    live_topic: tuple[str, str, str],
) -> None:
    """A subscription to an empty topic fetches nothing."""
    subject, _, topic = live_topic
    group = _group()
    instance = f'{V2}/{group}/instances/inst'
    assert (await _create(client, subject, group, 'inst')).status_code == 200
    subscribed = await client.post(
        f'{instance}/subscription',
        json={'topics': [topic]},
        headers=_headers(subject),
    )
    assert subscribed.status_code == 204
    got = await client.get(
        f'{instance}/subscription',
        headers=_headers(subject),
    )
    assert got.json() == {'topics': [topic]}
    # The first fetch joins the group; the second finds the topic empty
    for _ in range(2):
        fetched = await client.get(
            f'{instance}/records',
            params={'timeout': 3000},
            headers=_headers(subject),
        )
        assert fetched.status_code == 200
    assert fetched.json() == []
    unsubscribed = await client.delete(
        f'{instance}/subscription',
        headers=_headers(subject),
    )
    assert unsubscribed.status_code == 204
    got = await client.get(
        f'{instance}/subscription',
        headers=_headers(subject),
    )
    assert got.json() == {'topics': []}


async def test_full_lifecycle_subscribe_fetch_commit(
    client: httpx.AsyncClient,
    live_topic: tuple[str, str, str],
) -> None:
    """Produce three records, fetch them as binary, and commit."""
    subject, namespace, topic = live_topic
    values = [{'type': 'STRING', 'data': f'msg-{i}'} for i in range(3)]
    await _produce(client, subject, namespace, topic, values)
    group = _group()
    instance = f'{V2}/{group}/instances/inst'
    created = await _create(
        client,
        subject,
        group,
        'inst',
        format='binary',
        **{'auto.offset.reset': 'earliest', 'auto.commit.enable': 'false'},
    )
    assert created.status_code == 200
    await client.post(
        f'{instance}/subscription',
        json={'topics': [topic]},
        headers=_headers(subject),
    )
    records = await _fetch_all(client, subject, instance, 3)
    assert [r['offset'] for r in records] == [0, 1, 2]
    assert [base64.b64decode(r['value']) for r in records] == [
        b'msg-0',
        b'msg-1',
        b'msg-2',
    ]
    for record in records:
        assert record['topic'] == topic
        assert record['partition'] == 0
        assert record['key'] is None
        assert record['headers'] == [{'name': 'trace', 'value': 'AAE='}]

    committed = await client.post(
        f'{instance}/offsets',
        headers=_headers(subject),
    )
    assert (committed.status_code, committed.json()) == (200, [])
    offsets = await client.request(
        'GET',
        f'{instance}/offsets',
        json={'partitions': [{'topic': topic, 'partition': 0}]},
        headers=_headers(subject),
    )
    assert offsets.status_code == 200
    assert offsets.json() == {
        'offsets': [
            {'topic': topic, 'partition': 0, 'offset': 3, 'metadata': ''},
        ],
    }
    deleted = await client.delete(instance, headers=_headers(subject))
    assert deleted.status_code == 204


async def test_json_instance_returns_json_values(
    client: httpx.AsyncClient,
    live_topic: tuple[str, str, str],
) -> None:
    subject, namespace, topic = live_topic
    values = [
        {'type': 'JSON', 'data': {'step': 1, 'name': 'é'}},
        {'type': 'JSON', 'data': [1, 2]},
        # Text that is not JSON comes back as a string
        {'type': 'STRING', 'data': 'job done'},
    ]
    await _produce(client, subject, namespace, topic, values)
    group = _group()
    instance = f'{V2}/{group}/instances/inst'
    created = await _create(
        client,
        subject,
        group,
        'inst',
        format='json',
        **{'auto.offset.reset': 'earliest'},
    )
    assert created.status_code == 200
    await client.post(
        f'{instance}/subscription',
        json={'topics': [topic]},
        headers=_headers(subject),
    )
    # Without an Accept, the reply would be binary, which it is not
    refused = await client.get(
        f'{instance}/records',
        headers=_headers(subject),
    )
    assert (refused.status_code, refused.json()['error_code']) == (406, 40601)
    records = await _fetch_all(client, subject, instance, 3, accept=JSON)
    assert [r['value'] for r in records] == [
        {'step': 1, 'name': 'é'},
        [1, 2],
        'job done',
    ]


async def test_assign_and_seek(
    client: httpx.AsyncClient,
    live_topic: tuple[str, str, str],
) -> None:
    """Assign partition 0, then seek to an offset, the end and the start."""
    subject, namespace, topic = live_topic
    values = [{'type': 'STRING', 'data': f'offset-{i}'} for i in range(3)]
    await _produce(client, subject, namespace, topic, values)
    group = _group()
    instance = f'{V2}/{group}/instances/inst'
    assert (await _create(client, subject, group, 'inst')).status_code == 200
    partition = {'topic': topic, 'partition': 0}
    assigned = await client.post(
        f'{instance}/assignments',
        json={'partitions': [partition]},
        headers=_headers(subject),
    )
    assert assigned.status_code == 204
    got = await client.get(
        f'{instance}/assignments',
        headers=_headers(subject),
    )
    assert got.json() == {'partitions': [partition]}

    sought = await client.post(
        f'{instance}/positions',
        json={'offsets': [{**partition, 'offset': 1}]},
        headers=_headers(subject),
    )
    assert sought.status_code == 204
    records = await _fetch_all(client, subject, instance, 2)
    assert [r['offset'] for r in records] == [1, 2]

    at_end = await client.post(
        f'{instance}/positions/end',
        json={'partitions': [partition]},
        headers=_headers(subject),
    )
    assert at_end.status_code == 204
    fetched = await client.get(
        f'{instance}/records',
        params={'timeout': 3000},
        headers=_headers(subject),
    )
    assert fetched.json() == []

    at_start = await client.post(
        f'{instance}/positions/beginning',
        json={'partitions': [partition]},
        headers=_headers(subject),
    )
    assert at_start.status_code == 204
    records = await _fetch_all(client, subject, instance, 3)
    assert [r['offset'] for r in records] == [0, 1, 2]


async def test_delete_commits_an_auto_commit_instance(
    client: httpx.AsyncClient,
    live_topic: tuple[str, str, str],
) -> None:
    """The next instance of the group starts after what was fetched."""
    subject, namespace, topic = live_topic
    values = [{'type': 'STRING', 'data': f'msg-{i}'} for i in range(3)]
    await _produce(client, subject, namespace, topic, values)
    group = _group()
    partition = {'topic': topic, 'partition': 0}
    settings = {'auto.offset.reset': 'earliest', 'auto.commit.enable': 'true'}
    first = f'{V2}/{group}/instances/first'
    created = await _create(client, subject, group, 'first', **settings)
    assert created.status_code == 200
    await client.post(
        f'{first}/assignments',
        json={'partitions': [partition]},
        headers=_headers(subject),
    )
    records = await _fetch_all(client, subject, first, 3)
    assert [r['offset'] for r in records] == [0, 1, 2]
    deleted = await client.delete(first, headers=_headers(subject))
    assert deleted.status_code == 204

    second = f'{V2}/{group}/instances/second'
    created = await _create(client, subject, group, 'second', **settings)
    assert created.status_code == 200
    offsets = await client.request(
        'GET',
        f'{second}/offsets',
        json={'partitions': [partition]},
        headers=_headers(subject),
    )
    assert offsets.json()['offsets'][0]['offset'] == 3


def _cluster_lists(live_service: Any, kafka_topic: str) -> bool:
    """Whether the cluster lists the topic."""
    admin = live_service.web_service.kafka_service._admin_client()
    try:
        return kafka_topic in admin.list_topics()
    finally:
        admin.close()


async def test_unsubscribe_commits_an_auto_commit_instance(
    client: httpx.AsyncClient,
    live_topic: tuple[str, str, str],
) -> None:
    """An unsubscribe commits what the instance fetched, as a close does."""
    subject, namespace, topic = live_topic
    values = [{'type': 'STRING', 'data': f'msg-{i}'} for i in range(3)]
    await _produce(client, subject, namespace, topic, values)
    group = _group()
    settings = {'auto.offset.reset': 'earliest', 'auto.commit.enable': 'true'}
    first = f'{V2}/{group}/instances/first'
    created = await _create(client, subject, group, 'first', **settings)
    assert created.status_code == 200
    subscribed = await client.post(
        f'{first}/subscription',
        json={'topics': [topic]},
        headers=_headers(subject),
    )
    assert subscribed.status_code == 204
    records = await _fetch_all(client, subject, first, 3)
    assert [r['offset'] for r in records] == [0, 1, 2]
    unsubscribed = await client.delete(
        f'{first}/subscription',
        headers=_headers(subject),
    )
    assert unsubscribed.status_code == 204

    second = f'{V2}/{group}/instances/second'
    created = await _create(client, subject, group, 'second', **settings)
    assert created.status_code == 200
    offsets = await client.request(
        'GET',
        f'{second}/offsets',
        json={'partitions': [{'topic': topic, 'partition': 0}]},
        headers=_headers(subject),
    )
    assert offsets.json()['offsets'][0]['offset'] == 3


async def test_a_topic_deleted_under_an_instance_stays_deleted(
    client: httpx.AsyncClient,
    live_service: Any,
    live_topic: tuple[str, str, str],
) -> None:
    """Instances reading a topic the cluster then loses fetch nothing.

    The cluster creates a topic a client asks about if the client lets it;
    neither the subscribed nor the assigned instance may bring it back.
    """
    subject, namespace, topic = live_topic
    kafka_topic = f'{namespace}.{topic}'
    group = _group()
    subscriber = f'{V2}/{group}/instances/subscriber'
    assigned = f'{V2}/{group}-a/instances/assigned'
    assert (
        await _create(client, subject, group, 'subscriber')
    ).status_code == 200
    assert (
        await _create(client, subject, f'{group}-a', 'assigned')
    ).status_code == 200
    subscribed = await client.post(
        f'{subscriber}/subscription',
        json={'topics': [topic]},
        headers=_headers(subject),
    )
    assert subscribed.status_code == 204
    assigning = await client.post(
        f'{assigned}/assignments',
        json={'partitions': [{'topic': topic, 'partition': 0}]},
        headers=_headers(subject),
    )
    assert assigning.status_code == 204

    async def fetch_nothing(instance: str) -> None:
        fetched = await client.get(
            f'{instance}/records',
            params={'timeout': 3000},
            headers=_headers(subject),
        )
        assert fetched.status_code == 200, fetched.text
        assert fetched.json() == []

    for instance in (subscriber, assigned):
        await fetch_nothing(instance)
    kafka_service = live_service.web_service.kafka_service
    deleted = kafka_service.delete_topic(namespace, topic)
    assert deleted['status'] == 'success', deleted
    deadline = time.monotonic() + 60
    while _cluster_lists(live_service, kafka_topic):
        assert time.monotonic() < deadline, 'the topic was not deleted'
        await asyncio.sleep(1)
    for _ in range(3):
        for instance in (subscriber, assigned):
            await fetch_nothing(instance)
    await asyncio.sleep(2)
    assert not _cluster_lists(live_service, kafka_topic)


async def test_the_deprecated_aliases_reach_the_same_instance(
    client: httpx.AsyncClient,
    live_topic: tuple[str, str, str],
) -> None:
    subject, namespace, topic = live_topic
    group = _group()
    alias = f'/api/v3/{namespace}/consumers/{group}/instances/inst'
    created = await client.post(
        f'/api/v3/{namespace}/consumers/{group}',
        json={'name': 'inst'},
        headers=_headers(subject),
    )
    assert created.status_code == 200
    assert created.headers['deprecation'] == '@1792972800'
    assert created.json()['base_uri'] == f'http://test{alias}'
    subscribed = await client.post(
        f'{alias}/subscription',
        json={'topics': [topic]},
        headers=_headers(subject),
    )
    assert subscribed.status_code == 204
    got = await client.get(
        f'{V2}/{group}/instances/inst/subscription',
        headers=_headers(subject),
    )
    assert got.json() == {'topics': [topic]}
    assert 'deprecation' not in got.headers
    deleted = await client.delete(alias, headers=_headers(subject))
    assert deleted.status_code == 204
