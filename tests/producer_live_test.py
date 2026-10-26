"""Live test of the produce routes against the real cluster.

Produces through the app to a temporary topic and reads the records back
with a Kafka consumer. The token check is replaced by a pass, since CI has
no user token; the unit tests cover it. The namespace, the topic, DynamoDB
and Kafka are real.
"""

from __future__ import annotations

import base64
import contextlib
import json
import os
import time
from collections.abc import Iterator
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest

from web_service.producer_service import format_timestamp

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
    service.producer_service.close()


@pytest.fixture
def live_topic(live_service: Any) -> Iterator[tuple[str, str, str]]:
    """A user with a temporary topic; deleted with the user afterwards."""
    subject = f'test-subject-{os.urandom(8).hex()}'
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


def _read_back(
    kafka_topic: str,
    offset: int,
    count: int,
) -> list[Any]:
    """Read count records of the topic's partition from offset on."""
    from kafka import KafkaConsumer
    from kafka.structs import TopicPartition

    from web_service.services import MSKTokenProvider

    consumer = KafkaConsumer(
        bootstrap_servers=os.getenv('DEFAULT_SERVERS'),
        security_protocol='SASL_SSL',
        sasl_mechanism='OAUTHBEARER',
        sasl_oauth_token_provider=MSKTokenProvider(
            os.getenv('AWS_ACCOUNT_REGION') or '',
        ),
        enable_auto_commit=False,
    )
    try:
        tp = TopicPartition(kafka_topic, 0)
        consumer.assign([tp])
        consumer.seek(tp, offset)
        records: list[Any] = []
        deadline = time.monotonic() + 60
        while len(records) < count and time.monotonic() < deadline:
            for msgs in consumer.poll(timeout_ms=5000).values():
                records.extend(msgs)
        return records[:count]
    finally:
        consumer.close(autocommit=False, timeout_ms=10_000)


async def test_produce_and_read_back(
    live_service: Any,
    live_topic: tuple[str, str, str],
) -> None:
    """Produce one record and a batch of two, then read all three back."""
    subject, namespace, topic = live_topic
    headers = {'subject': subject, 'authorization': 'Bearer ' + 'a' * 50}
    base = f'/api/v3/{namespace}/topics/{topic}'
    # A recent time, so retention keeps the records while the test reads
    timestamp_ms = int(time.time() * 1000) - 60_000
    timestamp = format_timestamp(timestamp_ms)
    record = {
        'key': {'type': 'STRING', 'data': 'job-42'},
        'value': {'type': 'JSON', 'data': {'step': 7, 'name': 'é'}},
        'headers': [
            {
                'name': 'trace',
                'value': base64.b64encode(b'\x00\x01').decode(),
            },
        ],
        'timestamp': timestamp,
    }
    batch = {
        'entries': [
            {
                'id': 'a',
                'value': {
                    'type': 'BINARY',
                    'data': base64.b64encode(b'\xff\x00').decode(),
                },
            },
            {'id': 'b', 'value': {'type': 'STRING', 'data': 'done'}},
        ],
    }

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=live_service.app),
        base_url='http://test',
    ) as client:
        single = await client.post(
            f'{base}/records',
            json=record,
            headers=headers,
        )
        print(f'  Single: {single.status_code} {single.text}')
        assert single.status_code == 200
        reply = single.json()
        assert reply['topic_name'] == topic
        assert reply['partition_id'] == 0
        assert reply['timestamp'] == timestamp

        multi = await client.post(
            f'{base}/records:batch',
            json=batch,
            headers=headers,
        )
        print(f'  Batch: {multi.status_code} {multi.text}')
        assert multi.status_code == 207
        batch_reply = multi.json()
        assert batch_reply['failures'] == []
        assert [s['id'] for s in batch_reply['successes']] == ['a', 'b']
        offsets = [s['offset'] for s in batch_reply['successes']]
        assert offsets == [reply['offset'] + 1, reply['offset'] + 2]

    records = _read_back(f'{namespace}.{topic}', reply['offset'], 3)
    print(f'  Read back {len(records)} records')
    assert len(records) == 3
    first, second, third = records
    assert first.offset == reply['offset']
    assert first.key == b'job-42'
    assert json.loads(first.value) == {'step': 7, 'name': 'é'}
    assert first.value == '{"step":7,"name":"é"}'.encode()
    assert list(first.headers) == [('trace', b'\x00\x01')]
    assert first.timestamp == timestamp_ms
    assert (second.key, second.value) == (None, b'\xff\x00')
    assert third.value == b'done'
