"""Unit tests for the cap on topics per namespace.

These tests use mocks only and need no AWS or Kafka access.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from web_service.main import DiasporaService
from web_service.services import DEFAULT_MAX_TOPICS_PER_NAMESPACE
from web_service.services import DynamoDBService
from web_service.services import KafkaService
from web_service.services import NamespaceService
from web_service.services import parse_max_topics_per_namespace
from web_service.services import WebService

SUBJECT = 'test-subject'
NAMESPACE = 'test-namespace'


def _topics(count: int) -> list[str]:
    return [f'topic-{i:03d}' for i in range(count)]


def _db(topics: list[str]) -> MagicMock:
    """Mock DynamoDBService whose namespace topic set lives in memory."""
    stored = set(topics)
    db = MagicMock(spec=DynamoDBService)
    db.get_user_namespaces.return_value = [NAMESPACE]
    db.get_namespace_topics.side_effect = lambda _ns: sorted(stored)
    db.add_namespace_topic.side_effect = lambda _ns, t: stored.add(t)
    db.remove_namespace_topic.side_effect = lambda _ns, t: stored.discard(t)
    return db


def _web_service(
    topics: list[str],
    cap: int = DEFAULT_MAX_TOPICS_PER_NAMESPACE,
    kafka_status: str = 'success',
) -> tuple[WebService, MagicMock, MagicMock]:
    db = _db(topics)
    kafka = MagicMock(spec=KafkaService)
    kafka.bootstrap_servers = 'localhost:9092'
    kafka.create_topic.return_value = {
        'status': kafka_status,
        'message': 'Kafka result',
    }
    namespace_service = NamespaceService(
        dynamodb_service=db,
        max_topics_per_namespace=cap,
    )
    web_service = WebService(
        iam_service=MagicMock(),
        kafka_service=kafka,
        namespace_service=namespace_service,
    )
    return web_service, db, kafka


def test_create_refused_at_cap() -> None:
    """A new topic in a namespace at the cap fails before Kafka."""
    existing = _topics(50)
    web_service, db, kafka = _web_service(existing)

    result = web_service.create_topic(SUBJECT, NAMESPACE, 'new-topic')

    assert result == {
        'status': 'failure',
        'message': (
            f'Namespace {NAMESPACE} already holds 50 topics, the most '
            'allowed is 50; delete a topic to create another.'
        ),
    }
    kafka.create_topic.assert_not_called()
    db.add_namespace_topic.assert_not_called()
    db.remove_namespace_topic.assert_not_called()
    assert db.get_namespace_topics(NAMESPACE) == existing


def test_create_refused_over_cap_keeps_topics() -> None:
    """A namespace already past the cap keeps its topics but cannot add."""
    existing = _topics(55)
    web_service, db, kafka = _web_service(existing)

    result = web_service.create_topic(SUBJECT, NAMESPACE, 'new-topic')

    assert result['status'] == 'failure'
    assert result['message'] == (
        f'Namespace {NAMESPACE} already holds 55 topics, the most '
        'allowed is 50; delete a topic to create another.'
    )
    kafka.create_topic.assert_not_called()
    assert db.get_namespace_topics(NAMESPACE) == existing


def test_create_refused_at_custom_cap() -> None:
    """The cap given to NamespaceService is the one enforced."""
    web_service, db, kafka = _web_service(_topics(3), cap=3)

    result = web_service.create_topic(SUBJECT, NAMESPACE, 'new-topic')

    assert result['status'] == 'failure'
    assert 'the most allowed is 3' in result['message']
    kafka.create_topic.assert_not_called()
    db.add_namespace_topic.assert_not_called()


def test_create_allows_nth_topic() -> None:
    """The N-th topic is created, with the warning."""
    web_service, db, kafka = _web_service(_topics(49))

    result = web_service.create_topic(SUBJECT, NAMESPACE, 'new-topic')

    assert result['status'] == 'success'
    assert len(result['topics']) == 50
    assert 'new-topic' in result['topics']
    assert result['warning'] == (
        f'Namespace {NAMESPACE} now holds 50 of 50 topics; '
        'delete old topics before you reach 50.'
    )
    kafka.create_topic.assert_called_once_with(NAMESPACE, 'new-topic')


def test_create_warns_from_threshold() -> None:
    """A create that leaves 45 of 50 topics warns in message and field."""
    web_service, _, _ = _web_service(_topics(44))

    result = web_service.create_topic(SUBJECT, NAMESPACE, 'new-topic')

    warning = (
        f'Namespace {NAMESPACE} now holds 45 of 50 topics; '
        'delete old topics before you reach 50.'
    )
    assert set(result) == {'status', 'message', 'topics', 'warning'}
    assert result['status'] == 'success'
    assert result['warning'] == warning
    assert result['message'] == (
        f'Topic new-topic created in {NAMESPACE} {warning}'
    )


def test_create_below_threshold_has_no_warning() -> None:
    """A create that leaves 44 of 50 topics keeps today's reply."""
    web_service, _, _ = _web_service(_topics(43))

    result = web_service.create_topic(SUBJECT, NAMESPACE, 'new-topic')

    assert set(result) == {'status', 'message', 'topics'}
    assert result['status'] == 'success'
    assert result['message'] == f'Topic new-topic created in {NAMESPACE}'
    assert len(result['topics']) == 44


def test_repeated_create_at_cap_succeeds() -> None:
    """Creating a topic the namespace already lists is not counted."""
    existing = _topics(50)
    web_service, db, _ = _web_service(existing)

    result = web_service.create_topic(SUBJECT, NAMESPACE, existing[0])

    assert result == {
        'status': 'success',
        'message': f'Topic {existing[0]} already exists in {NAMESPACE}',
        'topics': existing,
    }
    db.add_namespace_topic.assert_not_called()


def test_kafka_failure_near_cap_rolls_back_without_warning() -> None:
    """A Kafka failure removes the added entry and returns no warning."""
    existing = _topics(44)
    web_service, db, _ = _web_service(existing, kafka_status='failure')

    result = web_service.create_topic(SUBJECT, NAMESPACE, 'new-topic')

    assert result['status'] == 'failure'
    assert 'warning' not in result
    assert result['message'].startswith('Failed to create Kafka topic')
    assert db.get_namespace_topics(NAMESPACE) == existing


@pytest.mark.parametrize(
    ('cap', 'threshold'),
    ((50, 45), (30, 27), (10, 9), (7, 7), (1, 1), (100, 90)),
)
def test_warning_threshold_is_ninety_percent_rounded_up(
    cap: int,
    threshold: int,
) -> None:
    """The warning starts at ceil(0.9 * cap) topics."""
    namespace_service = NamespaceService(
        dynamodb_service=MagicMock(spec=DynamoDBService),
        max_topics_per_namespace=cap,
    )
    assert namespace_service.topic_warning_threshold == threshold


def test_parse_max_topics_default_when_unset(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An unset variable gives the default without a log."""
    with caplog.at_level(logging.WARNING):
        assert parse_max_topics_per_namespace(None) == 50
    assert not caplog.records


@pytest.mark.parametrize(('value', 'expected'), (('1', 1), ('200', 200)))
def test_parse_max_topics_valid(value: str, expected: int) -> None:
    """A whole number of at least 1 is used as given."""
    assert parse_max_topics_per_namespace(value) == expected


@pytest.mark.parametrize('value', ('', 'abc', '2.5', '0', '-3'))
def test_parse_max_topics_invalid_falls_back(
    value: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An invalid or < 1 value falls back to 50 and is logged."""
    with caplog.at_level(logging.WARNING):
        assert parse_max_topics_per_namespace(value) == 50
    assert 'MAX_TOPICS_PER_NAMESPACE' in caplog.text


@pytest.mark.parametrize(
    ('value', 'expected'),
    ((None, 50), ('20', 20), ('nope', 50)),
)
def test_service_reads_max_topics_from_environment(
    value: str | None,
    expected: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """DiasporaService passes MAX_TOPICS_PER_NAMESPACE to NamespaceService."""
    for var in (
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY',
        'SERVER_CLIENT_ID',
        'SERVER_SECRET',
        'AWS_ACCOUNT_ID',
        'AWS_ACCOUNT_REGION',
        'MSK_CLUSTER_NAME',
    ):
        monkeypatch.setenv(var, 'dummy')
    if value is None:
        monkeypatch.delenv('MAX_TOPICS_PER_NAMESPACE', raising=False)
    else:
        monkeypatch.setenv('MAX_TOPICS_PER_NAMESPACE', value)

    with (
        patch('web_service.main.AuthManager'),
        patch('web_service.main.IAMService'),
        patch('web_service.main.KafkaService'),
        patch('web_service.main.DynamoDBService') as db_cls,
        patch('web_service.main.NamespaceService') as namespace_cls,
        patch('web_service.main.WebService'),
        patch('web_service.main.ConsumerService'),
    ):
        DiasporaService()

    namespace_cls.assert_called_once_with(
        dynamodb_service=db_cls.return_value,
        max_topics_per_namespace=expected,
    )
