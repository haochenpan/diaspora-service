"""Unit and integration tests for ConsumerService."""

from __future__ import annotations

import base64
import contextlib
import json
import logging
import os
import time
import warnings
from typing import Any
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from kafka.structs import OffsetAndMetadata
from kafka.structs import TopicPartition

from web_service.consumer_service import _encode_value
from web_service.consumer_service import ConsumerService
from web_service.consumer_service import ManagedConsumer

# Suppress verbose logging and warnings from external libraries
logging.getLogger('kafka').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
warnings.filterwarnings('ignore', category=DeprecationWarning)
warnings.filterwarnings(
    'ignore',
    message='.*ssl.PROTOCOL_TLS.*',
    category=DeprecationWarning,
)

# ============================================================================
# Test Constants
# ============================================================================

SUBJECT = 'test-user-123'
NAMESPACE = 'ns-test123'
GROUP_NAME = 'my-group'
INSTANCE_ID = 'my-instance'


# ============================================================================
# Unit Test Fixtures
# ============================================================================


@pytest.fixture
def consumer_service() -> ConsumerService:
    """Create a ConsumerService with the reaper thread disabled."""
    with patch('web_service.consumer_service.threading.Thread'):
        svc = ConsumerService(
            bootstrap_servers='localhost:9092',
            region='us-east-1',
        )
    return svc


@pytest.fixture
def consumer_service_no_bootstrap() -> ConsumerService:
    """Create a ConsumerService with bootstrap_servers=None."""
    with patch('web_service.consumer_service.threading.Thread'):
        svc = ConsumerService(
            bootstrap_servers=None,
            region='us-east-1',
        )
    return svc


@pytest.fixture
def mock_kafka_consumer() -> MagicMock:
    """Create a MagicMock mimicking KafkaConsumer."""
    consumer = MagicMock()
    consumer.subscription.return_value = set()
    consumer.assignment.return_value = set()
    consumer.poll.return_value = {}
    consumer.committed.return_value = None
    return consumer


@pytest.fixture
def populated_service(
    consumer_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
) -> ConsumerService:
    """A ConsumerService with one pre-registered consumer."""
    key = (SUBJECT, NAMESPACE, GROUP_NAME, INSTANCE_ID)
    managed = ManagedConsumer(
        consumer=mock_kafka_consumer,
        subject=SUBJECT,
        namespace=NAMESPACE,
        group_name=GROUP_NAME,
        instance_id=INSTANCE_ID,
        format_type='binary',
    )
    consumer_service._consumers[key] = managed
    return consumer_service


@pytest.fixture
def populated_service_json(
    consumer_service: ConsumerService,
) -> ConsumerService:
    """A ConsumerService with one pre-registered consumer (json format)."""
    mock_consumer = MagicMock()
    mock_consumer.subscription.return_value = set()
    mock_consumer.assignment.return_value = set()
    mock_consumer.poll.return_value = {}
    mock_consumer.committed.return_value = None

    key = (SUBJECT, NAMESPACE, GROUP_NAME, INSTANCE_ID)
    managed = ManagedConsumer(
        consumer=mock_consumer,
        subject=SUBJECT,
        namespace=NAMESPACE,
        group_name=GROUP_NAME,
        instance_id=INSTANCE_ID,
        format_type='json',
    )
    consumer_service._consumers[key] = managed
    return consumer_service


# ============================================================================
# Unit Tests - Helper Functions
# ============================================================================


def test_encode_value_binary_format() -> None:
    """Test _encode_value with binary format returns base64."""
    result = _encode_value(b'hello', 'binary')
    assert result == base64.b64encode(b'hello').decode('ascii')


def test_encode_value_json_format_utf8() -> None:
    """Test _encode_value with json format returns UTF-8 string."""
    result = _encode_value(b'{"key": "value"}', 'json')
    assert result == '{"key": "value"}'


def test_encode_value_json_format_non_utf8_fallback() -> None:
    """Test _encode_value with json format falls back to base64."""
    data = b'\x80\x81\x82'
    result = _encode_value(data, 'json')
    assert result == base64.b64encode(data).decode('ascii')


def test_encode_value_none_binary() -> None:
    """Test _encode_value with None and binary format returns None."""
    assert _encode_value(None, 'binary') is None


def test_encode_value_none_json() -> None:
    """Test _encode_value with None and json format returns None."""
    assert _encode_value(None, 'json') is None


# ============================================================================
# Unit Tests - ManagedConsumer
# ============================================================================


def test_managed_consumer_touch() -> None:
    """Test ManagedConsumer.touch updates last_access."""
    mc = ManagedConsumer(
        consumer=MagicMock(),
        subject='u',
        namespace='ns',
        group_name='g',
        instance_id='i',
        format_type='binary',
    )
    old_access = mc.last_access
    time.sleep(0.01)
    mc.touch()
    assert mc.last_access > old_access


def test_managed_consumer_is_expired_true() -> None:
    """Test ManagedConsumer.is_expired returns True for old consumer."""
    mc = ManagedConsumer(
        consumer=MagicMock(),
        subject='u',
        namespace='ns',
        group_name='g',
        instance_id='i',
        format_type='binary',
    )
    # Set last_access far in the past
    mc.last_access = time.monotonic() - 600
    assert mc.is_expired(300_000) is True


def test_managed_consumer_is_expired_false() -> None:
    """Test ManagedConsumer.is_expired returns False for fresh consumer."""
    mc = ManagedConsumer(
        consumer=MagicMock(),
        subject='u',
        namespace='ns',
        group_name='g',
        instance_id='i',
        format_type='binary',
    )
    assert mc.is_expired(300_000) is False


# ============================================================================
# Unit Tests - ConsumerService internal helpers
# ============================================================================


def test_strip_namespace_with_prefix(
    consumer_service: ConsumerService,
) -> None:
    """Test _strip_namespace removes namespace prefix."""
    result = consumer_service._strip_namespace(
        'ns-test123',
        'ns-test123.my-topic',
    )
    assert result == 'my-topic'


def test_strip_namespace_without_prefix(
    consumer_service: ConsumerService,
) -> None:
    """Test _strip_namespace returns unchanged if no prefix."""
    result = consumer_service._strip_namespace(
        'ns-test123',
        'other.my-topic',
    )
    assert result == 'other.my-topic'


def test_kafka_topic_name(
    consumer_service: ConsumerService,
) -> None:
    """Test _kafka_topic_name joins namespace and topic."""
    result = consumer_service._kafka_topic_name('ns-test123', 'my-topic')
    assert result == 'ns-test123.my-topic'


def test_kafka_group_id(
    consumer_service: ConsumerService,
) -> None:
    """Test _kafka_group_id joins namespace and group."""
    result = consumer_service._kafka_group_id('ns-test123', 'my-group')
    assert result == 'ns-test123.my-group'


def test_consumer_key(
    consumer_service: ConsumerService,
) -> None:
    """Test _consumer_key returns correct tuple."""
    result = consumer_service._consumer_key('user', 'ns', 'grp', 'inst')
    assert result == ('user', 'ns', 'grp', 'inst')


# ============================================================================
# Unit Tests - create_consumer
# ============================================================================


def test_create_consumer_success(
    consumer_service: ConsumerService,
) -> None:
    """Test create_consumer success with mocked KafkaConsumer."""
    with patch(
        'web_service.consumer_service.KafkaConsumer',
    ) as mock_kc_class:
        mock_kc_class.return_value = MagicMock()
        body, status = consumer_service.create_consumer(
            subject=SUBJECT,
            namespace=NAMESPACE,
            group_name=GROUP_NAME,
            name='test-inst',
            format_type='binary',
        )
    assert status == 200
    assert body['instance_id'] == 'test-inst'
    assert '/api/v3/' in body['base_uri']
    assert NAMESPACE in body['base_uri']
    assert GROUP_NAME in body['base_uri']


def test_create_consumer_auto_generated_id(
    consumer_service: ConsumerService,
) -> None:
    """Test create_consumer auto-generates instance_id when name=None."""
    with patch(
        'web_service.consumer_service.KafkaConsumer',
    ) as mock_kc_class:
        mock_kc_class.return_value = MagicMock()
        body, status = consumer_service.create_consumer(
            subject=SUBJECT,
            namespace=NAMESPACE,
            group_name=GROUP_NAME,
            name=None,
        )
    assert status == 200
    assert len(body['instance_id']) == 12
    assert body['instance_id'].isalnum()


def test_create_consumer_custom_name(
    consumer_service: ConsumerService,
) -> None:
    """Test create_consumer uses provided name as instance_id."""
    with patch(
        'web_service.consumer_service.KafkaConsumer',
    ) as mock_kc_class:
        mock_kc_class.return_value = MagicMock()
        body, status = consumer_service.create_consumer(
            subject=SUBJECT,
            namespace=NAMESPACE,
            group_name=GROUP_NAME,
            name='my-custom-name',
        )
    assert status == 200
    assert body['instance_id'] == 'my-custom-name'


def test_create_consumer_conflict(
    consumer_service: ConsumerService,
) -> None:
    """Test create_consumer returns 409 for duplicate instance."""
    with patch(
        'web_service.consumer_service.KafkaConsumer',
    ) as mock_kc_class:
        mock_kc_class.return_value = MagicMock()
        _body1, status1 = consumer_service.create_consumer(
            subject=SUBJECT,
            namespace=NAMESPACE,
            group_name=GROUP_NAME,
            name='dup-inst',
        )
        assert status1 == 200

        body2, status2 = consumer_service.create_consumer(
            subject=SUBJECT,
            namespace=NAMESPACE,
            group_name=GROUP_NAME,
            name='dup-inst',
        )
    assert status2 == 409
    assert body2['error_code'] == 40902


def test_create_consumer_no_bootstrap(
    consumer_service_no_bootstrap: ConsumerService,
) -> None:
    """Test create_consumer returns 500 when no bootstrap servers."""
    body, status = consumer_service_no_bootstrap.create_consumer(
        subject=SUBJECT,
        namespace=NAMESPACE,
        group_name=GROUP_NAME,
    )
    assert status == 500
    assert body['error_code'] == 50002
    assert 'not configured' in body['message']


def test_create_consumer_kafka_exception(
    consumer_service: ConsumerService,
) -> None:
    """Test create_consumer handles KafkaConsumer constructor failure."""
    with patch(
        'web_service.consumer_service.KafkaConsumer',
    ) as mock_kc_class:
        mock_kc_class.side_effect = Exception('Connection refused')
        body, status = consumer_service.create_consumer(
            subject=SUBJECT,
            namespace=NAMESPACE,
            group_name=GROUP_NAME,
            name='fail-inst',
        )
    assert status == 500
    assert body['error_code'] == 50002
    assert 'Failed to create consumer' in body['message']


def test_create_consumer_optional_params(
    consumer_service: ConsumerService,
) -> None:
    """Test create_consumer passes optional params to KafkaConsumer."""
    with patch(
        'web_service.consumer_service.KafkaConsumer',
    ) as mock_kc_class:
        mock_kc_class.return_value = MagicMock()
        consumer_service.create_consumer(
            subject=SUBJECT,
            namespace=NAMESPACE,
            group_name=GROUP_NAME,
            name='opt-inst',
            fetch_min_bytes='1024',
            consumer_request_timeout_ms='30000',
        )
    call_kwargs = mock_kc_class.call_args[1]
    assert call_kwargs['fetch_min_bytes'] == 1024
    assert call_kwargs['request_timeout_ms'] == 30000


def test_create_consumer_auto_commit_enable(
    consumer_service: ConsumerService,
) -> None:
    """Test create_consumer sets enable_auto_commit correctly."""
    with patch(
        'web_service.consumer_service.KafkaConsumer',
    ) as mock_kc_class:
        mock_kc_class.return_value = MagicMock()
        consumer_service.create_consumer(
            subject=SUBJECT,
            namespace=NAMESPACE,
            group_name=GROUP_NAME,
            name='ac-inst',
            auto_commit_enable='true',
        )
    call_kwargs = mock_kc_class.call_args[1]
    assert call_kwargs['enable_auto_commit'] is True


def test_create_consumer_base_uri_format(
    consumer_service: ConsumerService,
) -> None:
    """Test create_consumer returns correctly formatted base_uri."""
    with patch(
        'web_service.consumer_service.KafkaConsumer',
    ) as mock_kc_class:
        mock_kc_class.return_value = MagicMock()
        body, status = consumer_service.create_consumer(
            subject=SUBJECT,
            namespace=NAMESPACE,
            group_name=GROUP_NAME,
            name='uri-inst',
        )
    expected = f'/api/v3/{NAMESPACE}/consumers/{GROUP_NAME}/instances/uri-inst'
    assert body['base_uri'] == expected


# ============================================================================
# Unit Tests - delete_consumer
# ============================================================================


def test_delete_consumer_success(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
) -> None:
    """Test delete_consumer removes and closes the consumer."""
    body, status = populated_service.delete_consumer(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
    )
    assert status == 204
    assert body is None
    mock_kafka_consumer.close.assert_called_once_with(autocommit=False)
    # Consumer should be removed from registry
    assert len(populated_service._consumers) == 0


def test_delete_consumer_not_found(
    consumer_service: ConsumerService,
) -> None:
    """Test delete_consumer returns 404 for nonexistent consumer."""
    body, status = consumer_service.delete_consumer(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        'nonexistent',
    )
    assert status == 404
    assert body['error_code'] == 40403


def test_delete_consumer_close_exception(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
) -> None:
    """Test delete_consumer still returns 204 if close() raises."""
    mock_kafka_consumer.close.side_effect = Exception('Close failed')
    body, status = populated_service.delete_consumer(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
    )
    assert status == 204
    assert body is None


# ============================================================================
# Unit Tests - subscribe
# ============================================================================


def test_subscribe_topics_success(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
) -> None:
    """Test subscribe with topics list."""
    body, status = populated_service.subscribe(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
        topics=['t1', 't2'],
    )
    assert status == 204
    assert body is None
    mock_kafka_consumer.subscribe.assert_called_once_with(
        topics=[f'{NAMESPACE}.t1', f'{NAMESPACE}.t2'],
    )


def test_subscribe_pattern_success(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
) -> None:
    """Test subscribe with topic_pattern."""
    body, status = populated_service.subscribe(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
        topic_pattern='test-.*',
    )
    assert status == 204
    assert body is None
    mock_kafka_consumer.subscribe.assert_called_once_with(
        pattern=f'{NAMESPACE}\\.test-.*',
    )


def test_subscribe_no_topics_no_pattern(
    populated_service: ConsumerService,
) -> None:
    """Test subscribe with neither topics nor pattern returns 422."""
    body, status = populated_service.subscribe(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
        topics=None,
        topic_pattern=None,
    )
    assert status == 422
    assert body['error_code'] == 42204


def test_subscribe_consumer_not_found(
    consumer_service: ConsumerService,
) -> None:
    """Test subscribe returns 404 for nonexistent consumer."""
    body, status = consumer_service.subscribe(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        'nonexistent',
        topics=['t1'],
    )
    assert status == 404
    assert body['error_code'] == 40403


def test_subscribe_kafka_exception(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
) -> None:
    """Test subscribe handles KafkaConsumer.subscribe failure."""
    mock_kafka_consumer.subscribe.side_effect = Exception('Subscribe failed')
    body, status = populated_service.subscribe(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
        topics=['t1'],
    )
    assert status == 500
    assert body['error_code'] == 50002


# ============================================================================
# Unit Tests - get_subscription
# ============================================================================


def test_get_subscription_success(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
) -> None:
    """Test get_subscription returns stripped, sorted topics."""
    mock_kafka_consumer.subscription.return_value = {
        f'{NAMESPACE}.topicB',
        f'{NAMESPACE}.topicA',
    }
    body, status = populated_service.get_subscription(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
    )
    assert status == 200
    assert body['topics'] == ['topicA', 'topicB']


def test_get_subscription_empty(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
) -> None:
    """Test get_subscription returns empty list when no subscription."""
    mock_kafka_consumer.subscription.return_value = None
    body, status = populated_service.get_subscription(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
    )
    assert status == 200
    assert body['topics'] == []


def test_get_subscription_not_found(
    consumer_service: ConsumerService,
) -> None:
    """Test get_subscription returns 404 for nonexistent consumer."""
    body, status = consumer_service.get_subscription(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        'nonexistent',
    )
    assert status == 404
    assert body['error_code'] == 40403


# ============================================================================
# Unit Tests - unsubscribe
# ============================================================================


def test_unsubscribe_success(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
) -> None:
    """Test unsubscribe calls consumer.unsubscribe()."""
    body, status = populated_service.unsubscribe(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
    )
    assert status == 204
    assert body is None
    mock_kafka_consumer.unsubscribe.assert_called_once()


def test_unsubscribe_not_found(
    consumer_service: ConsumerService,
) -> None:
    """Test unsubscribe returns 404 for nonexistent consumer."""
    body, status = consumer_service.unsubscribe(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        'nonexistent',
    )
    assert status == 404
    assert body['error_code'] == 40403


def test_unsubscribe_kafka_exception(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
) -> None:
    """Test unsubscribe handles exception."""
    mock_kafka_consumer.unsubscribe.side_effect = Exception('Unsub failed')
    body, status = populated_service.unsubscribe(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
    )
    assert status == 500
    assert body['error_code'] == 50002


# ============================================================================
# Unit Tests - poll_records
# ============================================================================


def test_poll_records_success_binary(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
) -> None:
    """Test poll_records with binary format returns base64 encoded values."""
    tp = TopicPartition(f'{NAMESPACE}.my-topic', 0)
    msg = MagicMock()
    msg.key = b'key1'
    msg.value = b'value1'
    msg.offset = 42
    mock_kafka_consumer.poll.return_value = {tp: [msg]}

    body, status = populated_service.poll_records(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
    )
    assert status == 200
    assert len(body) == 1
    assert body[0]['topic'] == 'my-topic'
    assert body[0]['partition'] == 0
    assert body[0]['offset'] == 42
    assert body[0]['key'] == base64.b64encode(b'key1').decode('ascii')
    assert body[0]['value'] == base64.b64encode(b'value1').decode('ascii')


def test_poll_records_success_json(
    populated_service_json: ConsumerService,
) -> None:
    """Test poll_records with json format returns UTF-8 string."""
    # Get the mock consumer from the json-format service
    key = (SUBJECT, NAMESPACE, GROUP_NAME, INSTANCE_ID)
    managed = populated_service_json._consumers[key]
    mock_consumer = managed.consumer

    tp = TopicPartition(f'{NAMESPACE}.my-topic', 0)
    msg = MagicMock()
    msg.key = None
    msg.value = b'{"msg": "hello"}'
    msg.offset = 0
    mock_consumer.poll.return_value = {tp: [msg]}

    body, status = populated_service_json.poll_records(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
    )
    assert status == 200
    assert body[0]['value'] == '{"msg": "hello"}'
    assert body[0]['key'] is None


def test_poll_records_empty(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
) -> None:
    """Test poll_records returns empty list when no messages."""
    mock_kafka_consumer.poll.return_value = {}
    body, status = populated_service.poll_records(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
    )
    assert status == 200
    assert body == []


def test_poll_records_not_found(
    consumer_service: ConsumerService,
) -> None:
    """Test poll_records returns 404 for nonexistent consumer."""
    body, status = consumer_service.poll_records(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        'nonexistent',
    )
    assert status == 404
    assert body['error_code'] == 40403


def test_poll_records_kafka_exception(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
) -> None:
    """Test poll_records handles poll() failure."""
    mock_kafka_consumer.poll.side_effect = Exception('Poll failed')
    body, status = populated_service.poll_records(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
    )
    assert status == 500
    assert body['error_code'] == 50002


def test_poll_records_null_key_and_value(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
) -> None:
    """Test poll_records handles None key and value."""
    tp = TopicPartition(f'{NAMESPACE}.my-topic', 0)
    msg = MagicMock()
    msg.key = None
    msg.value = None
    msg.offset = 0
    mock_kafka_consumer.poll.return_value = {tp: [msg]}

    body, status = populated_service.poll_records(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
    )
    assert status == 200
    assert body[0]['key'] is None
    assert body[0]['value'] is None


# ============================================================================
# Unit Tests - commit_offsets
# ============================================================================


def test_commit_offsets_with_explicit_offsets(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
) -> None:
    """Test commit_offsets with explicit offset list."""
    offsets = [{'topic': 't1', 'partition': 0, 'offset': 42}]
    body, status = populated_service.commit_offsets(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
        offsets=offsets,
    )
    assert status == 200
    assert body is None
    call_kwargs = mock_kafka_consumer.commit.call_args[1]
    tp = TopicPartition(f'{NAMESPACE}.t1', 0)
    assert tp in call_kwargs['offsets']
    assert call_kwargs['offsets'][tp] == OffsetAndMetadata(42, None, None)


def test_commit_offsets_without_offsets(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
) -> None:
    """Test commit_offsets with no offsets commits all fetched."""
    body, status = populated_service.commit_offsets(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
        offsets=None,
    )
    assert status == 200
    assert body is None
    mock_kafka_consumer.commit.assert_called_once_with()


def test_commit_offsets_not_found(
    consumer_service: ConsumerService,
) -> None:
    """Test commit_offsets returns 404 for nonexistent consumer."""
    body, status = consumer_service.commit_offsets(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        'nonexistent',
    )
    assert status == 404
    assert body['error_code'] == 40403


def test_commit_offsets_kafka_exception(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
) -> None:
    """Test commit_offsets handles commit failure."""
    mock_kafka_consumer.commit.side_effect = Exception('Commit failed')
    body, status = populated_service.commit_offsets(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
    )
    assert status == 500
    assert body['error_code'] == 50002


# ============================================================================
# Unit Tests - get_committed
# ============================================================================


def test_get_committed_success(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
) -> None:
    """Test get_committed returns offsets with None mapped to -1."""
    # First call returns 42, second returns None
    mock_kafka_consumer.committed.side_effect = [42, None]
    partitions = [
        {'topic': 't1', 'partition': 0},
        {'topic': 't1', 'partition': 1},
    ]
    body, status = populated_service.get_committed(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
        partitions=partitions,
    )
    assert status == 200
    assert len(body['offsets']) == 2
    assert body['offsets'][0]['offset'] == 42
    assert body['offsets'][0]['metadata'] == ''
    assert body['offsets'][1]['offset'] == -1


def test_get_committed_not_found(
    consumer_service: ConsumerService,
) -> None:
    """Test get_committed returns 404 for nonexistent consumer."""
    body, status = consumer_service.get_committed(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        'nonexistent',
        partitions=[{'topic': 't1', 'partition': 0}],
    )
    assert status == 404
    assert body['error_code'] == 40403


def test_get_committed_kafka_exception(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
) -> None:
    """Test get_committed handles exception."""
    mock_kafka_consumer.committed.side_effect = Exception('Failed')
    body, status = populated_service.get_committed(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
        partitions=[{'topic': 't1', 'partition': 0}],
    )
    assert status == 500
    assert body['error_code'] == 50002


# ============================================================================
# Unit Tests - assign_partitions
# ============================================================================


def test_assign_partitions_success(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
) -> None:
    """Test assign_partitions assigns correct TopicPartitions."""
    partitions = [{'topic': 't1', 'partition': 0}]
    body, status = populated_service.assign_partitions(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
        partitions=partitions,
    )
    assert status == 204
    assert body is None
    expected_tp = TopicPartition(f'{NAMESPACE}.t1', 0)
    mock_kafka_consumer.assign.assert_called_once_with([expected_tp])


def test_assign_partitions_not_found(
    consumer_service: ConsumerService,
) -> None:
    """Test assign_partitions returns 404 for nonexistent consumer."""
    body, status = consumer_service.assign_partitions(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        'nonexistent',
        partitions=[{'topic': 't1', 'partition': 0}],
    )
    assert status == 404
    assert body['error_code'] == 40403


def test_assign_partitions_kafka_exception(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
) -> None:
    """Test assign_partitions handles exception."""
    mock_kafka_consumer.assign.side_effect = Exception('Assign failed')
    body, status = populated_service.assign_partitions(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
        partitions=[{'topic': 't1', 'partition': 0}],
    )
    assert status == 500
    assert body['error_code'] == 50002


# ============================================================================
# Unit Tests - get_assignments
# ============================================================================


def test_get_assignments_success(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
) -> None:
    """Test get_assignments returns sorted, stripped partitions."""
    mock_kafka_consumer.assignment.return_value = {
        TopicPartition(f'{NAMESPACE}.t1', 1),
        TopicPartition(f'{NAMESPACE}.t1', 0),
    }
    body, status = populated_service.get_assignments(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
    )
    assert status == 200
    assert body['partitions'] == [
        {'topic': 't1', 'partition': 0},
        {'topic': 't1', 'partition': 1},
    ]


def test_get_assignments_empty(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
) -> None:
    """Test get_assignments returns empty list when no assignments."""
    mock_kafka_consumer.assignment.return_value = None
    body, status = populated_service.get_assignments(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
    )
    assert status == 200
    assert body['partitions'] == []


def test_get_assignments_not_found(
    consumer_service: ConsumerService,
) -> None:
    """Test get_assignments returns 404 for nonexistent consumer."""
    body, status = consumer_service.get_assignments(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        'nonexistent',
    )
    assert status == 404
    assert body['error_code'] == 40403


# ============================================================================
# Unit Tests - seek
# ============================================================================


def test_seek_success(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
) -> None:
    """Test seek calls consumer.seek with correct args."""
    offsets = [{'topic': 't1', 'partition': 0, 'offset': 100}]
    body, status = populated_service.seek(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
        offsets=offsets,
    )
    assert status == 204
    assert body is None
    expected_tp = TopicPartition(f'{NAMESPACE}.t1', 0)
    mock_kafka_consumer.seek.assert_called_once_with(expected_tp, 100)


def test_seek_not_found(
    consumer_service: ConsumerService,
) -> None:
    """Test seek returns 404 for nonexistent consumer."""
    body, status = consumer_service.seek(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        'nonexistent',
        offsets=[{'topic': 't1', 'partition': 0, 'offset': 0}],
    )
    assert status == 404
    assert body['error_code'] == 40403


def test_seek_kafka_exception(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
) -> None:
    """Test seek handles exception."""
    mock_kafka_consumer.seek.side_effect = Exception('Seek failed')
    body, status = populated_service.seek(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
        offsets=[{'topic': 't1', 'partition': 0, 'offset': 0}],
    )
    assert status == 500
    assert body['error_code'] == 50002


# ============================================================================
# Unit Tests - seek_to_beginning
# ============================================================================


def test_seek_to_beginning_success(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
) -> None:
    """Test seek_to_beginning calls consumer.seek_to_beginning."""
    partitions = [{'topic': 't1', 'partition': 0}]
    body, status = populated_service.seek_to_beginning(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
        partitions=partitions,
    )
    assert status == 204
    assert body is None
    expected_tp = TopicPartition(f'{NAMESPACE}.t1', 0)
    mock_kafka_consumer.seek_to_beginning.assert_called_once_with(expected_tp)


def test_seek_to_beginning_not_found(
    consumer_service: ConsumerService,
) -> None:
    """Test seek_to_beginning returns 404 for nonexistent consumer."""
    body, status = consumer_service.seek_to_beginning(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        'nonexistent',
        partitions=[{'topic': 't1', 'partition': 0}],
    )
    assert status == 404
    assert body['error_code'] == 40403


def test_seek_to_beginning_kafka_exception(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
) -> None:
    """Test seek_to_beginning handles exception."""
    mock_kafka_consumer.seek_to_beginning.side_effect = Exception('Failed')
    body, status = populated_service.seek_to_beginning(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
        partitions=[{'topic': 't1', 'partition': 0}],
    )
    assert status == 500
    assert body['error_code'] == 50002


# ============================================================================
# Unit Tests - seek_to_end
# ============================================================================


def test_seek_to_end_success(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
) -> None:
    """Test seek_to_end calls consumer.seek_to_end."""
    partitions = [{'topic': 't1', 'partition': 0}]
    body, status = populated_service.seek_to_end(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
        partitions=partitions,
    )
    assert status == 204
    assert body is None
    expected_tp = TopicPartition(f'{NAMESPACE}.t1', 0)
    mock_kafka_consumer.seek_to_end.assert_called_once_with(expected_tp)


def test_seek_to_end_not_found(
    consumer_service: ConsumerService,
) -> None:
    """Test seek_to_end returns 404 for nonexistent consumer."""
    body, status = consumer_service.seek_to_end(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        'nonexistent',
        partitions=[{'topic': 't1', 'partition': 0}],
    )
    assert status == 404
    assert body['error_code'] == 40403


def test_seek_to_end_kafka_exception(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
) -> None:
    """Test seek_to_end handles exception."""
    mock_kafka_consumer.seek_to_end.side_effect = Exception('Failed')
    body, status = populated_service.seek_to_end(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
        partitions=[{'topic': 't1', 'partition': 0}],
    )
    assert status == 500
    assert body['error_code'] == 50002


# ============================================================================
# Unit Tests - shutdown_all
# ============================================================================


def test_shutdown_all_closes_consumers(
    consumer_service: ConsumerService,
) -> None:
    """Test shutdown_all closes all registered consumers."""
    mock1 = MagicMock()
    mock2 = MagicMock()
    consumer_service._consumers[('u1', 'ns', 'g', 'i1')] = ManagedConsumer(
        consumer=mock1,
        subject='u1',
        namespace='ns',
        group_name='g',
        instance_id='i1',
        format_type='binary',
    )
    consumer_service._consumers[('u2', 'ns', 'g', 'i2')] = ManagedConsumer(
        consumer=mock2,
        subject='u2',
        namespace='ns',
        group_name='g',
        instance_id='i2',
        format_type='binary',
    )
    consumer_service.shutdown_all()
    mock1.close.assert_called_once_with(autocommit=False)
    mock2.close.assert_called_once_with(autocommit=False)
    assert len(consumer_service._consumers) == 0


def test_shutdown_all_handles_close_exception(
    consumer_service: ConsumerService,
) -> None:
    """Test shutdown_all continues if close() raises."""
    mock1 = MagicMock()
    mock1.close.side_effect = Exception('Close error')
    consumer_service._consumers[('u', 'ns', 'g', 'i')] = ManagedConsumer(
        consumer=mock1,
        subject='u',
        namespace='ns',
        group_name='g',
        instance_id='i',
        format_type='binary',
    )
    # Should not raise
    consumer_service.shutdown_all()
    assert len(consumer_service._consumers) == 0


def test_shutdown_all_empty(
    consumer_service: ConsumerService,
) -> None:
    """Test shutdown_all on empty service does not error."""
    consumer_service.shutdown_all()
    assert len(consumer_service._consumers) == 0


# ============================================================================
# Unit Tests - Reaper
# ============================================================================


def test_reaper_cleans_expired_consumers(
    consumer_service: ConsumerService,
) -> None:
    """Test _reap_expired_consumers removes expired consumers."""
    mock_consumer = MagicMock()
    managed = ManagedConsumer(
        consumer=mock_consumer,
        subject='u',
        namespace='ns',
        group_name='g',
        instance_id='expired-inst',
        format_type='binary',
    )
    # Set last_access far in the past to make it expired
    managed.last_access = time.monotonic() - 600
    consumer_service._consumers[('u', 'ns', 'g', 'expired-inst')] = managed

    # The reaper loop is: while True: sleep() -> check -> reap -> sleep ...
    # Let sleep pass once (no-op) then raise on second call to break the loop
    call_count = 0

    def _mock_sleep(_seconds: float) -> None:
        nonlocal call_count
        call_count += 1
        if call_count > 1:
            raise StopIteration

    with (
        patch(
            'web_service.consumer_service.time.sleep',
            side_effect=_mock_sleep,
        ),
        pytest.raises(StopIteration),
    ):
        consumer_service._reap_expired_consumers()

    # Consumer should have been reaped
    assert len(consumer_service._consumers) == 0
    mock_consumer.close.assert_called_once_with(autocommit=False)


def test_reaper_does_not_reap_active_consumers(
    consumer_service: ConsumerService,
) -> None:
    """Test _reap_expired_consumers keeps active consumers."""
    mock_consumer = MagicMock()
    managed = ManagedConsumer(
        consumer=mock_consumer,
        subject='u',
        namespace='ns',
        group_name='g',
        instance_id='active-inst',
        format_type='binary',
    )
    # Fresh consumer (just created, not expired)
    consumer_service._consumers[('u', 'ns', 'g', 'active-inst')] = managed

    call_count = 0

    def _mock_sleep(_seconds: float) -> None:
        nonlocal call_count
        call_count += 1
        if call_count > 1:
            raise StopIteration

    with (
        patch(
            'web_service.consumer_service.time.sleep',
            side_effect=_mock_sleep,
        ),
        pytest.raises(StopIteration),
    ):
        consumer_service._reap_expired_consumers()

    # Consumer should still be present
    assert len(consumer_service._consumers) == 1
    mock_consumer.close.assert_not_called()


# ============================================================================
# Integration Test Fixtures
# ============================================================================


@pytest.fixture
def real_kafka_service() -> Any:
    """Create a KafkaService with real AWS services."""
    from web_service.services import KafkaService

    bootstrap_servers = os.getenv('DEFAULT_SERVERS')
    region = os.getenv('AWS_ACCOUNT_REGION')
    if not region:
        pytest.skip('Missing AWS_ACCOUNT_REGION')
    return KafkaService(bootstrap_servers=bootstrap_servers, region=region)


@pytest.fixture
def real_web_service() -> Any:
    """Create a WebService with real AWS services."""
    from web_service.services import DynamoDBService
    from web_service.services import IAMService
    from web_service.services import KafkaService
    from web_service.services import NamespaceService
    from web_service.services import WebService

    account_id = os.getenv('AWS_ACCOUNT_ID')
    region = os.getenv('AWS_ACCOUNT_REGION')
    cluster_name = os.getenv('MSK_CLUSTER_NAME')
    if not all([account_id, region, cluster_name]):
        pytest.skip('Missing required AWS environment variables')

    iam_service = IAMService(
        account_id=account_id or '',
        region=region or '',
        cluster_name=cluster_name or '',
    )
    kafka_service = KafkaService(
        bootstrap_servers=os.getenv('DEFAULT_SERVERS'),
        region=region or '',
    )
    db_service = DynamoDBService(
        region=region or '',
        keys_table_name=os.getenv(
            'KEYS_TABLE_NAME',
            'test-diaspora-keys-table',
        ),
        users_table_name=os.getenv(
            'USERS_TABLE_NAME',
            'test-diaspora-users-table',
        ),
        namespace_table_name=os.getenv(
            'NAMESPACE_TABLE_NAME',
            'test-diaspora-namespace-table',
        ),
    )
    namespace_service = NamespaceService(dynamodb_service=db_service)
    return WebService(
        iam_service=iam_service,
        kafka_service=kafka_service,
        namespace_service=namespace_service,
    )


@pytest.fixture
def consumer_service_real() -> ConsumerService:
    """Create a ConsumerService with real MSK connection."""
    bootstrap_servers = os.getenv('DEFAULT_SERVERS')
    region = os.getenv('AWS_ACCOUNT_REGION')
    if not bootstrap_servers or not region:
        pytest.skip('Missing DEFAULT_SERVERS or AWS_ACCOUNT_REGION')
    return ConsumerService(
        bootstrap_servers=bootstrap_servers,
        region=region,
    )


@pytest.fixture
def random_subject() -> str:
    """Generate a random subject for each test."""
    return f'test-subject-{os.urandom(8).hex()}'


@pytest.fixture
def cleanup_consumer_and_user(
    real_web_service: Any,
    request: pytest.FixtureRequest,
) -> Any:
    """Dual cleanup fixture for consumer instances and user resources.

    Returns a function to register (consumer_service, subject, namespace,
    group_name, instance_id) tuples for cleanup. Also cleans up the user.
    """
    consumers: list[tuple[ConsumerService, str, str, str, str]] = []
    subjects: list[str] = []
    cleanup_done = False

    def _register(
        cs: ConsumerService,
        subject: str,
        namespace: str,
        group_name: str,
        instance_id: str,
    ) -> None:
        entry = (cs, subject, namespace, group_name, instance_id)
        if entry not in consumers:
            consumers.append(entry)
        if subject not in subjects:
            subjects.append(subject)

    def _register_user(subject: str) -> None:
        if subject not in subjects:
            subjects.append(subject)

    _register.register_user = _register_user

    def _finalize() -> None:
        nonlocal cleanup_done
        if cleanup_done:
            return
        cleanup_done = True
        # Delete consumer instances
        for cs, subject, namespace, group_name, instance_id in consumers:
            with contextlib.suppress(Exception):
                cs.delete_consumer(
                    subject,
                    namespace,
                    group_name,
                    instance_id,
                )
        # Delete user and all their resources
        for subject in subjects:
            with contextlib.suppress(Exception):
                real_web_service.delete_user(subject)

    request.addfinalizer(_finalize)
    yield _register
    _finalize()


def _produce_messages(
    bootstrap_servers: str,
    region: str,
    kafka_topic: str,
    messages: list[bytes],
) -> None:
    """Produce test messages to a Kafka topic using MSK auth."""
    from kafka import KafkaProducer

    from web_service.services import MSKTokenProvider

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        security_protocol='SASL_SSL',
        sasl_mechanism='OAUTHBEARER',
        sasl_oauth_token_provider=MSKTokenProvider(region),
    )
    try:
        for msg in messages:
            producer.send(kafka_topic, value=msg)
        producer.flush()
    finally:
        producer.close()


# ============================================================================
# Integration Tests with Real AWS/MSK Services
# ============================================================================


@pytest.mark.integration
def test_create_consumer_instance_real(
    consumer_service_real: ConsumerService,
    random_subject: str,
    cleanup_consumer_and_user: Any,
) -> None:
    """Test create_consumer against real MSK."""
    namespace = f'ns-{os.urandom(4).hex()}'
    group_name = f'grp-{os.urandom(4).hex()}'
    instance_name = f'inst-{os.urandom(4).hex()}'

    print(
        f'\n[test_create_consumer_instance_real] '
        f'group={group_name}, instance={instance_name}',
    )

    body, status = consumer_service_real.create_consumer(
        subject=random_subject,
        namespace=namespace,
        group_name=group_name,
        name=instance_name,
        format_type='json',
        auto_offset_reset='earliest',
    )
    print(f'  Result: status={status}')
    print(json.dumps(body, indent=2, default=str))

    # Register for cleanup
    cleanup_consumer_and_user(
        consumer_service_real,
        random_subject,
        namespace,
        group_name,
        instance_name,
    )

    assert status == 200
    assert body['instance_id'] == instance_name
    assert f'/api/v3/{namespace}/consumers/' in body['base_uri']


@pytest.mark.integration
def test_create_consumer_conflict_real(
    consumer_service_real: ConsumerService,
    random_subject: str,
    cleanup_consumer_and_user: Any,
) -> None:
    """Test create_consumer conflict (duplicate name) against real MSK."""
    namespace = f'ns-{os.urandom(4).hex()}'
    group_name = f'grp-{os.urandom(4).hex()}'
    instance_name = f'inst-{os.urandom(4).hex()}'

    print(
        f'\n[test_create_consumer_conflict_real] '
        f'group={group_name}, instance={instance_name}',
    )

    body1, status1 = consumer_service_real.create_consumer(
        subject=random_subject,
        namespace=namespace,
        group_name=group_name,
        name=instance_name,
    )
    cleanup_consumer_and_user(
        consumer_service_real,
        random_subject,
        namespace,
        group_name,
        instance_name,
    )
    assert status1 == 200

    body2, status2 = consumer_service_real.create_consumer(
        subject=random_subject,
        namespace=namespace,
        group_name=group_name,
        name=instance_name,
    )
    print(f'  Second create: status={status2}')
    print(json.dumps(body2, indent=2, default=str))

    assert status2 == 409
    assert body2['error_code'] == 40902


@pytest.mark.integration
def test_delete_consumer_real(
    consumer_service_real: ConsumerService,
    random_subject: str,
    cleanup_consumer_and_user: Any,
) -> None:
    """Test delete_consumer against real MSK."""
    namespace = f'ns-{os.urandom(4).hex()}'
    group_name = f'grp-{os.urandom(4).hex()}'
    instance_name = f'inst-{os.urandom(4).hex()}'

    print(
        f'\n[test_delete_consumer_real] '
        f'group={group_name}, instance={instance_name}',
    )

    body_c, status_c = consumer_service_real.create_consumer(
        subject=random_subject,
        namespace=namespace,
        group_name=group_name,
        name=instance_name,
    )
    # Register for cleanup in case delete fails
    cleanup_consumer_and_user(
        consumer_service_real,
        random_subject,
        namespace,
        group_name,
        instance_name,
    )
    assert status_c == 200

    body, status = consumer_service_real.delete_consumer(
        random_subject,
        namespace,
        group_name,
        instance_name,
    )
    print(f'  Delete: status={status}')
    assert status == 204
    assert body is None


@pytest.mark.integration
def test_delete_nonexistent_consumer_real(
    consumer_service_real: ConsumerService,
) -> None:
    """Test delete_consumer returns 404 for nonexistent consumer."""
    print('\n[test_delete_nonexistent_consumer_real]')
    body, status = consumer_service_real.delete_consumer(
        'no-user',
        'no-ns',
        'no-group',
        'no-instance',
    )
    print(f'  status={status}')
    print(json.dumps(body, indent=2, default=str))
    assert status == 404
    assert body['error_code'] == 40403


@pytest.mark.integration
def test_subscribe_and_get_subscription_real(
    consumer_service_real: ConsumerService,
    real_web_service: Any,
    real_kafka_service: Any,
    random_subject: str,
    cleanup_consumer_and_user: Any,
) -> None:
    """Test subscribe and get_subscription against real MSK."""
    print(
        f'\n[test_subscribe_and_get_subscription_real] '
        f'subject={random_subject}',
    )

    # Setup: create user and topic
    create_result = real_web_service.create_user(random_subject)
    cleanup_consumer_and_user.register_user(random_subject)
    assert create_result['status'] == 'success'
    namespace = create_result['namespace']
    topic = f'test-topic-{os.urandom(4).hex()}'

    topic_result = real_web_service.namespace_service.create_topic(
        random_subject,
        namespace,
        topic,
    )
    assert topic_result['status'] == 'success'
    print(f'  Created topic: {namespace}.{topic}')

    # Create consumer
    group_name = f'grp-{os.urandom(4).hex()}'
    instance_name = f'inst-{os.urandom(4).hex()}'
    body_c, status_c = consumer_service_real.create_consumer(
        subject=random_subject,
        namespace=namespace,
        group_name=group_name,
        name=instance_name,
        format_type='json',
        auto_offset_reset='earliest',
    )
    cleanup_consumer_and_user(
        consumer_service_real,
        random_subject,
        namespace,
        group_name,
        instance_name,
    )
    assert status_c == 200

    # Subscribe
    body_s, status_s = consumer_service_real.subscribe(
        random_subject,
        namespace,
        group_name,
        instance_name,
        topics=[topic],
    )
    assert status_s == 204
    print(f'  Subscribed to: {topic}')

    # Get subscription
    body_g, status_g = consumer_service_real.get_subscription(
        random_subject,
        namespace,
        group_name,
        instance_name,
    )
    print(f'  Subscription: {json.dumps(body_g, default=str)}')
    assert status_g == 200
    assert topic in body_g['topics']


@pytest.mark.integration
def test_unsubscribe_real(
    consumer_service_real: ConsumerService,
    real_web_service: Any,
    random_subject: str,
    cleanup_consumer_and_user: Any,
) -> None:
    """Test unsubscribe against real MSK."""
    print(f'\n[test_unsubscribe_real] subject={random_subject}')

    # Setup: create user and topic
    create_result = real_web_service.create_user(random_subject)
    cleanup_consumer_and_user.register_user(random_subject)
    assert create_result['status'] == 'success'
    namespace = create_result['namespace']
    topic = f'test-topic-{os.urandom(4).hex()}'
    topic_result = real_web_service.namespace_service.create_topic(
        random_subject,
        namespace,
        topic,
    )
    assert topic_result['status'] == 'success'

    # Create and subscribe
    group_name = f'grp-{os.urandom(4).hex()}'
    instance_name = f'inst-{os.urandom(4).hex()}'
    body_c, status_c = consumer_service_real.create_consumer(
        subject=random_subject,
        namespace=namespace,
        group_name=group_name,
        name=instance_name,
    )
    cleanup_consumer_and_user(
        consumer_service_real,
        random_subject,
        namespace,
        group_name,
        instance_name,
    )
    assert status_c == 200

    consumer_service_real.subscribe(
        random_subject,
        namespace,
        group_name,
        instance_name,
        topics=[topic],
    )

    # Unsubscribe
    body_u, status_u = consumer_service_real.unsubscribe(
        random_subject,
        namespace,
        group_name,
        instance_name,
    )
    assert status_u == 204
    print('  Unsubscribed')

    # Verify subscription is empty
    body_g, status_g = consumer_service_real.get_subscription(
        random_subject,
        namespace,
        group_name,
        instance_name,
    )
    assert status_g == 200
    assert body_g['topics'] == []


@pytest.mark.integration
def test_poll_records_empty_real(
    consumer_service_real: ConsumerService,
    real_web_service: Any,
    random_subject: str,
    cleanup_consumer_and_user: Any,
) -> None:
    """Test poll_records returns empty on a topic with no messages."""
    print(f'\n[test_poll_records_empty_real] subject={random_subject}')

    # Setup: create user and topic
    create_result = real_web_service.create_user(random_subject)
    cleanup_consumer_and_user.register_user(random_subject)
    assert create_result['status'] == 'success'
    namespace = create_result['namespace']
    topic = f'test-topic-{os.urandom(4).hex()}'
    real_web_service.namespace_service.create_topic(
        random_subject,
        namespace,
        topic,
    )

    # Create consumer and subscribe
    group_name = f'grp-{os.urandom(4).hex()}'
    instance_name = f'inst-{os.urandom(4).hex()}'
    consumer_service_real.create_consumer(
        subject=random_subject,
        namespace=namespace,
        group_name=group_name,
        name=instance_name,
        auto_offset_reset='earliest',
    )
    cleanup_consumer_and_user(
        consumer_service_real,
        random_subject,
        namespace,
        group_name,
        instance_name,
    )
    consumer_service_real.subscribe(
        random_subject,
        namespace,
        group_name,
        instance_name,
        topics=[topic],
    )

    # First poll triggers partition assignment, second poll fetches data
    consumer_service_real.poll_records(
        random_subject,
        namespace,
        group_name,
        instance_name,
        timeout_ms=3000,
    )
    body, status = consumer_service_real.poll_records(
        random_subject,
        namespace,
        group_name,
        instance_name,
        timeout_ms=3000,
    )
    print(f'  Poll result: status={status}, records={len(body)}')
    assert status == 200
    assert isinstance(body, list)
    assert len(body) == 0


@pytest.mark.integration
def test_full_lifecycle_subscribe_poll_commit(
    consumer_service_real: ConsumerService,
    real_web_service: Any,
    random_subject: str,
    cleanup_consumer_and_user: Any,
) -> None:
    """Full lifecycle: create, subscribe, produce, poll, commit."""
    bootstrap_servers = os.getenv('DEFAULT_SERVERS')
    region = os.getenv('AWS_ACCOUNT_REGION')
    if not bootstrap_servers or not region:
        pytest.skip('Missing DEFAULT_SERVERS or AWS_ACCOUNT_REGION')

    print(
        f'\n[test_full_lifecycle_subscribe_poll_commit] '
        f'subject={random_subject}',
    )

    # Setup: create user and topic
    create_result = real_web_service.create_user(random_subject)
    cleanup_consumer_and_user.register_user(random_subject)
    assert create_result['status'] == 'success'
    namespace = create_result['namespace']
    topic = f'test-topic-{os.urandom(4).hex()}'
    real_web_service.namespace_service.create_topic(
        random_subject,
        namespace,
        topic,
    )
    kafka_topic = f'{namespace}.{topic}'

    # Produce messages
    messages = [b'msg-1', b'msg-2', b'msg-3']
    _produce_messages(bootstrap_servers, region, kafka_topic, messages)
    print(f'  Produced {len(messages)} messages to {kafka_topic}')

    # Create consumer
    group_name = f'grp-{os.urandom(4).hex()}'
    instance_name = f'inst-{os.urandom(4).hex()}'
    body_c, status_c = consumer_service_real.create_consumer(
        subject=random_subject,
        namespace=namespace,
        group_name=group_name,
        name=instance_name,
        format_type='binary',
        auto_offset_reset='earliest',
        auto_commit_enable='false',
    )
    cleanup_consumer_and_user(
        consumer_service_real,
        random_subject,
        namespace,
        group_name,
        instance_name,
    )
    assert status_c == 200

    # Subscribe
    consumer_service_real.subscribe(
        random_subject,
        namespace,
        group_name,
        instance_name,
        topics=[topic],
    )

    # Poll (first poll may be empty, triggers partition assignment)
    all_records: list[dict[str, Any]] = []
    for _attempt in range(5):
        body_p, status_p = consumer_service_real.poll_records(
            random_subject,
            namespace,
            group_name,
            instance_name,
            timeout_ms=5000,
        )
        assert status_p == 200
        if isinstance(body_p, list):
            all_records.extend(body_p)
        if len(all_records) >= len(messages):
            break
        print(f'  Poll attempt {_attempt + 1}: {len(all_records)} records')

    print(f'  Total records polled: {len(all_records)}')
    assert len(all_records) >= len(messages)

    # Verify records contain expected fields
    for record in all_records:
        assert 'topic' in record
        assert 'partition' in record
        assert 'offset' in record
        assert 'key' in record
        assert 'value' in record
        assert record['topic'] == topic

    # Commit offsets (commit all)
    body_co, status_co = consumer_service_real.commit_offsets(
        random_subject,
        namespace,
        group_name,
        instance_name,
    )
    print(f'  Commit: status={status_co}')
    assert status_co == 200

    # Get committed offsets
    body_gc, status_gc = consumer_service_real.get_committed(
        random_subject,
        namespace,
        group_name,
        instance_name,
        partitions=[{'topic': topic, 'partition': 0}],
    )
    print(f'  Committed offsets: {json.dumps(body_gc, default=str)}')
    assert status_gc == 200
    assert len(body_gc['offsets']) == 1
    assert body_gc['offsets'][0]['offset'] >= len(messages)

    # Delete consumer
    body_d, status_d = consumer_service_real.delete_consumer(
        random_subject,
        namespace,
        group_name,
        instance_name,
    )
    assert status_d == 204
    print('  Full lifecycle test completed successfully')


@pytest.mark.integration
def test_assign_and_get_assignments_real(
    consumer_service_real: ConsumerService,
    real_web_service: Any,
    random_subject: str,
    cleanup_consumer_and_user: Any,
) -> None:
    """Test manual partition assignment against real MSK."""
    print(
        f'\n[test_assign_and_get_assignments_real] subject={random_subject}',
    )

    # Setup: create user and topic
    create_result = real_web_service.create_user(random_subject)
    cleanup_consumer_and_user.register_user(random_subject)
    assert create_result['status'] == 'success'
    namespace = create_result['namespace']
    topic = f'test-topic-{os.urandom(4).hex()}'
    real_web_service.namespace_service.create_topic(
        random_subject,
        namespace,
        topic,
    )

    # Create consumer
    group_name = f'grp-{os.urandom(4).hex()}'
    instance_name = f'inst-{os.urandom(4).hex()}'
    consumer_service_real.create_consumer(
        subject=random_subject,
        namespace=namespace,
        group_name=group_name,
        name=instance_name,
    )
    cleanup_consumer_and_user(
        consumer_service_real,
        random_subject,
        namespace,
        group_name,
        instance_name,
    )

    # Assign partitions
    body_a, status_a = consumer_service_real.assign_partitions(
        random_subject,
        namespace,
        group_name,
        instance_name,
        partitions=[{'topic': topic, 'partition': 0}],
    )
    assert status_a == 204
    print('  Assigned partition 0')

    # Get assignments
    body_g, status_g = consumer_service_real.get_assignments(
        random_subject,
        namespace,
        group_name,
        instance_name,
    )
    print(f'  Assignments: {json.dumps(body_g, default=str)}')
    assert status_g == 200
    assert len(body_g['partitions']) == 1
    assert body_g['partitions'][0]['topic'] == topic
    assert body_g['partitions'][0]['partition'] == 0


@pytest.mark.integration
def test_seek_to_beginning_real(
    consumer_service_real: ConsumerService,
    real_web_service: Any,
    random_subject: str,
    cleanup_consumer_and_user: Any,
) -> None:
    """Test seek_to_beginning against real MSK with produced messages."""
    bootstrap_servers = os.getenv('DEFAULT_SERVERS')
    region = os.getenv('AWS_ACCOUNT_REGION')
    if not bootstrap_servers or not region:
        pytest.skip('Missing DEFAULT_SERVERS or AWS_ACCOUNT_REGION')

    print(
        f'\n[test_seek_to_beginning_real] subject={random_subject}',
    )

    # Setup: create user, topic, produce messages
    create_result = real_web_service.create_user(random_subject)
    cleanup_consumer_and_user.register_user(random_subject)
    assert create_result['status'] == 'success'
    namespace = create_result['namespace']
    topic = f'test-topic-{os.urandom(4).hex()}'
    real_web_service.namespace_service.create_topic(
        random_subject,
        namespace,
        topic,
    )
    kafka_topic = f'{namespace}.{topic}'
    _produce_messages(
        bootstrap_servers,
        region,
        kafka_topic,
        [b'seek-msg-1', b'seek-msg-2'],
    )

    # Create consumer and assign partition
    group_name = f'grp-{os.urandom(4).hex()}'
    instance_name = f'inst-{os.urandom(4).hex()}'
    consumer_service_real.create_consumer(
        subject=random_subject,
        namespace=namespace,
        group_name=group_name,
        name=instance_name,
        format_type='binary',
        auto_offset_reset='latest',
    )
    cleanup_consumer_and_user(
        consumer_service_real,
        random_subject,
        namespace,
        group_name,
        instance_name,
    )

    # Assign and seek to beginning
    consumer_service_real.assign_partitions(
        random_subject,
        namespace,
        group_name,
        instance_name,
        partitions=[{'topic': topic, 'partition': 0}],
    )
    body_sb, status_sb = consumer_service_real.seek_to_beginning(
        random_subject,
        namespace,
        group_name,
        instance_name,
        partitions=[{'topic': topic, 'partition': 0}],
    )
    assert status_sb == 204

    # Poll and verify we get records from the beginning
    all_records: list[dict[str, Any]] = []
    for _attempt in range(5):
        body_p, status_p = consumer_service_real.poll_records(
            random_subject,
            namespace,
            group_name,
            instance_name,
            timeout_ms=5000,
        )
        assert status_p == 200
        if isinstance(body_p, list):
            all_records.extend(body_p)
        if len(all_records) >= 2:
            break

    print(f'  Records after seek_to_beginning: {len(all_records)}')
    assert len(all_records) >= 2
    # First record should start at offset 0
    assert all_records[0]['offset'] == 0


@pytest.mark.integration
def test_seek_to_end_real(
    consumer_service_real: ConsumerService,
    real_web_service: Any,
    random_subject: str,
    cleanup_consumer_and_user: Any,
) -> None:
    """Test seek_to_end against real MSK."""
    bootstrap_servers = os.getenv('DEFAULT_SERVERS')
    region = os.getenv('AWS_ACCOUNT_REGION')
    if not bootstrap_servers or not region:
        pytest.skip('Missing DEFAULT_SERVERS or AWS_ACCOUNT_REGION')

    print(f'\n[test_seek_to_end_real] subject={random_subject}')

    # Setup: create user, topic, produce messages
    create_result = real_web_service.create_user(random_subject)
    cleanup_consumer_and_user.register_user(random_subject)
    assert create_result['status'] == 'success'
    namespace = create_result['namespace']
    topic = f'test-topic-{os.urandom(4).hex()}'
    real_web_service.namespace_service.create_topic(
        random_subject,
        namespace,
        topic,
    )
    kafka_topic = f'{namespace}.{topic}'
    _produce_messages(bootstrap_servers, region, kafka_topic, [b'end-msg'])

    # Create consumer, assign, seek to end
    group_name = f'grp-{os.urandom(4).hex()}'
    instance_name = f'inst-{os.urandom(4).hex()}'
    consumer_service_real.create_consumer(
        subject=random_subject,
        namespace=namespace,
        group_name=group_name,
        name=instance_name,
    )
    cleanup_consumer_and_user(
        consumer_service_real,
        random_subject,
        namespace,
        group_name,
        instance_name,
    )

    consumer_service_real.assign_partitions(
        random_subject,
        namespace,
        group_name,
        instance_name,
        partitions=[{'topic': topic, 'partition': 0}],
    )
    body_se, status_se = consumer_service_real.seek_to_end(
        random_subject,
        namespace,
        group_name,
        instance_name,
        partitions=[{'topic': topic, 'partition': 0}],
    )
    assert status_se == 204

    # Poll -- should get no records (already at end)
    body_p, status_p = consumer_service_real.poll_records(
        random_subject,
        namespace,
        group_name,
        instance_name,
        timeout_ms=3000,
    )
    print(f'  Records after seek_to_end: {len(body_p)}')
    assert status_p == 200
    assert isinstance(body_p, list)
    assert len(body_p) == 0


@pytest.mark.integration
def test_seek_to_offset_real(
    consumer_service_real: ConsumerService,
    real_web_service: Any,
    random_subject: str,
    cleanup_consumer_and_user: Any,
) -> None:
    """Test seek to a specific offset against real MSK."""
    bootstrap_servers = os.getenv('DEFAULT_SERVERS')
    region = os.getenv('AWS_ACCOUNT_REGION')
    if not bootstrap_servers or not region:
        pytest.skip('Missing DEFAULT_SERVERS or AWS_ACCOUNT_REGION')

    print(f'\n[test_seek_to_offset_real] subject={random_subject}')

    # Setup: create user, topic, produce messages
    create_result = real_web_service.create_user(random_subject)
    cleanup_consumer_and_user.register_user(random_subject)
    assert create_result['status'] == 'success'
    namespace = create_result['namespace']
    topic = f'test-topic-{os.urandom(4).hex()}'
    real_web_service.namespace_service.create_topic(
        random_subject,
        namespace,
        topic,
    )
    kafka_topic = f'{namespace}.{topic}'
    _produce_messages(
        bootstrap_servers,
        region,
        kafka_topic,
        [b'offset-0', b'offset-1', b'offset-2'],
    )

    # Create consumer, assign
    group_name = f'grp-{os.urandom(4).hex()}'
    instance_name = f'inst-{os.urandom(4).hex()}'
    consumer_service_real.create_consumer(
        subject=random_subject,
        namespace=namespace,
        group_name=group_name,
        name=instance_name,
        format_type='binary',
    )
    cleanup_consumer_and_user(
        consumer_service_real,
        random_subject,
        namespace,
        group_name,
        instance_name,
    )

    consumer_service_real.assign_partitions(
        random_subject,
        namespace,
        group_name,
        instance_name,
        partitions=[{'topic': topic, 'partition': 0}],
    )

    # Seek to offset 1 (skip first message)
    body_sk, status_sk = consumer_service_real.seek(
        random_subject,
        namespace,
        group_name,
        instance_name,
        offsets=[{'topic': topic, 'partition': 0, 'offset': 1}],
    )
    assert status_sk == 204

    # Poll and verify first record starts at offset 1
    all_records: list[dict[str, Any]] = []
    for _attempt in range(5):
        body_p, status_p = consumer_service_real.poll_records(
            random_subject,
            namespace,
            group_name,
            instance_name,
            timeout_ms=5000,
        )
        assert status_p == 200
        if isinstance(body_p, list):
            all_records.extend(body_p)
        if len(all_records) >= 2:
            break

    print(f'  Records after seek to offset 1: {len(all_records)}')
    assert len(all_records) >= 2
    assert all_records[0]['offset'] == 1
