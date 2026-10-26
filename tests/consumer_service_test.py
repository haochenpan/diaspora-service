"""Unit tests for ConsumerService.

The live tests of the consumer routes are in consumer_live_test.py.
"""

from __future__ import annotations

import base64
import json
import logging
import threading
import time
import warnings
from typing import Any
from unittest.mock import call
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from kafka.errors import KafkaError
from kafka.errors import KafkaTimeoutError
from kafka.structs import OffsetAndMetadata
from kafka.structs import TopicPartition

from web_service.consumer_records import EncodedField
from web_service.consumer_records import NotText
from web_service.consumer_service import CLOSE_COMMIT_TIMEOUT_MS
from web_service.consumer_service import CLOSE_TIMEOUT_MS
from web_service.consumer_service import CONSUMER_OPERATION_TIMEOUT_MS
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
    """Test EncodedField with binary format returns base64."""
    result = EncodedField.of(b'hello', 'binary').decoded()
    assert result == base64.b64encode(b'hello').decode('ascii')


def test_encode_value_json_format_utf8() -> None:
    """Test EncodedField with json format returns the JSON value."""
    field = EncodedField.of(b'{"key": "value"}', 'json')
    assert field.decoded() == {'key': 'value'}
    # Written as it was produced, not encoded again
    assert b''.join(field.pieces()) == b'{"key": "value"}'
    assert field.size == len(b'{"key": "value"}')


@pytest.mark.parametrize(
    'data',
    (
        b'plain text',
        b'{"a": 1} {"b": 2}',
        b'NaN',
        b'',
        b'[' * 100_000,
        'h\u00e9llo " \\ \n \u2028 \U0001f600'.encode(),
    ),
)
def test_encode_value_json_format_text(data: bytes) -> None:
    """A json instance returns UTF-8 text that is not JSON as a string."""
    field = EncodedField.of(data, 'json')
    assert field.decoded() == data.decode()
    body = b''.join(field.pieces())
    assert json.loads(body) == data.decode()
    assert body == json.dumps(data.decode(), ensure_ascii=False).encode()
    assert field.size == len(body)


@pytest.mark.parametrize('data', (b'\x80\x81\x82', b'{"a": "\xff"}'))
def test_encode_value_json_format_not_utf8(data: bytes) -> None:
    """A json instance cannot return bytes that are not UTF-8."""
    with pytest.raises(NotText, match='is not UTF-8 text'):
        EncodedField.of(data, 'json')


def test_encode_value_json_format_check_is_bounded() -> None:
    """A value whose check would cost too much memory is returned as text."""
    data = b'[' + b'[],' * 500_000 + b'[]]'
    with patch('web_service.consumer_records.json.JSONDecoder') as decoder:
        field = EncodedField.of(data, 'json')
    decoder.assert_not_called()
    assert field.decoded() == data.decode()


def test_encode_value_json_format_whitespace_and_scalars() -> None:
    """Any one JSON value is returned, with surrounding whitespace."""
    assert EncodedField.of(b' 42\n', 'json').decoded() == 42
    assert EncodedField.of(b'"text"', 'json').decoded() == 'text'
    assert EncodedField.of(b'null', 'json').decoded() is None


def test_encode_value_none_binary() -> None:
    """Test EncodedField with None and binary format returns None."""
    assert EncodedField.of(None, 'binary').decoded() is None


def test_encode_value_none_json() -> None:
    """Test EncodedField with None and json format returns None."""
    assert EncodedField.of(None, 'json').decoded() is None


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
    # The route adds base_uri, which depends on the request's URL
    assert body == {'instance_id': 'test-inst'}


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
        mock_kc_class.side_effect = KafkaError('Connection refused')
        body, status = consumer_service.create_consumer(
            subject=SUBJECT,
            namespace=NAMESPACE,
            group_name=GROUP_NAME,
            name='fail-inst',
        )
    assert status == 500
    assert body == {
        'error_code': 50002,
        'message': 'Failed to create consumer: Kafka error (KafkaError)',
    }


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
            consumer_request_timeout_ms='20000',
        )
    call_kwargs = mock_kc_class.call_args[1]
    assert call_kwargs['fetch_min_bytes'] == 1024
    # The proxy's own wait for records, not the Kafka consumer's
    assert 'request_timeout_ms' not in call_kwargs
    managed = consumer_service._consumers[
        (SUBJECT, NAMESPACE, GROUP_NAME, 'opt-inst')
    ]
    assert managed.default_wait_ms == 20000


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


@pytest.mark.parametrize(
    'name',
    ('a', 'inst-1', 'Inst_2.b', '0' + 'x' * 127),
)
def test_create_consumer_accepts_valid_names(
    consumer_service: ConsumerService,
    name: str,
) -> None:
    """An instance name of letters, digits, dots, dashes and underscores."""
    with patch('web_service.consumer_service.KafkaConsumer'):
        body, status = consumer_service.create_consumer(
            subject=SUBJECT,
            namespace=NAMESPACE,
            group_name=GROUP_NAME,
            name=name,
        )
    assert status == 200
    assert body == {'instance_id': name}


@pytest.mark.parametrize(
    'name',
    ('', '-inst', '.inst', 'a/b', 'a b', 'inst%2F', 'x' * 129, 'é'),
)
def test_create_consumer_refuses_invalid_names(
    consumer_service: ConsumerService,
    name: str,
) -> None:
    """A name that would not survive a URL path is refused with 42204."""
    with patch('web_service.consumer_service.KafkaConsumer') as mock_kc:
        body, status = consumer_service.create_consumer(
            subject=SUBJECT,
            namespace=NAMESPACE,
            group_name=GROUP_NAME,
            name=name,
        )
    mock_kc.assert_not_called()
    assert status == 422
    assert body == {
        'error_code': 42204,
        'message': (
            'Invalid consumer configuration: name must be 1 to 128 '
            'letters, digits, dots, dashes or underscores, starting with '
            'a letter or digit'
        ),
    }
    assert consumer_service._consumers == {}


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
    mock_kafka_consumer.close.assert_called_once_with(
        autocommit=False,
        timeout_ms=CLOSE_TIMEOUT_MS,
    )
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


@pytest.mark.parametrize('topics', (None, []))
def test_subscribe_no_topics(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
    topics: list[str] | None,
) -> None:
    """Test subscribe with a missing or empty topics list returns 422."""
    body, status = populated_service.subscribe(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
        topics=topics,
    )
    assert status == 422
    assert body['error_code'] == 42204
    mock_kafka_consumer.subscribe.assert_not_called()


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
    mock_kafka_consumer.subscribe.side_effect = KafkaError('Subscribe failed')
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


def test_get_subscription_drops_topics_outside_namespace(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test get_subscription drops and logs topics outside the namespace."""
    mock_kafka_consumer.subscription.return_value = {
        f'{NAMESPACE}.mine',
        'ns-other.theirs',
    }
    with caplog.at_level(logging.WARNING):
        body, status = populated_service.get_subscription(
            SUBJECT,
            NAMESPACE,
            GROUP_NAME,
            INSTANCE_ID,
        )
    assert status == 200
    assert body['topics'] == ['mine']
    assert 'ns-other.theirs' in caplog.text


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
    mock_kafka_consumer.unsubscribe.side_effect = KafkaError('Unsub failed')
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
    """Test poll_records with json format returns the JSON value."""
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
    assert body[0]['value'] == {'msg': 'hello'}
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
    mock_kafka_consumer.poll.side_effect = KafkaError('Poll failed')
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


def test_poll_records_drops_topics_outside_namespace(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test poll_records drops and logs records outside the namespace."""
    mine = TopicPartition(f'{NAMESPACE}.mine', 0)
    theirs = TopicPartition('ns-other.theirs', 0)
    msg = MagicMock()
    msg.key = None
    msg.value = b'v'
    msg.offset = 7
    mock_kafka_consumer.poll.return_value = {mine: [msg], theirs: [msg]}

    with caplog.at_level(logging.WARNING):
        body, status = populated_service.poll_records(
            SUBJECT,
            NAMESPACE,
            GROUP_NAME,
            INSTANCE_ID,
        )
    assert status == 200
    assert [r['topic'] for r in body] == ['mine']
    assert 'ns-other.theirs' in caplog.text


# ============================================================================
# Unit Tests - commit_offsets
# ============================================================================


def test_commit_offsets_with_explicit_offsets(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
) -> None:
    """Test commit_offsets with explicit offset list."""
    mock_kafka_consumer.assignment.return_value = {
        TopicPartition(f'{NAMESPACE}.t1', 0),
    }
    offsets = [{'topic': 't1', 'partition': 0, 'offset': 42}]
    body, status = populated_service.commit_offsets(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
        offsets=offsets,
    )
    # As in Confluent's REST Proxy, an empty list of offsets
    assert status == 200
    assert body == []
    call_kwargs = mock_kafka_consumer.commit.call_args[1]
    tp = TopicPartition(f'{NAMESPACE}.t1', 0)
    assert tp in call_kwargs['offsets']
    # As in Confluent's REST Proxy, the group's offset is the next record
    assert call_kwargs['offsets'][tp] == OffsetAndMetadata(43, '', -1)
    assert call_kwargs['timeout_ms'] == CONSUMER_OPERATION_TIMEOUT_MS


def test_commit_offsets_stores_metadata(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
) -> None:
    """An offset's metadata is committed with it."""
    mock_kafka_consumer.assignment.return_value = {
        TopicPartition(f'{NAMESPACE}.t1', 0),
    }
    offsets = [
        {'topic': 't1', 'partition': 0, 'offset': 9, 'metadata': 'batch 3'},
    ]
    body, status = populated_service.commit_offsets(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
        offsets=offsets,
    )
    assert (body, status) == ([], 200)
    tp = TopicPartition(f'{NAMESPACE}.t1', 0)
    committed = mock_kafka_consumer.commit.call_args.kwargs['offsets']
    assert committed == {tp: OffsetAndMetadata(10, 'batch 3', -1)}


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
    assert body == []
    mock_kafka_consumer.commit.assert_called_once_with(
        timeout_ms=CONSUMER_OPERATION_TIMEOUT_MS,
    )


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
    mock_kafka_consumer.commit.side_effect = KafkaError('Commit failed')
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
    """Test get_committed returns offsets and metadata, as Confluent does.

    A partition the group never committed is left out.
    """
    mock_kafka_consumer.committed.side_effect = [
        OffsetAndMetadata(42, 'note', -1),
        None,
        OffsetAndMetadata(7, None, -1),
    ]
    partitions = [
        {'topic': 't1', 'partition': 0},
        {'topic': 't2', 'partition': 0},
        {'topic': 't3', 'partition': 0},
    ]
    body, status = populated_service.get_committed(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
        partitions=partitions,
    )
    assert status == 200
    assert body == {
        'offsets': [
            {'topic': 't1', 'partition': 0, 'offset': 42, 'metadata': 'note'},
            {'topic': 't3', 'partition': 0, 'offset': 7, 'metadata': ''},
        ],
    }
    for c in mock_kafka_consumer.committed.call_args_list:
        assert c.kwargs['metadata'] is True


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
    mock_kafka_consumer.committed.side_effect = KafkaError('Failed')
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
    mock_kafka_consumer.assign.side_effect = KafkaError('Assign failed')
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


def test_get_assignments_drops_topics_outside_namespace(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test get_assignments drops and logs topics outside the namespace."""
    mock_kafka_consumer.assignment.return_value = {
        TopicPartition(f'{NAMESPACE}.mine', 0),
        TopicPartition('ns-other.theirs', 0),
    }
    with caplog.at_level(logging.WARNING):
        body, status = populated_service.get_assignments(
            SUBJECT,
            NAMESPACE,
            GROUP_NAME,
            INSTANCE_ID,
        )
    assert status == 200
    assert body['partitions'] == [{'topic': 'mine', 'partition': 0}]
    assert 'ns-other.theirs' in caplog.text


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
    mock_kafka_consumer.assignment.return_value = {
        TopicPartition(f'{NAMESPACE}.t1', 0),
    }
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
    mock_kafka_consumer.assignment.return_value = {
        TopicPartition(f'{NAMESPACE}.t1', 0),
    }
    mock_kafka_consumer.seek.side_effect = KafkaError('Seek failed')
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
    mock_kafka_consumer.assignment.return_value = {
        TopicPartition(f'{NAMESPACE}.t1', 0),
    }
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
    mock_kafka_consumer.assignment.return_value = {
        TopicPartition(f'{NAMESPACE}.t1', 0),
    }
    mock_kafka_consumer.seek_to_beginning.side_effect = KafkaError('Failed')
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
    mock_kafka_consumer.assignment.return_value = {
        TopicPartition(f'{NAMESPACE}.t1', 0),
    }
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
    mock_kafka_consumer.assignment.return_value = {
        TopicPartition(f'{NAMESPACE}.t1', 0),
    }
    mock_kafka_consumer.seek_to_end.side_effect = KafkaError('Failed')
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
    mock1.close.assert_called_once_with(
        autocommit=False,
        timeout_ms=CLOSE_TIMEOUT_MS,
    )
    mock2.close.assert_called_once_with(
        autocommit=False,
        timeout_ms=CLOSE_TIMEOUT_MS,
    )
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
    mock_consumer.close.assert_called_once_with(
        autocommit=False,
        timeout_ms=CLOSE_TIMEOUT_MS,
    )


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
# Unit Tests - Bounded waits and instance locking
# ============================================================================

# Each instance operation, with the arguments that pass validation
_INSTANCE_OPS: list[tuple[str, dict[str, Any], str]] = [
    ('subscribe', {'topics': ['t1']}, 'subscribe'),
    ('get_subscription', {}, 'subscription'),
    ('unsubscribe', {}, 'unsubscribe'),
    ('poll_records', {}, 'poll'),
    ('commit_offsets', {}, 'commit'),
    (
        'get_committed',
        {'partitions': [{'topic': 't1', 'partition': 0}]},
        'committed',
    ),
    (
        'assign_partitions',
        {'partitions': [{'topic': 't1', 'partition': 0}]},
        'assign',
    ),
    ('get_assignments', {}, 'assignment'),
    (
        'seek',
        {'offsets': [{'topic': 't1', 'partition': 0, 'offset': 1}]},
        'seek',
    ),
    (
        'seek_to_beginning',
        {'partitions': [{'topic': 't1', 'partition': 0}]},
        'seek_to_beginning',
    ),
    (
        'seek_to_end',
        {'partitions': [{'topic': 't1', 'partition': 0}]},
        'seek_to_end',
    ),
    ('delete_consumer', {}, 'close'),
]


def _managed(populated: ConsumerService) -> ManagedConsumer:
    return populated._consumers[(SUBJECT, NAMESPACE, GROUP_NAME, INSTANCE_ID)]


def _new_managed(instance_id: str) -> ManagedConsumer:
    return ManagedConsumer(
        consumer=MagicMock(),
        subject='u',
        namespace='ns',
        group_name='g',
        instance_id=instance_id,
        format_type='binary',
    )


@pytest.mark.parametrize(('method', 'kwargs', 'kafka_call'), _INSTANCE_OPS)
def test_busy_instance_returns_409(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
    method: str,
    kwargs: dict[str, Any],
    kafka_call: str,
) -> None:
    """A request on an instance another request holds gets 409, not a wait."""
    populated_service.lock_timeout_s = 0.05
    managed = _managed(populated_service)
    managed.lock.acquire()
    try:
        body, status = getattr(populated_service, method)(
            SUBJECT,
            NAMESPACE,
            GROUP_NAME,
            INSTANCE_ID,
            **kwargs,
        )
    finally:
        managed.lock.release()
    assert status == 409
    assert body == {
        'error_code': 40904,
        'message': 'Consumer instance is busy with another request.',
    }
    getattr(mock_kafka_consumer, kafka_call).assert_not_called()
    # The busy instance is kept
    assert len(populated_service._consumers) == 1


@pytest.mark.parametrize(('method', 'kwargs', 'kafka_call'), _INSTANCE_OPS)
def test_instance_closed_while_waiting_returns_404(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
    method: str,
    kwargs: dict[str, Any],
    kafka_call: str,
) -> None:
    """A request that finds the instance closed once it gets the lock: 404."""
    _managed(populated_service).closed = True
    body, status = getattr(populated_service, method)(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
        **kwargs,
    )
    assert status == 404
    assert body['error_code'] == 40403
    getattr(mock_kafka_consumer, kafka_call).assert_not_called()


def test_instance_lock_waits_for_a_short_request(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
) -> None:
    """A request waits for another that finishes within the lock timeout."""
    managed = _managed(populated_service)
    managed.lock.acquire()
    timer = threading.Timer(0.05, managed.lock.release)
    timer.daemon = True
    timer.start()
    body, status = populated_service.get_subscription(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
    )
    timer.join()
    assert status == 200
    assert body == {'topics': []}


def test_get_committed_bounds_each_lookup(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
) -> None:
    """Every committed() call gets a timeout within the operation bound."""
    mock_kafka_consumer.committed.return_value = OffsetAndMetadata(7, '', -1)
    populated_service.get_committed(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
        partitions=[
            {'topic': 't1', 'partition': 0},
            {'topic': 't2', 'partition': 0},
        ],
    )
    assert mock_kafka_consumer.committed.call_count == 2
    for c in mock_kafka_consumer.committed.call_args_list:
        assert 0 < c.kwargs['timeout_ms'] <= CONSUMER_OPERATION_TIMEOUT_MS


def test_commit_timeout_is_a_kafka_failure(
    populated_service: ConsumerService,
    mock_kafka_consumer: MagicMock,
) -> None:
    """A commit that times out is a retriable Kafka failure (50003)."""
    mock_kafka_consumer.commit.side_effect = KafkaTimeoutError('timed out')
    body, status = populated_service.commit_offsets(
        SUBJECT,
        NAMESPACE,
        GROUP_NAME,
        INSTANCE_ID,
    )
    assert status == 500
    assert body == {
        'error_code': 50003,
        'message': (
            'Failed to commit offsets: Kafka error (KafkaTimeoutError); '
            'retry it'
        ),
    }


def test_create_consumer_builds_outside_the_global_lock(
    consumer_service: ConsumerService,
) -> None:
    """A slow constructor holds back neither other creates nor the lock."""
    slow_started = threading.Event()
    release = threading.Event()
    lock_free_during_build: list[bool] = []

    def _construct(**kwargs: Any) -> MagicMock:
        if kwargs['group_id'] == f'{NAMESPACE}.slow-group':
            slow_started.set()
            free = consumer_service._global_lock.acquire(blocking=False)
            if free:
                consumer_service._global_lock.release()
            lock_free_during_build.append(free)
            release.wait(timeout=5)
        return MagicMock()

    results: dict[str, tuple[Any, int]] = {}

    def _slow_create() -> None:
        results['slow'] = consumer_service.create_consumer(
            subject=SUBJECT,
            namespace=NAMESPACE,
            group_name='slow-group',
            name='slow',
        )

    with patch(
        'web_service.consumer_service.KafkaConsumer',
        side_effect=_construct,
    ):
        thread = threading.Thread(target=_slow_create, daemon=True)
        thread.start()
        try:
            assert slow_started.wait(timeout=5)
            # Another instance's create completes meanwhile
            fast_body, fast_status = consumer_service.create_consumer(
                subject=SUBJECT,
                namespace=NAMESPACE,
                group_name=GROUP_NAME,
                name='fast',
            )
            # A second create of the name being built is refused
            dup_body, dup_status = consumer_service.create_consumer(
                subject=SUBJECT,
                namespace=NAMESPACE,
                group_name='slow-group',
                name='slow',
            )
            # The instance being built is not served yet
            _sub_body, sub_status = consumer_service.get_subscription(
                SUBJECT,
                NAMESPACE,
                'slow-group',
                'slow',
            )
        finally:
            release.set()
            thread.join(timeout=5)

    assert fast_status == 200
    assert fast_body['instance_id'] == 'fast'
    assert dup_status == 409
    assert dup_body['error_code'] == 40902
    assert sub_status == 404
    assert lock_free_during_build == [True]
    assert results['slow'][1] == 200
    assert (SUBJECT, NAMESPACE, 'slow-group', 'slow') in (
        consumer_service._consumers
    )
    assert consumer_service._pending == set()


def test_create_consumer_failure_frees_the_name(
    consumer_service: ConsumerService,
) -> None:
    """A failed build drops its reservation, so the name can be retried."""
    with patch('web_service.consumer_service.KafkaConsumer') as mock_kc:
        mock_kc.side_effect = Exception('Connection refused')
        _body, status = consumer_service.create_consumer(
            subject=SUBJECT,
            namespace=NAMESPACE,
            group_name=GROUP_NAME,
            name='retry-inst',
        )
        assert status == 500
        assert consumer_service._pending == set()
        assert consumer_service._consumers == {}

        mock_kc.side_effect = None
        mock_kc.return_value = MagicMock()
        body, status = consumer_service.create_consumer(
            subject=SUBJECT,
            namespace=NAMESPACE,
            group_name=GROUP_NAME,
            name='retry-inst',
        )
    assert status == 200
    assert body['instance_id'] == 'retry-inst'


def test_shutdown_during_create_closes_the_new_consumer(
    consumer_service: ConsumerService,
) -> None:
    """A create building at shutdown closes its consumer, unregistered."""
    started = threading.Event()
    release = threading.Event()
    built = MagicMock()

    def _construct(**_kwargs: Any) -> MagicMock:
        started.set()
        release.wait(timeout=5)
        return built

    results: list[tuple[Any, int]] = []
    with patch(
        'web_service.consumer_service.KafkaConsumer',
        side_effect=_construct,
    ):
        thread = threading.Thread(
            target=lambda: results.append(
                consumer_service.create_consumer(
                    subject=SUBJECT,
                    namespace=NAMESPACE,
                    group_name=GROUP_NAME,
                    name='late',
                ),
            ),
            daemon=True,
        )
        thread.start()
        assert started.wait(timeout=5)
        consumer_service.shutdown_all()
        release.set()
        thread.join(timeout=5)

    assert not thread.is_alive()
    assert results == [
        (
            {
                'error_code': 503,
                'message': (
                    'Failed to create consumer: the service is shutting '
                    'down; retry'
                ),
            },
            503,
        ),
    ]
    built.close.assert_called_once_with(
        autocommit=False,
        timeout_ms=CLOSE_TIMEOUT_MS,
    )
    assert consumer_service._consumers == {}
    assert consumer_service._pending == set()


def test_create_after_shutdown_is_refused(
    consumer_service: ConsumerService,
) -> None:
    """Once shutdown has begun, a create builds no consumer."""
    consumer_service.shutdown_all()
    with patch('web_service.consumer_service.KafkaConsumer') as mock_kc:
        body, status = consumer_service.create_consumer(
            subject=SUBJECT,
            namespace=NAMESPACE,
            group_name=GROUP_NAME,
            name='late',
        )
    mock_kc.assert_not_called()
    assert status == 503
    assert body == {
        'error_code': 503,
        'message': (
            'Failed to create consumer: the service is shutting down; retry'
        ),
    }
    assert consumer_service._consumers == {}


def test_reaper_skips_busy_and_closes_idle(
    consumer_service: ConsumerService,
) -> None:
    """The reaper keeps an expired instance in use and closes an idle one.

    The idle one is closed under its own lock, outside the global lock.
    """
    consumer_service.lock_timeout_s = 0.05
    idle = _new_managed('idle')
    busy = _new_managed('busy')
    idle.last_access = time.monotonic() - 600
    busy.last_access = time.monotonic() - 600
    consumer_service._consumers[('u', 'ns', 'g', 'idle')] = idle
    consumer_service._consumers[('u', 'ns', 'g', 'busy')] = busy

    seen: dict[str, bool] = {}

    def _close(**_kwargs: Any) -> None:
        seen['instance_lock_held'] = idle.lock.locked()
        free = consumer_service._global_lock.acquire(blocking=False)
        if free:
            consumer_service._global_lock.release()
        seen['global_lock_free'] = free

    idle.consumer.close.side_effect = _close
    busy.lock.acquire()
    try:
        consumer_service._reap_once()
    finally:
        busy.lock.release()

    idle.consumer.close.assert_called_once_with(
        autocommit=False,
        timeout_ms=CLOSE_TIMEOUT_MS,
    )
    assert seen == {'instance_lock_held': True, 'global_lock_free': True}
    assert idle.closed
    busy.consumer.close.assert_not_called()
    assert not busy.closed
    assert list(consumer_service._consumers) == [('u', 'ns', 'g', 'busy')]

    # Once free, the busy one is reaped on a later round
    consumer_service._reap_once()
    busy.consumer.close.assert_called_once()
    assert consumer_service._consumers == {}


def test_reaper_keeps_instance_used_since_scan(
    consumer_service: ConsumerService,
) -> None:
    """An instance used while the reaper waited for its lock is kept."""
    managed = _new_managed('inst')
    consumer_service._consumers[('u', 'ns', 'g', 'inst')] = managed
    # Expired at the scan, used before the reaper got its lock
    with patch.object(managed, 'is_expired', side_effect=[True, False]):
        consumer_service._reap_once()
    managed.consumer.close.assert_not_called()
    assert len(consumer_service._consumers) == 1
    assert not managed.lock.locked()


def test_shutdown_all_skips_busy_instance(
    consumer_service: ConsumerService,
) -> None:
    """Shutdown closes idle instances and leaves one still in use."""
    consumer_service.lock_timeout_s = 0.05
    idle = _new_managed('idle')
    busy = _new_managed('busy')
    consumer_service._consumers[('u', 'ns', 'g', 'idle')] = idle
    consumer_service._consumers[('u', 'ns', 'g', 'busy')] = busy
    busy.lock.acquire()
    try:
        consumer_service.shutdown_all()
    finally:
        busy.lock.release()
    idle.consumer.close.assert_called_once_with(
        autocommit=False,
        timeout_ms=CLOSE_TIMEOUT_MS,
    )
    busy.consumer.close.assert_not_called()
    assert consumer_service._consumers == {}


def _auto_commit_managed(instance_id: str) -> ManagedConsumer:
    managed = _new_managed(instance_id)
    managed.auto_commit = True
    return managed


def test_create_consumer_keeps_auto_commit(
    consumer_service: ConsumerService,
) -> None:
    """An instance remembers whether it commits its positions on close."""
    with patch('web_service.consumer_service.KafkaConsumer'):
        for name, setting in (('on', 'true'), ('off', 'false')):
            consumer_service.create_consumer(
                subject=SUBJECT,
                namespace=NAMESPACE,
                group_name=GROUP_NAME,
                name=name,
                auto_commit_enable=setting,
            )
    managed = {m.instance_id: m for m in consumer_service._consumers.values()}
    assert managed['on'].auto_commit is True
    assert managed['off'].auto_commit is False


def test_delete_with_auto_commit_commits_before_close(
    consumer_service: ConsumerService,
) -> None:
    """A delete of an auto-commit instance commits, then closes."""
    managed = _auto_commit_managed('inst')
    consumer_service._consumers[('u', 'ns', 'g', 'inst')] = managed
    body, status = consumer_service.delete_consumer('u', 'ns', 'g', 'inst')
    assert (body, status) == (None, 204)
    assert managed.consumer.mock_calls == [
        call.commit(timeout_ms=CLOSE_COMMIT_TIMEOUT_MS),
        call.close(autocommit=False, timeout_ms=CLOSE_TIMEOUT_MS),
    ]


def test_delete_without_auto_commit_commits_nothing(
    consumer_service: ConsumerService,
) -> None:
    """A delete of an instance without auto-commit only closes it."""
    managed = _new_managed('inst')
    consumer_service._consumers[('u', 'ns', 'g', 'inst')] = managed
    consumer_service.delete_consumer('u', 'ns', 'g', 'inst')
    assert managed.consumer.mock_calls == [
        call.close(autocommit=False, timeout_ms=CLOSE_TIMEOUT_MS),
    ]


def test_a_failed_commit_on_close_still_closes(
    consumer_service: ConsumerService,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A commit that fails on close is logged, and the close goes on."""
    managed = _auto_commit_managed('inst')
    managed.consumer.commit.side_effect = KafkaTimeoutError('timed out')
    consumer_service._consumers[('u', 'ns', 'g', 'inst')] = managed
    with caplog.at_level(logging.WARNING, logger='web_service'):
        body, status = consumer_service.delete_consumer(
            'u',
            'ns',
            'g',
            'inst',
        )
    assert (body, status) == (None, 204)
    managed.consumer.close.assert_called_once_with(
        autocommit=False,
        timeout_ms=CLOSE_TIMEOUT_MS,
    )
    assert 'Failed to commit the positions of consumer inst' in caplog.text


def test_reaper_commits_an_auto_commit_instance(
    consumer_service: ConsumerService,
) -> None:
    """The reaper commits an expired auto-commit instance before closing."""
    committing = _auto_commit_managed('committing')
    plain = _new_managed('plain')
    for managed in (committing, plain):
        managed.last_access = time.monotonic() - 600
        consumer_service._consumers[('u', 'ns', 'g', managed.instance_id)] = (
            managed
        )
    consumer_service._reap_once()
    assert committing.consumer.mock_calls == [
        call.commit(timeout_ms=CLOSE_COMMIT_TIMEOUT_MS),
        call.close(autocommit=False, timeout_ms=CLOSE_TIMEOUT_MS),
    ]
    plain.consumer.commit.assert_not_called()
    assert consumer_service._consumers == {}


def test_shutdown_commits_auto_commit_instances(
    consumer_service: ConsumerService,
) -> None:
    """Shutdown commits auto-commit instances and only closes the others."""
    committing = _auto_commit_managed('committing')
    plain = _new_managed('plain')
    consumer_service._consumers[('u', 'ns', 'g', 'committing')] = committing
    consumer_service._consumers[('u', 'ns', 'g', 'plain')] = plain
    consumer_service.shutdown_all()
    assert [c[0] for c in committing.consumer.mock_calls] == [
        'commit',
        'close',
    ]
    commit_ms = committing.consumer.commit.call_args.kwargs['timeout_ms']
    assert 0 < commit_ms <= CLOSE_COMMIT_TIMEOUT_MS
    plain.consumer.commit.assert_not_called()
    plain.consumer.close.assert_called_once()


def test_shutdown_closes_instances_at_once(
    consumer_service: ConsumerService,
) -> None:
    """Shutdown closes the instances concurrently, not one after another."""
    both = threading.Barrier(2, timeout=5)
    managed = [_auto_commit_managed('a'), _auto_commit_managed('b')]
    for m in managed:
        # Each commit returns only once the other has started
        m.consumer.commit.side_effect = lambda **_: both.wait()
        consumer_service._consumers[('u', 'ns', 'g', m.instance_id)] = m
    consumer_service.shutdown_all()
    assert not both.broken
    for m in managed:
        m.consumer.close.assert_called_once()


def test_shutdown_waits_no_longer_than_its_deadline(
    consumer_service: ConsumerService,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A close still running at the deadline is left, and shutdown returns."""
    release = threading.Event()
    stuck = _auto_commit_managed('stuck')
    stuck.consumer.commit.side_effect = lambda **_: release.wait(5)
    consumer_service._consumers[('u', 'ns', 'g', 'stuck')] = stuck
    try:
        with (
            patch('web_service.consumer_service.SHUTDOWN_TIMEOUT_S', 0.2),
            caplog.at_level(logging.WARNING, logger='web_service'),
        ):
            start = time.monotonic()
            consumer_service.shutdown_all()
            took = time.monotonic() - start
    finally:
        release.set()
    assert took < 2
    assert '1 consumers not closed within' in caplog.text


def test_shutdown_deadline_is_for_all_instances_together(
    consumer_service: ConsumerService,
) -> None:
    """Sixteen slow commits take the deadline once, not sixteen times."""
    managed = [_auto_commit_managed(f'i{i}') for i in range(16)]
    for m in managed:
        m.consumer.commit.side_effect = lambda **_: time.sleep(0.3)
        consumer_service._consumers[('u', 'ns', 'g', m.instance_id)] = m
    with patch('web_service.consumer_service.SHUTDOWN_TIMEOUT_S', 1):
        start = time.monotonic()
        consumer_service.shutdown_all()
        took = time.monotonic() - start
    assert took < 1
    for m in managed:
        assert m.consumer.commit.call_args.kwargs['timeout_ms'] <= 1000
        m.consumer.close.assert_called_once()


def test_close_waits_within_the_deadline(
    consumer_service: ConsumerService,
) -> None:
    """A deadline shortens the commit's and the close's waits."""
    managed = _auto_commit_managed('inst')
    consumer_service._close(
        managed,
        'Error closing consumer %s',
        deadline=time.monotonic() + 0.5,
    )
    commit_ms = managed.consumer.commit.call_args.kwargs['timeout_ms']
    close_ms = managed.consumer.close.call_args.kwargs['timeout_ms']
    assert 0 <= commit_ms <= 500
    assert 0 <= close_ms <= 500
    assert managed.closed
