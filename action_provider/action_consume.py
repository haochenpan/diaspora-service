"""Consume action for the Globus Action Provider."""

from __future__ import annotations

import contextlib
import json
import os
from collections import defaultdict
from datetime import datetime
from datetime import timezone
from typing import Any

from globus_action_provider_tools import ActionRequest
from globus_action_provider_tools import ActionStatusValue
from globus_action_provider_tools import AuthState
from globus_action_provider_tools.flask.types import ActionCallbackReturn
from kafka import KafkaConsumer
from kafka import TopicPartition
from kafka.consumer.fetcher import ConsumerRecord

from action_provider.utils import build_action_status
from action_provider.utils import MSKTokenProviderFromRole

CLIENT_ID = os.environ['CLIENT_ID']
CLIENT_SECRET = os.environ['CLIENT_SECRET']
CLIENT_SCOPE = os.environ['CLIENT_SCOPE']
DEFAULT_SERVERS = os.environ['DEFAULT_SERVERS']

TIMESTAMP_TYPE_MAPPING = {0: 'CREATE_TIME', 1: 'LOG_APPEND_TIME'}


def create_consumer(
    servers: str,
    open_id: str,
    topic: str,
    # group_id: str,
) -> KafkaConsumer:
    """Create a Kafka consumer with the user identity and requested topic.

    Note: the call should succeed even the user does not exist or
    does not have access to the topic.
    """
    conf = {
        'bootstrap_servers': servers,
        'security_protocol': 'SASL_SSL',
        'sasl_mechanism': 'OAUTHBEARER',
        'api_version': (3, 5, 1),
        'sasl_oauth_token_provider': MSKTokenProviderFromRole(open_id),
        'auto_offset_reset': 'earliest',
    }

    consumer = KafkaConsumer(**conf)
    consumer.subscribe([topic])
    return consumer


def update_messages(
    messages: dict[str, list[dict[str, Any]]],
    records: list[ConsumerRecord],
) -> None:
    """Update the messages dictionary with the list of records."""
    for record in records:
        msg_key = f'{record.topic}-{record.partition}'

        record_key = record.key.decode('utf-8') if record.key else None
        record_value = record.value.decode('utf-8') if record.value else None
        with contextlib.suppress(json.JSONDecodeError):
            record_value = json.loads(record_value) if record_value else None
        record_ts_type = TIMESTAMP_TYPE_MAPPING.get(
            record.timestamp_type,
            'UNKNOWN',
        )

        msg_val = {
            'topic': record.topic,
            'partition': record.partition,
            'offset': record.offset,
            'timestamp': record.timestamp,
            'timestampType': record_ts_type,
            'key': record_key,
            'value': record_value,
            'headers': record.headers,
        }

        messages[msg_key].append(msg_val)


def filter_messages(
    messages: dict[str, list[dict[str, Any]]],
    filters: list[dict[str:Any]],
):
    """Update the messages dictionary with the list of filters."""
    print(filters)

    ret_msgs: dict[str, list[dict[str, Any]]] = {}
    added_messages = set()

    for filter_pattern in filters:
        pattern = filter_pattern.get('Pattern')
        if not pattern:
            continue

        pattern_value = pattern.get('value')
        # print(pattern_value, type(pattern_value))  # For debugging

        for key, criteria in pattern_value.items():
            for topic_partition, msgs in messages.items():
                for message in msgs:
                    message_value = message['value']
                    match = all(
                        (
                            criterion.get('prefix') is None
                            or str(message_value.get(key, '')).startswith(
                                criterion.get('prefix'),
                            )
                        )
                        and (
                            criterion.get('suffix') is None
                            or str(message_value.get(key, '')).endswith(
                                criterion.get('suffix'),
                            )
                        )
                        for criterion in criteria
                    )
                    if match:
                        message_id = (topic_partition, message['offset'])
                        if message_id not in added_messages:
                            if topic_partition not in ret_msgs:
                                ret_msgs[topic_partition] = []
                            ret_msgs[topic_partition].append(message)
                            added_messages.add(message_id)

    return ret_msgs


def action_consume(
    request: ActionRequest,
    auth: AuthState,
) -> ActionCallbackReturn:
    """AP route for consuming events."""
    servers = request.body.get('servers', DEFAULT_SERVERS)
    caller_id = auth.effective_identity
    open_id = caller_id.split(':')[-1]
    topic = request.body['topic']

    consumer = create_consumer(
        servers,
        open_id,
        topic,
    )

    try:
        ts_curr = int(datetime.now(timezone.utc).timestamp() * 1000)
        ts_mill = request.body.get('ts', ts_curr)

        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            raise ValueError(
                'The topic does not exist or the user does not have access',
            )

        topic_partitions = [TopicPartition(topic, p) for p in partitions]
        timestamps = {tp: ts_mill for tp in topic_partitions}

        offsets = consumer.offsets_for_times(timestamps)
        consumer.poll(timeout_ms=10000)  # avoid unassigned partition exception
        for tp, offset in offsets.items():
            if offset:
                consumer.seek(tp, offset.offset)

        # key: topic-partition
        # val: {topic, partition, offset, timestamp,
        #       timestampType, value, headers}
        messages: dict[str, list[dict[str, Any]]] = defaultdict(lambda: [])

        while records := consumer.poll(timeout_ms=1000):
            for _, partition_records in records.items():
                update_messages(messages, partition_records)

        filters = request.body.get('filters', [])
        messages = filter_messages(messages, filters)
        status = build_action_status(
            auth,
            ActionStatusValue.SUCCEEDED,
            request,
            messages,
        )
        return status

    except Exception as e:
        result = {
            'error': str(e),
            'open_id': open_id,
            'request.body': request.body,
        }
        return build_action_status(
            auth,
            ActionStatusValue.FAILED,
            request,
            result,
        )

    finally:
        consumer.close()


if __name__ == '__main__':
    pass
