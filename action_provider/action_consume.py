"""Consume action for the Globus Action Provider."""

from __future__ import annotations

import contextlib
import json
import os
from collections import defaultdict
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

DEFAULT_SERVERS = os.environ['DEFAULT_SERVERS']
TIMESTAMP_TYPE_MAPPING = {0: 'CREATE_TIME', 1: 'LOG_APPEND_TIME'}


def create_consumer(
    servers: str,
    open_id: str,
    topic: str,
    group_id: str | None,
) -> KafkaConsumer:
    """Create a Kafka consumer with the user identity and requested topic.

    This function sets up the consumer with parameters such as server
    addresses, security protocols, and topic subscriptions.
    The 'group_id' parameter allows for consumer group management,
    where 'None' means no consumer group association.

    Note: the call should succeed even if the user does not exist or does not
      have access to the topic.

    Args:
        servers (str): A string specifying the Kafka bootstrap servers.
        open_id (str): The OpenID identifier for the user.
        topic (str): The topic from which messages will be consumed.
        group_id (str | None): The consumer group ID, or None for no
        group association.

    Returns:
        KafkaConsumer: An instance of KafkaConsumer configured with the
        specified parameters.
    """
    conf = {
        'bootstrap_servers': servers,
        'security_protocol': 'SASL_SSL',
        'sasl_mechanism': 'OAUTHBEARER',
        'api_version': (3, 5, 1),
        'sasl_oauth_token_provider': MSKTokenProviderFromRole(open_id),
        'auto_offset_reset': 'earliest',
        'group_id': group_id,
        'enable_auto_commit': False,  # deterministic and manual commit
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


def retrieve_messages(
    consumer: KafkaConsumer,
) -> dict[str, list[dict[str, Any]]]:
    """Retrieve messages from a Kafka consumer.

    This function polls a Kafka consumer for messages, processing and
    collecting them into a dictionary.

    Args:
        consumer (KafkaConsumer): The Kafka consumer instance to poll for
        messages.

    Returns:
        dict[str, list[dict[str, Any]]]: A dictionary of messages grouped by
                                         topic-partition.
    """
    messages: dict[str, list[dict[str, Any]]] = defaultdict(lambda: [])

    while records := consumer.poll(timeout_ms=1000):
        for _, partition_records in records.items():
            update_messages(messages, partition_records)

    return messages


def filter_messages_on_ts(
    messages: dict[str, list[dict[str, Any]]],
    ts: int,
) -> dict[str, list[dict[str, Any]]]:
    """Update the messages dictionary with the list of filters."""
    for topic_partition in messages:
        messages[topic_partition] = [
            message
            for message in messages[topic_partition]
            if message['timestamp'] >= ts
        ]
    return messages


def filter_messages_on_patterns(
    messages: dict[str, list[dict[str, Any]]],
    filters: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """Update the messages dictionary with the list of filters."""
    # print(filters)

    ret_msgs: dict[str, list[dict[str, Any]]] = {}
    added_messages = set()

    for filter_pattern in filters:
        pattern = filter_pattern['Pattern']
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


def filter_msgs(
    consumer: KafkaConsumer,
    ts: int | None,
    filters: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """Apply timestamp and pattern filters to the messages retrieved.

    The function retrieves messages and then filters them based on a provided
    timestamp and pattern filters. The 'filters' parameter is a list of
    pattern matching rules that define which messages to include based on their
      content.

    Args:
        consumer (KafkaConsumer): The Kafka consumer instance to poll for
        messages.
        ts (int | None): A timestamp to filter messages, where only messages
        newer than this timestamp will be included.
        filters (list[dict[str, Any]]): A list of filters specifying patterns
        to apply when filtering messages.

    Returns:
        dict[str, list[dict[str, Any]]]: A dictionary of messages grouped by
        topic-partition that meet the filtering criteria.
    """
    # if ts is not provided OR group_id is set, retrieve directly
    messages = retrieve_messages(consumer)

    # 1st filter on ts
    if ts:
        messages = filter_messages_on_ts(messages, ts)

    # 2nd filter on pattern syntax
    if len(filters) != 0:
        messages = filter_messages_on_patterns(messages, filters)

    return messages


def action_consume(
    request: ActionRequest,
    auth: AuthState,
) -> ActionCallbackReturn:
    """AP route for consuming events."""
    servers = request.body.get('servers', DEFAULT_SERVERS)
    caller_id = auth.effective_identity
    open_id = caller_id.split(':')[-1]
    topic = request.body['topic']
    group_id = request.body.get('group_id', None)
    print('topic:', topic)
    print('group_id:', group_id)

    consumer = create_consumer(
        servers,
        open_id,
        topic,
        group_id,
    )

    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            raise ValueError(
                'The topic does not exist or the user does not have access',
            )

        filters = request.body.get('filters', [])
        for filter_pattern in filters:
            pattern = filter_pattern.get('Pattern')
            if not pattern:
                raise ValueError(
                    f'Invalid filter pattern: {filter_pattern}',
                )

        # if ts and no group_id, seek to the offset then retrieve msgs
        ts = request.body.get('ts', None)
        if ts and group_id is None:
            topic_partitions = [TopicPartition(topic, p) for p in partitions]
            timestamps = {tp: ts for tp in topic_partitions}
            offsets = consumer.offsets_for_times(timestamps)
            consumer.poll(timeout_ms=10000)  # avoid unassigned partition error
            for tp, offset in offsets.items():
                if offset:  # TODO: sometimes the tests fail here, rerun!
                    consumer.seek(tp, offset.offset)

        messages = filter_msgs(consumer, ts, filters)
        status = build_action_status(
            auth,
            ActionStatusValue.SUCCEEDED,
            request,
            messages,
        )

        if group_id is not None:
            consumer.commit()

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
