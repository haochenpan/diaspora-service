"""Produce action for the Globus Action Provider."""

from __future__ import annotations

import json
import os

from globus_action_provider_tools import ActionRequest
from globus_action_provider_tools import ActionStatusValue
from globus_action_provider_tools import AuthState
from globus_action_provider_tools.flask.types import ActionCallbackReturn
from kafka import KafkaProducer

from action_provider.utils import build_action_status
from action_provider.utils import MSKTokenProviderFromRole

DEFAULT_SERVERS = os.environ['DEFAULT_SERVERS']


def create_producer(
    servers: str,
    open_id: str,
) -> KafkaProducer:
    """Create a Kafka producer with the user identity.

    Note: the call should succeed even the user does not exist.
    """
    conf = {
        'bootstrap_servers': servers,
        'security_protocol': 'SASL_SSL',
        'sasl_mechanism': 'OAUTHBEARER',
        'api_version': (3, 5, 1),
        'sasl_oauth_token_provider': MSKTokenProviderFromRole(open_id),
        'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
        'max_block_ms': 10000,
    }

    producer = KafkaProducer(**conf)
    return producer


def action_produce(
    request: ActionRequest,
    auth: AuthState,
) -> ActionCallbackReturn:
    """AP action for producing events."""
    servers = request.body.get('servers', DEFAULT_SERVERS)
    caller_id = auth.effective_identity
    open_id = caller_id.split(':')[-1]
    topic = request.body['topic']

    producer = create_producer(
        servers,
        open_id,
    )

    try:
        key = request.body.get('key', None)
        value = request.body.get('value', None)
        if value is not None:
            msg_futures = []
            msg_future = producer.send(
                topic,
                value=value,
                key=key.encode('utf-8') if key else None,
            )
            msg_futures.append(msg_future)

        else:
            msgs = request.body.get('msgs', None)
            keys = request.body.get('keys', None)

            if msgs is None:
                raise ValueError("'msgs' does not exist.")

            if len(msgs) == 0:
                raise ValueError("'msgs' is empty.")

            if isinstance(keys, list) and len(keys) != len(msgs):
                raise ValueError(
                    f"The len of 'keys' ({len(keys)}) must match "
                    f"that of 'msgs' ({len(msgs)}).",
                )

            if keys is None or isinstance(keys, str):
                keys = [keys] * len(msgs)

            msg_futures = []
            for msg, key in zip(msgs, keys):
                msg_future = producer.send(
                    topic,
                    value=msg,
                    key=key.encode('utf-8') if key else None,
                )
                msg_futures.append(msg_future)

        resolved_futures = [future.get(timeout=10) for future in msg_futures]

        result = {}
        for record_metadata in resolved_futures:
            msg_key = f'{record_metadata.partition}-{record_metadata.offset}'
            result[msg_key] = record_metadata.timestamp

        return build_action_status(
            auth,
            ActionStatusValue.SUCCEEDED,
            request,
            result,
        )

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
        producer.close()


if __name__ == '__main__':
    pass
