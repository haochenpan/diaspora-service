"""Unit tests: every DynamoDBService read is strongly consistent.

A default GetItem is eventually consistent and can miss a write made just
before it, such as a key stored a moment earlier.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest
from botocore.stub import Stubber

from web_service.services import DynamoDBService
from web_service.services import GLOBAL_NAMESPACES_KEY

KEYS_TABLE = 'test-diaspora-keys-table'
USERS_TABLE = 'test-diaspora-users-table'
NAMESPACE_TABLE = 'test-diaspora-namespace-table'

READS: list[
    tuple[str, Callable[[DynamoDBService], Any], str, dict[str, Any]]
] = [
    (
        'get_key',
        lambda db: db.get_key('subject-1'),
        KEYS_TABLE,
        {'subject': {'S': 'subject-1'}},
    ),
    (
        'get_user_namespaces',
        lambda db: db.get_user_namespaces('subject-1'),
        USERS_TABLE,
        {'subject': {'S': 'subject-1'}},
    ),
    (
        'get_global_namespaces',
        lambda db: db.get_global_namespaces(),
        USERS_TABLE,
        {'subject': {'S': GLOBAL_NAMESPACES_KEY}},
    ),
    (
        'get_namespace_topics',
        lambda db: db.get_namespace_topics('ns-1'),
        NAMESPACE_TABLE,
        {'namespace': {'S': 'ns-1'}},
    ),
]


@pytest.mark.parametrize(
    ('read', 'table', 'key'),
    [(read, table, key) for _, read, table, key in READS],
    ids=[name for name, *_ in READS],
)
def test_read_is_consistent(
    read: Callable[[DynamoDBService], Any],
    table: str,
    key: dict[str, Any],
) -> None:
    """Each read asks DynamoDB for a strongly consistent GetItem."""
    db = DynamoDBService(
        region='us-east-1',
        keys_table_name=KEYS_TABLE,
        users_table_name=USERS_TABLE,
        namespace_table_name=NAMESPACE_TABLE,
    )
    with Stubber(db.dynamodb) as stub:
        stub.add_response(
            'get_item',
            {},
            {'TableName': table, 'Key': key, 'ConsistentRead': True},
        )
        read(db)
        stub.assert_no_pending_responses()
