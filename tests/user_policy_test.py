"""Tests for the IAM policy the service gives each user."""

from __future__ import annotations

import uuid
from fnmatch import fnmatchcase
from unittest.mock import MagicMock

import pytest

from web_service.services import IAMService
from web_service.services import NamespaceService

ACCOUNT_ID = '123456789012'
REGION = 'us-east-1'
CLUSTER = 'test-cluster'
CLUSTER_UUID = '0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0-2'
SUBJECT = '3f1e2d4c-aaaa-bbbb-cccc-c0ffee000001'
NAMESPACE = 'ns-c0ffee000001'
OTHER_NAMESPACE = 'ns-c0ffee000002'

IAM = IAMService(account_id=ACCOUNT_ID, region=REGION, cluster_name=CLUSTER)


def _arn(kind: str, name: str) -> str:
    """The ARN MSK checks for a topic, group or transactional ID."""
    return (
        f'arn:aws:kafka:{REGION}:{ACCOUNT_ID}:{kind}/{CLUSTER}/'
        f'{CLUSTER_UUID}/{name}'
    )


def _allowed(arn: str) -> bool:
    """Whether the policy's one statement names the ARN.

    IAM's ``*`` matches any characters, ``/`` included, as fnmatch's does.
    """
    (statement,) = IAM.generate_user_policy(SUBJECT)['Statement']
    return any(fnmatchcase(arn, pattern) for pattern in statement['Resource'])


def test_policy_namespace_is_the_default_namespace() -> None:
    """The policy names the namespace the service creates for the user."""
    namespaces = NamespaceService(dynamodb_service=MagicMock())

    assert namespaces.generate_default(SUBJECT) == NAMESPACE


@pytest.mark.parametrize('kind', ('topic', 'group', 'transactional-id'))
def test_policy_allows_names_in_the_namespace(kind: str) -> None:
    """Names that start with the user's namespace and a dot are allowed."""
    assert _allowed(_arn(kind, f'{NAMESPACE}.workers'))
    assert _allowed(_arn(kind, f'{NAMESPACE}.a.b'))


@pytest.mark.parametrize('kind', ('topic', 'group', 'transactional-id'))
@pytest.mark.parametrize(
    'name',
    (
        'workers',
        f'{OTHER_NAMESPACE}.workers',
        f'{NAMESPACE}workers',
        f'x{NAMESPACE}.workers',
        NAMESPACE,
    ),
)
def test_policy_refuses_names_outside_the_namespace(
    kind: str,
    name: str,
) -> None:
    """Other users' names, and names without the prefix, are refused."""
    assert not _allowed(_arn(kind, name))


def test_policy_allows_connecting_to_the_cluster() -> None:
    """The cluster resource stays the whole cluster, for Connect."""
    cluster = f'arn:aws:kafka:{REGION}:{ACCOUNT_ID}:cluster/{CLUSTER}/'
    assert _allowed(cluster + CLUSTER_UUID)


def test_policy_of_another_user_names_its_own_namespace() -> None:
    """Two users' policies share no topic, group or transactional ID."""
    other = IAM.generate_user_policy(str(uuid.uuid4()))
    (statement,) = other['Statement']
    for kind in ('topic', 'group', 'transactional-id'):
        arn = _arn(kind, f'{NAMESPACE}.workers')
        assert not any(fnmatchcase(arn, p) for p in statement['Resource'])
