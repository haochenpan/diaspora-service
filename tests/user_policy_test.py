"""Tests for the IAM policy the service gives each user."""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from datetime import UTC
from fnmatch import fnmatchcase
from typing import Any
from unittest.mock import MagicMock
from urllib.parse import quote

import pytest
from botocore.stub import Stubber

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


POLICY_ARN = f'arn:aws:iam::{ACCOUNT_ID}:policy/msk-policy/{SUBJECT}'
OLD_DOCUMENT = {
    'Version': '2012-10-17',
    'Statement': [
        {'Effect': 'Allow', 'Action': 'kafka-cluster:*', 'Resource': '*'},
    ],
}


def _iam() -> IAMService:
    return IAMService(
        account_id=ACCOUNT_ID,
        region=REGION,
        cluster_name=CLUSTER,
    )


def _version(version_id: str, day: int, *, default: bool) -> dict[str, Any]:
    return {
        'VersionId': version_id,
        'IsDefaultVersion': default,
        'CreateDate': datetime(2026, 1, day, tzinfo=UTC),
    }


def _stub_default(stubber: Stubber, document: dict[str, Any]) -> None:
    """Stub the reads of the default version, v1, holding document."""
    stubber.add_response(
        'get_policy',
        {'Policy': {'Arn': POLICY_ARN, 'DefaultVersionId': 'v1'}},
        {'PolicyArn': POLICY_ARN},
    )
    stubber.add_response(
        'get_policy_version',
        # IAM sends the document URL-encoded; boto3 parses it
        {'PolicyVersion': {'Document': quote(json.dumps(document))}},
        {'PolicyArn': POLICY_ARN, 'VersionId': 'v1'},
    )


def _stub_create(stubber: Stubber, document: dict[str, Any]) -> None:
    stubber.add_response(
        'create_policy_version',
        {'PolicyVersion': {'VersionId': 'v6', 'IsDefaultVersion': True}},
        {
            'PolicyArn': POLICY_ARN,
            'PolicyDocument': json.dumps(document),
            'SetAsDefault': True,
        },
    )


def test_update_user_policy_leaves_a_current_policy() -> None:
    """A default version that matches the template is left as it is."""
    iam = _iam()
    with Stubber(iam.iam) as stubber:
        _stub_default(stubber, iam.generate_user_policy(SUBJECT))
        result = iam.update_user_policy(SUBJECT)
        stubber.assert_no_pending_responses()

    assert result['status'] == 'success'
    assert result['changed'] is False
    assert result['old_version'] == result['new_version'] == 'v1'


def test_update_user_policy_adds_a_default_version() -> None:
    """A policy that differs gets the template as its new default."""
    iam = _iam()
    template = iam.generate_user_policy(SUBJECT)
    with Stubber(iam.iam) as stubber:
        _stub_default(stubber, OLD_DOCUMENT)
        stubber.add_response(
            'list_policy_versions',
            {'Versions': [_version('v1', 1, default=True)]},
            {'PolicyArn': POLICY_ARN},
        )
        _stub_create(stubber, template)
        result = iam.update_user_policy(SUBJECT)
        stubber.assert_no_pending_responses()

    assert result['status'] == 'success'
    assert result['changed'] is True
    assert (result['old_version'], result['new_version']) == ('v1', 'v6')


def test_update_user_policy_deletes_the_oldest_version_at_the_limit() -> None:
    """At five versions, the oldest one that is not the default goes."""
    iam = _iam()
    template = iam.generate_user_policy(SUBJECT)
    versions = [
        _version('v1', 1, default=True),
        _version('v2', 9, default=False),
        _version('v3', 2, default=False),
        _version('v4', 5, default=False),
        _version('v5', 7, default=False),
    ]
    with Stubber(iam.iam) as stubber:
        _stub_default(stubber, OLD_DOCUMENT)
        stubber.add_response(
            'list_policy_versions',
            {'Versions': versions},
            {'PolicyArn': POLICY_ARN},
        )
        stubber.add_response(
            'delete_policy_version',
            {},
            {'PolicyArn': POLICY_ARN, 'VersionId': 'v3'},
        )
        _stub_create(stubber, template)
        result = iam.update_user_policy(SUBJECT)
        stubber.assert_no_pending_responses()

    assert result['changed'] is True
    assert result['new_version'] == 'v6'


def test_update_user_policy_reports_a_missing_policy() -> None:
    """A user without a policy is a failure, and nothing is created."""
    iam = _iam()
    with Stubber(iam.iam) as stubber:
        stubber.add_client_error(
            'get_policy',
            service_error_code='NoSuchEntity',
            http_status_code=404,
        )
        result = iam.update_user_policy(SUBJECT)
        stubber.assert_no_pending_responses()

    assert result['status'] == 'failure'
    assert 'NoSuchEntity' in result['message']
