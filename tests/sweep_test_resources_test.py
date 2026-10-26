"""Tests for the selection and deletion logic of the test-resource sweep."""

from __future__ import annotations

import importlib.util
import sys
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import MagicMock

import boto3
import pytest
from botocore.stub import Stubber
from kafka.errors import UnknownTopicOrPartitionError

from web_service.services import NamespaceService


def _load_sweep() -> ModuleType:
    path = (
        Path(__file__).resolve().parents[1]
        / 'scripts'
        / 'sweep_test_resources.py'
    )
    spec = importlib.util.spec_from_file_location('sweep_test_resources', path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


sweep = _load_sweep()

NOW = datetime(2026, 1, 2, 12, 0, tzinfo=timezone.utc)
CUTOFF = NOW - timedelta(hours=2)
OLD = NOW - timedelta(hours=3)
NEW = NOW - timedelta(minutes=30)
ACCOUNT = '000000000000'
SUBJECT_A = 'test-subject-0123456789abcdef'
SUBJECT_B = 'test-subject-fedcba9876543210'
TABLES = sweep.ServiceTables()
REAL_USER = '3f1e2d4c-aaaa-bbbb-cccc-c0ffee000001'
REAL_USER_NAMESPACE = 'ns-c0ffee000001'
ADMIN_POLICY_ARN = 'arn:aws:iam::aws:policy/AdministratorAccess'


def _client(service: str) -> Any:
    return boto3.client(
        service,
        region_name='us-east-1',
        aws_access_key_id='testing',
        aws_secret_access_key='testing',  # pragma: allowlist secret
    )


def _user(
    name: str,
    created: datetime,
    path: str = '/msk-users/',
) -> dict[str, Any]:
    return {
        'Path': path,
        'UserName': name,
        'UserId': 'AIDAEXAMPLEEXAMPLE01',
        'Arn': f'arn:aws:iam::{ACCOUNT}:user{path}{name}',
        'CreateDate': created,
    }


def _policy(
    name: str,
    created: datetime,
    *,
    attachments: int = 0,
    path: str = '/msk-policy/',
) -> dict[str, Any]:
    return {
        'PolicyName': name,
        'Arn': _policy_arn(name, path),
        'Path': path,
        'CreateDate': created,
        'AttachmentCount': attachments,
    }


def _policy_arn(name: str, path: str = '/msk-policy/') -> str:
    return f'arn:aws:iam::{ACCOUNT}:policy{path}{name}'


# ============================================================================
# Selection
# ============================================================================


def test_select_users_matches_only_old_test_subjects() -> None:
    users = [
        _user(SUBJECT_A, OLD),
        _user(SUBJECT_B, NEW),
        _user('test-subject-0123456789abcdeg', OLD),
        _user('test-subject-0123456789abcde', OLD),
        _user('test-subject-0123456789ABCDEF', OLD),
        _user('3f1e2d4c-aaaa-bbbb-cccc-0123456789ab', OLD),
        _user('test-user-123', OLD),
        _user(SUBJECT_A + '0', OLD),
        _user('x' + SUBJECT_A, OLD),
        _user('test-subject-1111111111111111', OLD, path='/'),
    ]

    assert sweep.select_users(users, CUTOFF) == [SUBJECT_A]


def test_select_policies_skips_young_foreign_and_attached() -> None:
    policies = [
        _policy(SUBJECT_A, OLD, attachments=1),
        _policy(SUBJECT_B, OLD),
        _policy('test-subject-2222222222222222', OLD, attachments=1),
        _policy('test-subject-3333333333333333', NEW),
        _policy('test-subject-4444444444444444', OLD, path='/'),
        _policy('3f1e2d4c-aaaa-bbbb-cccc-0123456789ab', OLD),
    ]

    selected = sweep.select_policies(policies, [SUBJECT_A], CUTOFF)

    assert selected == [_policy_arn(SUBJECT_A), _policy_arn(SUBJECT_B)]


@pytest.mark.parametrize(
    ('topic', 'namespace'),
    (
        (
            'test-namespace-0123456789abcdef.test-topic-fedcba9876543210',
            'test-namespace-0123456789abcdef',
        ),
        ('ns-0123456789ab.test-topic-1', 'ns-0123456789ab'),
        ('ns-0123456789ab.test-topic-1a2b3c4d', 'ns-0123456789ab'),
        (
            'ns-0123456789ab.test-topic-delete-during-create',
            'ns-0123456789ab',
        ),
        ('ns-0123456789ab.test-topic-3', None),
        ('ns-0123456789ab.test-topic-orders', None),
        ('ns-0123456789ab.test-topic-1a2b3c4', None),
        ('ns-0123456789ab.test-topic-1a2b3c4d5', None),
        ('ns-0123456789ab.test-topic-concurrentx', None),
        ('ns-0123456789ab.test-topic-concurrent-delete-1', None),
        ('ns-0123456789ab.test-topic-1-2', None),
        ('test-namespace-0123456789abcdef.test-topic-1', None),
        ('test-namespace-abc.test-topic-fedcba9876543210', None),
        ('ns-0123456789ab.my-topic', None),
        ('ns-0123456789ab.topic1', None),
        ('ns-0123456789ab.test-topic-', None),
        ('ns-0123456789ab.test-topic-1.extra', None),
        ('ns-0123456789AB.test-topic-1', None),
        ('ns-0123456789abc.test-topic-1', None),
        ('ns-test123.test-topic-1', None),
        ('additional-ns-01234567.test-topic-1', None),
        ('__consumer_offsets', None),
        ('test-topic-1', None),
    ),
)
def test_topic_namespace(topic: str, namespace: str | None) -> None:
    assert sweep.topic_namespace(topic) == namespace


# The leftover test topics found on the cluster; all must match.
LEFTOVER_TOPICS = (
    'ns-137d57d79bd0.test-topic-3e80ab46',
    'ns-14cf31e4299a.test-topic-411a3f39',
    'ns-1cc67d119357.test-topic-concurrent',
    'ns-428eaae3dd6d.test-topic-during-deletion',
    'ns-534c33cac20a.test-topic-concurrent',
    'ns-5c7bca486336.test-topic-8a26fba3',
    'ns-7875c5f75493.test-topic-c7ff0860',
    'ns-c8460bce8e4c.test-topic-1',
    'ns-d20ac3b19e88.test-topic-f6eedbfa',
    'ns-f894261d9a6d.test-topic-c4a1f0ba',
    'ns-f966d36dcaff.test-topic-5f37543c',
)


@pytest.mark.parametrize('topic', LEFTOVER_TOPICS)
def test_topic_namespace_matches_known_leftovers(topic: str) -> None:
    assert sweep.topic_namespace(topic) == topic.partition('.')[0]


@pytest.mark.parametrize(
    'suffix',
    (
        '1',
        '2',
        'concurrent',
        'concurrent-existing',
        'concurrent-delete',
        'during-deletion',
        'delete-during-create',
    ),
)
def test_topic_namespace_matches_fixed_test_topic_names(suffix: str) -> None:
    topic = f'ns-0123456789ab.test-topic-{suffix}'

    assert sweep.topic_namespace(topic) == 'ns-0123456789ab'


@pytest.mark.parametrize(
    'subject',
    (
        SUBJECT_A,
        '3f1e2d4c-aaaa-bbbb-cccc-0123456789ab',
        'abc',
        'no-dashes-here',
    ),
)
def test_default_namespace_matches_the_service(subject: str) -> None:
    service = NamespaceService(dynamodb_service=MagicMock())

    assert sweep.default_namespace(subject) == service.generate_default(
        subject,
    )


def test_select_topics_skips_recorded_namespaces() -> None:
    topics = [
        'ns-0123456789ab.test-topic-1',
        'ns-aaaaaaaaaaaa.test-topic-1',
        'ns-aaaaaaaaaaaa.orders',
        'test-namespace-0123456789abcdef.test-topic-fedcba9876543210',
    ]

    selected = sweep.select_topics(topics, {'ns-aaaaaaaaaaaa'})

    assert selected == [
        'ns-0123456789ab.test-topic-1',
        'test-namespace-0123456789abcdef.test-topic-fedcba9876543210',
    ]


# ============================================================================
# Collection against stubbed AWS clients
# ============================================================================


def _stub_listing(
    iam_stub: Stubber,
    users: list[dict[str, Any]],
    policies: list[dict[str, Any]],
) -> None:
    iam_stub.add_response(
        'list_users',
        {'Users': users, 'IsTruncated': False},
        {'PathPrefix': '/msk-users/'},
    )
    iam_stub.add_response(
        'list_policies',
        {'Policies': policies, 'IsTruncated': False},
        {'Scope': 'Local', 'PathPrefix': '/msk-policy/'},
    )


def test_collect_selects_only_test_resources() -> None:
    iam, dynamodb = _client('iam'), _client('dynamodb')
    kafka_admin = MagicMock()
    kafka_admin.list_topics.return_value = [
        '__consumer_offsets',
        'ns-123e4567e89b.orders',
        'ns-123e4567e89b.test-topic-1',
        'ns-0123456789ab.test-topic-1',
        'ns-abcdefabcdef.test-topic-2',
        'test-namespace-0123456789abcdef.test-topic-fedcba9876543210',
        # Default namespace of a real user, with no record in the tables.
        f'{REAL_USER_NAMESPACE}.test-topic-1',
        # Default namespace of a test user too young to sweep.
        f'{sweep.default_namespace(SUBJECT_B)}.test-topic-1',
        # Default namespace of the old test user being swept.
        f'{sweep.default_namespace(SUBJECT_A)}.test-topic-2',
    ]
    with Stubber(iam) as iam_stub, Stubber(dynamodb) as db_stub:
        _stub_listing(
            iam_stub,
            [
                _user(SUBJECT_A, OLD),
                _user(SUBJECT_B, NEW),
                _user(REAL_USER, OLD),
            ],
            [
                _policy(SUBJECT_A, OLD, attachments=1),
                _policy(SUBJECT_B, NEW, attachments=1),
            ],
        )
        db_stub.add_response(
            'get_item',
            {'Item': {'namespaces': {'SS': ['ns-123e4567e89b']}}},
            {
                'TableName': 'diaspora-users-table',
                'Key': {'subject': {'S': '__global_namespaces__'}},
                'ConsistentRead': True,
            },
        )
        for namespace, item in (
            ('ns-0123456789ab', None),
            (sweep.default_namespace(SUBJECT_A), None),
            ('ns-abcdefabcdef', {'namespace': {'S': 'ns-abcdefabcdef'}}),
            ('test-namespace-0123456789abcdef', None),
        ):
            db_stub.add_response(
                'get_item',
                {'Item': item} if item else {},
                {
                    'TableName': 'diaspora-namespace-table',
                    'Key': {'namespace': {'S': namespace}},
                    'ConsistentRead': True,
                },
            )

        plan = sweep.collect(iam, dynamodb, kafka_admin, CUTOFF, TABLES)

        iam_stub.assert_no_pending_responses()
        db_stub.assert_no_pending_responses()

    assert plan.users == [SUBJECT_A]
    assert plan.policies == [_policy_arn(SUBJECT_A)]
    assert plan.topics == [
        'ns-0123456789ab.test-topic-1',
        f'{sweep.default_namespace(SUBJECT_A)}.test-topic-2',
        'test-namespace-0123456789abcdef.test-topic-fedcba9876543210',
    ]
    assert plan.errors == []


@pytest.mark.parametrize(
    'response',
    (
        {},
        {'Item': {'subject': {'S': '__global_namespaces__'}}},
        {'Item': {'namespaces': {'SS': []}}},
    ),
)
def test_collect_selects_no_topics_without_namespace_list(
    response: dict[str, Any],
) -> None:
    iam = _client('iam')
    dynamodb = MagicMock()
    dynamodb.get_item.return_value = response
    kafka_admin = MagicMock()
    kafka_admin.list_topics.return_value = ['ns-0123456789ab.test-topic-1']
    with Stubber(iam) as iam_stub:
        _stub_listing(iam_stub, [_user(SUBJECT_A, OLD)], [])

        plan = sweep.collect(iam, dynamodb, kafka_admin, CUTOFF, TABLES)

    assert plan.users == [SUBJECT_A]
    assert plan.topics == []
    assert len(plan.errors) == 1
    dynamodb.get_item.assert_called_once()


def test_collect_reads_no_records_without_test_topics() -> None:
    iam = _client('iam')
    dynamodb = MagicMock()
    kafka_admin = MagicMock()
    kafka_admin.list_topics.return_value = ['ns-0123456789ab.orders']
    with Stubber(iam) as iam_stub:
        _stub_listing(iam_stub, [], [])

        plan = sweep.collect(iam, dynamodb, kafka_admin, CUTOFF, TABLES)

    assert plan.topics == []
    assert plan.errors == []
    dynamodb.get_item.assert_not_called()


def test_collect_selects_no_topics_when_records_unreadable() -> None:
    iam, dynamodb = _client('iam'), _client('dynamodb')
    kafka_admin = MagicMock()
    kafka_admin.list_topics.return_value = ['ns-0123456789ab.test-topic-1']
    with Stubber(iam) as iam_stub, Stubber(dynamodb) as db_stub:
        _stub_listing(iam_stub, [_user(SUBJECT_A, OLD)], [])
        db_stub.add_client_error(
            'get_item',
            service_error_code='ResourceNotFoundException',
        )

        plan = sweep.collect(iam, dynamodb, kafka_admin, CUTOFF, TABLES)

    assert plan.users == [SUBJECT_A]
    assert plan.topics == []
    assert len(plan.errors) == 1


def test_collect_without_kafka_skips_topics() -> None:
    iam, dynamodb = _client('iam'), _client('dynamodb')
    with Stubber(iam) as iam_stub, Stubber(dynamodb):
        _stub_listing(iam_stub, [], [])

        plan = sweep.collect(iam, dynamodb, None, CUTOFF, TABLES)

    assert plan.topics == []
    assert plan.skipped
    assert plan.errors == []


# ============================================================================
# Deletion against stubbed AWS clients
# ============================================================================


def test_apply_deletes_user_keys_policy_and_topics() -> None:
    iam = _client('iam')
    kafka_admin = MagicMock()
    kafka_admin.delete_topics.side_effect = [
        None,
        UnknownTopicOrPartitionError(),
    ]
    arn = _policy_arn(SUBJECT_A)
    plan = sweep.Plan(
        users=[SUBJECT_A],
        policies=[arn],
        topics=[
            'ns-0123456789ab.test-topic-1',
            'ns-0123456789ab.test-topic-2',
        ],
    )
    with Stubber(iam) as stub:
        stub.add_response(
            'list_attached_user_policies',
            {
                'AttachedPolicies': [
                    {'PolicyName': SUBJECT_A, 'PolicyArn': arn},
                ],
                'IsTruncated': False,
            },
            {'UserName': SUBJECT_A},
        )
        stub.add_response(
            'list_user_policies',
            {'PolicyNames': [], 'IsTruncated': False},
            {'UserName': SUBJECT_A},
        )
        stub.add_response(
            'list_access_keys',
            {
                'AccessKeyMetadata': [
                    {
                        'UserName': SUBJECT_A,
                        'AccessKeyId': 'AKIAEXAMPLE0000001',
                    },
                ],
                'IsTruncated': False,
            },
            {'UserName': SUBJECT_A},
        )
        stub.add_response(
            'delete_access_key',
            {},
            {'UserName': SUBJECT_A, 'AccessKeyId': 'AKIAEXAMPLE0000001'},
        )
        stub.add_response(
            'detach_user_policy',
            {},
            {'UserName': SUBJECT_A, 'PolicyArn': arn},
        )
        stub.add_response('delete_user', {}, {'UserName': SUBJECT_A})
        stub.add_response(
            'list_policy_versions',
            {
                'Versions': [
                    {'VersionId': 'v2', 'IsDefaultVersion': True},
                    {'VersionId': 'v1', 'IsDefaultVersion': False},
                ],
                'IsTruncated': False,
            },
            {'PolicyArn': arn},
        )
        stub.add_response(
            'delete_policy_version',
            {},
            {'PolicyArn': arn, 'VersionId': 'v1'},
        )
        stub.add_response('delete_policy', {}, {'PolicyArn': arn})

        failures = sweep.apply(plan, iam, kafka_admin)

        stub.assert_no_pending_responses()

    assert failures == []
    assert [c.args[0] for c in kafka_admin.delete_topics.call_args_list] == [
        ['ns-0123456789ab.test-topic-1'],
        ['ns-0123456789ab.test-topic-2'],
    ]


def test_apply_leaves_user_with_unexpected_policy() -> None:
    iam = _client('iam')
    plan = sweep.Plan(users=[SUBJECT_A])
    with Stubber(iam) as stub:
        stub.add_response(
            'list_attached_user_policies',
            {
                'AttachedPolicies': [
                    {
                        'PolicyName': 'AdministratorAccess',
                        'PolicyArn': ADMIN_POLICY_ARN,
                    },
                ],
                'IsTruncated': False,
            },
            {'UserName': SUBJECT_A},
        )
        stub.add_response(
            'list_user_policies',
            {'PolicyNames': [], 'IsTruncated': False},
            {'UserName': SUBJECT_A},
        )

        failures = sweep.apply(plan, iam, None)

        stub.assert_no_pending_responses()

    assert len(failures) == 1
    assert SUBJECT_A in failures[0]


def test_main_dry_run_deletes_nothing(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    iam, dynamodb = _client('iam'), _client('dynamodb')
    clients = {'iam': iam, 'dynamodb': dynamodb}
    monkeypatch.setattr(
        sweep.boto3,
        'client',
        lambda service, **_: clients[service],
    )
    with Stubber(iam) as iam_stub, Stubber(dynamodb):
        _stub_listing(
            iam_stub,
            [_user(SUBJECT_A, datetime(2020, 1, 1, tzinfo=timezone.utc))],
            [],
        )

        code = sweep.main(['--region', 'us-east-1', '--bootstrap-servers', ''])

        iam_stub.assert_no_pending_responses()

    assert code == 0
    out = capsys.readouterr().out
    assert f'would delete IAM user {SUBJECT_A}' in out
    assert '1 resource(s) selected' in out
