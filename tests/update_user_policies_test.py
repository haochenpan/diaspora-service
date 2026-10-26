"""Tests for the script that brings users' IAM policies up to date."""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import datetime
from datetime import timezone
from pathlib import Path
from types import ModuleType
from typing import Any
from urllib.parse import quote

import boto3
import pytest
from botocore.stub import Stubber

from web_service.services import GLOBAL_NAMESPACES_KEY
from web_service.services import IAMService


def _load_script() -> ModuleType:
    path = (
        Path(__file__).resolve().parents[1]
        / 'scripts'
        / 'update_user_policies.py'
    )
    spec = importlib.util.spec_from_file_location('update_user_policies', path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


script = _load_script()

ACCOUNT = '000000000000'
CREATED = datetime(2026, 1, 1, tzinfo=timezone.utc)
# Subjects in the order the script visits them
CURRENT = 'aaaaaaaa-0000-0000-0000-000000000001'
STALE = 'bbbbbbbb-0000-0000-0000-000000000002'
NO_USER = 'cccccccc-0000-0000-0000-000000000003'
NO_POLICY = 'dddddddd-0000-0000-0000-000000000004'
OTHER_BOUNDARY = 'eeeeeeee-0000-0000-0000-000000000005'
TEST_SUBJECT = 'test-subject-test0123abcd'
BOUNDARY = f'arn:aws:iam::{ACCOUNT}:policy/diaspora-service/msk-user-boundary'
OLD_DOCUMENT = {
    'Version': '2012-10-17',
    'Statement': [
        {'Effect': 'Allow', 'Action': 'kafka-cluster:*', 'Resource': '*'},
    ],
}


def _client(service: str) -> Any:
    return boto3.client(
        service,
        region_name='us-east-1',
        aws_access_key_id='testing',
        aws_secret_access_key='testing',  # pragma: allowlist secret
    )


def _iam_service() -> IAMService:
    service = IAMService(
        account_id=ACCOUNT,
        region='us-east-1',
        cluster_name='test-cluster',
    )
    service.iam = _client('iam')
    return service


def _arn(subject: str) -> str:
    return f'arn:aws:iam::{ACCOUNT}:policy/msk-policy/{subject}'


def _user(name: str) -> dict[str, Any]:
    return {
        'Path': '/msk-users/',
        'UserName': name,
        'UserId': 'AIDAEXAMPLEEXAMPLE01',
        'Arn': f'arn:aws:iam::{ACCOUNT}:user/msk-users/{name}',
        'CreateDate': CREATED,
    }


def _policy(name: str) -> dict[str, Any]:
    return {'PolicyName': name, 'Path': '/msk-policy/', 'Arn': _arn(name)}


def _stub_policy(
    stubber: Stubber,
    subject: str,
    document: dict[str, Any],
    version: str = 'v1',
) -> None:
    stubber.add_response(
        'get_policy',
        {'Policy': {'Arn': _arn(subject), 'DefaultVersionId': version}},
        {'PolicyArn': _arn(subject)},
    )
    stubber.add_response(
        'get_policy_version',
        {'PolicyVersion': {'Document': quote(json.dumps(document))}},
        {'PolicyArn': _arn(subject), 'VersionId': version},
    )


def _stub_user(
    stubber: Stubber,
    subject: str,
    boundary: str | None,
) -> None:
    user = _user(subject)
    if boundary is not None:
        user['PermissionsBoundary'] = {
            'PermissionsBoundaryType': 'Policy',
            'PermissionsBoundaryArn': boundary,
        }
    stubber.add_response('get_user', {'User': user}, {'UserName': subject})


def _stub_listing(iam: Stubber, dynamodb: Stubber) -> None:
    """Stub IAM and the users table holding the module's subjects."""
    users = [CURRENT, STALE, OTHER_BOUNDARY, TEST_SUBJECT]
    iam.add_response(
        'list_users',
        {'Users': [_user(name) for name in users]},
        {'PathPrefix': '/msk-users/'},
    )
    policies = [CURRENT, STALE, NO_USER, OTHER_BOUNDARY, TEST_SUBJECT]
    iam.add_response(
        'list_policies',
        {'Policies': [_policy(name) for name in policies]},
        {'Scope': 'Local', 'PathPrefix': '/msk-policy/'},
    )
    dynamodb.add_response(
        'scan',
        {
            'Items': [
                {'subject': {'S': name}}
                for name in (
                    GLOBAL_NAMESPACES_KEY,
                    CURRENT,
                    NO_POLICY,
                    'not-a-subject',
                )
            ],
        },
        {
            'TableName': 'diaspora-users-table',
            'ProjectionExpression': 'subject',
            'ConsistentRead': True,
        },
    )


def _collect(boundary: str | None) -> tuple[Any, IAMService]:
    service = _iam_service()
    template = service.generate_user_policy
    dynamodb = _client('dynamodb')
    with Stubber(service.iam) as iam, Stubber(dynamodb) as db:
        _stub_listing(iam, db)
        _stub_policy(iam, CURRENT, template(CURRENT))
        _stub_user(iam, CURRENT, BOUNDARY)
        _stub_policy(iam, STALE, OLD_DOCUMENT, version='v3')
        _stub_user(iam, STALE, None)
        _stub_policy(iam, NO_USER, OLD_DOCUMENT)
        _stub_policy(iam, OTHER_BOUNDARY, template(OTHER_BOUNDARY))
        _stub_user(iam, OTHER_BOUNDARY, BOUNDARY.replace('msk-user', 'x'))
        plan = script.collect(
            service,
            dynamodb,
            'diaspora-users-table',
            boundary,
        )
        iam.assert_no_pending_responses()
        db.assert_no_pending_responses()
    return plan, service


def test_collect_selects_what_differs() -> None:
    """Stale policies and unbounded users are selected, and nothing else."""
    plan, _ = _collect(BOUNDARY)

    assert plan.policies == [(STALE, 'v3'), (NO_USER, 'v1')]
    assert plan.boundaries == [(STALE, BOUNDARY)]
    assert plan.current == 2
    assert plan.unbounded == 1
    assert plan.errors == []
    notes = '\n'.join(plan.notes)
    assert f'{NO_POLICY}: in the table, but has no policy' in notes
    assert 'not-a-subject: not a subject, skipped' in notes
    assert f'user {OTHER_BOUNDARY} has another boundary' in notes
    assert TEST_SUBJECT not in notes


def test_collect_without_a_boundary_selects_no_users() -> None:
    """Without --boundary, unbounded users are only counted."""
    plan, _ = _collect(None)

    assert plan.boundaries == []
    assert plan.unbounded == 1
    assert not any('another boundary' in note for note in plan.notes)


def test_apply_records_each_change() -> None:
    """Each new default version and boundary goes into the record."""
    service = _iam_service()
    plan = script.Plan(
        policies=[(STALE, 'v3')],
        boundaries=[(STALE, BOUNDARY)],
    )
    record: dict[str, list[dict[str, str]]] = {
        'policies': [],
        'boundaries': [],
    }
    with Stubber(service.iam) as iam:
        _stub_policy(iam, STALE, OLD_DOCUMENT, version='v3')
        iam.add_response(
            'list_policy_versions',
            {
                'Versions': [
                    {'VersionId': 'v3', 'IsDefaultVersion': True},
                ],
            },
            {'PolicyArn': _arn(STALE)},
        )
        iam.add_response(
            'create_policy_version',
            {'PolicyVersion': {'VersionId': 'v4', 'IsDefaultVersion': True}},
            {
                'PolicyArn': _arn(STALE),
                'PolicyDocument': json.dumps(
                    service.generate_user_policy(STALE),
                ),
                'SetAsDefault': True,
            },
        )
        iam.add_response(
            'put_user_permissions_boundary',
            {},
            {'UserName': STALE, 'PermissionsBoundary': BOUNDARY},
        )
        failures = script.apply(plan, service, record)
        iam.assert_no_pending_responses()

    assert failures == []
    assert record == {
        'policies': [
            {
                'policy_arn': _arn(STALE),
                'old_version': 'v3',
                'new_version': 'v4',
            },
        ],
        'boundaries': [{'user': STALE, 'boundary': BOUNDARY}],
    }


def test_apply_skips_a_policy_updated_since_the_listing() -> None:
    """A policy already current when applied is not recorded."""
    service = _iam_service()
    plan = script.Plan(policies=[(STALE, 'v3')])
    record: dict[str, list[dict[str, str]]] = {
        'policies': [],
        'boundaries': [],
    }
    with Stubber(service.iam) as iam:
        _stub_policy(iam, STALE, service.generate_user_policy(STALE), 'v4')
        failures = script.apply(plan, service, record)
        iam.assert_no_pending_responses()

    assert failures == []
    assert record['policies'] == []


def test_undo_sets_back_only_the_versions_it_recorded() -> None:
    """Undo restores a recorded default and leaves changed ones alone."""
    iam = _client('iam')
    record = {
        'policies': [
            {
                'policy_arn': _arn(CURRENT),
                'old_version': 'v1',
                'new_version': 'v2',
            },
            {
                'policy_arn': _arn(STALE),
                'old_version': 'v3',
                'new_version': 'v4',
            },
            {
                'policy_arn': _arn(NO_USER),
                'old_version': 'v1',
                'new_version': 'v2',
            },
            {
                'policy_arn': f'arn:aws:iam::{ACCOUNT}:policy/other/x',
                'old_version': 'v1',
                'new_version': 'v2',
            },
        ],
        'boundaries': [{'user': STALE, 'boundary': BOUNDARY}],
    }
    with Stubber(iam) as stubber:
        for subject, default in ((CURRENT, 'v2'), (STALE, 'v3')):
            stubber.add_response(
                'get_policy',
                {'Policy': {'DefaultVersionId': default}},
                {'PolicyArn': _arn(subject)},
            )
            if subject == CURRENT:
                stubber.add_response(
                    'set_default_policy_version',
                    {},
                    {'PolicyArn': _arn(CURRENT), 'VersionId': 'v1'},
                )
        stubber.add_response(
            'get_policy',
            {'Policy': {'DefaultVersionId': 'v5'}},
            {'PolicyArn': _arn(NO_USER)},
        )
        failures = script.undo(iam, record)
        stubber.assert_no_pending_responses()

    assert len(failures) == 2
    assert 'the default is v5, not the recorded v2' in failures[0]
    assert 'not a user policy' in failures[1]


def test_main_refuses_to_overwrite_a_record(tmp_path: Path) -> None:
    """--apply stops before any AWS call when the record file exists."""
    existing = tmp_path / 'record.json'
    existing.write_text('{}')

    with pytest.raises(SystemExit):
        script.parse_args(
            [
                '--apply',
                '--record',
                str(existing),
                '--account-id',
                ACCOUNT,
                '--region',
                'us-east-1',
                '--cluster-name',
                'test-cluster',
            ],
        )


def _stub_one_stale_user(iam: Stubber, dynamodb: Stubber) -> None:
    iam.add_response(
        'list_users',
        {'Users': [_user(STALE)]},
        {'PathPrefix': '/msk-users/'},
    )
    iam.add_response(
        'list_policies',
        {'Policies': [_policy(STALE)]},
        {'Scope': 'Local', 'PathPrefix': '/msk-policy/'},
    )
    dynamodb.add_response('scan', {'Items': [{'subject': {'S': STALE}}]})
    _stub_policy(iam, STALE, OLD_DOCUMENT)
    _stub_user(iam, STALE, None)


@pytest.mark.parametrize('apply', (False, True))
def test_main_writes_only_with_apply(
    apply: bool,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A dry run makes no IAM write; --apply makes them and records them."""
    clients = {'iam': _client('iam'), 'dynamodb': _client('dynamodb')}
    monkeypatch.setattr(boto3, 'client', lambda name, **_: clients[name])
    record = tmp_path / 'record.json'
    args = [
        '--boundary',
        BOUNDARY,
        '--record',
        str(record),
        '--account-id',
        ACCOUNT,
        '--region',
        'us-east-1',
        '--cluster-name',
        'test-cluster',
    ]
    with (
        Stubber(clients['iam']) as iam,
        Stubber(clients['dynamodb']) as dynamodb,
    ):
        _stub_one_stale_user(iam, dynamodb)
        if apply:
            _stub_policy(iam, STALE, OLD_DOCUMENT)
            iam.add_response(
                'list_policy_versions',
                {'Versions': [{'VersionId': 'v1', 'IsDefaultVersion': True}]},
            )
            iam.add_response(
                'create_policy_version',
                {'PolicyVersion': {'VersionId': 'v2'}},
            )
            iam.add_response('put_user_permissions_boundary', {})
        code = script.main(['--apply', *args] if apply else args)
        iam.assert_no_pending_responses()

    assert code == 0
    if apply:
        written = json.loads(record.read_text())
        assert written['policies'][0]['new_version'] == 'v2'
        assert written['boundaries'] == [{'user': STALE, 'boundary': BOUNDARY}]
    else:
        assert not record.exists()
