"""Tests for the script that brings the service's users' IAM up to date."""

from __future__ import annotations

import importlib.util
import json
import stat
import sys
from datetime import datetime
from datetime import UTC
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
CREATED = datetime(2026, 1, 1, tzinfo=UTC)
# Subjects in the order the script visits them
CURRENT = 'aaaaaaaa-0000-0000-0000-000000000001'
STALE = 'bbbbbbbb-0000-0000-0000-000000000002'
NO_USER = 'cccccccc-0000-0000-0000-000000000003'
NO_POLICY = 'dddddddd-0000-0000-0000-000000000004'
OTHER_BOUNDARY = 'eeeeeeee-0000-0000-0000-000000000005'
TEST_SUBJECT = 'test-subject-test0123abcd'
BOUNDARY = f'arn:aws:iam::{ACCOUNT}:policy/diaspora-service/msk-user-boundary'
TEST_BOUNDARY = (
    f'arn:aws:iam::{ACCOUNT}:policy/diaspora-test/test-user-boundary'
)
OLD_DOCUMENT = {
    'Version': '2012-10-17',
    'Statement': [
        {'Effect': 'Allow', 'Action': 'kafka-cluster:*', 'Resource': '*'},
    ],
}
COMMON = ['--account-id', ACCOUNT, '--region', 'us-east-1']
POLICIES_ARGS = ['policies', *COMMON, '--cluster-name', 'test-cluster']
BOUNDARIES_ARGS = ['boundaries', *COMMON, '--boundary', BOUNDARY]


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


def _user(
    name: str,
    boundary: str | None = None,
    path: str = '/msk-users/',
) -> dict[str, Any]:
    user: dict[str, Any] = {
        'Path': path,
        'UserName': name,
        'UserId': 'AIDAEXAMPLEEXAMPLE01',
        'Arn': f'arn:aws:iam::{ACCOUNT}:user{path}{name}',
        'CreateDate': CREATED,
    }
    if boundary is not None:
        user['PermissionsBoundary'] = {
            'PermissionsBoundaryType': 'Policy',
            'PermissionsBoundaryArn': boundary,
        }
    return user


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


def _stub_get_user(
    stubber: Stubber,
    name: str,
    boundary: str | None,
    path: str = '/msk-users/',
) -> None:
    stubber.add_response(
        'get_user',
        {'User': _user(name, boundary, path)},
        {'UserName': name},
    )


def _stub_listing(iam: Stubber, dynamodb: Stubber) -> None:
    """Stub IAM and the users table holding the module's subjects."""
    users = [_user(n) for n in (CURRENT, STALE, OTHER_BOUNDARY, TEST_SUBJECT)]
    # A subject-named user in a nested path is not the service's
    users.append(_user(NO_USER, path='/msk-users/x/'))
    iam.add_response(
        'list_users',
        {'Users': users},
        {'PathPrefix': '/msk-users/'},
    )
    policies = [CURRENT, STALE, NO_USER, OTHER_BOUNDARY, TEST_SUBJECT]
    iam.add_response(
        'list_policies',
        {'Policies': [_policy(name) for name in policies]},
        {'Scope': 'Local', 'PathPrefix': '/msk-policy/'},
    )
    names = (GLOBAL_NAMESPACES_KEY, CURRENT, NO_POLICY, 'not-a-subject')
    dynamodb.add_response(
        'scan',
        {'Items': [{'subject': {'S': name}} for name in names]},
        {
            'TableName': 'diaspora-users-table',
            'ProjectionExpression': 'subject',
            'ConsistentRead': True,
        },
    )


def _listing(service: IAMService) -> Any:
    dynamodb = _client('dynamodb')
    with Stubber(service.iam) as iam, Stubber(dynamodb) as db:
        _stub_listing(iam, db)
        listing = script.list_service_users(
            service.iam,
            dynamodb,
            'diaspora-users-table',
        )
        db.assert_no_pending_responses()
    return listing


def test_listing_finds_the_service_subjects() -> None:
    """Subjects come from IAM's paths and names and from the table."""
    listing = _listing(_iam_service())

    assert listing.subjects == [
        CURRENT,
        STALE,
        NO_USER,
        NO_POLICY,
        OTHER_BOUNDARY,
    ]
    assert NO_USER not in listing.users
    assert NO_USER in listing.all_users
    assert TEST_SUBJECT in listing.all_users
    assert listing.notes == ['not-a-subject: not a subject, skipped']


def test_collect_policies_selects_the_stale_ones() -> None:
    """Policies whose default version differs are selected."""
    service = _iam_service()
    listing = _listing(service)
    template = service.generate_user_policy
    with Stubber(service.iam) as iam:
        _stub_policy(iam, CURRENT, template(CURRENT))
        _stub_policy(iam, STALE, OLD_DOCUMENT, version='v3')
        _stub_policy(iam, NO_USER, OLD_DOCUMENT)
        _stub_policy(iam, OTHER_BOUNDARY, template(OTHER_BOUNDARY))
        plan = script.collect_policies(service, listing)
        iam.assert_no_pending_responses()

    assert [(s, v) for s, v, _ in plan.changes] == [
        (STALE, 'v3'),
        (NO_USER, 'v1'),
    ]
    assert plan.current == 2
    assert f'{NO_POLICY}: listed, but has no policy' in plan.notes


def test_collect_boundaries_counts_every_unbounded_user() -> None:
    """Only the service's users are selected; every unbounded one counts."""
    service = _iam_service()
    listing = _listing(service)
    with Stubber(service.iam) as iam:
        _stub_get_user(iam, CURRENT, BOUNDARY)
        _stub_get_user(iam, STALE, None)
        _stub_get_user(iam, NO_USER, None, path='/msk-users/x/')
        _stub_get_user(iam, OTHER_BOUNDARY, TEST_BOUNDARY)
        _stub_get_user(iam, TEST_SUBJECT, None)
        plan = script.collect_boundaries(service.iam, listing, BOUNDARY)
        iam.assert_no_pending_responses()

    assert [user for user, _, _ in plan.changes] == [STALE]
    assert plan.unbounded == 3
    assert plan.current == 1
    notes = '\n'.join(plan.notes)
    assert f'user {OTHER_BOUNDARY} has another boundary' in notes
    assert f'user {NO_USER} has no boundary, skipped' in notes
    assert f'user {TEST_SUBJECT} has no boundary, skipped' in notes


def test_dry_run_prints_how_the_policies_differ(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The dry run prints each distinct difference once, with its count."""
    service = _iam_service()
    old = service.generate_user_policy(STALE)
    old['Statement'][0]['Resource'][0] = (
        'arn:aws:kafka:us-east-1:000000000000:group/test-cluster/*'
    )
    old_too = service.generate_user_policy(NO_USER)
    old_too['Statement'][0]['Resource'][0] = (
        'arn:aws:kafka:us-east-1:000000000000:group/test-cluster/*'
    )
    plan = script.Plan(
        changes=[(STALE, 'v1', old), (NO_USER, 'v1', old_too)],
    )

    script.print_policies(plan, service, dry=True)

    out = capsys.readouterr().out
    assert out.count('differ from the current policy this way') == 1
    assert '2 policies, ' in out
    group = 'arn:aws:kafka:us-east-1:000000000000:group/test-cluster/*'
    assert f'-        "{group}",' in out
    assert f'+        "{group}/ns-<suffix>.*",' in out


def _record(tmp_path: Path, kind: str) -> Any:
    return script.Record(tmp_path / 'record.json', kind, ACCOUNT)


def test_apply_policies_records_each_update(tmp_path: Path) -> None:
    """Each new default version is in the record file once it is made."""
    service = _iam_service()
    plan = script.Plan(changes=[(STALE, 'v3', OLD_DOCUMENT)])
    record = _record(tmp_path, 'policies')
    with Stubber(service.iam) as iam:
        _stub_policy(iam, STALE, OLD_DOCUMENT, version='v3')
        iam.add_response(
            'list_policy_versions',
            {'Versions': [{'VersionId': 'v3', 'IsDefaultVersion': True}]},
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
        failures = script.apply_policies(plan, service, record)
        iam.assert_no_pending_responses()

    assert failures == []
    written = json.loads(record.path.read_text())
    assert written == {
        'kind': 'policies',
        'account_id': ACCOUNT,
        'changes': [
            {
                'policy_arn': _arn(STALE),
                'old_version': 'v3',
                'new_version': 'v4',
            },
        ],
    }
    assert stat.S_IMODE(record.path.stat().st_mode) == 0o600


def test_apply_policies_skips_a_policy_updated_since(tmp_path: Path) -> None:
    """A policy already current when applied is not recorded."""
    service = _iam_service()
    plan = script.Plan(changes=[(STALE, 'v3', OLD_DOCUMENT)])
    record = _record(tmp_path, 'policies')
    with Stubber(service.iam) as iam:
        _stub_policy(iam, STALE, service.generate_user_policy(STALE), 'v4')
        failures = script.apply_policies(plan, service, record)
        iam.assert_no_pending_responses()

    assert failures == []
    assert json.loads(record.path.read_text())['changes'] == []


def test_record_keeps_changes_made_before_a_crash(tmp_path: Path) -> None:
    """The file holds each change as soon as it is added."""
    record = _record(tmp_path, 'boundaries')
    record.add({'user': STALE, 'old_boundary': None, 'new_boundary': BOUNDARY})

    written = json.loads(record.path.read_text())
    assert len(written['changes']) == 1
    assert sorted(p.name for p in tmp_path.iterdir()) == ['record.json']
    with pytest.raises(FileExistsError):
        _record(tmp_path, 'boundaries')


def test_apply_boundaries_records_each_user(tmp_path: Path) -> None:
    """A user still without a boundary gets it, and the record says so."""
    iam_client = _client('iam')
    plan = script.Plan(changes=[(STALE, None, None), (CURRENT, None, None)])
    record = _record(tmp_path, 'boundaries')
    with Stubber(iam_client) as iam:
        _stub_get_user(iam, STALE, None)
        iam.add_response(
            'put_user_permissions_boundary',
            {},
            {'UserName': STALE, 'PermissionsBoundary': BOUNDARY},
        )
        # Given a boundary since the listing: left as it is
        _stub_get_user(iam, CURRENT, TEST_BOUNDARY)
        failures = script.apply_boundaries(plan, iam_client, BOUNDARY, record)
        iam.assert_no_pending_responses()

    assert failures == []
    assert json.loads(record.path.read_text())['changes'] == [
        {'user': STALE, 'old_boundary': None, 'new_boundary': BOUNDARY},
    ]


def test_undo_policies_sets_back_only_what_it_recorded() -> None:
    """Undo restores a recorded default and leaves changed ones alone."""
    iam = _client('iam')

    def change(arn: str, old: str = 'v1', new: str = 'v2') -> dict[str, str]:
        return {'policy_arn': arn, 'old_version': old, 'new_version': new}

    changes = [
        change(_arn(CURRENT)),
        change(_arn(STALE), 'v3', 'v4'),
        change(_arn(NO_USER)),
        change(f'arn:aws:iam::{ACCOUNT}:policy/other/x'),
        change(f'arn:aws:iam::{ACCOUNT}:policy/msk-policy/x/{CURRENT}'),
        change(f'arn:aws:iam::111111111111:policy/msk-policy/{CURRENT}'),
    ]
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
        failures = script.undo_policies(iam, changes, ACCOUNT)
        stubber.assert_no_pending_responses()

    assert len(failures) == 4
    assert 'the default is v5, not the recorded v2' in failures[0]
    assert all('not a user policy' in f for f in failures[1:])


def test_undo_boundaries_removes_only_the_boundary_it_set() -> None:
    """Undo removes the recorded boundary, and leaves a changed one."""
    iam = _client('iam')

    def change(user: str, new: str = BOUNDARY) -> dict[str, Any]:
        return {'user': user, 'old_boundary': None, 'new_boundary': new}

    changes = [
        change(STALE),
        change(CURRENT),
        change(OTHER_BOUNDARY),
        change(TEST_SUBJECT),
        change(NO_USER, TEST_BOUNDARY),
    ]
    with Stubber(iam) as stubber:
        _stub_get_user(stubber, STALE, BOUNDARY)
        stubber.add_response(
            'delete_user_permissions_boundary',
            {},
            {'UserName': STALE},
        )
        _stub_get_user(stubber, CURRENT, None)
        _stub_get_user(stubber, OTHER_BOUNDARY, TEST_BOUNDARY)
        failures = script.undo_boundaries(iam, changes, ACCOUNT)
        stubber.assert_no_pending_responses()

    assert len(failures) == 3
    assert 'not the recorded one' in failures[0]
    assert 'not a subject' in failures[1]
    assert 'not the service boundary' in failures[2]


@pytest.mark.parametrize(
    'boundary',
    (
        TEST_BOUNDARY,
        'arn:aws:iam::111111111111:policy/diaspora-service/msk-user-boundary',
        BOUNDARY + 'x',
    ),
)
def test_boundaries_refuses_another_boundary(boundary: str) -> None:
    """Only the service's boundary, in this account, is accepted."""
    with pytest.raises(SystemExit, match='--boundary must be'):
        script.check_boundary(_client('iam'), boundary, ACCOUNT)


def test_boundaries_refuses_a_missing_boundary() -> None:
    """The boundary policy must exist."""
    iam = _client('iam')
    with Stubber(iam) as stubber:
        stubber.add_client_error(
            'get_policy',
            'NoSuchEntity',
            http_status_code=404,
        )
        with pytest.raises(SystemExit, match='NoSuchEntity'):
            script.check_boundary(iam, BOUNDARY, ACCOUNT)


def test_policies_refuses_an_unknown_cluster() -> None:
    """The cluster name must be a cluster of the account."""
    kafka = _client('kafka')
    with Stubber(kafka) as stubber:
        stubber.add_response(
            'list_clusters_v2',
            {'ClusterInfoList': [{'ClusterName': 'diaspora'}]},
        )
        with pytest.raises(SystemExit, match="no MSK cluster named 'other'"):
            script.check_cluster(kafka, 'other')


@pytest.mark.parametrize(
    'argv',
    (
        # --apply needs a record
        [*POLICIES_ARGS, '--apply'],
        # boundaries needs --boundary
        ['boundaries', *COMMON],
        # --boundary is not for policies
        [*POLICIES_ARGS, '--boundary', BOUNDARY],
        # --undo takes its boundary from the record
        ['boundaries', *COMMON, '--undo', 'r.json', '--boundary', BOUNDARY],
        # policies needs the cluster name
        ['policies', *COMMON],
    ),
)
def test_parse_args_refuses(
    argv: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Each mistake stops the script before any AWS call."""
    monkeypatch.delenv('MSK_CLUSTER_NAME', raising=False)
    with pytest.raises(SystemExit):
        script.parse_args(argv)


def test_parse_args_refuses_a_record_in_the_repository() -> None:
    """A record would name the account's ARNs, so not in the checkout."""
    record = script.REPOSITORY / 'user-policies.json'
    with pytest.raises(SystemExit):
        script.parse_args([*POLICIES_ARGS, '--apply', '--record', str(record)])


def test_parse_args_refuses_an_existing_record(tmp_path: Path) -> None:
    """--apply never overwrites a record."""
    existing = tmp_path / 'record.json'
    existing.write_text('{}')
    with pytest.raises(SystemExit):
        script.parse_args(
            [*POLICIES_ARGS, '--apply', '--record', str(existing)],
        )


def test_undo_refuses_a_record_of_the_other_step(tmp_path: Path) -> None:
    """A boundaries record cannot undo policies, nor the reverse."""
    record = tmp_path / 'record.json'
    record.write_text(
        json.dumps(
            {'kind': 'boundaries', 'account_id': ACCOUNT, 'changes': []},
        ),
    )
    with pytest.raises(SystemExit, match='records a boundaries run'):
        script.main(['policies', *COMMON, '--undo', str(record)])


def _clients(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    clients = {name: _client(name) for name in ('iam', 'dynamodb', 'kafka')}
    monkeypatch.setattr(boto3, 'client', lambda name, **_: clients[name])
    return clients


def _stub_one_user_listing(iam: Stubber, dynamodb: Stubber) -> None:
    iam.add_response('list_users', {'Users': [_user(STALE)]})
    iam.add_response('list_policies', {'Policies': [_policy(STALE)]})
    dynamodb.add_response('scan', {'Items': [{'subject': {'S': STALE}}]})


@pytest.mark.parametrize('apply', (False, True))
def test_main_policies_writes_only_with_apply(
    apply: bool,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A dry run makes no IAM write; --apply makes and records them."""
    clients = _clients(monkeypatch)
    record = tmp_path / 'record.json'
    with (
        Stubber(clients['iam']) as iam,
        Stubber(clients['dynamodb']) as dynamodb,
        Stubber(clients['kafka']) as kafka,
    ):
        kafka.add_response(
            'list_clusters_v2',
            {'ClusterInfoList': [{'ClusterName': 'test-cluster'}]},
        )
        _stub_one_user_listing(iam, dynamodb)
        _stub_policy(iam, STALE, OLD_DOCUMENT)
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
        extra = ['--apply', '--record', str(record)] if apply else []
        code = script.main([*POLICIES_ARGS, *extra])
        iam.assert_no_pending_responses()

    assert code == 0
    if apply:
        written = json.loads(record.read_text())
        assert written['changes'][0]['new_version'] == 'v2'
    else:
        assert not record.exists()


def test_main_boundaries_applies_and_undoes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """--apply gives the boundary; --undo of its record removes it."""
    clients = _clients(monkeypatch)
    record = tmp_path / 'record.json'
    with (
        Stubber(clients['iam']) as iam,
        Stubber(clients['dynamodb']) as dynamodb,
    ):
        iam.add_response('get_policy', {'Policy': {}}, {'PolicyArn': BOUNDARY})
        _stub_one_user_listing(iam, dynamodb)
        _stub_get_user(iam, STALE, None)
        _stub_get_user(iam, STALE, None)
        iam.add_response('put_user_permissions_boundary', {})
        assert (
            script.main([*BOUNDARIES_ARGS, '--apply', '--record', str(record)])
            == 0
        )

        _stub_get_user(iam, STALE, BOUNDARY)
        iam.add_response('delete_user_permissions_boundary', {})
        assert script.main(['boundaries', *COMMON, '--undo', str(record)]) == 0
        iam.assert_no_pending_responses()
