"""Tests for the policy templates of the service's own AWS identity."""

from __future__ import annotations

import ast
import json
import re
from fnmatch import fnmatchcase
from pathlib import Path
from typing import Any

import boto3
import pytest

from web_service.services import IAMService

ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / 'deploy' / 'iam'
PLACEHOLDERS = {'<account-id>', '<region>', '<cluster-name>', '<cluster-uuid>'}
VALUES = {
    '<account-id>': '123456789012',
    '<region>': 'us-east-1',
    '<cluster-name>': 'test-cluster',
    '<cluster-uuid>': '0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0-2',
}
BOUNDARY_ARN = (
    'arn:aws:iam::<account-id>:policy/diaspora-service/msk-user-boundary'
)
# IAMService methods that only scripts/update_user_policies.py calls, as an
# administrator; the service's identity is not allowed their IAM actions
ADMIN_ONLY = frozenset({'get_user_policy', 'update_user_policy'})
# boto3 calls that are not operations of their own
WAITERS = {'table_exists': 'DescribeTable'}


def _policy(name: str, *, filled: bool = False) -> dict[str, Any]:
    text = (TEMPLATES / name).read_text()
    if filled:
        for placeholder, value in VALUES.items():
            text = text.replace(placeholder, value)
    return json.loads(text)


def _statements(name: str) -> dict[str, dict[str, Any]]:
    return {s['Sid']: s for s in _policy(name)['Statement']}


def _list(value: str | list[str]) -> list[str]:
    return [value] if isinstance(value, str) else value


def _allows(policy: dict[str, Any], action: str, resource: str) -> bool:
    """Whether a statement of the policy allows the action on the resource.

    Conditions are ignored; IAM's ``*`` matches as fnmatch's does.
    """
    return any(
        action in _list(s['Action'])
        and any(fnmatchcase(resource, r) for r in _list(s['Resource']))
        for s in policy['Statement']
    )


@pytest.mark.parametrize(
    'name',
    ('service-policy.json', 'user-boundary-policy.json'),
)
def test_templates_use_only_placeholders(name: str) -> None:
    """The templates name no account, only the documented placeholders."""
    text = (TEMPLATES / name).read_text()

    assert set(re.findall(r'<[^>]*>', text)) <= PLACEHOLDERS
    assert not re.search(r'\d{12}', text)
    for statement in _policy(name)['Statement']:
        assert statement['Effect'] == 'Allow'
        assert re.fullmatch(r'[A-Za-z0-9]+', statement['Sid'])


# The classes that hold boto3 clients, and the attribute of each client
CLIENTS = {'IAMService': 'iam', 'DynamoDBService': 'dynamodb'}


def test_only_the_service_classes_make_boto3_clients() -> None:
    """No other module of the service calls AWS through its own client."""
    for path in (ROOT / 'web_service').glob('*.py'):
        if path.name != 'services.py':
            assert 'boto3' not in path.read_text(), path.name


def _aws_calls() -> set[tuple[str, str]]:
    """The (client, boto3 method) of each IAM and DynamoDB call it makes."""
    tree = ast.parse((ROOT / 'web_service' / 'services.py').read_text())
    calls = set()
    for cls in tree.body:
        if not (isinstance(cls, ast.ClassDef) and cls.name in CLIENTS):
            continue
        client = CLIENTS[cls.name]
        for function in cls.body:
            if not isinstance(function, ast.FunctionDef):
                continue
            if function.name in ADMIN_ONLY:
                continue
            for node in ast.walk(function):
                if not (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and isinstance(node.func.value, ast.Attribute)
                    and isinstance(node.func.value.value, ast.Name)
                    and node.func.value.value.id == 'self'
                    and node.func.value.attr == client
                ):
                    continue
                method = node.func.attr
                if method == 'get_waiter':
                    arg = node.args[0]
                    assert isinstance(arg, ast.Constant)
                    assert isinstance(arg.value, str)
                    method = f'waiter:{arg.value}'
                calls.add((client, method))
    return calls


def _action(client: str, method: str) -> str:
    if method.startswith('waiter:'):
        return f'{client}:{WAITERS[method.removeprefix("waiter:")]}'
    boto = boto3.client(
        client,
        region_name='us-east-1',
        aws_access_key_id='testing',
        aws_secret_access_key='testing',  # pragma: allowlist secret
    )
    return f'{client}:{boto.meta.method_to_api_mapping[method]}'


def test_service_policy_grants_the_aws_calls_of_the_service() -> None:
    """The IAM and DynamoDB actions granted are those the service calls."""
    calls = _aws_calls()
    granted = {
        action
        for statement in _policy('service-policy.json')['Statement']
        for action in _list(statement['Action'])
    }

    assert ('iam', 'create_user') in calls
    assert ('dynamodb', 'waiter:table_exists') in calls
    used = {_action(*call) for call in calls}
    assert {a for a in granted if a.split(':')[0] in CLIENTS.values()} == used


def test_admin_only_methods_are_not_called_by_the_service() -> None:
    """The service never calls the methods its identity may not run."""
    for path in (ROOT / 'web_service').glob('*.py'):
        tree = ast.parse(path.read_text())
        for function in ast.walk(tree):
            if not isinstance(function, ast.FunctionDef):
                continue
            if function.name in ADMIN_ONLY:
                continue
            for node in ast.walk(function):
                if isinstance(node, ast.Attribute):
                    assert node.attr not in ADMIN_ONLY, (
                        f'{path.name}: {function.name} calls {node.attr}'
                    )


def test_service_policy_reaches_only_the_service_users_and_policies() -> None:
    """IAM rights cover the users and policies the service names."""
    statements = _statements('service-policy.json')
    for statement in statements.values():
        actions = _list(statement['Action'])
        if not any(a.startswith('iam:') for a in actions):
            continue
        for resource in _list(statement['Resource']):
            assert resource in (
                'arn:aws:iam::<account-id>:user/msk-users/*',
                'arn:aws:iam::<account-id>:policy/msk-policy/*',
            )
    granted = {a for s in statements.values() for a in _list(s['Action'])}
    # Rights that would let it raise a user above the boundary
    for action in (
        'iam:PutUserPermissionsBoundary',
        'iam:DeleteUserPermissionsBoundary',
        'iam:PutUserPolicy',
        'iam:CreatePolicyVersion',
        'iam:SetDefaultPolicyVersion',
        'iam:AddUserToGroup',
    ):
        assert action not in granted
    # The boundary lies outside the policies the service may change
    assert '/msk-policy/' not in BOUNDARY_ARN


def test_service_policy_requires_the_boundary() -> None:
    """Creating a user, or attaching to one, needs the boundary."""
    statements = _statements('service-policy.json')
    for sid in (
        'CreateUsersOnlyWithTheBoundary',
        'AttachUserPoliciesOnlyToBoundedUsers',
    ):
        condition = statements[sid]['Condition']['StringEquals']
        assert condition['iam:PermissionsBoundary'] == BOUNDARY_ARN
    attach = statements['AttachUserPoliciesOnlyToBoundedUsers']['Condition']
    assert attach['ArnLike']['iam:PolicyARN'] == (
        'arn:aws:iam::<account-id>:policy/msk-policy/*'
    )


def test_boundary_allows_what_the_user_policy_grants() -> None:
    """The boundary grants the user policy's actions, on the cluster only."""
    boundary = _policy('user-boundary-policy.json', filled=True)
    iam = IAMService(
        account_id=VALUES['<account-id>'],
        region=VALUES['<region>'],
        cluster_name=VALUES['<cluster-name>'],
    )
    subject = '3f1e2d4c-aaaa-bbbb-cccc-c0ffee000001'
    user_policy = iam.generate_user_policy(subject)
    (user_statement,) = user_policy['Statement']
    (boundary_statement,) = boundary['Statement']

    assert set(_list(boundary_statement['Action'])) == set(
        user_statement['Action'],
    )
    base = (
        f'arn:aws:kafka:{VALUES["<region>"]}:{VALUES["<account-id>"]}:'
        f'{{}}/{VALUES["<cluster-name>"]}/{VALUES["<cluster-uuid>"]}'
    )
    resources = [base.format('cluster')] + [
        f'{base.format(kind)}/ns-c0ffee000001.workers'
        for kind in ('topic', 'group', 'transactional-id')
    ]
    for action in user_statement['Action']:
        for resource in resources:
            if _allows(user_policy, action, resource):
                assert _allows(boundary, action, resource)
    assert not _allows(boundary, 'iam:CreateUser', '*')
