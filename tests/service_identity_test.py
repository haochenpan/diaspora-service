"""Tests for the policy templates of the service's own AWS identity.

``deploy/iam/service-actions.json`` lists each AWS action the service
calls, with an example resource and request context, and the calls in
``web_service/`` that need it. These tests check the service's policy
against that list, and the list against the code.
"""

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
CODE = ROOT / 'web_service'
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
SUBJECT = '3f1e2d4c-aaaa-bbbb-cccc-c0ffee000001'
# IAMService methods that only scripts/update_user_policies.py calls, as an
# administrator; the service's identity is not allowed their IAM actions
ADMIN_ONLY = frozenset({'get_user_policy', 'update_user_policy'})
# The classes that hold boto3 clients, and the attribute of each client
CLIENTS = {'IAMService': 'iam', 'DynamoDBService': 'dynamodb'}
# The MSK actions each kafka-python call of the service needs, from AWS's
# "Semantics of IAM authorization policy actions and resources" and the
# Kafka ACLs each request needs; every call also needs Connect
KAFKA_ACTIONS = {
    'admin.create_topics': {'CreateTopic'},
    'admin.delete_topics': {'DeleteTopic', 'DescribeTopic'},
    'producer.send': {'WriteData', 'DescribeTopic'},
    'metadata.partitions_for_topic': {'DescribeTopic'},
    # Fetches metadata for the topics
    'consumer.subscribe': {'DescribeTopic'},
    # Joins the group and fetches
    'consumer.poll': {
        'ReadData',
        'DescribeTopic',
        'DescribeGroup',
        'AlterGroup',
    },
    'consumer.commit': {'ReadData', 'AlterGroup'},
    'consumer.committed': {'DescribeTopic', 'DescribeGroup'},
    'consumer.seek_to_beginning': {'DescribeTopic'},
    'consumer.seek_to_end': {'DescribeTopic'},
    # Leaves the group
    'consumer.close': {'AlterGroup'},
}
# kafka-python calls that send nothing the cluster authorizes
KAFKA_LOCAL = frozenset(
    {
        'admin.close',
        'producer.close',
        'built.close',
        'metadata.request_update',
        'consumer.assign',
        'consumer.assignment',
        'consumer.seek',
        'consumer.subscription',
        'consumer.unsubscribe',
    },
)
KAFKA_CLIENTS = frozenset(
    {'KafkaAdminClient', 'KafkaConsumer', 'KafkaProducer'},
)


def _fill(text: str) -> str:
    for placeholder, value in VALUES.items():
        text = text.replace(placeholder, value)
    return text


def _policy(name: str, *, filled: bool = False) -> dict[str, Any]:
    text = (TEMPLATES / name).read_text()
    return json.loads(_fill(text) if filled else text)


def _uses() -> list[dict[str, Any]]:
    return _policy('service-actions.json')['Uses']


def _statements(name: str) -> dict[str, dict[str, Any]]:
    return {s['Sid']: s for s in _policy(name)['Statement']}


def _list(value: str | list[str]) -> list[str]:
    return [value] if isinstance(value, str) else value


def _allows(
    policy: dict[str, Any],
    action: str,
    resource: str,
    context: dict[str, str] | None = None,
) -> bool:
    """Whether a statement of the policy allows the request.

    Handles the Allow statements, StringEquals and ArnLike conditions the
    templates use; a condition key missing from context fails. IAM's
    ``*`` and ``?`` match as fnmatch's do, ``/`` included.
    """
    context = context or {}
    for statement in policy['Statement']:
        if action not in _list(statement['Action']):
            continue
        if not any(
            fnmatchcase(resource, r) for r in _list(statement['Resource'])
        ):
            continue
        conditions = statement.get('Condition', {})
        if all(
            key in context
            and (
                context[key] == value
                if operator == 'StringEquals'
                else fnmatchcase(context[key], value)
            )
            for operator, keys in conditions.items()
            for key, value in keys.items()
        ):
            return True
    return False


@pytest.mark.parametrize(
    'name',
    (
        'service-policy.json',
        'user-boundary-policy.json',
        'service-actions.json',
    ),
)
def test_templates_use_only_placeholders(name: str) -> None:
    """The templates name no account, only the documented placeholders."""
    text = (TEMPLATES / name).read_text()

    assert set(re.findall(r'<[^>]*>', text)) <= PLACEHOLDERS
    assert not re.search(r'\d{12}', text)
    for statement in _policy(name).get('Statement', []):
        assert statement['Effect'] == 'Allow'
        assert re.fullmatch(r'[A-Za-z0-9]+', statement['Sid'])


def test_service_policy_allows_each_use() -> None:
    """Each listed use is allowed, on each of its resources."""
    policy = _policy('service-policy.json', filled=True)
    for use in _uses():
        context = {k: _fill(v) for k, v in use['Context'].items()}
        for resource in use['Resources']:
            assert _allows(policy, use['Action'], _fill(resource), context), (
                use['Action'],
                resource,
            )


def test_service_policy_grants_only_the_listed_actions() -> None:
    """The policy grants no action that no use needs."""
    granted = {
        action
        for statement in _policy('service-policy.json')['Statement']
        for action in _list(statement['Action'])
    }

    assert granted == {use['Action'] for use in _uses()}


def _calls(path: Path) -> set[str]:
    """Each call in the file as receiver.method, and each class called.

    The receiver is the last name before the method: ``consumer`` for
    ``managed.consumer.poll()``, ``iam`` for ``self.iam.create_user()``.
    """
    calls = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Name):
            calls.add(func.id)
        elif isinstance(func, ast.Attribute):
            receiver = func.value
            if isinstance(receiver, ast.Attribute):
                calls.add(f'{receiver.attr}.{func.attr}')
            elif isinstance(receiver, ast.Name):
                calls.add(f'{receiver.id}.{func.attr}')
    return calls


def test_each_listed_call_is_in_the_code() -> None:
    """A use names only calls the code makes."""
    for use in _uses():
        for call in use['Calls']:
            file, name = call.split(':')
            assert name in _calls(CODE / file), call


def _aws_calls() -> set[str]:
    """Each IAM and DynamoDB call of the service, as file:client.method.

    Only calls on the service classes' own clients (``self.iam``,
    ``self.dynamodb``) are AWS calls; ``self.dynamodb`` is a
    DynamoDBService in NamespaceService.
    """
    tree = ast.parse((CODE / 'services.py').read_text())
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
                if (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and isinstance(node.func.value, ast.Attribute)
                    and isinstance(node.func.value.value, ast.Name)
                    and node.func.value.value.id == 'self'
                    and node.func.value.attr == client
                ):
                    calls.add(f'services.py:{client}.{node.func.attr}')
    return calls


def test_only_the_service_classes_make_boto3_clients() -> None:
    """No other module of the service calls AWS through its own client."""
    for path in CODE.glob('*.py'):
        if path.name != 'services.py':
            assert 'boto3' not in path.read_text(), path.name


def test_iam_and_dynamodb_uses_are_the_calls_of_the_code() -> None:
    """The listed IAM and DynamoDB uses are exactly the code's calls."""
    calls = _aws_calls()
    listed = {
        call
        for use in _uses()
        if use['Action'].split(':')[0] in CLIENTS.values()
        for call in use['Calls']
    }

    assert 'services.py:iam.create_user' in calls
    assert listed == calls
    for use in _uses():
        service, action = use['Action'].split(':')
        if service not in CLIENTS.values():
            continue
        boto = boto3.client(
            service,
            region_name='us-east-1',
            aws_access_key_id='testing',
            aws_secret_access_key='testing',  # pragma: allowlist secret
        )
        for call in use['Calls']:
            method = call.split('.')[-1]
            if method == 'get_waiter':
                # The table_exists waiter calls DescribeTable
                assert action == 'DescribeTable'
            else:
                assert boto.meta.method_to_api_mapping[method] == action


def test_kafka_uses_cover_each_kafka_call_of_the_code() -> None:
    """Each Kafka call needs its actions, and each is listed for it."""
    listed: dict[str, set[str]] = {}
    for use in _uses():
        service, action = use['Action'].split(':')
        if service != 'kafka-cluster':
            continue
        for call in use['Calls']:
            listed.setdefault(call.split(':')[1], set()).add(action)
    receivers = {name.split('.')[0] for name in {*KAFKA_ACTIONS, *KAFKA_LOCAL}}

    for path in CODE.glob('*.py'):
        for call in _calls(path):
            if call in KAFKA_CLIENTS:
                assert 'Connect' in listed[call], (path.name, call)
            elif call.split('.')[0] in receivers:
                assert call in KAFKA_ACTIONS or call in KAFKA_LOCAL, (
                    f'{path.name}: classify {call} in this test'
                )
                needed = KAFKA_ACTIONS.get(call, set())
                assert needed <= listed.get(call, set()), (path.name, call)


def test_admin_only_methods_are_not_called_by_the_service() -> None:
    """The service never calls the methods its identity may not run."""
    for path in CODE.glob('*.py'):
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


def test_service_policy_reaches_only_subject_named_users_and_policies() -> (
    None
):
    """IAM rights cover names shaped like subjects, at the service's paths."""
    policy = _policy('service-policy.json', filled=True)
    account = VALUES['<account-id>']
    user = f'arn:aws:iam::{account}:user/msk-users/'
    managed = f'arn:aws:iam::{account}:policy/msk-policy/'

    assert _allows(policy, 'iam:DeleteUser', user + SUBJECT)
    assert _allows(policy, 'iam:DeletePolicy', managed + SUBJECT)
    for name in (
        # A subject's name under a nested path, which a user created by
        # the identity could take before the service creates the subject
        f'x/{SUBJECT}',
        f'{SUBJECT}/x',
        # The live tests' users are theirs, not the service's
        'test-subject-test0123abcd',
        'diaspora-web-service-container',
    ):
        assert not _allows(policy, 'iam:DeleteUser', user + name), name
        assert not _allows(policy, 'iam:CreateAccessKey', user + name), name
        assert not _allows(policy, 'iam:DeletePolicy', managed + name), name


def test_service_policy_cannot_raise_a_user_above_the_boundary() -> None:
    """No right changes a boundary, an inline policy or a policy version."""
    granted = {
        a
        for s in _policy('service-policy.json')['Statement']
        for a in _list(s['Action'])
    }
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
    policy = _policy('service-policy.json', filled=True)
    account = VALUES['<account-id>']
    user = f'arn:aws:iam::{account}:user/msk-users/{SUBJECT}'
    managed = f'arn:aws:iam::{account}:policy/msk-policy/{SUBJECT}'
    boundary = _fill(BOUNDARY_ARN)
    other = f'arn:aws:iam::{account}:policy/diaspora-test/test-user-boundary'
    admin = 'arn:aws:iam::aws:policy/AdministratorAccess'

    for context in ({}, {'iam:PermissionsBoundary': other}):
        assert not _allows(policy, 'iam:CreateUser', user, context)
    assert not _allows(
        policy,
        'iam:AttachUserPolicy',
        user,
        {'iam:PolicyARN': managed},
    )
    assert not _allows(
        policy,
        'iam:AttachUserPolicy',
        user,
        {'iam:PolicyARN': admin, 'iam:PermissionsBoundary': boundary},
    )


def test_boundary_allows_what_the_user_policy_grants() -> None:
    """The boundary grants the user policy's actions, on the cluster only."""
    boundary = _policy('user-boundary-policy.json', filled=True)
    iam = IAMService(
        account_id=VALUES['<account-id>'],
        region=VALUES['<region>'],
        cluster_name=VALUES['<cluster-name>'],
    )
    user_policy = iam.generate_user_policy(SUBJECT)
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
