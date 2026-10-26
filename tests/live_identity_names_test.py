"""Tests for the resource names the live tests use."""

from __future__ import annotations

import json
import re
import uuid
from fnmatch import fnmatchcase
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from tests.live_identity import BOUNDARY_POLICY
from tests.live_identity import KAFKA_NAMESPACE_PREFIX
from tests.live_identity import live_namespace
from tests.live_identity import live_subject
from tests.live_identity import NAMESPACE_PREFIX
from tests.live_identity import SUBJECT_PREFIX
from web_service.services import NamespaceService

NAMESPACES = NamespaceService(dynamodb_service=MagicMock())


def test_live_subject_default_namespace_is_a_test_namespace() -> None:
    """A test user's default namespace starts with the test prefix."""
    subject = live_subject()
    namespace = NAMESPACES.generate_default(subject)

    assert subject.startswith(SUBJECT_PREFIX)
    assert namespace.startswith(NAMESPACE_PREFIX)
    assert NamespaceService.validate_name(namespace) is None


def test_live_namespace_is_valid() -> None:
    """A test namespace has the test prefix and is a valid name."""
    namespace = live_namespace()

    assert namespace.startswith(NAMESPACE_PREFIX)
    assert NamespaceService.validate_name(namespace) is None


def test_real_default_namespaces_never_have_the_test_prefix() -> None:
    """A real user's default namespace cannot look like a test one."""
    for _ in range(100):
        subject = str(uuid.uuid4())
        namespace = NAMESPACES.generate_default(subject)
        assert not namespace.startswith(NAMESPACE_PREFIX)
        assert not subject.startswith(SUBJECT_PREFIX)


TEMPLATES = Path(__file__).parent / 'live_identity'
PLACEHOLDERS = {'<account-id>', '<region>', '<cluster-name>', '<cluster-uuid>'}


def _statements(name: str) -> dict[str, dict[str, Any]]:
    policy = json.loads((TEMPLATES / name).read_text())
    return {statement['Sid']: statement for statement in policy['Statement']}


def _resources(statement: dict[str, Any]) -> list[str]:
    resource = statement['Resource']
    return [resource] if isinstance(resource, str) else resource


@pytest.mark.parametrize(
    'name',
    ('identity-policy.json', 'boundary-policy.json'),
)
def test_policy_templates_use_only_placeholders(name: str) -> None:
    """The templates name no account, only the documented placeholders."""
    text = (TEMPLATES / name).read_text()

    assert set(re.findall(r'<[^>]*>', text)) <= PLACEHOLDERS
    assert not re.search(r'\d{12}', text)
    for sid in _statements(name):
        assert re.fullmatch(r'[A-Za-z0-9]+', sid)


def _names(statements: dict[str, dict[str, Any]], sid: str) -> list[str]:
    return [r.rsplit('/', 1)[1] for r in _resources(statements[sid])]


def _matches(name: str, patterns: list[str]) -> bool:
    return any(fnmatchcase(name, pattern) for pattern in patterns)


def test_identity_policy_matches_the_test_names() -> None:
    """The identity's name patterns cover the names the tests generate."""
    statements = _statements('identity-policy.json')
    subject = live_subject()
    namespace = NAMESPACES.generate_default(subject)
    old_subject = 'test-subject-0123456789abcdef'

    # Only users and policies named the way the tests are now can be
    # created or given keys; older test users have no boundary.
    for sid in (
        'CreateTestUsersOnlyWithTheBoundary',
        'AttachTestPoliciesOnlyToBoundedTestUsers',
        'CreateKeysOnlyForCurrentTestUsers',
        'CreateTestPolicies',
    ):
        assert _matches(subject, _names(statements, sid))
        assert not _matches(old_subject, _names(statements, sid))
        assert not _matches(
            f'{subject}/{uuid.uuid4()}',
            _names(statements, sid),
        )
    # The sweep deletes test users and policies named either way.
    for sid in ('DeleteTestUserKeysAndTestUsers', 'DeleteTestPolicies'):
        assert _matches(subject, _names(statements, sid))
        assert _matches(old_subject, _names(statements, sid))

    topics = (
        f'{namespace}.test-topic-1',
        f'{live_namespace()}.test-topic-1',
        f'{KAFKA_NAMESPACE_PREFIX}0123.test-topic-0123',
    )
    for topic in topics:
        assert _matches(topic, _names(statements, 'UseTestTopics'))
    group = f'{namespace}.grp-0123abcd'
    assert _matches(group, _names(statements, 'UseTestConsumerGroups'))
    real_topic = 'ns-4266141740ab.test-topic-1'
    assert not _matches(real_topic, _names(statements, 'UseTestTopics'))

    boundary = statements['CreateTestUsersOnlyWithTheBoundary']['Condition']
    assert boundary['StringEquals']['iam:PermissionsBoundary'] == (
        f'arn:aws:iam::<account-id>:policy/{BOUNDARY_POLICY}'
    )


def test_boundary_policy_matches_the_test_names() -> None:
    """A bounded user reaches only test topics and groups."""
    (statement,) = _statements('boundary-policy.json').values()
    resources = _resources(statement)
    topics = [r.rsplit('/', 1)[1] for r in resources if ':topic/' in r]
    groups = [r.rsplit('/', 1)[1] for r in resources if ':group/' in r]
    namespace = NAMESPACES.generate_default(live_subject())

    assert _matches(f'{namespace}.test-topic-1', topics)
    assert _matches(f'{namespace}.grp-0123abcd', groups)
    assert not _matches('ns-4266141740ab.test-topic-1', topics)
    assert not _matches('ns-4266141740ab.grp-0123abcd', groups)
    assert not any(r.endswith('/*') for r in resources if ':cluster/' not in r)


def test_workflow_sets_the_same_boundary() -> None:
    """The live job points the tests at the boundary the templates name."""
    workflow = Path(__file__).parents[1] / '.github/workflows/tests.yml'

    assert f':policy/{BOUNDARY_POLICY}' in workflow.read_text()
