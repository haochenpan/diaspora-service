"""Tests for the resource names the live tests use."""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock

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
