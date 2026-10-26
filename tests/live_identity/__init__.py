"""Names of the AWS and Kafka resources the live tests create.

The live tests run as their own IAM identity, whose policy
(``identity-policy.json`` beside this file) lets it touch only IAM users,
IAM policies, Kafka topics and Kafka consumer groups with these names:

- IAM users and policies: ``test-subject-test<8 hex>`` (``live_subject``).
  Real users are named after Globus subjects, which are UUIDs.
- The default namespace of such a user: ``ns-test<8 hex>``, since the
  service takes the last 12 characters of the subject without dashes. A
  namespace for a test that needs one without a user: ``live_namespace``.
  A real user's default namespace is ``ns-`` and 12 hex digits, so it can
  never start with ``ns-t``.
- Kafka topics and consumer groups: ``<namespace>.<name>`` in one of those
  namespaces, or in ``test-namespace-<hex>`` (``kafka_service_test.py``).

DynamoDB items go to the ``test-diaspora-*`` tables, which hold nothing
else, so their keys are not limited.

A new live test that creates any of these resources takes its names from
here, or the identity is denied and the nightly sweep never finds what the
test leaves behind.
"""

from __future__ import annotations

import os

# The environment variable naming the permissions boundary that the
# service, and the live tests, set on every IAM user they create.
PERMISSIONS_BOUNDARY_ENV = 'IAM_USER_PERMISSIONS_BOUNDARY_ARN'

# The path and name of that boundary policy (``boundary-policy.json``).
BOUNDARY_POLICY = 'diaspora-test/test-user-boundary'

SUBJECT_PREFIX = 'test-subject-test'
NAMESPACE_PREFIX = 'ns-test'
KAFKA_NAMESPACE_PREFIX = 'test-namespace-'


def live_subject() -> str:
    """Return a new subject whose IAM user the live identity may manage."""
    return f'{SUBJECT_PREFIX}{os.urandom(4).hex()}'


def live_namespace() -> str:
    """Return a new namespace whose topics and groups the identity may use."""
    return f'{NAMESPACE_PREFIX}{os.urandom(4).hex()}'


def permissions_boundary_arn() -> str | None:
    """Return the boundary to set on the IAM users the tests create."""
    return os.getenv(PERMISSIONS_BOUNDARY_ENV) or None
