"""List, and with --apply delete, resources left behind by the live tests.

The live tests (``pytest -m integration``) create IAM users, IAM policies
and Kafka topics and delete them when they finish. A test that fails or
times out can leave some behind. This script finds those leftovers by the
exact names the tests use and nothing broader:

- IAM users under ``/msk-users/`` named ``test-subject-<16 hex>``, created
  more than ``--min-age-hours`` ago, with their access keys and their
  policy.
- IAM policies under ``/msk-policy/`` with the same names and age, either
  attached to nothing or attached only to a user selected above.
- Kafka topics named ``test-namespace-<16 hex>.test-topic-<16 hex>`` or
  ``ns-<12 hex>.test-topic-<suffix>``, with a suffix the tests use, whose
  namespace the service has no record of (in neither its users table's
  namespace list nor its namespace table) and is not the default
  namespace of any IAM user other than a selected test user. Kafka keeps
  no topic creation time, so topics are not filtered by age; run the
  script when no live test run is in progress.

Without ``--apply`` it only prints what it would delete. It exits non-zero
if listing or any deletion fails. If the service's namespace list cannot
be read, or is missing or empty, it selects no topics.
"""

from __future__ import annotations

import argparse
import contextlib
import os
import re
import sys
from collections.abc import Iterable
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any

import boto3
from kafka.admin import KafkaAdminClient
from kafka.errors import UnknownTopicOrPartitionError

from web_service.services import ADMIN_REQUEST_TIMEOUT_MS
from web_service.services import GLOBAL_NAMESPACES_KEY
from web_service.services import MSKTokenProvider

IAM_USER_PATH = '/msk-users/'
IAM_POLICY_PATH = '/msk-policy/'
TEST_SUBJECT = re.compile(r'test-subject-[0-9a-f]{16}')

# Each pattern captures the namespace part of the Kafka topic name.
TEST_TOPICS = (
    # KafkaService tests: random namespace and topic.
    re.compile(r'(test-namespace-[0-9a-f]{16})\.test-topic-[0-9a-f]{16}'),
    # WebService, NamespaceService and consumer tests: a test user's
    # default namespace, with the fixed or 8-hex topic names they use.
    re.compile(
        r'(ns-[0-9a-f]{12})\.test-topic-'
        r'(?:1|2|concurrent(?:-existing|-delete)?|during-deletion'
        r'|delete-during-create|[0-9a-f]{8})',
    ),
)

DEFAULT_MIN_AGE_HOURS = 2.0
TOPIC_DELETE_TIMEOUT_MS = 30_000


@dataclass(frozen=True)
class ServiceTables:
    """The service's DynamoDB tables that record namespaces."""

    users: str = 'diaspora-users-table'
    namespaces: str = 'diaspora-namespace-table'


@dataclass
class Plan:
    """Resources selected for deletion."""

    users: list[str] = field(default_factory=list)
    policies: list[str] = field(default_factory=list)
    topics: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def select_users(
    users: Iterable[dict[str, Any]],
    cutoff: datetime,
) -> list[str]:
    """Return the names of test users created at or before ``cutoff``."""
    return sorted(
        user['UserName']
        for user in users
        if user.get('Path') == IAM_USER_PATH
        and TEST_SUBJECT.fullmatch(user['UserName'])
        and user['CreateDate'] <= cutoff
    )


def select_policies(
    policies: Iterable[dict[str, Any]],
    users: Iterable[str],
    cutoff: datetime,
) -> list[str]:
    """Return the ARNs of test policies created at or before ``cutoff``.

    A policy still attached to something is selected only when its user,
    whose name it shares, is in ``users`` too.
    """
    selected_users = set(users)
    return sorted(
        policy['Arn']
        for policy in policies
        if policy.get('Path') == IAM_POLICY_PATH
        and TEST_SUBJECT.fullmatch(policy['PolicyName'])
        and policy['CreateDate'] <= cutoff
        and (
            policy.get('AttachmentCount', 0) == 0
            or policy['PolicyName'] in selected_users
        )
    )


def default_namespace(subject: str) -> str:
    """Return the service's default namespace for a subject.

    The same rule as ``NamespaceService.generate_default``.
    """
    return f'ns-{subject.replace("-", "")[-12:]}'


def topic_namespace(topic: str) -> str | None:
    """Return the namespace of a topic the live tests create, else None."""
    for pattern in TEST_TOPICS:
        match = pattern.fullmatch(topic)
        if match:
            return match.group(1)
    return None


def select_topics(topics: Iterable[str], excluded: set[str]) -> list[str]:
    """Return the test topics whose namespace is not in ``excluded``."""
    selected = []
    for topic in topics:
        namespace = topic_namespace(topic)
        if namespace is not None and namespace not in excluded:
            selected.append(topic)
    return sorted(selected)


def _paginate(
    client: Any,
    operation: str,
    key: str,
    **kwargs: Any,
) -> list[Any]:
    items: list[Any] = []
    for page in client.get_paginator(operation).paginate(**kwargs):
        items.extend(page[key])
    return items


def recorded_namespaces(
    dynamodb: Any,
    namespaces: Iterable[str],
    tables: ServiceTables,
) -> set[str]:
    """Return which of ``namespaces`` the service has a record of.

    Raises on any read error, including a missing table, and when the
    users table has no namespace list or an empty one, so that a wrong
    table name never makes every namespace look unrecorded.
    """
    response = dynamodb.get_item(
        TableName=tables.users,
        Key={'subject': {'S': GLOBAL_NAMESPACES_KEY}},
        ConsistentRead=True,
    )
    recorded = set(
        response.get('Item', {}).get('namespaces', {}).get('SS', []),
    )
    if not recorded:
        raise RuntimeError(
            f'{tables.users} has no {GLOBAL_NAMESPACES_KEY} namespace list',
        )
    for namespace in sorted(set(namespaces) - recorded):
        response = dynamodb.get_item(
            TableName=tables.namespaces,
            Key={'namespace': {'S': namespace}},
            ConsistentRead=True,
        )
        if 'Item' in response:
            recorded.add(namespace)
    return recorded


def collect(
    iam: Any,
    dynamodb: Any,
    kafka_admin: Any | None,
    cutoff: datetime,
    tables: ServiceTables,
) -> Plan:
    """List the account and the cluster, and select the leftovers."""
    plan = Plan()
    users = _paginate(iam, 'list_users', 'Users', PathPrefix=IAM_USER_PATH)
    plan.users = select_users(users, cutoff)
    policies = _paginate(
        iam,
        'list_policies',
        'Policies',
        Scope='Local',
        PathPrefix=IAM_POLICY_PATH,
    )
    plan.policies = select_policies(policies, plan.users, cutoff)

    if kafka_admin is None:
        plan.skipped.append('Kafka topics: no bootstrap servers given')
        return plan
    # The default namespace of every other IAM user (real users, and test
    # users too young to sweep) is never swept.
    in_use = {
        default_namespace(user['UserName'])
        for user in users
        if user['UserName'] not in plan.users
    }
    try:
        topics = [t for t in kafka_admin.list_topics() if topic_namespace(t)]
        if not topics:
            return plan
        namespaces = {topic_namespace(t) or '' for t in topics}
        recorded = recorded_namespaces(dynamodb, namespaces - in_use, tables)
    except Exception as e:
        plan.errors.append(f'Kafka topics not selected: {e}')
        return plan
    plan.topics = select_topics(topics, recorded | in_use)
    return plan


def _delete_user(iam: Any, name: str) -> None:
    attached = _paginate(
        iam,
        'list_attached_user_policies',
        'AttachedPolicies',
        UserName=name,
    )
    unexpected = [
        p['PolicyArn']
        for p in attached
        if not p['PolicyArn'].endswith(f':policy{IAM_POLICY_PATH}{name}')
    ]
    inline = _paginate(iam, 'list_user_policies', 'PolicyNames', UserName=name)
    if unexpected or inline:
        raise RuntimeError(
            f'user {name} has policies the tests do not create: '
            f'{unexpected + inline}',
        )
    keys = _paginate(
        iam,
        'list_access_keys',
        'AccessKeyMetadata',
        UserName=name,
    )
    for key in keys:
        iam.delete_access_key(UserName=name, AccessKeyId=key['AccessKeyId'])
    for policy in attached:
        iam.detach_user_policy(UserName=name, PolicyArn=policy['PolicyArn'])
    iam.delete_user(UserName=name)


def _delete_policy(iam: Any, arn: str) -> None:
    versions = _paginate(
        iam,
        'list_policy_versions',
        'Versions',
        PolicyArn=arn,
    )
    for version in versions:
        if not version['IsDefaultVersion']:
            iam.delete_policy_version(
                PolicyArn=arn,
                VersionId=version['VersionId'],
            )
    iam.delete_policy(PolicyArn=arn)


def _delete_topics(kafka_admin: Any, topics: Iterable[str]) -> list[str]:
    failures = []
    for topic in topics:
        try:
            kafka_admin.delete_topics(
                [topic],
                timeout_ms=TOPIC_DELETE_TIMEOUT_MS,
            )
            print(f'deleted Kafka topic {topic}')
        except UnknownTopicOrPartitionError:
            print(f'Kafka topic {topic} was already gone')
        except Exception as e:
            failures.append(f'Kafka topic {topic}: {e}')
    return failures


def apply(plan: Plan, iam: Any, kafka_admin: Any | None) -> list[str]:
    """Delete everything in the plan; return a message per failure."""
    failures = []
    for name in plan.users:
        try:
            _delete_user(iam, name)
            print(f'deleted IAM user {name}')
        except Exception as e:
            failures.append(f'IAM user {name}: {e}')
    for arn in plan.policies:
        try:
            _delete_policy(iam, arn)
            print(f'deleted IAM policy {arn}')
        except Exception as e:
            failures.append(f'IAM policy {arn}: {e}')
    if kafka_admin is not None:
        failures.extend(_delete_topics(kafka_admin, plan.topics))
    return failures


def print_plan(plan: Plan, *, dry_run: bool) -> None:
    """Print the selected resources."""
    verb = 'would delete' if dry_run else 'deleting'
    for name in plan.users:
        print(f'{verb} IAM user {name} (with its access keys)')
    for arn in plan.policies:
        print(f'{verb} IAM policy {arn}')
    for topic in plan.topics:
        print(f'{verb} Kafka topic {topic}')
    for reason in plan.skipped:
        print(f'skipped {reason}')
    for error in plan.errors:
        print(f'error: {error}', file=sys.stderr)
    total = len(plan.users) + len(plan.policies) + len(plan.topics)
    print(f'{total} resource(s) selected')


def _kafka_admin(bootstrap_servers: str, region: str) -> KafkaAdminClient:
    return KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        security_protocol='SASL_SSL',
        sasl_mechanism='OAUTHBEARER',
        sasl_oauth_token_provider=MSKTokenProvider(region),
        request_timeout_ms=ADMIN_REQUEST_TIMEOUT_MS,
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse the command line."""
    parser = argparse.ArgumentParser(
        description='List or delete resources left behind by the live tests.',
    )
    parser.add_argument(
        '--apply',
        action='store_true',
        help='delete the selected resources (default: only list them)',
    )
    parser.add_argument(
        '--min-age-hours',
        type=float,
        default=DEFAULT_MIN_AGE_HOURS,
        help='skip IAM resources younger than this (default: %(default)s)',
    )
    parser.add_argument(
        '--region',
        default=os.getenv('AWS_ACCOUNT_REGION'),
        help='AWS region (default: $AWS_ACCOUNT_REGION)',
    )
    parser.add_argument(
        '--bootstrap-servers',
        default=os.getenv('DEFAULT_SERVERS'),
        help='MSK bootstrap servers (default: $DEFAULT_SERVERS)',
    )
    parser.add_argument(
        '--users-table',
        default=ServiceTables.users,
        help="the service's users table (default: %(default)s)",
    )
    parser.add_argument(
        '--namespace-table',
        default=ServiceTables.namespaces,
        help="the service's namespace table (default: %(default)s)",
    )
    args = parser.parse_args(argv)
    if not args.region:
        parser.error('--region or AWS_ACCOUNT_REGION is required')
    return args


def main(argv: Sequence[str] | None = None) -> int:
    """Run the sweep."""
    args = parse_args(argv)
    iam = boto3.client('iam')
    dynamodb = boto3.client('dynamodb', region_name=args.region)
    kafka_admin = (
        _kafka_admin(args.bootstrap_servers, args.region)
        if args.bootstrap_servers
        else None
    )
    try:
        plan = collect(
            iam,
            dynamodb,
            kafka_admin,
            cutoff=datetime.now(timezone.utc)
            - timedelta(hours=args.min_age_hours),
            tables=ServiceTables(args.users_table, args.namespace_table),
        )
        print_plan(plan, dry_run=not args.apply)
        failures = apply(plan, iam, kafka_admin) if args.apply else []
    finally:
        if kafka_admin is not None:
            with contextlib.suppress(Exception):
                kafka_admin.close()
    for failure in failures:
        print(f'failed to delete {failure}', file=sys.stderr)
    return 1 if failures or plan.errors else 0


if __name__ == '__main__':
    sys.exit(main())
