"""List, and with --apply make, the changes that bring users' IAM up to date.

The service writes each user's IAM policy when it creates the user, so a
change to ``IAMService.generate_user_policy`` reaches only users created
after it. This script finds the users and policies the service created
before, by the names and paths it gives them:

- IAM users under ``/msk-users/`` and managed policies under
  ``/msk-policy/`` whose name is a subject (a UUID), and the subjects in
  the service's users table. The live tests' users and policies
  (``test-subject-*``) belong to the tests and the sweep, and are skipped.

For each of those subjects it selects:

- the policy, when its default version differs from the template. With
  ``--apply`` the template becomes its new default version
  (``IAMService.update_user_policy``).
- with ``--boundary ARN``, the IAM user, when it has no permissions
  boundary. With ``--apply`` the user gets ARN as its boundary. A user
  with another boundary is reported and left as it is.

Without ``--apply`` it only prints what it would change. With ``--apply``
it also writes what it changed to a record file (``--record``; it never
overwrites one). ``--undo RECORD`` makes each policy's default version in
the record the version that was the default before; a policy whose
default has changed since is reported and left as it is. Undo leaves the
boundaries in place: the boundary allows everything a user's policy
grants, so removing it gives a user nothing back.

Running the script again changes only what still differs. It exits
non-zero if listing or any change fails. Run it with an administrator's
credentials and the service's ``AWS_ACCOUNT_ID``, ``AWS_ACCOUNT_REGION``
and ``MSK_CLUSTER_NAME``, so it builds the policy the service builds.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections.abc import Iterable
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

import boto3

from web_service.services import GLOBAL_NAMESPACES_KEY
from web_service.services import IAMService

IAM_USER_PATH = '/msk-users/'
IAM_POLICY_PATH = '/msk-policy/'
SUBJECT = re.compile(
    r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}',
)
TEST_SUBJECT_PREFIX = 'test-subject-'
DEFAULT_USERS_TABLE = 'diaspora-users-table'


@dataclass
class Plan:
    """The changes selected, and what was found but not selected."""

    # (subject, default version) of each policy that differs
    policies: list[tuple[str, str]] = field(default_factory=list)
    # (user, boundary) of each user to give a boundary
    boundaries: list[tuple[str, str]] = field(default_factory=list)
    current: int = 0
    unbounded: int = 0
    notes: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


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


def _names(items: Iterable[dict[str, Any]], key: str, path: str) -> set[str]:
    return {item[key] for item in items if item.get('Path') == path}


def table_subjects(dynamodb: Any, table: str) -> set[str]:
    """Return the subjects of the users table's items."""
    items = _paginate(
        dynamodb,
        'scan',
        'Items',
        TableName=table,
        ProjectionExpression='subject',
        ConsistentRead=True,
    )
    return {item['subject']['S'] for item in items} - {GLOBAL_NAMESPACES_KEY}


def select_subjects(
    subjects: Iterable[str],
    plan: Plan,
) -> list[str]:
    """Return the subjects the service named, noting the others."""
    selected = []
    for subject in sorted(set(subjects)):
        if SUBJECT.fullmatch(subject):
            selected.append(subject)
        elif not subject.startswith(TEST_SUBJECT_PREFIX):
            plan.notes.append(f'{subject}: not a subject, skipped')
    return selected


def _check_policy(iam_service: IAMService, subject: str, plan: Plan) -> None:
    """Select the subject's policy if its default version differs."""
    try:
        version, document = iam_service.get_user_policy(subject)
    except Exception as e:
        plan.errors.append(f'policy of {subject}: {e}')
        return
    if document == iam_service.generate_user_policy(subject):
        plan.current += 1
    else:
        plan.policies.append((subject, version))


def _check_user(
    iam: Any,
    subject: str,
    boundary: str | None,
    plan: Plan,
) -> None:
    """Select the subject's IAM user if it has no permissions boundary."""
    try:
        user = iam.get_user(UserName=subject)['User']
    except Exception as e:
        plan.errors.append(f'user {subject}: {e}')
        return
    current = user.get('PermissionsBoundary', {}).get(
        'PermissionsBoundaryArn',
    )
    if current is None:
        plan.unbounded += 1
        if boundary:
            plan.boundaries.append((subject, boundary))
    elif boundary and current != boundary:
        plan.notes.append(
            f'user {subject} has another boundary, {current}; left as it is',
        )


def collect(
    iam_service: IAMService,
    dynamodb: Any,
    users_table: str,
    boundary: str | None,
) -> Plan:
    """List IAM and the users table, and select what differs."""
    plan = Plan()
    iam = iam_service.iam
    users = _names(
        _paginate(iam, 'list_users', 'Users', PathPrefix=IAM_USER_PATH),
        'UserName',
        IAM_USER_PATH,
    )
    policies = _names(
        _paginate(
            iam,
            'list_policies',
            'Policies',
            Scope='Local',
            PathPrefix=IAM_POLICY_PATH,
        ),
        'PolicyName',
        IAM_POLICY_PATH,
    )
    in_table = table_subjects(dynamodb, users_table)

    for subject in select_subjects(users | policies | in_table, plan):
        if subject in policies:
            _check_policy(iam_service, subject, plan)
        else:
            where = 'an IAM user' if subject in users else 'in the table'
            plan.notes.append(f'{subject}: {where}, but has no policy')
        if subject in users:
            _check_user(iam, subject, boundary, plan)
    return plan


def print_plan(plan: Plan, iam_service: IAMService, *, dry_run: bool) -> None:
    """Print the selected changes."""
    verb = 'would update' if dry_run else 'updating'
    for subject, version in plan.policies:
        arn = iam_service.policy_arn_prefix + subject
        print(f'{verb} {arn}: its default version {version} differs')
    verb = 'would set' if dry_run else 'setting'
    for user, boundary in plan.boundaries:
        print(f'{verb} the permissions boundary of user {user} to {boundary}')
    for note in plan.notes:
        print(note)
    for error in plan.errors:
        print(f'error: {error}', file=sys.stderr)
    print(
        f'{len(plan.policies)} policies to update, {plan.current} up to '
        f'date; {len(plan.boundaries)} users to give the boundary, '
        f'{plan.unbounded} users without a boundary',
    )


def apply(
    plan: Plan,
    iam_service: IAMService,
    record: dict[str, list[dict[str, str]]],
) -> list[str]:
    """Make the planned changes, adding each to record; return failures."""
    failures = []
    for subject, _ in plan.policies:
        result = iam_service.update_user_policy(subject)
        if result['status'] != 'success':
            failures.append(f'policy of {subject}: {result["message"]}')
        elif result['changed']:
            record['policies'].append(
                {
                    'policy_arn': iam_service.policy_arn_prefix + subject,
                    'old_version': result['old_version'],
                    'new_version': result['new_version'],
                },
            )
            print(result['message'])
        else:
            print(f'policy of {subject} was already up to date')
    for user, boundary in plan.boundaries:
        try:
            iam_service.iam.put_user_permissions_boundary(
                UserName=user,
                PermissionsBoundary=boundary,
            )
        except Exception as e:
            failures.append(f'boundary of user {user}: {e}')
            continue
        record['boundaries'].append({'user': user, 'boundary': boundary})
        print(f'set the permissions boundary of user {user}')
    return failures


def undo(iam: Any, record: dict[str, Any]) -> list[str]:
    """Make each recorded policy's old version its default again."""
    failures = []
    for entry in record.get('policies', []):
        arn = entry['policy_arn']
        old = entry['old_version']
        if f':policy{IAM_POLICY_PATH}' not in arn:
            failures.append(f'{arn}: not a user policy, skipped')
            continue
        try:
            default = iam.get_policy(PolicyArn=arn)['Policy'][
                'DefaultVersionId'
            ]
            if default == old:
                print(f'{arn}: {old} is already the default')
            elif default == entry['new_version']:
                iam.set_default_policy_version(PolicyArn=arn, VersionId=old)
                print(f'{arn}: default set back to {old}')
            else:
                failures.append(
                    f'{arn}: the default is {default}, not the recorded '
                    f'{entry["new_version"]}; left as it is',
                )
        except Exception as e:
            failures.append(f'{arn}: {e}')
    boundaries = record.get('boundaries', [])
    if boundaries:
        print(f'left the permissions boundary on {len(boundaries)} users')
    return failures


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse the command line."""
    parser = argparse.ArgumentParser(
        description="Bring the service's users' IAM policies up to date.",
    )
    action = parser.add_mutually_exclusive_group()
    action.add_argument(
        '--apply',
        action='store_true',
        help='make the selected changes (default: only list them)',
    )
    action.add_argument(
        '--undo',
        type=Path,
        metavar='RECORD',
        help='set back the default versions an --apply run recorded',
    )
    # No default from IAM_USER_PERMISSIONS_BOUNDARY_ARN: a shell set up
    # for the live tests holds their boundary, which allows test names only
    parser.add_argument(
        '--boundary',
        metavar='ARN',
        help='give users without a permissions boundary this one',
    )
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    parser.add_argument(
        '--record',
        type=Path,
        default=Path(f'user-policies-{stamp}.json'),
        help='where --apply writes its record (default: %(default)s)',
    )
    parser.add_argument(
        '--account-id',
        default=os.getenv('AWS_ACCOUNT_ID'),
        help='AWS account ID (default: $AWS_ACCOUNT_ID)',
    )
    parser.add_argument(
        '--region',
        default=os.getenv('AWS_ACCOUNT_REGION'),
        help='AWS region (default: $AWS_ACCOUNT_REGION)',
    )
    parser.add_argument(
        '--cluster-name',
        default=os.getenv('MSK_CLUSTER_NAME'),
        help='MSK cluster name (default: $MSK_CLUSTER_NAME)',
    )
    parser.add_argument(
        '--users-table',
        default=DEFAULT_USERS_TABLE,
        help="the service's users table (default: %(default)s)",
    )
    args = parser.parse_args(argv)
    if args.apply and args.record.exists():
        parser.error(f'{args.record} exists; name a new --record')
    if args.undo is None:
        for name in ('account_id', 'region', 'cluster_name'):
            if not getattr(args, name):
                option = name.replace('_', '-')
                parser.error(f'--{option} or its variable is required')
    return args


def run_undo(path: Path) -> int:
    """Undo the run recorded in the file at path."""
    failures = undo(boto3.client('iam'), json.loads(path.read_text()))
    for failure in failures:
        print(f'failed: {failure}', file=sys.stderr)
    return 1 if failures else 0


def main(argv: Sequence[str] | None = None) -> int:
    """Run the update, or its undo."""
    args = parse_args(argv)
    if args.undo is not None:
        return run_undo(args.undo)

    iam_service = IAMService(
        account_id=args.account_id,
        region=args.region,
        cluster_name=args.cluster_name,
    )
    dynamodb = boto3.client('dynamodb', region_name=args.region)
    plan = collect(iam_service, dynamodb, args.users_table, args.boundary)
    print_plan(plan, iam_service, dry_run=not args.apply)
    failures: list[str] = []
    if args.apply:
        record: dict[str, list[dict[str, str]]] = {
            'policies': [],
            'boundaries': [],
        }
        # Opened before any change, so an existing file stops the run
        with args.record.open('x') as f:
            try:
                failures = apply(plan, iam_service, record)
            finally:
                json.dump(record, f, indent=2)
                f.write('\n')
        print(f'wrote the record of the changes to {args.record}')
    for failure in failures:
        print(f'failed: {failure}', file=sys.stderr)
    return 1 if failures or plan.errors else 0


if __name__ == '__main__':
    sys.exit(main())
