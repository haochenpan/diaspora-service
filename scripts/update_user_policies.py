"""Bring the IAM of the users the service created up to date, as an admin.

The service writes each user's IAM policy, and sets the user's permissions
boundary, only when it creates the user. This script changes the users it
created before, in two separate steps, each with its own undo:

``policies``
    Makes the current policy (``IAMService.generate_user_policy``) the
    default version of each user policy whose default version differs
    (``IAMService.update_user_policy``). After a change to the policy, this
    is what applies it to existing users; for example, it limits their
    consumer groups and transactional IDs to their namespace. It needs the
    service's ``MSK_CLUSTER_NAME``, and refuses a name that is not a
    cluster of the account.
``boundaries``
    Gives each user without a permissions boundary the boundary
    ``--boundary`` names, which must be
    ``arn:aws:iam::<account>:policy/diaspora-service/msk-user-boundary``
    and must exist. The service's own AWS identity can manage only users
    that have it. A user with another boundary is reported and left as it
    is.

Both find the users by the names and paths the service gives them: IAM
users under ``/msk-users/`` and managed policies under ``/msk-policy/``
whose names are subjects (UUIDs), and the subjects in the service's users
table. Other names, the live tests' ``test-subject-*`` among them, are
skipped.

Without ``--apply`` a step only prints what it would change; ``policies``
also prints how the policies differ. With ``--apply`` it writes each change
to the record file ``--record`` names as soon as the change is made, so a
crash loses none. The record file must not exist yet and must lie outside
this repository, since it names the account's ARNs. ``--undo RECORD`` puts
back what the record says the step changed: each policy's previous
default version, or no boundary. A policy or user changed again since is
reported and left as it is.

Running a step again changes only what still differs. Do not run two at
once: each reads IAM before it writes. It exits non-zero if listing or any
change fails. Run it with an administrator's credentials, and with the
service's ``AWS_ACCOUNT_ID``, ``AWS_ACCOUNT_REGION`` and
``MSK_CLUSTER_NAME``, so that ``policies`` builds the policy the service
builds.
"""

from __future__ import annotations

import argparse
import difflib
import json
import os
import re
import sys
import tempfile
from collections.abc import Iterable
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Any

import boto3

from web_service.services import GLOBAL_NAMESPACES_KEY
from web_service.services import IAMService

REPOSITORY = Path(__file__).resolve().parents[1]
IAM_USER_PATH = '/msk-users/'
IAM_POLICY_PATH = '/msk-policy/'
BOUNDARY_PATH = '/diaspora-service/'
BOUNDARY_NAME = 'msk-user-boundary'
UUID = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
SUBJECT = re.compile(UUID)
DEFAULT_USERS_TABLE = 'diaspora-users-table'

POLICIES = 'policies'
BOUNDARIES = 'boundaries'


def boundary_arn(account_id: str) -> str:
    """The ARN of the boundary of the service's users."""
    return f'arn:aws:iam::{account_id}:policy{BOUNDARY_PATH}{BOUNDARY_NAME}'


@dataclass
class Listing:
    """The service's users, policies and table subjects, as listed."""

    # Every IAM user under /msk-users/, at any depth
    all_users: list[str] = field(default_factory=list)
    # The subjects the service named: users and policies directly under
    # their paths, and subjects in the users table
    subjects: list[str] = field(default_factory=list)
    users: set[str] = field(default_factory=set)
    policies: set[str] = field(default_factory=set)
    notes: list[str] = field(default_factory=list)


@dataclass
class Plan:
    """The changes a step selected, and what it found but did not select."""

    # policies: (subject, default version, its document) of each stale one
    # boundaries: (user, None) of each user to give the boundary
    changes: list[tuple[str, Any, Any]] = field(default_factory=list)
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


def list_service_users(iam: Any, dynamodb: Any, table: str) -> Listing:
    """List the IAM users, IAM policies and table subjects of the service."""
    listing = Listing()
    users = _paginate(iam, 'list_users', 'Users', PathPrefix=IAM_USER_PATH)
    listing.all_users = sorted(user['UserName'] for user in users)
    listing.users = {
        u['UserName'] for u in users if u.get('Path') == IAM_USER_PATH
    }
    policies = _paginate(
        iam,
        'list_policies',
        'Policies',
        Scope='Local',
        PathPrefix=IAM_POLICY_PATH,
    )
    listing.policies = {
        p['PolicyName'] for p in policies if p.get('Path') == IAM_POLICY_PATH
    }
    names = listing.users | listing.policies | table_subjects(dynamodb, table)
    for name in sorted(names):
        if SUBJECT.fullmatch(name):
            listing.subjects.append(name)
        elif not name.startswith('test-subject-'):
            listing.notes.append(f'{name}: not a subject, skipped')
    return listing


def check_cluster(kafka: Any, cluster_name: str) -> None:
    """Exit unless the account has a cluster named cluster_name."""
    clusters = _paginate(kafka, 'list_clusters_v2', 'ClusterInfoList')
    names = sorted(c['ClusterName'] for c in clusters)
    if cluster_name not in names:
        sys.exit(f'no MSK cluster named {cluster_name!r}; found {names}')


def collect_policies(iam_service: IAMService, listing: Listing) -> Plan:
    """Select each user policy whose default version differs."""
    plan = Plan(notes=list(listing.notes))
    for subject in listing.subjects:
        if subject not in listing.policies:
            where = 'an IAM user' if subject in listing.users else 'listed'
            plan.notes.append(f'{subject}: {where}, but has no policy')
            continue
        try:
            version, document = iam_service.get_user_policy(subject)
        except Exception as e:
            plan.errors.append(f'policy of {subject}: {e}')
            continue
        if document == iam_service.generate_user_policy(subject):
            plan.current += 1
        else:
            plan.changes.append((subject, version, document))
    return plan


def collect_boundaries(iam: Any, listing: Listing, boundary: str) -> Plan:
    """Select each of the service's users that has no boundary.

    ``unbounded`` counts every user under /msk-users/ without a boundary,
    selected or not, the same count the switch to the service's identity
    waits on.
    """
    plan = Plan(notes=list(listing.notes))
    selected = set(listing.subjects) & listing.users
    for name in listing.all_users:
        try:
            user = iam.get_user(UserName=name)['User']
        except Exception as e:
            plan.errors.append(f'user {name}: {e}')
            continue
        current = user.get('PermissionsBoundary', {}).get(
            'PermissionsBoundaryArn',
        )
        if current is None:
            plan.unbounded += 1
            if name in selected:
                plan.changes.append((name, None, None))
            else:
                plan.notes.append(f'user {name} has no boundary, skipped')
        elif current == boundary:
            plan.current += 1
        elif name in selected:
            plan.notes.append(
                f'user {name} has another boundary, {current}; left as it is',
            )
    return plan


def _diff(old: Any, new: Any, subject: str) -> list[str]:
    """The lines that differ, with the subject's names made generic."""

    def lines(document: Any) -> list[str]:
        text = json.dumps(document, indent=2, sort_keys=True)
        suffix = subject.replace('-', '')[-12:]
        return text.replace(suffix, '<suffix>').splitlines()

    return [
        line
        for line in difflib.unified_diff(lines(old), lines(new), lineterm='')
        if not line.startswith(('---', '+++'))
    ]


def print_policies(plan: Plan, iam_service: IAMService, *, dry: bool) -> None:
    """Print the stale policies, and each distinct way they differ."""
    verb = 'would update' if dry else 'updating'
    diffs: dict[tuple[str, ...], list[str]] = {}
    for subject, version, document in plan.changes:
        arn = iam_service.policy_arn_prefix + subject
        print(f'{verb} {arn}: its default version {version} differs')
        new = iam_service.generate_user_policy(subject)
        diffs.setdefault(tuple(_diff(document, new, subject)), []).append(
            subject,
        )
    for lines, subjects in diffs.items():
        print(
            f'\n{len(subjects)} policies, {subjects[0]} first, differ from '
            "the current policy this way (<suffix> is the subject's "
            'namespace suffix):',
        )
        print('\n'.join(lines))
    _print_common(plan)
    print(
        f'{len(plan.changes)} policies to update, {plan.current} up to date',
    )


def print_boundaries(plan: Plan, boundary: str, *, dry: bool) -> None:
    """Print the users to give the boundary."""
    verb = 'would set' if dry else 'setting'
    for user, _, _ in plan.changes:
        print(f'{verb} the permissions boundary of user {user} to {boundary}')
    _print_common(plan)
    print(
        f'{len(plan.changes)} users to give the boundary, {plan.current} '
        f'with it; {plan.unbounded} users under {IAM_USER_PATH} without a '
        'boundary',
    )


def _print_common(plan: Plan) -> None:
    for note in plan.notes:
        print(note)
    for error in plan.errors:
        print(f'error: {error}', file=sys.stderr)


class Record:
    """The record file of an --apply run, rewritten after each change."""

    def __init__(self, path: Path, kind: str, account_id: str) -> None:
        """Create the record file, which must not exist, empty."""
        self.path = path
        self.data: dict[str, Any] = {
            'kind': kind,
            'account_id': account_id,
            'changes': [],
        }
        # Exclusive create: an existing file stops the run before a change
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.close(fd)
        self._write()

    def add(self, change: dict[str, Any]) -> None:
        """Add a change and write the whole record."""
        self.data['changes'].append(change)
        self._write()

    def _write(self) -> None:
        """Replace the file in one step, so a crash leaves a whole record."""
        fd, tmp = tempfile.mkstemp(
            dir=self.path.parent,
            prefix=f'.{self.path.name}.',
        )
        try:
            with os.fdopen(fd, 'w') as f:
                json.dump(self.data, f, indent=2)
                f.write('\n')
            os.replace(tmp, self.path)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)


def apply_policies(
    plan: Plan,
    iam_service: IAMService,
    record: Record,
) -> list[str]:
    """Update each stale policy, recording each update; return failures."""
    failures = []
    for subject, _, _ in plan.changes:
        result = iam_service.update_user_policy(subject)
        if result['status'] != 'success':
            failures.append(f'policy of {subject}: {result["message"]}')
        elif result['changed']:
            record.add(
                {
                    'policy_arn': iam_service.policy_arn_prefix + subject,
                    'old_version': result['old_version'],
                    'new_version': result['new_version'],
                },
            )
            print(result['message'])
        else:
            print(f'policy of {subject} was already up to date')
    return failures


def apply_boundaries(
    plan: Plan,
    iam: Any,
    boundary: str,
    record: Record,
) -> list[str]:
    """Give each selected user the boundary, recording each; return failures.

    A user is given the boundary only if it still has none.
    """
    failures = []
    for user, _, _ in plan.changes:
        try:
            current = iam.get_user(UserName=user)['User']
            if 'PermissionsBoundary' in current:
                print(f'user {user} has a boundary now; left as it is')
                continue
            iam.put_user_permissions_boundary(
                UserName=user,
                PermissionsBoundary=boundary,
            )
        except Exception as e:
            failures.append(f'boundary of user {user}: {e}')
            continue
        record.add(
            {'user': user, 'old_boundary': None, 'new_boundary': boundary},
        )
        print(f'set the permissions boundary of user {user}')
    return failures


def undo_policies(
    iam: Any,
    changes: Iterable[dict[str, Any]],
    account_id: str,
) -> list[str]:
    """Make each recorded policy's old version its default again."""
    arn_pattern = re.compile(
        rf'arn:aws:iam::{re.escape(account_id)}:policy'
        rf'{IAM_POLICY_PATH}{UUID}',
    )
    failures = []
    for change in changes:
        arn = change['policy_arn']
        old = change['old_version']
        if not arn_pattern.fullmatch(arn):
            failures.append(f'{arn}: not a user policy, skipped')
            continue
        try:
            policy = iam.get_policy(PolicyArn=arn)['Policy']
            default = policy['DefaultVersionId']
            if default == old:
                print(f'{arn}: {old} is already the default')
            elif default == change['new_version']:
                iam.set_default_policy_version(PolicyArn=arn, VersionId=old)
                print(f'{arn}: default set back to {old}')
            else:
                failures.append(
                    f'{arn}: the default is {default}, not the recorded '
                    f'{change["new_version"]}; left as it is',
                )
        except Exception as e:
            failures.append(f'{arn}: {e}')
    return failures


def undo_boundaries(
    iam: Any,
    changes: Iterable[dict[str, Any]],
    account_id: str,
) -> list[str]:
    """Put back each recorded user's previous boundary, or none."""
    failures = []
    for change in changes:
        user = change['user']
        if not SUBJECT.fullmatch(user):
            failures.append(f'user {user}: not a subject, skipped')
            continue
        if change['new_boundary'] != boundary_arn(account_id):
            failures.append(f'user {user}: not the service boundary, skipped')
            continue
        try:
            current = (
                iam.get_user(UserName=user)['User']
                .get('PermissionsBoundary', {})
                .get('PermissionsBoundaryArn')
            )
            if current == change['old_boundary']:
                print(f'user {user} already has its previous boundary')
            elif current != change['new_boundary']:
                failures.append(
                    f'user {user}: its boundary is {current}, not the '
                    'recorded one; left as it is',
                )
            elif change['old_boundary'] is None:
                iam.delete_user_permissions_boundary(UserName=user)
                print(f'removed the permissions boundary of user {user}')
            else:
                iam.put_user_permissions_boundary(
                    UserName=user,
                    PermissionsBoundary=change['old_boundary'],
                )
                print(f'set back the permissions boundary of user {user}')
        except Exception as e:
            failures.append(f'user {user}: {e}')
    return failures


def check_boundary(iam: Any, boundary: str, account_id: str) -> None:
    """Exit unless boundary is the service's boundary and exists."""
    expected = boundary_arn(account_id)
    if boundary != expected:
        sys.exit(f'--boundary must be {expected}')
    try:
        iam.get_policy(PolicyArn=boundary)
    except Exception as e:
        sys.exit(f'--boundary {boundary}: {e}')


def check_record_path(parser: argparse.ArgumentParser, path: Path) -> None:
    """Refuse a record path that exists or lies inside this repository."""
    if path.exists():
        parser.error(f'{path} exists; name a new --record')
    if path.resolve().is_relative_to(REPOSITORY):
        parser.error(f'{path} is inside the repository; keep it outside')


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse the command line."""
    parser = argparse.ArgumentParser(
        description="Bring the service's users' IAM up to date.",
    )
    parser.add_argument('step', choices=(POLICIES, BOUNDARIES))
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
        help='put back what an --apply run of the same step recorded',
    )
    parser.add_argument(
        '--record',
        type=Path,
        help='the new file where --apply records each change; required',
    )
    parser.add_argument(
        '--boundary',
        metavar='ARN',
        help='the boundary that boundaries gives users without one',
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
    required = ['account_id', 'region']
    if args.step == POLICIES and args.undo is None:
        required.append('cluster_name')
    for name in required:
        if not getattr(args, name):
            parser.error(
                f'--{name.replace("_", "-")} or its variable is required',
            )
    if args.step == BOUNDARIES and args.undo is None and not args.boundary:
        parser.error('boundaries needs --boundary')
    if args.step == POLICIES and args.boundary:
        parser.error('--boundary is for the boundaries step')
    if args.undo is not None and (args.boundary or args.record):
        parser.error('--undo takes its boundary and changes from the record')
    if args.apply:
        if args.record is None:
            parser.error('--apply needs --record')
        check_record_path(parser, args.record)
    return args


def run_undo(args: argparse.Namespace) -> int:
    """Undo the run recorded in the file args.undo names."""
    record = json.loads(args.undo.read_text())
    if record.get('kind') != args.step:
        sys.exit(f'{args.undo} records a {record.get("kind")} run')
    if record.get('account_id') != args.account_id:
        sys.exit(f'{args.undo} records another account')
    undo = undo_policies if args.step == POLICIES else undo_boundaries
    failures = undo(boto3.client('iam'), record['changes'], args.account_id)
    for failure in failures:
        print(f'failed: {failure}', file=sys.stderr)
    return 1 if failures else 0


def main(argv: Sequence[str] | None = None) -> int:
    """Run one step, or its undo."""
    args = parse_args(argv)
    if args.undo is not None:
        return run_undo(args)

    iam_service = IAMService(
        account_id=args.account_id,
        region=args.region,
        cluster_name=args.cluster_name or '',
    )
    iam = iam_service.iam
    if args.step == POLICIES:
        check_cluster(
            boto3.client('kafka', region_name=args.region),
            args.cluster_name,
        )
    else:
        check_boundary(iam, args.boundary, args.account_id)
    listing = list_service_users(
        iam,
        boto3.client('dynamodb', region_name=args.region),
        args.users_table,
    )
    if args.step == POLICIES:
        plan = collect_policies(iam_service, listing)
        print_policies(plan, iam_service, dry=not args.apply)
    else:
        plan = collect_boundaries(iam, listing, args.boundary)
        print_boundaries(plan, args.boundary, dry=not args.apply)

    failures: list[str] = []
    if args.apply:
        record = Record(args.record, args.step, args.account_id)
        if args.step == POLICIES:
            failures = apply_policies(plan, iam_service, record)
        else:
            failures = apply_boundaries(plan, iam, args.boundary, record)
        print(
            f'recorded {len(record.data["changes"])} changes in {args.record}',
        )
    for failure in failures:
        print(f'failed: {failure}', file=sys.stderr)
    return 1 if failures or plan.errors else 0


if __name__ == '__main__':
    sys.exit(main())
