# Testing

The tests come in two sets:

- **Unit tests**: every test not marked `integration`. They mock or stub
  AWS and Kafka and need no account, credentials or network. Any of them
  that opens a network connection fails.
- **Live tests**: the tests marked `integration`. They create real IAM
  users, DynamoDB items and Kafka topics in the AWS account and on the MSK
  cluster that the credentials point to, and delete them afterwards. Every
  test in `db_service_test.py`, `iam_service_test.py`,
  `kafka_service_test.py`, `namespace_service_test.py` and
  `web_service_test.py` is marked `integration` by `tests/conftest.py`;
  other files mark their live tests one by one.

## Run the unit tests locally

Install the dev extras first (`pip install -e ".[dev]"`, as in
[Running locally](1-running.md#set-up)). The service checks a few
environment variables at import, so give it placeholders, and point the
AWS SDK away from any credentials you have:

```bash
AWS_CONFIG_FILE=/dev/null \
AWS_SHARED_CREDENTIALS_FILE=/dev/null \
AWS_EC2_METADATA_DISABLED=true \
AWS_ACCESS_KEY_ID=dummy-not-real \
AWS_SECRET_ACCESS_KEY=dummy-not-real \
SERVER_CLIENT_ID=00000000-0000-0000-0000-000000000000 \
SERVER_SECRET=dummy-not-real \
AWS_ACCOUNT_ID=000000000000 \
AWS_ACCOUNT_REGION=us-east-1 \
MSK_CLUSTER_NAME=dummy \
pytest tests/ -m "not integration"
```

## Run the live tests locally

The live tests need the service's environment variables with real values
(AWS keys, `AWS_ACCOUNT_ID`, `AWS_ACCOUNT_REGION`, `MSK_CLUSTER_NAME`,
`DEFAULT_SERVERS` and the Globus client settings):

```bash
pytest tests/ -m integration -vv -s
```

They write to the account and the cluster the credentials point to. Do not
run them while another live run is in progress: some tests assert on
records shared by all runs. With the live tests' own keys (see below),
also set `IAM_USER_PERMISSIONS_BOUNDARY_ARN` to the boundary policy's ARN,
or every test that creates an IAM user is denied.

Their DynamoDB items go to the `test-diaspora-*` tables. Most live tests
name those tables in code, but the live tests in
`consumer_service_test.py` take the tables from `KEYS_TABLE_NAME`,
`USERS_TABLE_NAME` and `NAMESPACE_TABLE_NAME` when these are set. Leave
them unset, or set them to the test tables as `tests.yml` does, so that no
test writes to the production tables.

With coverage, over both sets:

```bash
coverage erase
coverage run -m pytest tests/
coverage report
```

## What runs in CI

| Workflow | Job | Tests | When | Secrets |
| --- | --- | --- | --- | --- |
| `tests.yml` | `unit` | unit | pull requests; pushes to `main`, `test-me-*` and tags; nightly; manual dispatch | none |
| `tests.yml` | `live` | live, then the sweep (nightly only) | pushes to `main`, nightly, manual dispatch | the live tests' AWS keys, or the service's while those are not set |
| `deployment.yml` | `unit` | unit | before every deploy | none |

- The deploy job in `deployment.yml` waits for its `unit` job, so a failing
  unit test on `main` stops the deploy.
- The `live` job never runs on pull requests, tag pushes or `test-me-*`
  pushes. A manual dispatch runs the live tests of the branch it is
  dispatched on, whichever branch that is.
- `live` runs in the `live-tests` concurrency group, so no two live runs
  happen at the same time. GitHub keeps only one pending run per group: a
  run that is waiting can be cancelled by a newer one.
- In the nightly run, after the live tests whether they passed or failed
  (not when the job was cancelled), the last step of `live` runs
  `scripts/sweep_test_resources.py --apply`. It deletes what earlier
  live runs left behind: IAM users and policies named
  `test-subject-test<8 hex>` older than two hours, and Kafka topics named
  `test-namespace-<16 hex>.test-topic-<16 hex>` or
  `ns-test<8 hex>.test-topic-<suffix>` (with the suffixes the tests use)
  whose namespace the service has no record of and that is not the default
  namespace of another IAM user. It also selects leftovers with the names
  the tests used before (`test-subject-<16 hex>`,
  `ns-<12 hex>.test-topic-<suffix>`). The live tests' own identity can
  delete IAM users and policies with those names, but it cannot see or
  delete topics with those names; only the service's keys can (see
  [Create the identity](#create-the-identity)). Kafka keeps no creation
  time for topics, so topics are not filtered by age: a local live run
  around the nightly time can lose its topics. Without `--apply` the
  script only lists what it would delete.

## The live tests' AWS identity

The live tests and the sweep run against the service's production account
and MSK cluster, as an IAM user of their own whose policy lets them touch
only what they create. Its keys are the repository secrets
`LIVE_TESTS_AWS_ACCESS_KEY_ID` and `LIVE_TESTS_AWS_SECRET_ACCESS_KEY`;
while either of them is not set, `live` uses the service's keys.

The policy, [`tests/live_identity/identity-policy.json`](https://github.com/haochenpan/diaspora-service/blob/main/tests/live_identity/identity-policy.json),
allows:

- Creating IAM users under `/msk-users/` and managed policies under
  `/msk-policy/` whose names are `test-subject-test` followed by exactly
  8 characters, attaching such a policy to such a user, and creating
  access keys for such a user. A user can be created, or given a policy,
  only with the boundary below as its permissions boundary.
- Reading any IAM user named `test-subject-*`, detaching its policies,
  deleting its access keys and deleting it, and deleting any policy of
  that name, so the sweep also removes test users named the way the tests
  used to name them.
- Listing all IAM users and policies, which the sweep needs.
- Reading and writing items in the three `test-diaspora-*` tables.
- In the service's own tables, reading only the global namespace list and
  the records of test namespaces. Nothing else in them, and never the keys
  table.
- On the cluster: connecting; creating, reading, writing and deleting
  topics in namespaces starting `ns-test` or `test-namespace-`; and using
  consumer groups in namespaces starting `ns-test`.

Every IAM user the tests create gets the permissions boundary
[`tests/live_identity/boundary-policy.json`](https://github.com/haochenpan/diaspora-service/blob/main/tests/live_identity/boundary-policy.json),
so whatever policy is attached to it, the user can do no more than read
and write test topics and groups. The service sets that boundary when
`IAM_USER_PERMISSIONS_BOUNDARY_ARN` is set, and sets none when it is not.

### Naming live-test resources

The identity can touch only resources with these names, and the sweep
finds only these, so every live test takes its names from
`tests/live_identity`:

- a subject (and so an IAM user and policy): `live_subject()`, which
  returns `test-subject-test<8 hex>`; the shared `random_subject` fixture
  in `tests/conftest.py` returns one. Its default namespace is
  `ns-test<8 hex>`.
- a namespace without a user: `live_namespace()`, which returns
  `ns-test<8 hex>`.
- Kafka topics and consumer groups: `<namespace>.<name>` in one of those
  namespaces, or in `test-namespace-<hex>`.

A real user's subject is a UUID, so its default namespace is `ns-` and 12
hex digits, which never starts with `ns-test`.

### Create the identity

Run these with an administrator's credentials, at a time when no live run
is in progress.

First create the boundary, the identity's policy, the user and its access
key. Fill in the account ID, region, MSK cluster name and the cluster's
UUID (the last part of its ARN, from `aws kafka list-clusters-v2`):

```bash
ACCOUNT_ID=<account-id>
REGION=<region>
CLUSTER_NAME=<cluster-name>
CLUSTER_UUID=<cluster-uuid>

render() {
  sed -e "s/<account-id>/$ACCOUNT_ID/g" -e "s/<region>/$REGION/g" \
    -e "s/<cluster-name>/$CLUSTER_NAME/g" \
    -e "s/<cluster-uuid>/$CLUSTER_UUID/g" "$1"
}

aws iam create-policy --path /diaspora-test/ \
  --policy-name test-user-boundary \
  --policy-document "$(render tests/live_identity/boundary-policy.json)"
aws iam create-policy --path /diaspora-test/ \
  --policy-name diaspora-live-tests \
  --policy-document "$(render tests/live_identity/identity-policy.json)"
aws iam create-user --user-name diaspora-live-tests
aws iam attach-user-policy --user-name diaspora-live-tests \
  --policy-arn "arn:aws:iam::$ACCOUNT_ID:policy/diaspora-test/diaspora-live-tests"
aws iam create-access-key --user-name diaspora-live-tests
```

Do not name the user `test-subject-*`, or its policy would let it manage
itself.

Just before storing the key, delete with the service's keys everything
earlier live runs left behind, whatever its age. Until the secrets are
set, live runs use the service's keys and create test users without a
boundary, and the identity cannot see or delete the topics named the way
the tests used to name them. The sweep reads the region and the bootstrap
servers from `AWS_ACCOUNT_REGION` and `DEFAULT_SERVERS`. Without `--apply`
it lists what it would delete:

```bash
python scripts/sweep_test_resources.py --min-age-hours 0
python scripts/sweep_test_resources.py --apply --min-age-hours 0
```

Then store the key's `AccessKeyId` and `SecretAccessKey` as the repository
secrets. Each command prompts for the value, which keeps the key out of
the shell history:

```bash
gh secret set LIVE_TESTS_AWS_ACCESS_KEY_ID --repo haochenpan/diaspora-service
gh secret set LIVE_TESTS_AWS_SECRET_ACCESS_KEY --repo haochenpan/diaspora-service
```

Once both secrets are set, `live` runs as the identity and sets
`IAM_USER_PERMISSIONS_BOUNDARY_ARN` to
`arn:aws:iam::<account-id>:policy/diaspora-test/test-user-boundary`.

To change what the identity may do, edit the template and apply it as a
new default version of the policy. IAM keeps at most five versions of a
policy, so when it holds five, first delete the oldest version that is
not the default, replacing `v1` below with the ID the list prints with
the earliest date:

```bash
POLICY_ARN="arn:aws:iam::$ACCOUNT_ID:policy/diaspora-test/diaspora-live-tests"
aws iam list-policy-versions --policy-arn "$POLICY_ARN" \
  --query 'Versions[?!IsDefaultVersion].[VersionId,CreateDate]' --output text
aws iam delete-policy-version --policy-arn "$POLICY_ARN" --version-id v1
aws iam create-policy-version --set-as-default --policy-arn "$POLICY_ARN" \
  --policy-document "$(render tests/live_identity/identity-policy.json)"
```
