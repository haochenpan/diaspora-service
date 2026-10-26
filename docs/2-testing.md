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

The service checks a few environment variables at import, so give it
placeholders, and point the AWS SDK away from any credentials you have:

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
records shared by all runs.

With coverage, over both sets:

```bash
coverage erase
coverage run -m pytest tests/
coverage report
```

### Run `tox`

```bash
source secrets2.sh  # pragma: allowlist secret
tox
```

## What runs in CI

| Workflow | Job | Tests | When | Secrets |
| --- | --- | --- | --- | --- |
| `tests.yml` | `unit` | unit | pull requests; pushes to `main`, `test-me-*` and tags; nightly; manual dispatch | none |
| `tests.yml` | `live` | live | pushes to `main`, nightly, manual dispatch | the service's AWS keys |
| `tests.yml` | `sweep` | none | nightly, after `live` | the service's AWS keys |
| `deployment.yml` | `unit` | unit | before every deploy | none |

- The deploy job in `deployment.yml` waits for its `unit` job, so a failing
  unit test on `main` stops the deploy.
- The `live` job never runs on pull requests or `test-me-*` pushes. A
  manual dispatch runs the live tests of the branch it is dispatched on,
  whichever branch that is.
- `live` and `sweep` share the `live-tests` concurrency group, so no two
  of them run at the same time. GitHub keeps only one pending run per
  group: a run that is waiting can be cancelled by a newer one.
- After the nightly live run, unless it was cancelled, the `sweep` job
  runs `scripts/sweep_test_resources.py --apply`. It deletes what earlier
  live runs left behind: IAM users and policies named
  `test-subject-<16 hex>` older than two hours, and Kafka topics named
  `test-namespace-<16 hex>.test-topic-<16 hex>` or
  `ns-<12 hex>.test-topic-<suffix>` (with the suffixes the tests use)
  whose namespace the service has no record of and that is not the default
  namespace of another IAM user. Kafka keeps no creation time for topics,
  so topics are not filtered by age: a local live run around the nightly
  time can lose its topics. Without `--apply` the script only lists what
  it would delete.
