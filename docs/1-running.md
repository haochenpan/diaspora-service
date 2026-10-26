# Running locally

This page runs the service on your machine, from a virtual environment or
in a Docker container. The service reads its configuration from the
[environment variables](#environment-variables) at the end of this page;
[Deployment](3-deploying.md#container-environment) describes how the
production container sets them.

## What a local run changes

!!! warning "With real AWS keys, a local run changes the production account"
    The service has no local or mock mode. It acts on the AWS account that
    its keys belong to, and on the MSK cluster that `DEFAULT_SERVERS`
    names:

    - IAM: `POST /api/v3/user` and `POST /api/v3/key` create IAM users,
      policies and access keys, and the `DELETE` routes delete them. When
      the service's keys table holds no key for the caller,
      `POST /api/v3/key`
      deletes every access key the caller's IAM user holds before it
      creates a new one. No variable changes where the IAM users go.
    - DynamoDB: without `KEYS_TABLE_NAME`, `USERS_TABLE_NAME` and
      `NAMESPACE_TABLE_NAME`, the service reads and writes the production
      tables.
    - MSK: with `DEFAULT_SERVERS` set, the topic routes create and delete
      topics on that cluster, and the consumer routes read from it.

    IAM users and namespaces are named after the caller's Globus identity.
    If you call a local service with the Globus identity you use with the
    production service, it acts on the same IAM user as production and,
    with `DEFAULT_SERVERS` set, on the same topics.

## Keep a local run away from production data

The test tables keep the service's records out of the production tables,
but they do not isolate IAM. IAM users, policies and access keys always go
to the account that the AWS keys belong to, named after the caller's
Globus identity.

- Point the three tables at the test tables, which the live tests also
  use. `.env.example` does this:

    ```bash
    KEYS_TABLE_NAME=test-diaspora-keys-table
    USERS_TABLE_NAME=test-diaspora-users-table
    NAMESPACE_TABLE_NAME=test-diaspora-namespace-table
    ```

    If a table name is misspelt, the service creates a new table with
    that name in the account at its first write.

- Leave `DEFAULT_SERVERS` unset unless you need the cluster. The topic
  routes then change only the service's records, and creating a consumer
  or recreating a topic fails with `Kafka endpoint is not configured`.
- Call the service with a Globus identity that has no resources in the
  production service. By default the SDK requests tokens only for the
  production service's Globus client ([Call the API](#call-the-api)). A
  local service that takes SDK tokens therefore runs with the production
  client, and the SDK signs you in with your own Globus identity, the one
  production also knows.
- When you are done, and only for an identity that has no resources in
  production, call `DELETE /api/v3/user`. It deletes the IAM user, its
  policy, its access keys, its default namespace and that namespace's
  topics. For an identity you also use with production, it deletes the
  production IAM user and policy.

If you call a local service on the test tables with an identity that has a
key in production, your first `POST /api/v3/key` finds no key in the test
keys table. It deletes the access keys of your production IAM user and
creates a new one. Clients that hold your old key, such as the SDK on your
machine, can no longer connect to the cluster. The production keys table
still holds the deleted key, and the production service's
`POST /api/v3/key` keeps returning it with `fresh` set to `false`. To
recover, call `DELETE /api/v3/key` on the production service, then
`POST /api/v3/key` there: the first deletes the IAM user's keys and the
stored key, and the second creates and stores a new one. After that, a
local `DELETE /api/v3/key` or `DELETE /api/v3/user` with that identity
still acts on the production IAM user, whatever the test tables hold. A
local `POST /api/v3/key` keeps returning the deleted key from the test
keys table. No route removes that item without also deleting the IAM
user's keys, so delete it from the table directly.

To check only that the service starts, run it with the placeholder values
of the unit tests ([Run the unit tests locally](2-testing.md#run-the-unit-tests-locally)).
It makes no AWS call and serves its API documentation at `/`. API requests
with a well-formed token reach Globus but not AWS, and fail: Globus
rejects the placeholder client credentials, so they get `500`
([Authentication](architecture.md#authentication) lists the other
answers).

## Set up

The service needs Python 3.14, which CI and the container image run. A
full local run also needs AWS keys for the service's account and the ID
and secret of the service's Globus confidential client. Without them, you
can run the unit tests and start the service with placeholders.

Install the locked dependencies, the service and the locked dev tools, as
CI does ([Dependencies](contributing.md#dependencies)):

```bash
python3.14 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
pip install --no-deps -e .
pip install -r requirements-dev.txt -c requirements.txt
cp .env.example .env
```

The required values are keys for the production service's AWS account
and the production service's Globus client, so read
[What a local run changes](#what-a-local-run-changes) before you fill them
in. Fill in the required values in `.env`, which git ignores. Write each value
without quotes and without a trailing comment: `docker run --env-file`
takes every value literally.

## Run from the virtual environment

In development mode, with reloading on code changes:

```bash
(set -a; . ./.env; set +a; exec uvicorn web_service.main:app --port 8000 --reload)
```

As the container runs it, with gunicorn and one Uvicorn worker:

```bash
(set -a; . ./.env; set +a; exec gunicorn --bind 127.0.0.1:8000 -k uvicorn_worker.UvicornWorker web_service.main:app)
```

The parentheses run each command in a subshell, so the values from `.env`
stay out of your shell. Run one worker only: the service keeps consumer
instances in the worker's memory
([One worker](3-deploying.md#one-worker)). gunicorn reads its worker count
from `WEB_CONCURRENCY` and extra options from `GUNICORN_CMD_ARGS`, so a
value of either in your shell can change the number of workers.

The API documentation is at <http://127.0.0.1:8000/>.

## Run in Docker

Build the image from the repository root, then run it with `.env`:

```bash
docker build -t diaspora-service -f web_service/Dockerfile .
docker run --rm -p 127.0.0.1:8000:8000 --env-file .env diaspora-service
```

The Dockerfile pins the platform `linux/amd64`, which Lightsail runs. The
image runs the service as a user other than root.

## Call the API

Every API route needs two headers: `subject`, the caller's Globus identity ID,
and `authorization`, a Globus bearer token. The service accepts a token
only when it names `SERVER_CLIENT_ID` in its audience and carries the
scope `https://auth.globus.org/scopes/<SERVER_CLIENT_ID>/action_all`. The
[API reference](web/api.md) shows how to get both headers with the
Diaspora Event SDK. By default the SDK requests tokens for the production
service's Globus client, so its tokens work with a local service only when
`SERVER_CLIENT_ID` is that client's ID.

## Environment variables

The service checks the required variables when `web_service.main` is
imported. A missing or empty one stops it with
`AssertionError: <NAME> environment variable is not set.`

| Variable | Required | Default | What it does |
| --- | --- | --- | --- |
| `AWS_ACCESS_KEY_ID` | yes | | The AWS access key the service uses for IAM, DynamoDB and MSK. |
| `AWS_SECRET_ACCESS_KEY` | yes | | The secret of that key. |
| `SERVER_CLIENT_ID` | yes | | The ID of the Globus confidential client that tokens must be issued for. |
| `SERVER_SECRET` | yes | | The secret of that client, used to introspect tokens. |
| `AWS_ACCOUNT_ID` | yes | | The account in the ARNs of the IAM policies the service writes. |
| `AWS_ACCOUNT_REGION` | yes | | The region of the DynamoDB tables and the MSK cluster. |
| `MSK_CLUSTER_NAME` | yes | | The cluster named in those IAM policies. |
| `DEFAULT_SERVERS` | no | unset | The MSK bootstrap brokers for IAM authentication, as comma-separated `host:port` pairs. The service returns it to users with their key. Unset, the service makes no Kafka calls. |
| `KEYS_TABLE_NAME` | no | `diaspora-keys-table` | The DynamoDB table of access keys. |
| `USERS_TABLE_NAME` | no | `diaspora-users-table` | The DynamoDB table of the namespaces each user owns. |
| `NAMESPACE_TABLE_NAME` | no | `diaspora-namespace-table` | The DynamoDB table of the topics in each namespace. |
| `MAX_TOPICS_PER_NAMESPACE` | no | `50` | The most topics a namespace may hold. A value that is not a whole number of at least 1 is logged and replaced by 50. |
| `CONSUMER_INSTANCE_TIMEOUT_MS` | no | `300000` (5 minutes) | How long a consumer instance may sit idle before the service closes it. A value that is not an integer, including an empty one, stops the service at start with a `ValueError`. |
| `IAM_USER_PERMISSIONS_BOUNDARY_ARN` | no | unset | The ARN of a managed policy that the service sets as the permissions boundary of every IAM user it creates. Production leaves it unset; the live tests set it ([Testing](2-testing.md#the-live-tests-aws-identity)). |

The AWS SDK reads the two key variables itself, and with them
`AWS_SESSION_TOKEN` when you use temporary credentials.
