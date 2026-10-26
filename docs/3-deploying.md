# Deployment

The service runs as one container on the AWS Lightsail container service
`diaspora-web-service` in `us-east-1`. The `deployment` workflow
([`.github/workflows/deployment.yml`](https://github.com/haochenpan/diaspora-service/blob/main/.github/workflows/deployment.yml))
builds the image from `web_service/Dockerfile` and deploys it. Parts of the
workflow are adapted from
[fdiesel/github-action-deploy-aws-lightsail-container](https://github.com/fdiesel/github-action-deploy-aws-lightsail-container).
Rollback, logs, triage and credential rotation are in the
[runbook](5-runbook.md).

In this page a *deploy* is one run of the workflow, and a *deployment* is
what Lightsail runs: a numbered version that names the container, its
image and its environment.

## What triggers a deploy

- A push to `main`, which includes merging a pull request. A push whose
  changed files are all under `docs/`, are Markdown files, or are
  `mkdocs.yml` does not deploy; a push that changes code and docs together
  does.
- A manual run of the workflow (see [Deploy by hand](#deploy-by-hand)).

Nothing else deploys: not pull requests, other branches, tags or a
schedule.

Deploys run one at a time in the `production` concurrency group. A deploy
in progress is never cancelled. While one runs, GitHub keeps only the
newest waiting run and cancels older waiting runs. After several quick
merges, the running deploy finishes, and then the newest waiting run
deploys the latest `main`.

## What the workflow does

1. The `unit` job runs the unit tests (the reusable `unit-tests.yml`
   workflow, without secrets; see [Testing](2-testing.md)). If a test
   fails, nothing is deployed.
2. The `deploy-lightsail-containers` job then runs these steps:
    1. It configures AWS credentials from the `AWS_ACCESS_KEY_ID` and
       `AWS_SECRET_ACCESS_KEY` repository secrets for `us-east-1`, and
       checks them with `aws sts get-caller-identity`.
    2. It installs the Lightsail CLI plugin (`lightsailctl`), which
       `aws lightsail push-container-image` needs.
    3. It checks out the commit and builds the image from
       `web_service/Dockerfile`, tagged `docker`.
    4. It creates the container service with power `micro` (0.25 vCPU,
       1 GB of memory) and scale 1. When the service already exists, this
       step fails and the job continues.
    5. It pushes the image with the label `docker`. Lightsail stores it as
       `:diaspora-web-service.docker.<N>`, where `<N>` grows by one with
       each push.
    6. It creates a deployment with one container named `flask` that runs
       the new image, listens on port 8000 over HTTP and is the public
       endpoint. The container's environment comes from the `DOCKER_*`
       secrets ([Secrets](#secrets)). Each deployment carries its whole
       environment, so a variable the workflow does not pass is unset in
       the container.
    7. It waits for the deployment to become active
       ([The wait step](#the-wait-step)).
    8. It deletes the stored images that no deployment still needs. It
       keeps the images of the active deployment, of any deployment still
       activating, and of the most recent inactive deployment, which is
       the rollback target. This step runs only after the wait step
       succeeds, and an error in it does not fail the run.

### The wait step

The step reads the version number of the new deployment, then checks the
service every 30 seconds. It succeeds when that version is the current
deployment and its state is `ACTIVE`. While a new deployment activates,
Lightsail keeps the previous one serving; if the new one fails, the
previous one stays active.

| Error in the run log | What it usually means | What to do |
| --- | --- | --- |
| `No pending deployment found for diaspora-web-service` | Right after the create call, Lightsail showed no pending deployment. | Check the service's state ([Check a deploy](#check-a-deploy)) and the previous step's log. |
| `Deployment <version> failed; check the container logs` | Lightsail marked the deployment `FAILED`: the container did not start, or did not pass Lightsail's health check, which requests `/`. A missing environment variable is one cause. | Read the container log and [triage the failure](5-runbook.md#triage-a-failed-deploy). |
| `Deployment <version> is no longer pending and did not become active` | The version is neither current nor pending. Another deployment, made from the console or the CLI while the workflow waited, can replace it; workflow runs do not overlap, so it was not another run. | List the deployments and their states ([Roll back a deploy](5-runbook.md#roll-back-a-deploy) shows the command). |
| `Deployment <version> is not active after 20 minutes` | Lightsail was still activating the deployment when the step stopped waiting. It can still become active or fail. | Check the service's state and the container log. The image cleanup did not run. |

## Secrets

The deploy uses two sets of repository secrets. `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY` are the workflow's own AWS key. It needs access to
the Lightsail container service only, and the container never sees it. The
`DOCKER_*` secrets become the container's environment, without the
prefix. The `live` and `sweep` jobs of `tests.yml` read the same `DOCKER_*`
secrets, so a change to one of them also reaches the live tests.

| Secret | Container variable | Notes |
| --- | --- | --- |
| `AWS_ACCESS_KEY_ID` | none | The deploy's AWS access key ID. |
| `AWS_SECRET_ACCESS_KEY` | none | The deploy's AWS secret access key. |
| `DOCKER_AWS_ACCESS_KEY_ID` | `AWS_ACCESS_KEY_ID` | Required. |
| `DOCKER_AWS_SECRET_ACCESS_KEY` | `AWS_SECRET_ACCESS_KEY` | Required. |
| `DOCKER_SERVER_CLIENT_ID` | `SERVER_CLIENT_ID` | Required. |
| `DOCKER_SERVER_SECRET` | `SERVER_SECRET` | Required. |
| `DOCKER_AWS_ACCOUNT_ID` | `AWS_ACCOUNT_ID` | Required. |
| `DOCKER_AWS_ACCOUNT_REGION` | `AWS_ACCOUNT_REGION` | Required. |
| `DOCKER_MSK_CLUSTER_NAME` | `MSK_CLUSTER_NAME` | Required. |
| `DOCKER_DEFAULT_SERVERS` | `DEFAULT_SERVERS` | Needed for any Kafka call. |
| `DOCKER_CLIENT_ID` | `CLIENT_ID` | Unused by the service. `tests.yml` runs the live tests only when this secret is set, so runs without the repository's secrets skip them. |
| `DOCKER_CLIENT_SECRET` | `CLIENT_SECRET` | Unused. |
| `DOCKER_CLIENT_SCOPE` | `CLIENT_SCOPE` | Unused. |
| `DOCKER_MSK_CLUSTER_ARN_SUFFIX` | `MSK_CLUSTER_ARN_SUFFIX` | Unused. |

## Container environment

The service checks seven variables when `web_service.main` is imported,
which happens when the gunicorn worker starts. Each must be set and
non-empty. A missing one stops the worker with
`AssertionError: <NAME> environment variable is not set.`, and Lightsail
marks the deployment failed.

| Variable | Required | Default | What it sets |
| --- | --- | --- | --- |
| `AWS_ACCESS_KEY_ID` | yes | | The AWS key the service uses for IAM, DynamoDB and MSK IAM authentication. |
| `AWS_SECRET_ACCESS_KEY` | yes | | The secret of that key. |
| `SERVER_CLIENT_ID` | yes | | The ID of the service's Globus confidential client. Every token must name it in its audience and carry the scope `https://auth.globus.org/scopes/<SERVER_CLIENT_ID>/action_all`. |
| `SERVER_SECRET` | yes | | The secret of that client, used to introspect tokens. |
| `AWS_ACCOUNT_ID` | yes | | The account in the ARNs of the IAM policies the service writes. |
| `AWS_ACCOUNT_REGION` | yes | | The region of the DynamoDB tables and the MSK cluster. |
| `MSK_CLUSTER_NAME` | yes | | The cluster named in those IAM policies. |
| `DEFAULT_SERVERS` | no | none | The MSK bootstrap brokers for IAM authentication, as comma-separated `host:port` pairs. Without it the service starts, but topic creates and deletes change only the service's own records and not the cluster, and topic recreates and consumer creates fail with `Kafka endpoint is not configured`. |
| `KEYS_TABLE_NAME` | no | `diaspora-keys-table` | The DynamoDB table of access keys. |
| `USERS_TABLE_NAME` | no | `diaspora-users-table` | The DynamoDB table of users. |
| `NAMESPACE_TABLE_NAME` | no | `diaspora-namespace-table` | The DynamoDB table of namespaces. |
| `MAX_TOPICS_PER_NAMESPACE` | no | `50` | The most topics a namespace may hold. A value that is not a whole number of at least 1 is logged and replaced by 50. |
| `CONSUMER_INSTANCE_TIMEOUT_MS` | no | `300000` (5 minutes) | How long a consumer instance may sit idle before the service closes it. The service looks for idle instances every 30 seconds. A value that is not an integer stops the worker at start with a `ValueError`. |

The workflow passes none of the optional variables, so production runs
with their defaults. To set one, add it to the `environment` block in
`deployment.yml`.

## One worker

The Dockerfile starts gunicorn without `--workers`, so it runs one
Uvicorn worker, and the service runs at scale 1. Consumer instances live in that one process's memory, so
the service must stay at one worker on one node. With a second worker or
node, a request can reach a process that does not hold its instance and
gets `404` with error code `40403` (`Consumer instance not found`). Do not
pass `--workers` to gunicorn, do not set `WEB_CONCURRENCY` (gunicorn reads
its worker count from it), and keep the scale at 1.

Every deploy or restart stops the process and drops every consumer
instance. Clients then get `40403` and must create a new instance. A new
instance in the same consumer group starts from the offsets the group last
committed to Kafka.

## Deploy by hand

### From GitHub

```bash
gh workflow run deployment.yml --ref main
gh run list --workflow deployment.yml --limit 1   # shows the run's ID
gh run watch <run-id>
```

A manual run goes through the same unit tests, queue, wait step and
cleanup as a push. It deploys the branch named by `--ref`; a run on
another branch is replaced by the next deploy of `main`.

### From your machine

Use this only when GitHub Actions cannot run the workflow. It skips the
unit tests, the wait step and the image cleanup. You need Docker, the AWS
CLI with the
[Lightsail container services plugin](https://docs.aws.amazon.com/en_us/lightsail/latest/userguide/amazon-lightsail-install-software.html),
`jq`, and AWS credentials with the deploy's Lightsail access.

1. Build the image from the repository root. The Dockerfile pins
   `linux/amd64`, the platform Lightsail runs.

    ```bash
    docker build -t docker -f web_service/Dockerfile .
    ```

2. Push it and keep the name Lightsail gives it:

    ```bash
    output=$(aws lightsail push-container-image --region us-east-1 \
        --service-name diaspora-web-service --label docker --image docker)
    image=$(echo "$output" | sed -n 's/.*Refer to this image as "\(.*\)" in deployments.*/\1/p')
    echo "$image"   # for example :diaspora-web-service.docker.<N>
    ```

3. Export the container's environment. The `SVC_` prefix keeps these
   values apart from the AWS CLI's own credentials. They are secrets: keep
   them out of shell history and out of files you commit.

    ```bash
    export SVC_AWS_ACCESS_KEY_ID='<container access key ID>'
    export SVC_AWS_SECRET_ACCESS_KEY='<container secret access key>'
    export SVC_SERVER_CLIENT_ID='<Globus client ID>'
    export SVC_SERVER_SECRET='<Globus client secret>'
    export SVC_AWS_ACCOUNT_ID='<account ID>'
    export SVC_AWS_ACCOUNT_REGION='<region>'
    export SVC_MSK_CLUSTER_NAME='<cluster name>'
    export SVC_DEFAULT_SERVERS='<host:port,host:port>'
    ```

4. Create the deployment. The command prints the deployment with its
   environment, so the example discards its output.

    ```bash
    containers=$(jq -n --arg image "$image" '{
      flask: {
        image: $image,
        ports: {"8000": "HTTP"},
        environment: {
          AWS_ACCESS_KEY_ID: env.SVC_AWS_ACCESS_KEY_ID,
          AWS_SECRET_ACCESS_KEY: env.SVC_AWS_SECRET_ACCESS_KEY,
          SERVER_CLIENT_ID: env.SVC_SERVER_CLIENT_ID,
          SERVER_SECRET: env.SVC_SERVER_SECRET,
          AWS_ACCOUNT_ID: env.SVC_AWS_ACCOUNT_ID,
          AWS_ACCOUNT_REGION: env.SVC_AWS_ACCOUNT_REGION,
          MSK_CLUSTER_NAME: env.SVC_MSK_CLUSTER_NAME,
          DEFAULT_SERVERS: env.SVC_DEFAULT_SERVERS
        }
      }
    }')

    aws lightsail create-container-service-deployment --region us-east-1 \
        --service-name diaspora-web-service \
        --containers "$containers" \
        --public-endpoint '{"containerName": "flask", "containerPort": 8000}' > /dev/null
    ```

    To set an optional variable, add it to the `environment` object. Keep
    the container name `flask`: the public endpoint and the logs refer to
    it.

5. [Check the deploy](#check-a-deploy).

If the container service does not exist, create it first with
`aws lightsail create-container-service --region us-east-1 --service-name diaspora-web-service --power micro --scale 1`.

## Check a deploy

```bash
aws lightsail get-container-services --region us-east-1 \
    --service-name diaspora-web-service \
    --query 'containerServices[0].{state: state, current: currentDeployment.[version, state], image: currentDeployment.containers.flask.image, next: nextDeployment.[version, state]}'
```

A finished deploy shows the service `RUNNING`, the current deployment
`ACTIVE` with the new image, and `next` as `null`. Keep the `--query`:
without it, the command prints the container's environment, secrets
included.

Then check that the service answers. It serves its API documentation at
`/`, so this prints `200`:

```bash
curl -sS -o /dev/null -w '%{http_code}\n' https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/
```
