# Runbook

Tasks for operating the deployed service: rolling back, reading the logs,
triaging a failed deploy, watching the brokers' CPU credits and rotating
credentials. [Deployment](3-deploying.md) describes the workflow, the
secrets and the container's environment that these tasks refer to. The
Lightsail commands use the region `us-east-1`; angle brackets mark values
to fill in.

## Roll back a deploy

Roll back when a deployment became active but the service misbehaves. A
deployment that fails needs no rollback: Lightsail keeps the previous one
active.

The workflow's image cleanup keeps the images of the active deployment, of
any deployment still activating, and of the most recent inactive
deployment. After a deploy, Lightsail therefore holds the current image
and the one before it. To see the deployments and the stored images:

```bash
aws lightsail get-container-service-deployments --region us-east-1 \
    --service-name diaspora-web-service \
    --query 'deployments[].{version: version, state: state, image: containers.flask.image}' \
    --output table
aws lightsail get-container-images --region us-east-1 \
    --service-name diaspora-web-service --query 'containerImages[].image'
```

Lightsail lists up to 50 deployment versions, but only the versions whose
image is still stored can be deployed again.

1. Deploy the previous image, from the console or the CLI.
    - Console: in the Lightsail console, open **Containers**, then
      `diaspora-web-service`, then the **Deployments** tab. Under
      **Deployment versions**, open the actions menu of the most recent
      inactive version and choose **Create new deployment**. This reuses
      that version's image and environment, so a secret changed since
      then is back at its old value. To keep the current environment
      instead, choose **Modify your deployment** under the current
      deployment and pick the previous stored image for `flask`.
    - CLI: follow steps 3 and 4 of
      [Deploy from your machine](3-deploying.md#from-your-machine) with
      `image=':diaspora-web-service.docker.<N>'`, the previous image. The
      call replaces the whole environment, so set every variable.
2. [Check the deploy](3-deploying.md#check-a-deploy).
3. Revert the change on `main`, for example with `git revert <commit>` in
   a pull request. The next push to `main` deploys `main`, which would
   bring the bad change back. Merging the revert deploys `main` without
   it.

To go back further than the stored previous image, revert on `main` and
let the workflow build and deploy it.

## Read the logs

```bash
aws lightsail get-container-log --region us-east-1 \
    --service-name diaspora-web-service --container-name flask \
    --query 'logEvents[].[createdAt, message]' --output text
```

The container is named `flask` in every deployment; `docker` is the image
label, not the container name. The log holds what gunicorn and the service
write to stdout and stderr. `--start-time` and `--end-time` (for example
`2026-10-26T07:00:00Z`) limit it to a period, and `--filter-pattern`
(for example `AssertionError`) to matching lines. Lightsail keeps the last
three days of log entries. In the console, the **Deployments** tab has an
**Open log** link for each container.

## Triage a failed deploy

Find the failed step in the run on GitHub Actions.

| Failed step | Usual cause | Fix |
| --- | --- | --- |
| `unit` | A unit test fails on `main`. Nothing was deployed. | Fix the test or the code on `main`. |
| `Configure AWS credentials` or `Verify AWS credential` | The deploy's key in the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` secrets is wrong, deactivated or deleted. | Update the secrets ([Rotate an AWS access key](#rotate-an-aws-access-key)). |
| `Push Docker image to Lightsail and deploy` | An AWS CLI call failed; the step log shows its error. | Fix the cause the error names, then deploy again. |
| `Wait for the deployment to become active` | The deployment failed or did not finish in time ([The wait step](3-deploying.md#the-wait-step)). | Read the container log, as below. |

When a deployment fails, [read the log](#read-the-logs) around the time
of the deploy. A worker that cannot start ends with gunicorn's
`Reason: Worker failed to boot.`, and the traceback above it names the
cause:

- `AssertionError: <NAME> environment variable is not set.` means the
  `DOCKER_<NAME>` secret is missing or empty. Set it with
  `gh secret set DOCKER_<NAME>`, then deploy again with
  `gh workflow run deployment.yml --ref main`.
- `ValueError: invalid literal for int()` at start means
  `CONSUMER_INSTANCE_TIMEOUT_MS` is set to a value that is not an integer.

To reproduce a start-up failure locally, build and run the image as in
[Running](1-running.md).

## Broker CPU credits

The MSK cluster's brokers are burstable instances, `kafka.t3.small` as of
October 2026 (see the
[Kafka console page](kafka-console/intro.md#broker-cpu-credits)). A
burstable broker earns CPU credits at a fixed rate and spends them while
it runs above its baseline. When its credit balance reaches zero, it is
throttled to its baseline. In October 2026 the Kafka console's load spent
one broker's credits, and topic creation and deletion stalled until the
console's instance was stopped.

Each broker's balance is the `CPUCreditBalance` metric in CloudWatch, in
the cluster's region (the `AWS_ACCOUNT_REGION` value). In the CloudWatch
console, open **Metrics**, then **All metrics**, then **AWS/Kafka**, then
**Broker ID, Cluster Name**, and select `CPUCreditBalance` for each broker.
From the CLI, for one broker:

```bash
aws cloudwatch get-metric-statistics --region <region> \
    --namespace AWS/Kafka --metric-name CPUCreditBalance \
    --dimensions Name="Cluster Name",Value=<cluster-name> Name="Broker ID",Value=<broker-id> \
    --start-time <start-time> --end-time <end-time> \
    --period 3600 --statistics Minimum
```

`<cluster-name>` is the `MSK_CLUSTER_NAME` value, the broker IDs start at
1, and the times are in ISO 8601, such as `2026-10-25T00:00:00Z`. If a balance keeps falling, find the load that spends it. The Kafka
console's statistics refresh is one known source, and its page says how
to stop the console's instance.

## Rotate credentials

The service depends on three credentials, each stored in GitHub secrets:

| Credential | Secrets | Also used by |
| --- | --- | --- |
| The deploy's AWS key | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | |
| The container's AWS key | `DOCKER_AWS_ACCESS_KEY_ID`, `DOCKER_AWS_SECRET_ACCESS_KEY` | The `live` and `sweep` jobs of `tests.yml` |
| The Globus confidential client's secret | `DOCKER_SERVER_SECRET` | The `live` job of `tests.yml` |

The running container keeps the values it started with. A new value
reaches it only through a new deployment, and the workflow builds each
deployment's environment from the secrets as they are at that run.

### Rotate an AWS access key

An IAM user can hold two access keys at once, so the old key keeps working
until you deactivate it.

1. Find the IAM user the key belongs to, and list its keys:
   `aws iam list-access-keys --user-name <user>`. If it already has two,
   delete the one not in use first.
2. Create a key: `aws iam create-access-key --user-name <user>`. The output
   is the only time AWS shows the secret.
3. Update the matching secrets from the table, for example
   `gh secret set DOCKER_AWS_ACCESS_KEY_ID` and
   `gh secret set DOCKER_AWS_SECRET_ACCESS_KEY`; each prompts for its
   value.
4. Deploy: `gh workflow run deployment.yml --ref main`. A new deploy key
   is checked by the run's `Verify AWS credential` step. For a new
   container key, [check the deploy](3-deploying.md#check-a-deploy) and
   read the log for AWS errors.
5. Check when the old key was last used:
   `aws iam get-access-key-last-used --access-key-id <old-key-id>`. A use
   after the new deployment became active means something still holds the
   old key. Anything else that holds it, such as a local file you source for the
   live tests, needs the new key too.
6. Deactivate the old key:
   `aws iam update-access-key --user-name <user> --access-key-id <old-key-id> --status Inactive`.
   When nothing has failed with the old key inactive, delete it with
   `aws iam delete-access-key --user-name <user> --access-key-id <old-key-id>`.

### Rotate the Globus client secret

1. In [Globus developer settings](https://app.globus.org/settings/developers),
   open the confidential client whose client ID is the `SERVER_CLIENT_ID`
   value, and add a client secret. Globus shows it only once.
2. Update the secret: `gh secret set DOCKER_SERVER_SECRET`.
3. Deploy: `gh workflow run deployment.yml --ref main`, then
   [check the deploy](3-deploying.md#check-a-deploy) and make one
   authenticated call, such as `GET /api/v3/namespace`
   ([API Reference](web/api.md) shows how to get a token).
4. Delete the old secret in the Globus developer settings.

Keep the client itself. Its ID is part of the scope every token must carry
(`https://auth.globus.org/scopes/<SERVER_CLIENT_ID>/action_all`), so a new
client would make the service reject every token issued for the old
one.
