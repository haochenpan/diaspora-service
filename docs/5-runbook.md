# Runbook

Tasks for operating the deployed service: rolling back, reading the logs,
triaging a failed deploy, watching the brokers' CPU credits, rotating
credentials and updating the users' IAM. [Deployment](3-deploying.md) describes the workflow, the
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

1. Hold merges to `main` until step 4 lands. The next push to `main`
   deploys `main`, which would bring the bad change back.

2. Deploy the previous image with the current environment, from the
   console or the CLI.
    - Console: in the Lightsail console, open **Containers**, then
      `diaspora-web-service`, then the **Deployments** tab. Choose
      **Modify your deployment**, then **Choose stored image** for
      `flask`, pick the previous image, and choose **Save and deploy**.
    - CLI: set `image=':diaspora-web-service.docker.<N>'`, the previous
      image, then run steps 3 and 4 of
      [Deploy from your machine](3-deploying.md#from-your-machine). They
      copy the current deployment's environment and change only the
      image.

    Under **Deployment versions**, the actions menu (⋮) of an older
    version also has **Create new deployment**. It reuses that version's
    environment as well as its image, so a secret changed since then
    returns to its old value.

3. [Check the deploy](3-deploying.md#check-a-deploy).
4. Revert the change on `main`, for example with `git revert <commit>` in
   a pull request. Merging the revert deploys `main` without the change.

To go back further than the stored previous image, revert on `main` and
let the workflow build and deploy it.

## Read the logs

`aws lightsail get-container-log` returns one page of entries at a time,
and the AWS CLI does not fetch the following pages for you. A call without
`--start-time` can return an old page that misses the latest deployment.
This loop prints every entry since `start`, page by page:

```bash
start='2026-10-26T07:00:00Z'   # the earliest time to read, in UTC
token=()
while :; do
  page=$(aws lightsail get-container-log --region us-east-1 \
      --service-name diaspora-web-service --container-name flask \
      --start-time "$start" "${token[@]}" --output json)
  jq -r '.logEvents[] | "\(.createdAt)\t\(.message)"' <<< "$page"
  next=$(jq -r '.nextPageToken // empty' <<< "$page")
  if [ -z "$next" ] || [ "$(jq '.logEvents | length' <<< "$page")" -eq 0 ]; then
    break
  fi
  token=(--page-token "$next")
done
```

Each page carries a `nextPageToken`; the loop passes it back with
`--page-token` and stops at a page with no entries. Lightsail can return a
token with the last page, so an empty page is the end. To keep only
matching lines, add `--filter-pattern`, for example
`--filter-pattern AssertionError`, and to end the period, `--end-time`.

The container is named `flask` in every deployment; `docker` is the image
label, not the container name. The log holds what gunicorn and the service
write to stdout and stderr. Lightsail keeps the last three days of log
entries.

In the console, the **Deployments** tab has an **Open log** link for the
current deployment's container. For an earlier or failed version, open
the version's actions menu (⋮) under **Deployment versions**, choose
**Show details**, then **Open log**.

## Triage a failed deploy

Find the failed step in the run on GitHub Actions.

| Failed step | Usual cause | Fix |
| --- | --- | --- |
| `unit` | A unit test fails on `main`. Nothing was deployed. | Fix the test or the code on `main`. |
| `Configure AWS credentials` or `Verify AWS credential` | The deploy's key in the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` secrets is wrong, deactivated or deleted. | Update the secrets ([Rotate an AWS access key](#rotate-an-aws-access-key)). |
| `Install AWS Lightsail plugin` | The plugin's download failed, or its checksum did not match. | Rerun the job; if the checksum still fails, check the pinned release in `deployment.yml`. |
| `Checkout code` | GitHub could not check out the commit. | Rerun the job. |
| `Build Docker container for AWS Lightsail` | `docker build` failed; the step log shows the failing Dockerfile instruction. | Reproduce it with `docker build -f web_service/Dockerfile .` and fix it on `main`. |
| `Idempotently create Lightsail container service` | This step's error is expected on every run: the service exists. The step does not fail the run. | Nothing. |
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
it runs above its baseline. When the balance runs out, AWS's
[MSK metrics reference](https://docs.aws.amazon.com/msk/latest/developerguide/metrics-details.html)
says only that this "can have a negative impact on your cluster's
performance". In October 2026 the Kafka console's load spent one
broker's credits, and topic creation and deletion stalled until the
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
1, and the times are in ISO 8601, such as `2026-10-25T00:00:00Z`.

If a balance keeps falling, find the load that spends it. The Kafka
console's statistics refresh is one known source; its page says to stop
the console's instance in that case.

## Rotate credentials

The deploy and the service use three credentials, each stored in GitHub
secrets:

| Credential | Secrets | Also used by |
| --- | --- | --- |
| The deploy's AWS key, used by the workflow | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | |
| The container's AWS key, used by the service: the key of the IAM user the service runs as ([The service's AWS identity](3-deploying.md#the-services-aws-identity)) | `DOCKER_AWS_ACCESS_KEY_ID`, `DOCKER_AWS_SECRET_ACCESS_KEY` | The `live` job of `tests.yml` and its nightly sweep, while the live tests' own key is not set |
| The Globus confidential client's secret, used by the service | `DOCKER_SERVER_SECRET` | The `live` job of `tests.yml` |

The running container keeps the values it started with. A new value
reaches it only through a new deployment, and the workflow builds each
deployment's environment from the secrets as they are at that run. Every
such deploy restarts the service and drops its consumer instances, whose
clients then get `40403` ([One worker](3-deploying.md#one-worker)).

### Rotate an AWS access key

An IAM user can hold two access keys at once, so the old key keeps working
until you deactivate it. The deploy's key and the container's key may
belong to the same IAM user; check before you delete either.

1. Find the IAM user the key belongs to, and list its keys:
   `aws iam list-access-keys --user-name <user>`. If it already has two,
   run `aws iam get-access-key-last-used --access-key-id <key-id>` on
   each. Its `ServiceName` says which credential the key is: the deploy's
   key was last used by Lightsail or STS, the container's by IAM, DynamoDB
   or Kafka. Delete only a key with no recent use.
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
   old key. Anything else that holds it, such as a local file you source
   for the live tests, needs the new key too.
6. Deactivate the old key:
   `aws iam update-access-key --user-name <user> --access-key-id <old-key-id> --status Inactive`.
   When nothing has failed with the old key inactive, delete it with
   `aws iam delete-access-key --user-name <user> --access-key-id <old-key-id>`.

### Rotate the Globus client secret

1. In [Globus developer settings](https://app.globus.org/settings/developers),
   open the confidential client whose client ID is the `SERVER_CLIENT_ID`
   value, and add a client secret. Globus shows it only once.
2. Update the secret: `gh secret set DOCKER_SERVER_SECRET`. <!-- pragma: allowlist secret -->
3. Deploy: `gh workflow run deployment.yml --ref main`, then
   [check the deploy](3-deploying.md#check-a-deploy) and make one
   authenticated call, such as `GET /api/v3/namespace`
   ([API Reference](web/api.md) shows how to get a token).
4. Delete the old secret in the Globus developer settings.

Keep the client itself. Its ID is part of the scope every token must carry
(`https://auth.globus.org/scopes/<SERVER_CLIENT_ID>/action_all`), so a new
client would make the service reject every token issued for the old
one.

## Update the users' IAM

The service writes a user's IAM policy (`IAMService.generate_user_policy`
in `web_service/services.py`), and sets the user's permissions boundary,
only when it creates the user. `scripts/update_user_policies.py` brings
the users created before up to date, in two steps that run, and undo,
apart:

- `policies` makes the current policy the default version of each user
  policy that differs. This is what applies a change to the policy to
  existing users. For example, it limits their consumer groups and
  transactional IDs to names that start with their namespace and a dot,
  so a user's Kafka client whose group IDs lack that prefix stops working.
  IAM keeps at most five versions of a policy, so when a policy already
  has five, the step first deletes the oldest version that is not the
  default.
- `boundaries` gives each user without a permissions boundary the
  service's boundary,
  `arn:aws:iam::<account-id>:policy/diaspora-service/msk-user-boundary`.
  It refuses any other ARN, or a boundary policy that does not exist. The
  service's own identity can manage only users with that boundary
  ([Users created before the boundary](3-deploying.md#users-created-before-the-boundary)).
  The boundary allows everything a user's policy grants, so it takes no
  right away from a user.

Both find the users by the names and paths the service gives them: IAM
users under `/msk-users/` and managed policies under `/msk-policy/` whose
names are subjects (UUIDs), and the subjects in `diaspora-users-table`.
They skip every other name, such as the live tests' `test-subject-*`
users, and print the names they skip.

Run a step after the change it applies is deployed, from a checkout of
that commit with the dev install, with an administrator's credentials.
Set `AWS_ACCOUNT_ID`, `AWS_ACCOUNT_REGION` and `MSK_CLUSTER_NAME` to the
service's values; `policies` refuses a cluster name the account has no
cluster of. Do not run two at once.

1. List what the step would change:

    ```bash
    python scripts/update_user_policies.py policies
    python scripts/update_user_policies.py boundaries --boundary "$BOUNDARY"
    ```

    `policies` prints each policy it would update, then each distinct way
    the policies differ from the current policy, as a diff of the policy
    document. `boundaries` prints each user it would give the boundary,
    and counts the users under `/msk-users/` without one. Neither changes
    anything.

2. Make the changes, naming a new record file outside the repository:

    ```bash
    python scripts/update_user_policies.py policies \
      --apply --record ~/policies-$(date -u +%Y%m%dT%H%M%SZ).json
    ```

    The record lists each change as soon as it is made: for a policy, its
    ARN, its default version before and the new one; for a user, its name,
    its boundary before (none) and the new one. Keep it until the change
    has proved itself. The step refuses a record file that exists or lies
    inside the repository, since it names the account's ARNs.

3. Run step 1 again. It reports 0 policies to update, or 0 users to give
   the boundary; a second `--apply` changes nothing.

To undo a run, give the same step its record:

```bash
python scripts/update_user_policies.py policies --undo ~/policies-<UTC time>.json
python scripts/update_user_policies.py boundaries --undo ~/boundaries-<UTC time>.json
```

`policies --undo` makes each policy's previous version its default
again; the new versions stay as versions that are not the default.
`boundaries --undo` removes the boundary from each user the run gave it
to. Either leaves a policy or user changed again since the run as it is,
and reports it.
