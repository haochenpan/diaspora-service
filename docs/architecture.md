# Architecture

The service is one FastAPI application, `web_service.main:app`, that runs
in one process. It authenticates each caller with Globus, then acts for
that caller with its own AWS key: it manages the caller's IAM user, keeps
the caller's records in three DynamoDB tables, and creates, deletes and
reads topics on one Amazon MSK cluster.

## The request path

1. A client sends an HTTPS request to the public endpoint of the Lightsail
   container service. Lightsail forwards it over HTTP to port 8000 of the
   one container, which `deployment.yml` names `flask`
   ([Deployment](3-deploying.md#what-the-workflow-does)).
2. In the container, gunicorn runs one Uvicorn worker, which serves the
   FastAPI application. `web_service/main.py` builds the application and
   its routes in the class `DiasporaService`.
3. The route handler checks the caller's token with Globus
   ([Authentication](#authentication)).
4. The handler calls the service classes in `web_service/services.py`:
    - `WebService` runs each user, key and topic operation across the
      classes below.
    - `IAMService` creates and deletes IAM users, their policies and their
      access keys.
    - `NamespaceService` checks namespace and topic names and keeps the
      namespace and topic records, through `DynamoDBService`.
    - `DynamoDBService` reads and writes the three tables.
    - `KafkaService` creates and deletes topics with a Kafka admin client.
5. The consumer routes call `ConsumerService` in
   `web_service/consumer_service.py`, which keeps Kafka consumers in
   memory ([State in memory](#state-in-memory)).

Calls to Globus, IAM, DynamoDB and Kafka block, so the handlers run them
in FastAPI's thread pool. On the event loop, they would stall every other
request.

The service reaches AWS with the key in `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY`. Its Kafka clients connect to the brokers in
`DEFAULT_SERVERS` over SASL_SSL with the OAUTHBEARER mechanism, using
tokens that `aws-msk-iam-sasl-signer-python` signs with that key for the
region in `AWS_ACCOUNT_REGION`. The service does not use the users' IAM
keys for its own calls.

## Authentication

Every API route takes two headers: `subject`, the caller's Globus identity ID,
and `authorization`, `Bearer` followed by a Globus access token. The class
`AuthManager` in `web_service/utils.py` checks them in this order:

1. `subject` is a UUID.
2. The token has the `Bearer` prefix, is 33 to 254 characters long, and
   holds only letters, digits and underscores.
3. Globus token introspection, called with the service's confidential
   client (`SERVER_CLIENT_ID` and `SERVER_SECRET`), reports the token
   active.
4. The token's `sub` is the `subject` header.
5. The token's audience includes `SERVER_CLIENT_ID`.
6. The token's scopes include
   `https://auth.globus.org/scopes/<SERVER_CLIENT_ID>/action_all`.

A request missing one of the headers gets FastAPI's `422`. A failed check ends
the request: the user, key, namespace and topic routes answer `200` with
`{"status": "error", "reason": ...}`, and the consumer routes answer
`401`. Step 3 fails this way only when Globus answers that the token is
inactive. When the introspection call itself fails, for example because
Globus rejects `SERVER_CLIENT_ID` and `SERVER_SECRET`, the Globus SDK
raises an `AuthAPIError` that the service does not catch, and the route
answers `500`. After the checks, the service trusts `subject` as the
caller's identity and names the caller's resources after it.

## Each user's resources

A namespace is the prefix of the Kafka names of a user's topics and
consumer groups. Each user gets one, the default namespace, when the
service creates the user.

| Resource | Name | Created by | Deleted by |
| --- | --- | --- | --- |
| IAM user | the subject, under the path `/msk-users/` | `POST /api/v3/user`, or `POST /api/v3/key` when the keys table holds no key for the user | `DELETE /api/v3/user` |
| IAM managed policy, attached to the user | the subject, under the path `/msk-policy/` | the same as the user | `DELETE /api/v3/user` |
| IAM access key | chosen by IAM | `POST /api/v3/key` | `POST /api/v3/key` when the keys table holds no key for the user, `DELETE /api/v3/key`, `DELETE /api/v3/user` |
| Default namespace | `ns-` and the last 12 characters of the subject without dashes | the same as the user | `DELETE /api/v3/user` |
| Kafka topic | `<namespace>.<topic>`, with 1 partition and replication factor 2 | `POST /api/v3/{namespace}/{topic}` | `DELETE /api/v3/{namespace}/{topic}`, `DELETE /api/v3/user` |
| Kafka consumer group | `<namespace>.<group_name>` | the first consumer that joins it | not deleted by the service |

The policy lets the IAM user connect to the cluster in `MSK_CLUSTER_NAME`;
read and write the topics whose names start with the user's default
namespace and a dot; and use any consumer group and transactional ID on
the cluster. With the user's access key, a Kafka client therefore has the
permissions to produce to and consume from the user's topics directly,
without the service. When `IAM_USER_PERMISSIONS_BOUNDARY_ARN` is
set, the service also sets the policy it names as the user's permissions
boundary.

`POST /api/v3/key` returns the key stored for the user when the keys table
holds one, with `fresh` set to `false`. Otherwise it creates the user and
the default namespace if they are missing, deletes every access key the IAM
user holds, creates a new key, stores it, and returns it with `fresh` set
to `true`. Both answers include `endpoint`, the value of `DEFAULT_SERVERS`.

Namespace and topic names are 3 to 32 characters of letters, digits,
dashes and underscores, and do not start or end with a dash or an
underscore. A namespace is unique across all users. A namespace holds at
most `MAX_TOPICS_PER_NAMESPACE` topics, 50 by default.

## The DynamoDB tables

Three environment variables name the tables, and their defaults are the
production tables. Every table has one string partition key. When a table
does not exist, a write that adds an item, or adds to a set, creates it
with on-demand billing; deletes from a missing table are ignored, and
reads from it find nothing.

| Variable (default) | Partition key | Items |
| --- | --- | --- |
| `KEYS_TABLE_NAME` (`diaspora-keys-table`) | `subject` | One per user that holds a key: `access_key`, `secret_key` and `create_date` of the user's current IAM access key. |
| `USERS_TABLE_NAME` (`diaspora-users-table`) | `subject` | One per user: `namespaces`, the set of namespaces the user owns. One more item, with the key `__global_namespaces__`, holds every namespace any user owns, which keeps namespace names unique. |
| `NAMESPACE_TABLE_NAME` (`diaspora-namespace-table`) | `namespace` | One per namespace that has had a topic: `topics`, the set of the namespace's topic names, without the namespace prefix. |

Every read is strongly consistent, so a read sees every write made
before it. Before a Kafka call, the service checks in these tables that
the caller owns the namespace.

## State in memory

`ConsumerService` keeps the consumer instances that the consumer routes
create. The routes follow the consumer API of the Confluent REST Proxy
v2. Each instance holds one `KafkaConsumer`, connected as the service, and
the service finds it by the subject, the namespace, the group name and the
instance ID. Only the subject that created an instance can use it. A
background thread looks every 30 seconds for instances idle for longer
than `CONSUMER_INSTANCE_TIMEOUT_MS` (5 minutes by default) and closes them.
When the process stops, it closes them all.

`WebService` and `NamespaceService` each hold a lock. The lock in
`WebService` serializes user and key operations, including the Kafka
topic deletes of `DELETE /api/v3/user`. The lock in `NamespaceService`
serializes only the DynamoDB step of namespace and topic operations: the
topic routes call Kafka outside any lock. Both are Python threading
locks, so they serialize requests only within one process.

Because of this state, the service runs one worker on one node (Lightsail
scale 1). A second worker or node would not hold the consumer instances
the first created, so requests for them would get `404` with error code
`40403` (`Consumer instance not found`). Two processes could also run
user, key or topic operations on the same records at the same time, since
neither would hold the other's locks. Every restart or deploy drops the
consumer instances, and clients must create new ones
([One worker](3-deploying.md#one-worker)).
