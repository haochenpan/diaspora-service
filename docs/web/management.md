# Management API

The management routes create a user and its access key, list the user's namespaces and topics, and create, delete and recreate topics. They are under `/api/v3`:

| Route | Does |
| --- | --- |
| [`POST /api/v3/user`](#create-user) | Creates the IAM user and the default namespace |
| [`DELETE /api/v3/user`](#delete-user) | Deletes the default namespace's topics, the namespace, the key and the IAM user |
| [`POST /api/v3/key`](#create-key) | Returns the user's access key, creating the user, the namespace and the key if needed |
| [`DELETE /api/v3/key`](#delete-key) | Deletes the user's access keys |
| [`GET /api/v3/namespace`](#list-namespaces-and-topics) | Lists the user's namespaces and their topics |
| [`POST /api/v3/{namespace}/{topic}`](#create-topic) | Creates a topic |
| [`DELETE /api/v3/{namespace}/{topic}`](#delete-topic) | Deletes a topic and its records |
| [`PUT /api/v3/{namespace}/{topic}/recreate`](#recreate-topic) | Deletes a topic and creates it again, empty |

Every route takes the `subject` and `authorization` headers ([Getting `subject` and `authorization`](api.md#getting-subject-and-authorization)) and no body. The examples use the variables that page exports: `$SUBJECT`, `$AUTHORIZATION` and `$NAMESPACE`.

The SDK's `Client` calls these routes with `create_user`, `delete_user`, `create_key`, `delete_key`, `list_namespaces`, `create_topic`, `delete_topic` and `recreate_topic`, in the default namespace. [DiasporaDemo.ipynb](https://github.com/globus-labs/diaspora-event-sdk/blob/main/diaspora_event_sdk/examples/DiasporaDemo.ipynb) shows them, and the Kafka producer and consumer that use the key.

## Replies

A management route answers HTTP 200 with a JSON object whose `status` says how the request went, so a client must read `status`, not the HTTP status:

- `"status": "success"`: the request was done. `message` says what was done, and each route adds its own fields, given below.
- `"status": "failure"`: the request was refused or failed. `message` says why; the messages are given below for each route.
- `"status": "error"`: the token check failed. `reason` says why, as in the table below, and nothing else is done.

```json
{"status": "failure", "message": "Namespace ns-0123456789ab not found for 01234567-89ab-cdef-0123-456789abcdef"}
```

```json
{"status": "error", "reason": "Token is inactive."}
```

The token check runs first on every route and fails with one of these reasons:

| `reason` | Cause |
| --- | --- |
| `Invalid user ID; expected UUID.` | `subject` is not a UUID |
| `Auth token must start with "Bearer ".` | `authorization` does not start with `Bearer ` |
| `Invalid token format.` | The token after `Bearer ` is not 33 to 254 letters, digits or underscores |
| `Token is inactive.` | Globus reports the token as expired, revoked or unknown |
| `Token does not belong to the user.` | The token's identity is not `subject` |
| `Not in the audience set. introspected = [...] server_client_id = ...` | The token is not for this service; the reason lists the token's audiences and the service's client ID |
| `Token lacks the scope for action.` | The token lacks the service's `action_all` scope |

Two replies have a real HTTP status:

- A request without the `subject` or `authorization` header answers 422 with FastAPI's `{"detail": [...]}`, which names each missing header.
- An error the service does not handle, such as a DynamoDB or Globus Auth call that fails, answers 500 with the text `Internal Server Error`. What the request did before the error stays done; retry it, since every route can be repeated.

## Names and limits

- A user's default namespace is `ns-` followed by the last 12 characters of the subject without dashes, such as `ns-0123456789ab` for the subject `01234567-89ab-cdef-0123-456789abcdef`. `POST /api/v3/user` and `POST /api/v3/key` create it. No route creates or deletes another namespace.
- A topic name is 3 to 32 characters, each a letter, a digit, `-` or `_`, and does not start or end with `-` or `_`. Names are case-sensitive.
- The topic `my-topic` of the namespace `ns-0123456789ab` is the Kafka topic `ns-0123456789ab.my-topic`. Every topic has one partition and replication factor 2.
- A namespace holds at most 50 topics, a limit the service sets with the `MAX_TOPICS_PER_NAMESPACE` environment variable. Creating a new topic in a namespace that already holds that many returns `"status": "failure"` with a message that says to delete a topic first. Once a create adds a topic and leaves the namespace with at least 90% of the limit (45 of 50), the reply appends a warning to `message` and returns the same sentence in a `warning` field, which is absent otherwise. At 45 of 50 the warning reads "Namespace my-namespace now holds 45 of 50 topics; a create fails once it holds 50, so delete topics you no longer need." and at 50 of 50 it reads "Namespace my-namespace now holds 50 of 50 topics, the most allowed; delete a topic before you create another." Repeating the create of a topic that already exists, recreating a topic and deleting one are not limited.
- The service tries each create or delete of a Kafka topic up to 3 times before it reports a failure.

Every route can be repeated: a second create of something that exists, or a delete of something that is gone, succeeds.

## Users

### Create user

```bash
curl -X POST "https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/api/v3/user" \
  -H "subject: $SUBJECT" \
  -H "authorization: $AUTHORIZATION"
```

`POST /api/v3/user` creates an IAM user named by the subject, and an IAM policy that lets it use, on the Kafka cluster, only topics, consumer group IDs and transactional IDs that start with the default namespace and a dot. It then creates the default namespace. A user, policy or namespace that already exists is kept. The route creates no access key: [`POST /api/v3/key`](#create-key) does, and creates the user too, so a client that wants a key needs only that route.

```json
{
  "status": "success",
  "message": "User 01234567-89ab-cdef-0123-456789abcdef created with namespace ns-0123456789ab",
  "subject": "01234567-89ab-cdef-0123-456789abcdef",
  "namespace": "ns-0123456789ab"
}
```

| `message` of a failure | Cause |
| --- | --- |
| `Failed to create IAM user: Failed to create user and policy: IAM did not complete the request` | IAM refused a call |
| `Failed to create namespace: Namespace already taken: <namespace>` | Another user holds the default namespace's name; the service deletes the IAM user |

### Delete user

```bash
curl -X DELETE "https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/api/v3/user" \
  -H "subject: $SUBJECT" \
  -H "authorization: $AUTHORIZATION"
```

`DELETE /api/v3/user` deletes, in this order: each topic of the default namespace, from the cluster and then from the namespace, with its records; the default namespace; the stored access key; and the IAM user, with its access keys and its policy. Consumer instances the user holds are not closed; they read nothing from the deleted topics. Deleting a user that does not exist succeeds.

```json
{"status": "success", "message": "User 01234567-89ab-cdef-0123-456789abcdef deleted"}
```

| `message` of a failure | Cause |
| --- | --- |
| `Cannot delete user <subject>: user has <n> additional namespace(s). Delete namespaces first.` | The user holds namespaces other than the default one; nothing is deleted |
| `Failed to delete Kafka topic: Failed to delete Kafka topic <namespace>.<topic> after 3 attempts: the Kafka cluster did not complete it; retry shortly` | The cluster did not delete a topic. The topics deleted before it stay deleted; that topic, the rest, the namespace, the key and the user are kept |
| `Failed to delete IAM user: Failed to delete user and policy: IAM did not complete the request` | IAM refused a call. The topics, the namespace and the stored key are already deleted |

## Keys

### Create key

```bash
curl -X POST "https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/api/v3/key" \
  -H "subject: $SUBJECT" \
  -H "authorization: $AUTHORIZATION"
```

`POST /api/v3/key` returns the user's AWS access key and the cluster's endpoint:

- If the service holds a key for the user, it returns that key, with `"fresh": false` and the message `Access key already exists for <subject>`. It creates nothing and does not check the key with IAM, so every call returns the same key until [`DELETE /api/v3/key`](#delete-key) deletes it.
- Otherwise it creates the user and the default namespace as [`POST /api/v3/user`](#create-user) does, deletes every access key the IAM user has, creates one, stores it, and returns it with `"fresh": true`.

To replace a key, delete it and create one.

```json
{
  "status": "success",
  "message": "Access key created for 01234567-89ab-cdef-0123-456789abcdef",
  "access_key": "<access key ID>",
  "secret_key": "<secret access key>",
  "create_date": "2026-10-26T15:04:05+00:00",
  "endpoint": "<bootstrap servers>",
  "fresh": true
}
```

`create_date` is when IAM created the key, in ISO 8601 with a UTC offset. `endpoint` is the cluster's bootstrap servers for IAM authentication, a comma-separated list of `host:port`, or an empty string if the service has none set. The reply holds the secret key: keep it out of logs and notebooks you share.

The key lets a Kafka client connect to the cluster at `endpoint`, with MSK IAM authentication. Such a client may use only topics, consumer group IDs and transactional IDs that start with your default namespace and a dot, such as `ns-0123456789ab.workers`; the cluster refuses any other with an authorization error (`GroupAuthorizationFailedError` in kafka-python for a group).

| `message` of a failure | Cause |
| --- | --- |
| `Failed to create user: Failed to create IAM user: ...` or `Failed to create user: Failed to create namespace: ...` | Creating the user failed, as in [Create user](#create-user) |
| `Failed to delete existing IAM access keys: Failed to delete access keys: IAM did not complete the request` | IAM refused to list or delete the user's keys |
| `Failed to create IAM access key: Failed to create access key: IAM did not complete the request` | IAM refused to create the key |

If the service cannot store a new key, the reply is 500 and the key is not returned; a retry deletes it and creates another.

### Delete key

```bash
curl -X DELETE "https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/api/v3/key" \
  -H "subject: $SUBJECT" \
  -H "authorization: $AUTHORIZATION"
```

`DELETE /api/v3/key` deletes every access key of the IAM user, then the key the service stores. Kafka clients can no longer connect with the key. The user, the namespace and the topics are kept, and the produce and consume routes, which take the Globus token, still work. Deleting a key that does not exist, or the key of a user that does not exist, succeeds.

```json
{"status": "success", "message": "Access key deleted for 01234567-89ab-cdef-0123-456789abcdef"}
```

| `message` of a failure | Cause |
| --- | --- |
| `Failed to delete IAM access keys: Failed to delete access keys: IAM did not complete the request` | IAM refused to list or delete the keys. The stored key is kept, since it may still be valid |

## Namespaces

### List namespaces and topics

```bash
curl -X GET "https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/api/v3/namespace" \
  -H "subject: $SUBJECT" \
  -H "authorization: $AUTHORIZATION"
```

`GET /api/v3/namespace` lists the user's namespaces and the topics each holds, both sorted by name. The topics are those the service records for the namespace, named without it; the route does not ask the cluster. A user not yet created has no namespaces.

```json
{
  "status": "success",
  "message": "Found 1 namespaces for 01234567-89ab-cdef-0123-456789abcdef",
  "namespaces": {"ns-0123456789ab": ["my-topic", "other-topic"]}
}
```

The route fails only on the token check or with a 500.

## Topics

The topic routes take the namespace and the topic, named without the namespace, in the path. The namespace must be one the user holds; for any other, a route fails with the message `Namespace <namespace> not found for <subject>`, and asks nothing of the cluster.

### Create topic

```bash
curl -X POST "https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/api/v3/$NAMESPACE/my-topic" \
  -H "subject: $SUBJECT" \
  -H "authorization: $AUTHORIZATION"
```

`POST /api/v3/{namespace}/{topic}` checks the topic's name and the namespace, adds the topic to the namespace, and creates the Kafka topic. If the cluster does not create it, the route removes the topic from the namespace again. If the namespace already holds the topic, the reply is a success with the message `Topic <topic> already exists in <namespace>`, the topic is not counted again, and the Kafka topic is created if the cluster lacks it.

```json
{"status": "success", "message": "Topic my-topic created in ns-0123456789ab", "topics": ["my-topic"]}
```

`topics` lists the namespace's topics after the create. Near the limit, `message` ends with a warning and the reply has a `warning` field ([Names and limits](#names-and-limits)).

| `message` of a failure | Cause |
| --- | --- |
| `Name must be between 3 and 32 characters` | The topic name is too short or too long |
| `Name can only contain letters, numbers, dash, and underscore` | The topic name has another character, such as a dot |
| `Name cannot start or end with hyphen or underscore` | The topic name starts or ends with `-` or `_` |
| `Namespace <namespace> not found for <subject>` | The user does not hold the namespace |
| `Namespace <namespace> already holds 50 topics, the most allowed is 50; delete a topic to create another.` | The namespace is at the limit |
| `Namespace <namespace> holds <n> topics, more than the 50 allowed; delete <k> topic(s) to create another.` | The namespace is past the limit, which was lowered after it was filled |
| `Failed to create Kafka topic: Failed to create Kafka topic <namespace>.<topic> after 3 attempts: the Kafka cluster did not complete it; retry shortly` | The cluster did not create the topic |

The name is checked before the namespace.

### Delete topic

```bash
curl -X DELETE "https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/api/v3/$NAMESPACE/my-topic" \
  -H "subject: $SUBJECT" \
  -H "authorization: $AUTHORIZATION"
```

`DELETE /api/v3/{namespace}/{topic}` deletes the Kafka topic, with every record in it, and then removes the topic from the namespace. A topic the cluster does not have counts as deleted. If the cluster does not delete the topic, the namespace keeps it, so a retry can delete it. A produce request to a deleted topic answers 404, and a consumer instance subscribed to it reads nothing from it.

```json
{"status": "success", "message": "Topic my-topic deleted from ns-0123456789ab", "topics": ["other-topic"]}
```

`topics` lists the namespace's topics after the delete. Deleting a topic the namespace does not hold succeeds with the message `Topic <topic> not found in <namespace>`; the route still deletes the Kafka topic of that name if the cluster has one, and then says `deleted`.

| `message` of a failure | Cause |
| --- | --- |
| `Namespace <namespace> not found for <subject>` | The user does not hold the namespace |
| `Invalid topic name <topic>` | The topic name contains a dot. No other name rule is checked |
| `Failed to delete Kafka topic: Failed to delete Kafka topic <namespace>.<topic> after 3 attempts: the Kafka cluster did not complete it; retry shortly` | The cluster did not delete the topic |

### Recreate topic

```bash
curl -X PUT "https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/api/v3/$NAMESPACE/my-topic/recreate" \
  -H "subject: $SUBJECT" \
  -H "authorization: $AUTHORIZATION"
```

`PUT /api/v3/{namespace}/{topic}/recreate` deletes the Kafka topic and creates it again with the same name, so the topic keeps its name and loses every record. The topic must be one the namespace holds, and the namespace's topics do not change. The route deletes the topic, waits 5 s, and creates it; while the cluster still holds the old topic, it tries the create again after 2 s, 3 times in all. So a recreate takes at least 5 s. Use it to empty a topic.

```json
{"status": "success", "message": "Topic recreated"}
```

| `message` of a failure | Cause |
| --- | --- |
| `Namespace <namespace> not found for <subject>` | The user does not hold the namespace |
| `Topic <topic> not found in <namespace>` | The namespace does not hold the topic |
| `Failed to delete Kafka topic <namespace>.<topic> after 3 attempts: the Kafka cluster did not complete it; retry shortly` | The cluster did not delete the topic |
| `Failed to create Kafka topic <namespace>.<topic> after 3 attempts: it still exists; retry shortly` | The cluster was still deleting the old topic |
| `Failed to create Kafka topic <namespace>.<topic> after 3 attempts: the Kafka cluster did not complete it; retry shortly` | The cluster did not create the topic |
| `Kafka endpoint is not configured` | The service runs without a cluster |

After a failed create the namespace still holds the topic, but the cluster may not: produce requests to it answer 404 until a retry of the recreate, or a [create](#create-topic) of the topic, makes it again.
