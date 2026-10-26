# Consume API

The consumer routes take the paths and bodies of Confluent's REST Proxy v2, under `/api/v2/consumers`. They act in the caller's default namespace, `ns-` and the last 12 characters of the subject without dashes (`$NAMESPACE`). Topics are named without the namespace, and the group `my-group` is the Kafka consumer group `$NAMESPACE.my-group`. Every route takes the `subject` and `authorization` headers ([Getting `subject` and `authorization`](api.md#getting-subject-and-authorization)).

The SDK's `RESTConsumer` wraps these routes, and creates an instance again when the service has dropped it ([Instance lifecycle](#instance-lifecycle)). [ConsumerRESTDemo.ipynb](https://github.com/globus-labs/diaspora-event-sdk/blob/main/diaspora_event_sdk/examples/ConsumerRESTDemo.ipynb) consumes records with plain HTTP requests and then with the SDK.

## Examples

```bash
# Create consumer instance
curl -X POST "https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/api/v2/consumers/my-group" \
  -H "subject: $SUBJECT" \
  -H "authorization: $AUTHORIZATION" \
  -H "Content-Type: application/vnd.kafka.v2+json" \
  -d '{"name": "my-instance", "format": "json", "auto.offset.reset": "earliest"}'
```

```json
{"instance_id": "my-instance", "base_uri": "https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/api/v2/consumers/my-group/instances/my-instance"}
```

```bash
export INSTANCE="https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/api/v2/consumers/my-group/instances/my-instance"

# Subscribe to a list of topics (topic patterns are not supported)
curl -X POST "$INSTANCE/subscription" \
  -H "subject: $SUBJECT" \
  -H "authorization: $AUTHORIZATION" \
  -H "Content-Type: application/vnd.kafka.v2+json" \
  -d '{"topics": ["my-topic"]}'

# Fetch records of a json instance
curl -X GET "$INSTANCE/records?timeout=5000" \
  -H "subject: $SUBJECT" \
  -H "authorization: $AUTHORIZATION" \
  -H "Accept: application/vnd.kafka.json.v2+json"
```

```json
[{"topic": "my-topic", "key": null, "value": {"step": 7, "loss": 0.31}, "partition": 0, "offset": 12, "headers": [{"name": "trace", "value": "AAE="}]}]
```

```bash
# Commit offsets
curl -X POST "$INSTANCE/offsets" \
  -H "subject: $SUBJECT" \
  -H "authorization: $AUTHORIZATION"

# Delete consumer instance
curl -X DELETE "$INSTANCE" \
  -H "subject: $SUBJECT" \
  -H "authorization: $AUTHORIZATION"
```

## Routes

In this table, `…` stands for `/api/v2/consumers/{group}/instances/{instance}`. A route with no body in the table takes none.

| Route | Body | Success |
| --- | --- | --- |
| `POST /api/v2/consumers/{group}` | `name`, `format`, `auto.offset.reset`, `auto.commit.enable`, `fetch.min.bytes` and `consumer.request.timeout.ms`, each optional; no body takes every default | 200, `{"instance_id", "base_uri"}` |
| `DELETE …` | | 204 |
| `POST …/subscription` | `{"topics": [...]}` | 204 |
| `GET …/subscription` | | 200, `{"topics": [...]}` |
| `DELETE …/subscription` | | 204 |
| `POST …/assignments` | `{"partitions": [{"topic", "partition"}]}` | 204 |
| `GET …/assignments` | | 200, `{"partitions": [...]}` |
| `POST …/offsets` | `{"offsets": [{"topic", "partition", "offset", "metadata"}]}`, or none to commit every position | 200, `[]` |
| `GET …/offsets` | `{"partitions": [{"topic", "partition"}]}` | 200, `{"offsets": [{"topic", "partition", "offset", "metadata"}]}` |
| `POST …/positions` | `{"offsets": [{"topic", "partition", "offset"}]}` | 204 |
| `POST …/positions/beginning` | `{"partitions": [...]}` | 204 |
| `POST …/positions/end` | `{"partitions": [...]}` | 204 |
| `GET …/records` | none; the query takes `timeout` and `max_bytes` | 200, a list of records |

`base_uri` is the instance's absolute URL. The scheme and host come from the `X-Forwarded-Proto` and `X-Forwarded-Host` headers when present and when they hold a plain scheme (`http` or `https`) and host, and otherwise from the request's `Host` and scheme.

A request body may have the content type `application/vnd.kafka.v2+json`, another v2 type (`application/vnd.kafka.binary.v2+json`, `application/vnd.kafka.json.v2+json` and the `avro`, `jsonschema` and `protobuf` ones), `application/json`, or none. Any other type is refused with 415 and error code 415. Every reply but a fetch's has the type `application/vnd.kafka.v2+json`.

## Records

A fetch chooses its reply's type from the `Accept` header, as Confluent's REST Proxy does. `application/vnd.kafka.binary.v2+json`, `application/vnd.kafka.v2+json`, `*/*` or no `Accept` reads a binary instance, and `application/vnd.kafka.json.v2+json` reads a json instance; the reply has the type chosen. A type of the other format is refused with 406 and error code 40601. An `Accept` that names no v2 type, such as `application/json`, is refused with 406 and error code 406. HTTP libraries that send `Accept: */*` or `application/json` by default must set the header to read a json instance.

Each record is `{"topic", "key", "value", "partition", "offset", "headers"}`, as in Confluent's REST Proxy v2, plus `headers` in the produce routes' form: a list of `{"name": string, "value": base64 string or null}`. A binary instance returns keys and values as base64, so it returns the bytes exactly. A json instance returns a key or value that is one JSON value as that value, written as it was produced, and any other UTF-8 text as a JSON string of the text. So the produce routes' `JSON` keys and values come back as JSON, and `STRING` ones as strings. A client cannot tell `"abc"` produced as JSON from `abc` produced as text; produce JSON-typed keys and values for json instances, or read with a binary instance.

A json instance cannot return a key or value that is not UTF-8. A record with one ends its partition's part of the reply: the records before it are returned, the partition is sought back to it, and the other partitions go on. A fetch whose first record has one is refused with 408 and error code 40801, and so is every fetch after it, until the client seeks past the record or reads it with a binary instance. The check that a key or value is one JSON value is stricter than Confluent's: text after the value, as in `{"a":1} {"b":2}`, makes the whole of it text. A check that would take more than 32 MiB of memory is not made, and the value is returned as text. A JSON string of ASCII text is checked up to about 15 MiB, larger than any record, but an array of small objects (`[{"a":1},...]`) only up to about 1.5 MiB, and a deeply nested array less.

## Instances, fetches and commits

A consumer instance serves one request at a time. A request that waits more than 10 seconds for another request on the same instance returns 409 with error code 40904; retry it. A fetch does not wait: if another request holds the instance, it returns 409 with 40904 at once.

At create, the group and `name` are each 1 to 128 letters, digits, dots, dashes or underscores, starting with a letter or a digit; without a name, the service picks 12 hex digits. Confluent's deprecated `id` field, if set, is taken as the name. `auto.offset.reset` must be `earliest`, `latest` (the default) or `none`, and `auto.commit.enable` must be `true` or `false` (the default), in any case. `fetch.min.bytes` and `consumer.request.timeout.ms` must be whole numbers of at least 0, given as numbers or strings. A create that breaks one of these rules is refused with 422 and error code 42204, and the message names the group, the name or the setting. `fetch.min.bytes` is passed to the Kafka consumer. `consumer.request.timeout.ms` is how long a fetch that names no `timeout` waits for records, 5000 ms if it is not set; it is not passed to the Kafka consumer.

A fetch waits for records for its `timeout`, in milliseconds. A `timeout` of 0 or less, or none, waits for the instance's `consumer.request.timeout.ms`. Either wait is capped at 30000 ms. As in Confluent's REST Proxy, `max_bytes` counts the bytes of the records' keys and values, and a reply holds fewer than that; unlike there, the first record is returned however large. 0, a negative value or none sets no limit but the reply's 2 MiB. The records left out come first in the next fetch. A fetch polls in turns of at most half a second (see [Limits](#limits)), and its turns and its waits for a turn together take no longer than its `timeout`. It returns as soon as a turn finds records, and returns `[]` if none did.

A commit with offsets names, for each partition, the offset of the last record the client processed, as in Confluent's REST Proxy, and the group commits the next one, with the offset's `metadata` if given. A commit without a body commits the position of each assigned partition, which is already the next record to read. So after a commit of offset 41, or after a commit without a body once a fetch returned offset 41, `GET …/offsets` returns 42 for that partition. It leaves out a partition the group has not committed, and a request without a body is refused with 404 and 40402, as in Confluent's REST Proxy.

An instance returns records as `binary` or `json`, named in `format` in any case. Octopus runs no schema registry, so a create with `avro`, `jsonschema` or `protobuf` is refused with 422, as is any other format.

Every topic has one partition, 0, and a request that names another partition is refused with 404. A seek or a commit with offsets takes only partitions assigned to the instance. A partition is assigned by `assignments`, or after a subscribe by the group once the instance has fetched. A seek or commit on any other partition is refused with 409. A seek to the beginning or end with an empty list seeks every assigned partition, and is refused with 409 if none is assigned. An instance either subscribes to topics or is assigned partitions: to switch, unsubscribe first, or the request is refused with 409.

A subscribe or an assign takes only topics the namespace holds, and refuses any other with 404 and 40401. A topic the namespace holds but the cluster does not list, such as one deleted while an instance reads it, is not created by the subscription or the assignment: the instance's fetches return no records from it.

## Instance lifecycle

An instance lives in the memory of the one worker that created it. A deploy or a restart of the service drops every instance, and an instance idle for `CONSUMER_INSTANCE_TIMEOUT_MS` (5 minutes by default; the service looks every 30 s) is closed. The next request for an instance that is gone gets 404 with error code 40403.

An instance created with `auto.commit.enable` set to `true` commits its positions at these times, and no others:

- During a fetch, when 5 s or more have passed since its last commit: the Kafka consumer commits the positions it had before that fetch's poll.
- When it is assigned partitions or unsubscribes, and, while it is subscribed, before its group rebalances during a fetch: the Kafka consumer commits its positions first.
- When it is deleted, when the service closes it for being idle, and when the service shuts down, before it closes. The commit waits at most 5 s; at shutdown, all the instances share 8 s.

Its position in a partition is the record after the last one a fetch returned to the client: records that a reply left out, that did not reach the client, or that a fetch cancelled at shutdown had polled are sought back to before any commit. An idle instance stays in its consumer group until the service closes it, so the commit at that close goes through. A commit that fails or runs out of time is logged, and the instance closes anyway, so the group keeps its earlier commit; at shutdown, an instance still busy after 8 s is neither committed nor closed. An instance without auto-commit commits nothing by itself: only the client's commits count.

After a 404 with 40403, the client creates the instance again with the same group and name, subscribes or assigns again, and fetches. The group starts from its last commit, so the records fetched after that commit come again; a client that needs each record once should carry an id in the record and drop repeats. A group that has never committed starts where the instance's `auto.offset.reset` says, and with `latest`, the default, it skips the records produced while no instance was reading.

## Limits

The consumer routes have these limits:

- A user (`subject`) may hold 4 consumer instances, and the service 12 in all. A create past either is refused with 429.
- A request may list at most as many topics, partitions or offsets as a namespace may hold topics (50, set by `MAX_TOPICS_PER_NAMESPACE`); a longer list is refused with 422. A request body may be 64 KiB; a larger one is refused with 413 before it is read.
- A fetch asks each broker for at most 1 MiB of records. A broker still returns a first record larger than that, so a fetch can read a record of up to 8 MiB from each broker.
- A reply holds at most 2 MiB of JSON. The records past that are left for the next fetch, which returns them first. A fetch always returns its first record, however large: a reply over 2 MiB holds that record alone and is sent in chunks, without a `Content-Length`.
- A user may have 2 fetches in progress. A fetch past those is refused at once with 429 and error code 42901.
- The service polls for 3 fetches at once, in turns of at most half a second, which fetches take in the order they ask. A turn that finds nothing goes to the back of the line; a turn that finds records keeps its place until its reply is sent. So a fetch whose records are ready waits about half a second for every 3 fetches ahead of it: about 1.5 s behind 11 quiet long polls, the most there can be, since the service holds 12 instances and an instance serves one fetch at a time. Replies being sent hold their turns longer. The default wait of 5000 ms gets through behind 11 quiet long polls; a shorter `timeout` can be refused. A fetch that gets no turn within its wait for records is refused with 429 and 42901. Like the refusal for a user's third fetch, it carries `Retry-After: 1` and fetches nothing.
- A reply must be sent within 30 s. A reply whose sending has begun and takes longer is not fetched again: the service closes the connection, and the next fetch starts after the reply's records. A reply over 2 MiB holds one record of up to 8 MiB, which takes up to about 10.7 MiB as base64 and up to 48 MiB as the JSON string of text of control characters, so a client may need to read such a reply at 1.6 MiB/s or faster. In `binary`, the reply for an 8 MiB record is about 10.7 MiB, which needs about 0.36 MiB/s. The skipped records stay in the topic, and the positions route can seek back to them, to the offset after the last record the client received, until a commit without a body, or auto-commit, commits past them. `max_bytes` does not make a one-record reply smaller.
- A reply the client did not receive is fetched again by the next fetch, unless it was already handed to the network or took more than 30 s to send. This covers a client that leaves while its fetch polls or while a reply is sent, and a reply that fails to encode. The service cannot tell whether a client read a reply it handed to the network whole, so such a reply counts as received, as a reply read by a Kafka consumer does.
- When a reply is cut at 2 MiB or at `max_bytes`, the partitions take turns to come first in the next replies, so one partition's records cannot keep another's out. Within a partition, records keep their order.

## Errors

Every error reply of a consumer route is `{"error_code": <int>, "message": <str>}`, with two exceptions: a path that matches no route answers 404, and a method a route does not take answers 405, both with FastAPI's `{"detail": <str>}`. Error messages do not include Kafka's own error text.

| Code | Status | Meaning |
| --- | --- | --- |
| 40002 | 400 | Kafka refused a topic name or the request as invalid, or the cluster does not support the request (`UnsupportedVersionError`) |
| 40003 | 400 | The body is not valid JSON |
| 40101 | 401 | The token check failed: the `subject` is not a UUID, or the token is malformed, inactive, another user's or without the scope |
| 40301 | 403 | The service is not authorized for the topic or the consumer group |
| 40401 | 404 | The namespace or topic is not found, in the namespace or in the cluster |
| 40402 | 404 | A partition other than 0, or `GET …/offsets` without a body |
| 40403 | 404 | The consumer instance is not found |
| 406 | 406 | A fetch's `Accept` names no v2 type |
| 40601 | 406 | A fetch's `Accept` names a format other than the instance's |
| 40801 | 408 | A json instance's next record has a key or value that is not UTF-8 |
| 40902 | 409 | The group already has an instance of that name |
| 40903 | 409 | A seek or commit on a partition not assigned to the instance, a subscribe while partitions are assigned or an assign while subscribed, or a commit the group refused because it rebalanced |
| 40904 | 409 | Another request held the instance for more than 10 s; retry |
| 41301 | 413 | The request body is over 64 KiB |
| 415 | 415 | The request's content type is not a v2 type or `application/json` |
| 422 | 422 | A header or field is missing or invalid, a list is too long, or the format is not `binary` or `json` |
| 42204 | 422 | At create, the name or a setting breaks its rule in [Instances, fetches and commits](#instances-fetches-and-commits); at subscribe, the list of topics is empty or `topic_pattern` is set |
| 42901 | 429 | The user holds 4 instances or the service 12, the user has 2 fetches in progress, the fetch got no turn within its wait, or the cluster throttled the request |
| 500 | 500 | An error inside the service |
| 50002 | 500 | Kafka failed the request, or the service has no Kafka endpoint |
| 50003 | 500 | A retriable Kafka error, or Kafka did not answer in time; retry |
| 503 | 503 | A create while the service shuts down; retry once it is back |
| 50302 | 503 | The service cannot reach the Kafka cluster; retry |

## Differences from Confluent's REST Proxy

The consumer routes follow the paths, bodies, statuses and codes of Confluent's REST Proxy v2 except where noted here.

| Case | Octopus | Confluent | Why |
| --- | --- | --- | --- |
| Namespace and topic names | The routes act in the caller's default namespace; topics and groups are named without it | No namespaces | Each user's topics and groups are prefixed with the user's namespace |
| Group and instance names | 1 to 128 letters, digits, dots, dashes or underscores, starting with a letter or digit; else 422, 42204 | No rule | Both are segments of `base_uri`, and clients drop a segment such as `..` |
| Request content type `application/json` | Accepted | 415, 415 | Clients that send JSON as `application/json` keep working |
| Record headers | Each record has `headers` | v2 records have none | The produce routes write headers, so the consumer routes return them |
| A json key or value that is UTF-8 text but not JSON | Returned as a JSON string of the text | 408, 40801 | A topic with text, such as the produce routes' `STRING` values, does not block a json instance |
| A json key or value that is not UTF-8 | 408, 40801 when it is the first in the reply; otherwise the records before it are returned and the next fetch starts at it | 408, 40801, and the records the fetch had read are not returned | No record is lost |
| Text after a JSON value, as in `{"a":1} {"b":2}` | The whole is text, returned as a string | The first value is returned and the rest ignored | The value is returned as it was produced |
| `max_bytes` | The first record is returned however large | A first record of `max_bytes` bytes or more is not returned, so every fetch returns `[]` | A fetch always moves on |
| Format `avro`, `jsonschema` or `protobuf` | Refused, 422, 422 | Decoded with a schema registry | Octopus runs no schema registry. Confluent refuses a format it does not know with 422, 422 |
| Body not JSON | 400, 40003 | 400, 400 | The produce routes' code for the same case |
| Body over 64 KiB | 413, 41301 | No limit of its own | It bounds the service's memory; the produce routes' code |
| More than 4 instances for a user, or 12 in all | 429, 42901 | No limit | Each instance holds a Kafka client in the service's memory; 42901 is Confluent's code for too many requests |
| A list longer than a namespace's topics | 422, 422 | No limit | A namespace holds no more topics, each with one partition, so a longer list names a topic or partition twice or one that does not exist |
| Token check failed | 401, 40101 | 401 from the web server's HTTP authentication, without a Confluent error code; Confluent's 40101 is for a failed Kafka authentication | A code where a client looks for one |
| The service fails to authenticate to Kafka | 500, 50002 | 401, 40101 | The credentials are the service's, not the caller's |
| A commit the group refuses after a rebalance (`CommitFailedError`) | 409, 40903 | 500, 50002 | The client recovers by fetching again, then committing |
| A commit for a partition not assigned to the instance | 409, 40903 | Passed to the Kafka consumer | The same answer as a seek on such a partition, which Confluent also answers with 409, 40903 |
| A partition other than 0 | 404, 40402, at once | Passed to the Kafka consumer | Every topic has one partition |
| Kafka reports an unknown topic or partition | 404, 40401 | 404, 40403 | Confluent also uses 40403 for an instance not found |
| Most bytes a fetch asks for | 1 MiB | 50 MiB, the Kafka consumer's default `fetch.max.bytes` | With 50 MiB, one fetch from 50 topics took 128 MiB of memory in a test, and the service has 1 GB |
| More than 2 fetches for a user, or no turn to poll within the fetch's wait | 429, 42901, with `Retry-After: 1` | No limit | Each fetch can hold a record of 8 MiB from each broker in the service's memory |
| Reply over 2 MiB of JSON | The records past 2 MiB are left for the next fetch; a first record over 2 MiB is returned alone | Up to `max_bytes` bytes, and the proxy's `consumer.request.max.bytes` | It bounds the service's memory |
| A json key or value whose check would take over 32 MiB, such as a large deeply nested array | Returned as a JSON string of its text | Decoded | It bounds the service's memory |
| `timeout` | 0 or less, or none, waits for the instance's `consumer.request.timeout.ms`; a positive `timeout` waits for itself; either wait is capped at 30000 ms | 0 or less, or none, waits for `consumer.request.timeout.ms`; a positive `timeout` waits for the smaller of itself and `consumer.request.timeout.ms` | A client can wait longer than its instance's default, up to 30 s |
| `consumer.request.timeout.ms` | The wait of a fetch that names no `timeout`, 5000 ms if not set, capped at 30000 ms; not passed to the Kafka consumer | The same, with the proxy's own default and no cap | The 30 s cap on every fetch wait |
| `fetch.min.bytes` | Passed to the Kafka consumer, so each broker waits for that many bytes before it replies | The least bytes the proxy gathers for a reply before the wait ends | The service gathers no records past one poll of the Kafka consumer |
| Commit of offset N | The group commits N + 1, the next record to read | The same | kafka-python's `commit` takes the next record to read, so the service adds the 1 |

## The earlier `/api/v3` consumer routes

The consumer routes of earlier versions, under `/api/v3/{namespace}/consumers`, are removed, and a request to them is refused with 404, or with 405 for a group named `recreate`, whose path is the topic recreate route's. Clients use `/api/v2/consumers`, which acts in the caller's default namespace.

A client of the earlier routes also sees these changes:

- A request body with a content type other than a v2 type or `application/json` is refused with 415. `curl -d` without `-H "Content-Type: ..."` sends `application/x-www-form-urlencoded`, so it is refused.
- A create with a group or name that breaks the rule in [Instances, fetches and commits](#instances-fetches-and-commits) is refused with 422, including an empty name, which the earlier routes took.
- `base_uri` is an absolute URL. A client that built an instance's URL as the host followed by `base_uri` must use `base_uri` alone.
- A fetch from a json instance must send `Accept: application/vnd.kafka.json.v2+json`. Without it, or with `*/*`, it is refused with 406 and 40601, and with `application/json` with 406 and 406.
- A json instance returns JSON keys and values as JSON values, not as strings, and records carry `headers`.
- A json instance refuses a fetch whose first record has a key or value that is not UTF-8 with 408 and 40801, where the earlier routes returned it as base64. Read such records with a binary instance.
- Replies have the content type `application/vnd.kafka.v2+json`, or a fetch's chosen v2 type, not `application/json`.
- `max_bytes` counts bytes, a commit answers `[]`, and `GET …/offsets` leaves out partitions never committed.
