# API Reference

- [Live docs](https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/#/)
- [Live openapi.json](https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/openapi.json)

## Getting `subject` and `authorization`

Every endpoint requires a `subject` header (your Globus OpenID `sub` claim) and an `authorization` header (a Globus OAuth2 Bearer token). The easiest way to obtain both is through the [diaspora-event-sdk](https://github.com/globus-labs/diaspora-event-sdk):

```bash
pip install diaspora-event-sdk
```

```python
from diaspora_event_sdk import Client as GlobusClient

c = GlobusClient()  # triggers Globus login flow on first use

subject = c.subject_openid
authorization = c.web_client.authorizer.get_authorization_header()
namespace = f'ns-{subject.replace("-", "")[-12:]}'

print(f'export SUBJECT="{subject}"')
print(f'export AUTHORIZATION="{authorization}"')
print(f'export NAMESPACE="{namespace}"')
```

Use these values in the `subject` and `authorization` headers when calling the live endpoints below.

## Try it out

Once you have your `subject` and `authorization`, you can interact with the live API directly from the [Swagger UI](https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/#/). Click "Try it out" and fill in the header fields on each endpoint.

Alternatively, use `curl`:

### User

```bash
# Create user
curl -X POST "https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/api/v3/user" \
  -H "subject: $SUBJECT" \
  -H "authorization: $AUTHORIZATION"

# Delete user
curl -X DELETE "https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/api/v3/user" \
  -H "subject: $SUBJECT" \
  -H "authorization: $AUTHORIZATION"
```

### Authentication

```bash
# Create key
curl -X POST "https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/api/v3/key" \
  -H "subject: $SUBJECT" \
  -H "authorization: $AUTHORIZATION"

# Delete key
curl -X DELETE "https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/api/v3/key" \
  -H "subject: $SUBJECT" \
  -H "authorization: $AUTHORIZATION"
```

The key also lets a Kafka client connect to the cluster at the `endpoint` that Create key returns, with MSK IAM authentication. Such a client may use only topics, consumer group IDs and transactional IDs that start with your default namespace and a dot, such as `ns-0123456789ab.workers`; the cluster refuses any other with an authorization error (`GroupAuthorizationFailedError` in kafka-python for a group).

### Namespace

```bash
# List namespaces and topics
curl -X GET "https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/api/v3/namespace" \
  -H "subject: $SUBJECT" \
  -H "authorization: $AUTHORIZATION"
```

### Topic

```bash
# Create topic
curl -X POST "https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/api/v3/$NAMESPACE/my-topic" \
  -H "subject: $SUBJECT" \
  -H "authorization: $AUTHORIZATION"

# Delete topic
curl -X DELETE "https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/api/v3/$NAMESPACE/my-topic" \
  -H "subject: $SUBJECT" \
  -H "authorization: $AUTHORIZATION"

# Recreate topic
curl -X PUT "https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/api/v3/$NAMESPACE/my-topic/recreate" \
  -H "subject: $SUBJECT" \
  -H "authorization: $AUTHORIZATION"
```

A namespace holds at most 50 topics, a limit the service sets with the `MAX_TOPICS_PER_NAMESPACE` environment variable. Creating a new topic in a namespace that already holds that many returns `"status": "failure"` with a message that says to delete a topic first. Once a create adds a topic and leaves the namespace with at least 90% of the limit (45 of 50), the reply appends a warning to `message` and returns the same sentence in a `warning` field, which is absent otherwise. At 45 of 50 the warning reads "Namespace my-namespace now holds 45 of 50 topics; a create fails once it holds 50, so delete topics you no longer need." and at 50 of 50 it reads "Namespace my-namespace now holds 50 of 50 topics, the most allowed; delete a topic before you create another." Repeating the create of a topic that already exists, recreating a topic and deleting one are not limited.

### Producing

The produce routes take the request syntax of Confluent's REST API v3, with the namespace in place of the cluster. They reply once every in-sync replica has acknowledged the record, so a client that only reaches port 443 can publish as well as consume.

```bash
# Produce one record
curl -X POST "https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/api/v3/$NAMESPACE/topics/my-topic/records" \
  -H "subject: $SUBJECT" \
  -H "authorization: $AUTHORIZATION" \
  -H "Content-Type: application/json" \
  -d '{"key": {"type": "STRING", "data": "job-42"},
       "value": {"type": "JSON", "data": {"step": 7, "loss": 0.31}},
       "headers": [{"name": "trace", "value": "AAE="}]}'
```

```json
{"topic_name": "my-topic", "partition_id": 0, "offset": 12, "timestamp": "2026-10-26T15:04:05.123Z"}
```

```bash
# Produce a batch of records
curl -X POST "https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/api/v3/$NAMESPACE/topics/my-topic/records:batch" \
  -H "subject: $SUBJECT" \
  -H "authorization: $AUTHORIZATION" \
  -H "Content-Type: application/json" \
  -d '{"entries": [
        {"id": "1", "value": {"type": "JSON", "data": {"step": 8}}},
        {"id": "2", "value": {"type": "AVRO", "data": {"step": 9}}}]}'
```

```json
{
  "successes": [
    {"id": "1", "topic_name": "my-topic", "partition_id": 0, "offset": 13, "timestamp": "2026-10-26T15:04:05.456Z"}
  ],
  "failures": [
    {"id": "2", "error_code": 42206, "message": "value type AVRO needs a schema registry; Octopus runs no schema registry, so use BINARY, JSON or STRING"}
  ]
}
```

A produce request needs `Content-Type: application/json`; a `charset` parameter, if present, must be UTF-8. Any other content type is refused with 415.

A record has the optional fields `key`, `value`, `headers`, `partition_id` and `timestamp`, and needs a key or a value:

- `key` and `value` are `{"type": ..., "data": ...}`. `BINARY` data is base64 and is written decoded; `JSON` data is any JSON value, written compactly as UTF-8; `STRING` data is a string, written as UTF-8. Absent or null `data` writes a null key or value. Octopus runs no schema registry, so the `AVRO`, `PROTOBUF` and `JSONSCHEMA` types, the schema fields (`schema`, `schema_id`, `schema_version`, `subject`, `subject_name_strategy`) and a key or value without a `type` are refused with 422.
- A record whose key and value are both null or absent is refused with 400: kafka-python, which the service sends with, cannot send one.
- `headers` is a list of `{"name": string, "value": base64 string or null}`; a null value is written as an empty one, because kafka-python cannot send a null header value.
- `partition_id` may only be 0: every topic has one partition, and any other partition is not found (404).
- `timestamp` is ISO-8601 with a UTC offset (for example `2026-10-26T15:04:05.123Z`) and becomes the record's timestamp; without it, the record takes the time it is produced. The reply gives the record's timestamp in UTC.

The single-record route takes exactly one record: an empty body is refused with 422 and a body that holds more than one JSON value with 400. The batch route takes 1 to 500 entries, each a record with an `id` of 1 to 80 letters, digits, dashes or underscores that is distinct within the batch. It answers 207 once every entry has been acknowledged or has failed, within the 30 s, with each entry in `successes` or `failures`. An empty body, missing or empty `entries`, more than 500 entries, or a missing, invalid or repeated id refuses the whole batch with 422.

A record holds at most 8 MiB (8,388,608 bytes), including about 100 bytes of Kafka's per-record overhead: its key, value and header names and values, once decoded, must fit in the rest. A request body holds at most 16 MiB, and so do a batch's records once encoded: JSON data is written compactly, but a number is written in full (`1e15` as `1000000000000000.0`), so a batch can grow past its body and is then refused with 413.

A body whose JSON would take too much memory to parse is refused with 413 before it is parsed. The service estimates the cost from the body's bytes and refuses an estimate over 192 MiB. The estimate counts 4 MiB for any body; `w` bytes for each byte of the body, where `w` is 1, 2 or 4 by the widest character the body holds or escapes (`\u4e2d` counts 2, `\ud83d\ude00` counts 4); `2w + 2` bytes for each byte of JSON it may write, which can be longer than the body by up to 14 bytes for each number with an exponent and 3 for each decimal point; and 160 bytes for each `{` or `[`, 32 for each `"`, 40 for each `,` and 48 for each `:`. So a body of long ASCII strings or base64 passes up to the 16 MiB, a JSON array of four-digit numbers (`[1000,1000,...]`) up to about 14.5 MiB, a string of emoji up to about 13.4 MiB, an object with unique keys (`{"1":1,"2":1,...}`) up to about 9.9 MiB, an array of single digits or of numbers like `1e15` up to about 7.5 MiB, and one of empty objects or arrays (`[{},{},...]`) up to about 2.6 MiB.

The service holds at most 48 MiB of produce bodies and records at once, and at most 16 MiB for any one user (`subject`). A request holds the bytes of its body that have arrived, whatever its `Content-Length` declares, and once parsed the size of its records if that is larger. Once its body has arrived, a request takes one of the 8 places the service has for parsing and sending; a request still sending its body takes none. A user may have at most 2 requests in flight, at any stage. A request past a limit is refused with 429 and `Retry-After: 1`, as soon as it passes it: when it arrives, while its body arrives, or once it is read or parsed. When it arrives, a request is also refused before its body is sent if every place is taken or its `Content-Length` is more than the bytes free, for the service or for its user; nothing is held for it until its bytes arrive. A request's body must arrive within 20 s of its admission, which needs about 7 Mbit/s for a body of 16 MiB; a slower body is refused with 408. The service closes the connection after a 408, a 413 or a 429.

The records' 30 s start once the body is read and parsed, and the records of a request share them to be acknowledged. A record not sent by then fails with 50003 and is not written; a record sent but not acknowledged by then fails with 50003 and may still be written. A 200 or a success in a 207 does not rule out a second copy either: the producer is not idempotent and retries a failed write within the 30 s, so a write that reached the cluster but whose acknowledgement was lost can be written twice. Consumers that need each record once should carry an id in the record and drop repeats.

While the service cannot reach the Kafka cluster, it answers 503 for 5 s before it tries again, with `Retry-After` set to the seconds left. A topic that the namespace holds but the cluster does not list answers 404 once the cluster has answered twice without it and 2 s have passed (a topic just created can take a moment to reach every broker). A failed token check answers 401 with `{"status": "error", "reason": ...}`, the body the user, key and topic routes give for it, and a namespace or topic not found answers 404 with 40401.

| Code | Status | Meaning |
| --- | --- | --- |
| 40002 | 400 | The topic name contains a dot |
| 40003 | 400 | The body is not one JSON object in UTF-8 |
| 40004 | 400 | The body holds more than one JSON value |
| 40005 | 400 | A record field is invalid (bad base64, unknown type or field, timestamp without an offset, no key or value) |
| 40301 | 403 | The service is not authorized to write to the topic |
| 40401 | 404 | The namespace or topic is not found, in the namespace or in the cluster |
| 40402 | 404 | `partition_id` is not 0 |
| 40801 | 408 | The body did not arrive within 20 s |
| 41301 | 413 | The request body is over 16 MiB |
| 41302 | 413 | A record is over 8 MiB, counting Kafka's overhead |
| 41303 | 413 | The body's JSON would take too much memory to parse |
| 41304 | 413 | A batch's records are over 16 MiB once encoded |
| 41501 | 415 | The body is not `Content-Type: application/json` in UTF-8 |
| 42206 | 422 | The body is empty, or the record needs a schema registry |
| 42208 | 422 | The batch is empty or over 500 entries, or an entry's `id` is missing, invalid or repeated |
| 42901 | 429 | The service, or your share of it, holds as many produce requests or bytes as it can; retry after `Retry-After` |
| 50002 | 500 | Kafka refused the record, or the service is shutting down |
| 50003 | 500 | A retriable Kafka error, or the record was not acknowledged within the 30 s; retry it |
| 50302 | 503 | The service cannot reach the Kafka cluster; retry after `Retry-After` |

In a batch, 40005, 40301, 40401 (from the cluster), 40402, 41302, 42206, 50002, 50003 and 50302 fail the one entry and the others go on; the other codes refuse the whole request. Error messages do not include Kafka's own error text.

#### Differences from Confluent's REST API

The routes follow Confluent's statuses and codes except where noted here.

| Case | Octopus | Confluent | Why |
| --- | --- | --- | --- |
| Body over 16 MiB | 413, 41301 | 400, 400 | 413 says what is wrong, and the code tells it apart |
| Record over 8 MiB | 413, 41302 | 413, 413 | A five-digit code like the rest |
| Body not JSON, more than one value, invalid field | 400 with 40003, 40004, 40005 | 400, 400 | One code for each cause |
| Content type not JSON | 415, 41501 | 415, 415 | A five-digit code like the rest |
| More than one JSON value in a single-record body | Refused | Read as a stream of records | One request is one record; several go to `records:batch` |
| Schema fields with type `BINARY`, `JSON` or `STRING` | Refused, 422 | Ignored | Without a schema registry the fields cannot mean what the client asked |
| Payload errors in a record (a schema type or field, no type) | 422, `error_code` 42206 | 422, `error_code` 422 and `Error: 42206 : ...` in the message | The code where a client looks for it; Confluent already puts 42206 in `error_code` for an empty body and 42208 for an invalid batch, as here |
| Token check failed | 401 with `status` and `reason` | 401 from the web server's HTTP authentication, without a Confluent error code; Confluent's 40101 is for a failed Kafka authentication | The same body as the user, key and topic routes |
| `partition_id` above 0 | 404, 40402, at once | Negative ids: 404, 40402. Ids past the partition count: the Kafka producer waits `max.block.ms`, then fails with "not present in metadata", which Confluent answers with 404, 40401 | Every topic has one partition, so the answer is known at once |
| No key and no value | Refused, 400 | Sent | kafka-python cannot send it |
| Null header value | Written empty | Written null | kafka-python cannot send a null header value |
| Too many requests or bytes | 429, 42901 | 429, 429 for its own rate limit | Confluent's code for a quota, which this is |
| Topic missing from the cluster | 404, 40401 after 2 s | 404, 40401 after `max.block.ms` (60 s by default) | Same reply, sooner |
| Body slower than 20 s | 408, 40801 | No limit of its own; Confluent sends 408, 40801 for a serialization error | A stalled upload would hold its share; the code is Confluent's for a 408 |
| Parse cost, records over 16 MiB | 413 with 41303, 41304 | No such limits | They bound the service's memory |

### Consumer

The consumer routes take the paths and bodies of Confluent's REST Proxy v2, under `/api/v2/consumers`. They act in the caller's default namespace, `ns-` and the last 12 characters of the subject without dashes (`$NAMESPACE` above). Topics are named without the namespace, and the group `my-group` is the Kafka consumer group `$NAMESPACE.my-group`.

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

`base_uri` is the instance's absolute URL. The scheme and host come from the `X-Forwarded-Proto` and `X-Forwarded-Host` headers that the load balancer sets, when they hold a plain scheme (`http` or `https`) and host, and otherwise from the request.

A request body may have the content type `application/vnd.kafka.v2+json`, another v2 type (`application/vnd.kafka.binary.v2+json`, `application/vnd.kafka.json.v2+json` and the `avro`, `jsonschema` and `protobuf` ones), `application/json`, or none. Any other type is refused with 415 and error code 415. Every reply but a fetch's has the type `application/vnd.kafka.v2+json`.

A fetch chooses its reply's type from the `Accept` header, as Confluent's REST Proxy does. `application/vnd.kafka.binary.v2+json`, `application/vnd.kafka.v2+json`, `*/*` or no `Accept` reads a binary instance, and `application/vnd.kafka.json.v2+json` reads a json instance; the reply has the type chosen. A type of the other format is refused with 406 and error code 40601. An `Accept` that names no v2 type, such as `application/json`, is refused with 406 and error code 406. HTTP libraries that send `Accept: */*` or `application/json` by default must set the header to read a json instance.

Each record is `{"topic", "key", "value", "partition", "offset", "headers"}`, as in Confluent's REST Proxy v2, plus `headers` in the produce routes' form: a list of `{"name": string, "value": base64 string or null}`. A binary instance returns keys and values as base64. A json instance returns each key and value as the JSON value it holds, as it was produced.

A json instance returns a key or value only if it is one JSON value in UTF-8. A record that is not ends its partition's part of the reply: the records before it are returned, the partition is sought back to it, and the other partitions go on. A fetch whose first record is not JSON is refused with 408 and error code 40801, and so is every fetch after it, until the client seeks past the record or reads it with a binary instance. A key or value whose check would take more than 32 MiB of memory counts as not JSON. A JSON string of ASCII text passes up to about 15 MiB, larger than any record, and an array of small objects (`[{"a":1},...]`) up to about 1.5 MiB.

A consumer instance serves one request at a time. A request that waits more than 10 seconds for another request on the same instance returns 409 with error code 40904; retry it. A fetch does not wait: if another request holds the instance, it returns 409 with 40904 at once.

At create, `name` is 1 to 128 letters, digits, dots, dashes or underscores, starting with a letter or a digit; without one, the service picks 12 hex digits. Confluent's deprecated `id` field, if set, is taken as the name. `auto.offset.reset` must be `earliest`, `latest` (the default) or `none`, and `auto.commit.enable` must be `true` or `false` (the default), in any case. `fetch.min.bytes` and `consumer.request.timeout.ms` must be whole numbers of at least 0, given as numbers or strings. A create that breaks one of these rules is refused with 422 and error code 42204, and the message names the setting. `fetch.min.bytes` is passed to the Kafka consumer. `consumer.request.timeout.ms` is how long a fetch that names no `timeout` waits for records, 5000 ms if it is not set; it is not passed to the Kafka consumer.

A fetch waits for records for its `timeout`, in milliseconds. A `timeout` of 0 or less, or none, waits for the instance's `consumer.request.timeout.ms`. Either wait is capped at 30000 ms. As in Confluent's REST Proxy, `max_bytes` counts the bytes of the records' keys and values, and a reply holds fewer than that; unlike there, the first record is returned however large. 0, a negative value or none sets no limit but the reply's 2 MiB. The records left out come first in the next fetch. A fetch polls in turns of at most half a second (see the limits below), and its turns and its waits for a turn together take no longer than its `timeout`. It returns as soon as a turn finds records, and returns `[]` if none did.

A commit with offsets names, for each partition, the offset of the last record the client processed, as in Confluent's REST Proxy, and the group commits the next one, with the offset's `metadata` if given. A commit without a body commits the position of each assigned partition, which is already the next record to read. So after a commit of offset 41, or after a commit without a body once a fetch returned offset 41, `GET …/offsets` returns 42 for that partition. It leaves out a partition the group has not committed, and a request without a body is refused with 404 and 40402, as in Confluent's REST Proxy.

An instance returns records as `binary` or `json`, named in `format` in any case. Octopus runs no schema registry, so a create with `avro`, `jsonschema` or `protobuf` is refused with 422, as is any other format.

Every topic has one partition, 0, and a request that names another partition is refused with 404. A seek or a commit with offsets takes only partitions assigned to the instance. A partition is assigned by `assignments`, or after a subscribe by the group once the instance has fetched. A seek or commit on any other partition is refused with 409. A seek to the beginning or end with an empty list seeks every assigned partition, and is refused with 409 if none is assigned. An instance either subscribes to topics or is assigned partitions: to switch, unsubscribe first, or the request is refused with 409.

#### The deprecated `/api/v3` consumer routes

The consumer routes of earlier versions, under `/api/v3/{namespace}/consumers`, remain as deprecated aliases of the v2 routes, with the namespace in the path. They take the same bodies and give the same replies, and a create through them returns a `base_uri` under `/api/v3`. Their replies carry the header `Deprecation: @1792972800` (RFC 9745: deprecated since 26 October 2026), and the OpenAPI document marks them deprecated. An instance is the same instance through either prefix. A later version removes the aliases, so clients should move to `/api/v2/consumers`.

#### Instance lifecycle

An instance lives in the memory of the one worker that created it. A deploy or a restart of the service drops every instance, and an instance idle for `CONSUMER_INSTANCE_TIMEOUT_MS` (5 minutes by default; the service looks every 30 s) is closed. The next request for an instance that is gone gets 404 with error code 40403.

An instance created with `auto.commit.enable` set to `true` commits its positions before it closes, when it is deleted, when it is closed for being idle, and when the service shuts down. Its position in a partition is the record after the last one a fetch returned to the client, since records that a reply left out or that did not reach the client are sought back to. The commit waits at most 5 s; at shutdown, all the instances share 8 s. A commit that fails or runs out of time is logged, and the instance closes anyway, so the group keeps its earlier commit. The Kafka consumer of such an instance also commits every 5 s while it fetches. An instance without auto-commit commits nothing when it closes: only the client's commits count.

After a 404 with 40403, the client creates the instance again with the same group and name, subscribes or assigns again, and fetches. The group starts from its last commit, so the records fetched after that commit come again; a client that needs each record once should carry an id in the record and drop repeats. A group that has never committed starts where the instance's `auto.offset.reset` says, and with `latest`, the default, it skips the records produced while no instance was reading.

The consumer routes have these limits:

- A user (`subject`) may hold 4 consumer instances, and the service 16 in all. A create past either is refused with 429.
- A request may list at most as many topics, partitions or offsets as a namespace may hold topics (50, set by `MAX_TOPICS_PER_NAMESPACE`); a longer list is refused with 422. A request body may be 64 KiB; a larger one is refused with 413 before it is read.
- A fetch asks each broker for at most 1 MiB of records. A broker still returns a first record larger than that, so a fetch can read a record of up to 8 MiB from each broker.
- A reply holds at most 2 MiB of JSON. The records past that are left for the next fetch, which returns them first. A fetch always returns its first record, however large: a reply over 2 MiB holds that record alone and is sent in chunks, without a `Content-Length`.
- A user may have 2 fetches in progress. A fetch past those is refused at once with 429 and error code 42901.
- The service polls for 3 fetches at once, in turns of at most half a second, which fetches take in the order they ask. A turn that finds nothing goes to the back of the line; a turn that finds records keeps its place until its reply is sent. So a fetch whose records are ready waits about half a second for every 3 fetches ahead of it: about 2.5 s behind 15 quiet long polls, the most there can be, since the service holds 16 instances and an instance serves one fetch at a time. Replies being sent hold their turns longer. The default wait of 5000 ms gets through behind 15 quiet long polls; a shorter `timeout` can be refused. A fetch that gets no turn within its wait for records is refused with 429 and 42901. Like the refusal for a user's third fetch, it carries `Retry-After: 1` and fetches nothing.
- A reply must be sent within 30 s. A reply whose sending has begun and takes longer is not fetched again: the service closes the connection, and the next fetch starts after the reply's records. A reply over 2 MiB holds one record of up to 8 MiB, which takes up to about 10.7 MiB as base64 and its own size as JSON, so a client may need to read such a reply at 0.36 MiB/s or faster. The skipped records stay in the topic, and the positions route can seek back to them, to the offset after the last record the client received, until a commit without a body, or auto-commit, commits past them. `max_bytes` does not make a one-record reply smaller.
- A reply the client did not receive is fetched again by the next fetch, unless it was already handed to the network or took more than 30 s to send. This covers a client that leaves while its fetch polls or while a reply is sent, and a reply that fails to encode. The service cannot tell whether a client read a reply it handed to the network whole, so such a reply counts as received, as a reply read by a Kafka consumer does.
- When a reply is cut at 2 MiB or at `max_bytes`, the partitions take turns to come first in the next replies, so one partition's records cannot keep another's out. Within a partition, records keep their order.

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
| 40801 | 408 | A json instance's next record has a key or value that is not JSON |
| 40902 | 409 | The group already has an instance of that name |
| 40903 | 409 | A seek or commit on a partition not assigned to the instance, a subscribe while partitions are assigned or an assign while subscribed, or a commit the group refused because it rebalanced |
| 40904 | 409 | Another request held the instance for more than 10 s; retry |
| 41301 | 413 | The request body is over 64 KiB |
| 415 | 415 | The request's content type is not a v2 type or `application/json` |
| 422 | 422 | A header or field is missing or invalid, a list is too long, or the format is not `binary` or `json` |
| 42204 | 422 | At create, the name or a setting breaks its rule above; at subscribe, the list of topics is empty or `topic_pattern` is set |
| 42901 | 429 | The user holds 4 instances or the service 16, the user has 2 fetches in progress, the fetch got no turn within its wait, or the cluster throttled the request |
| 500 | 500 | An error inside the service |
| 50002 | 500 | Kafka failed the request, or the service has no Kafka endpoint |
| 50003 | 500 | A retriable Kafka error, or Kafka did not answer in time; retry |
| 503 | 503 | A create while the service shuts down; retry once it is back |
| 50302 | 503 | The service cannot reach the Kafka cluster; retry |

#### Differences from Confluent's REST Proxy

The consumer routes follow the paths, bodies, statuses and codes of Confluent's REST Proxy v2 except where noted here.

| Case | Octopus | Confluent | Why |
| --- | --- | --- | --- |
| Namespace and topic names | The routes act in the caller's default namespace; topics and groups are named without it | No namespaces | Each user's topics and groups are prefixed with the user's namespace |
| `/api/v3/{namespace}/consumers` | Deprecated aliases of the v2 routes | No such routes | Clients of earlier versions keep working until a later version removes them |
| Instance name | 1 to 128 letters, digits, dots, dashes or underscores, starting with a letter or digit; else 422, 42204 | No rule | The name is a segment of `base_uri` |
| Request content type `application/json` | Accepted | 415, 415 | Clients that send JSON as `application/json` keep working |
| Record headers | Each record has `headers` | v2 records have none | The produce routes write headers, so the consumer routes return them |
| A json record that is not JSON | 408, 40801 when it is the first in the reply; otherwise the records before it are returned and the next fetch starts at it | 408, 40801, and the records the fetch had read are not returned | No record is lost |
| `max_bytes` | The first record is returned however large | A first record of `max_bytes` bytes or more is not returned, so every fetch returns `[]` | A fetch always moves on |
| Format `avro`, `jsonschema` or `protobuf` | Refused, 422, 422 | Decoded with a schema registry | Octopus runs no schema registry. Confluent refuses a format it does not know with 422, 422 |
| Body not JSON | 400, 40003 | 400, 400 | The produce routes' code for the same case |
| Body over 64 KiB | 413, 41301 | No limit of its own | It bounds the service's memory; the produce routes' code |
| More than 4 instances for a user, or 16 in all | 429, 42901 | No limit | Each instance holds a Kafka client in the service's memory; 42901 is Confluent's code for too many requests |
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
| A json key or value whose check would take over 32 MiB | Not JSON: 408, 40801, as above | Decoded | It bounds the service's memory |
| `timeout` | 0 or less, or none, waits for the instance's `consumer.request.timeout.ms`; a positive `timeout` waits for itself; either wait is capped at 30000 ms | 0 or less, or none, waits for `consumer.request.timeout.ms`; a positive `timeout` waits for the smaller of itself and `consumer.request.timeout.ms` | A client can wait longer than its instance's default, up to 30 s |
| `consumer.request.timeout.ms` | The wait of a fetch that names no `timeout`, 5000 ms if not set, capped at 30000 ms; not passed to the Kafka consumer | The same, with the proxy's own default and no cap | The 30 s cap on every fetch wait |
| `fetch.min.bytes` | Passed to the Kafka consumer, so each broker waits for that many bytes before it replies | The least bytes the proxy gathers for a reply before the wait ends | The service gathers no records past one poll of the Kafka consumer |
| Commit of offset N | The group commits N + 1, the next record to read | The same | kafka-python's `commit` takes the next record to read, so the service adds the 1 |
