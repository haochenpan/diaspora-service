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

The single-record route takes exactly one record: an empty body is refused with 422 and a body that holds more than one JSON value with 400. The batch route takes 1 to 500 entries, each a record with an `id` of 1 to 80 letters, digits, dashes or underscores that is distinct within the batch. It answers 207 once every entry has been acknowledged or has failed, within the request's 30 s, with each entry in `successes` or `failures`. An empty body, missing or empty `entries`, more than 500 entries, or a missing, invalid or repeated id refuses the whole batch with 422.

A record holds at most 8 MiB (8,388,608 bytes), including about 100 bytes of Kafka's per-record overhead: its key, value and header names and values, once decoded, must fit in the rest. A request body holds at most 16 MiB. A body whose JSON would take too much memory to parse is refused with 413 before it is parsed. The service estimates the cost as 3 bytes for each byte of an ASCII body (9 for a body with other characters), plus 160 for each `{` or `[`, 32 for each `"` and 40 for each `,`, and refuses an estimate over 192 MiB. So a body of long strings passes up to the 16 MiB, a JSON array of four-digit numbers (`[1000,1000,...]`) too, one of single digits up to about 8 MiB, and one of empty objects or arrays (`[{},{},...]`) up to about 2.7 MiB.

The service serves at most 8 produce requests at once, holding at most 48 MiB of their bodies. It counts a request by its `Content-Length`, or as 16 MiB if it sends none. A request over either limit is refused at once with 429 and `Retry-After: 1`, before its body is read.

The 30 s of a request start when it arrives, and its records share them to be acknowledged. A record not sent by then fails with 50003 and is not written; a record sent but not acknowledged by then fails with 50003 and may still be written. A 200 or a success in a 207 does not rule out a second copy either: the producer is not idempotent and retries a failed write within the 30 s, so a write that reached the cluster but whose acknowledgement was lost can be written twice. Consumers that need each record once should carry an id in the record and drop repeats. While the service cannot reach the Kafka cluster, it answers 503 with `Retry-After: 1` for a few seconds before it tries again. The token, namespace and topic checks answer as on the consumer routes, with 401 or 404.

| Code | Status | Meaning |
| --- | --- | --- |
| 40002 | 400 | The topic name contains a dot |
| 40003 | 400 | The body is not one JSON object in UTF-8 |
| 40004 | 400 | The body holds more than one JSON value |
| 40005 | 400 | A record field is invalid (bad base64, unknown type or field, timestamp without an offset, no key or value) |
| 40401 | 404 | The namespace or topic is not found |
| 40402 | 404 | `partition_id` is not 0 |
| 41301 | 413 | The request body is over 16 MiB |
| 41302 | 413 | A record is over 8 MiB, counting Kafka's overhead |
| 41303 | 413 | The body's JSON would take too much memory to parse |
| 41501 | 415 | The body is not `Content-Type: application/json` in UTF-8 |
| 42206 | 422 | The body is empty, or the record needs a schema registry |
| 42208 | 422 | The batch is empty or over 500 entries, or an entry's `id` is missing, invalid or repeated |
| 42901 | 429 | The service is serving as many produce requests or bytes as it holds; retry after `Retry-After` |
| 50002 | 500 | Kafka refused the record, or the service is shutting down |
| 50003 | 500 | A retriable Kafka error, or the record was not acknowledged within the request's 30 s; retry it |
| 50302 | 503 | The service cannot reach the Kafka cluster; retry after `Retry-After` |

In a batch, 40005, 40402, 41302, 42206, 50002, 50003 and 50302 fail the one entry and the others go on; the other codes refuse the whole request. Error messages do not include Kafka's own error text.

### Consumer

```bash
# Create consumer instance
curl -X POST "https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/api/v3/$NAMESPACE/consumers/my-group" \
  -H "subject: $SUBJECT" \
  -H "authorization: $AUTHORIZATION" \
  -H "Content-Type: application/json" \
  -d '{"format": "json", "auto.offset.reset": "earliest"}'

# Subscribe to a list of topics (topic patterns are not supported)
curl -X POST "https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/api/v3/$NAMESPACE/consumers/my-group/instances/$INSTANCE_ID/subscription" \
  -H "subject: $SUBJECT" \
  -H "authorization: $AUTHORIZATION" \
  -H "Content-Type: application/json" \
  -d '{"topics": ["my-topic"]}'

# Fetch records
curl -X GET "https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/api/v3/$NAMESPACE/consumers/my-group/instances/$INSTANCE_ID/records?timeout=5000" \
  -H "subject: $SUBJECT" \
  -H "authorization: $AUTHORIZATION"

# Commit offsets
curl -X POST "https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/api/v3/$NAMESPACE/consumers/my-group/instances/$INSTANCE_ID/offsets" \
  -H "subject: $SUBJECT" \
  -H "authorization: $AUTHORIZATION"

# Delete consumer instance
curl -X DELETE "https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/api/v3/$NAMESPACE/consumers/my-group/instances/$INSTANCE_ID" \
  -H "subject: $SUBJECT" \
  -H "authorization: $AUTHORIZATION"
```

A consumer instance serves one request at a time. A request that waits more than 10 seconds for another request on the same instance returns 409 with error code 40904; retry it. The fetch `timeout` is capped at 30000 ms: a larger value waits 30000 ms.
