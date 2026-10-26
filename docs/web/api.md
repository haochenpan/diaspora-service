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
    {"id": "2", "error_code": 40004, "message": "value type AVRO needs a schema registry, which Octopus does not run; use BINARY, JSON or STRING"}
  ]
}
```

A record has the optional fields `key`, `value`, `headers`, `partition_id` and `timestamp`, and needs a key or a value:

- `key` and `value` are `{"type": ..., "data": ...}`. `BINARY` data is base64 and is written decoded; `JSON` data is any JSON value, written compactly as UTF-8; `STRING` data is a string, written as UTF-8. Absent or null `data` writes a null key or value. Octopus runs no schema registry, so the `AVRO`, `PROTOBUF` and `JSONSCHEMA` types, the schema fields (`schema`, `schema_id`, `schema_version`, `subject`, `subject_name_strategy`) and a key or value without a `type` are refused.
- `headers` is a list of `{"name": string, "value": base64 string or null}`; a null value is written as an empty one.
- `partition_id` may only be 0: every topic has one partition.
- `timestamp` is ISO-8601 with a UTC offset (for example `2026-10-26T15:04:05.123Z`) and becomes the record's timestamp; without it, the record takes the time it is produced. The reply gives the record's timestamp in UTC.

The single-record route takes exactly one record: a body that holds more than one JSON value is refused with 400. The batch route takes 1 to 500 entries, each a record with an `id` of 1 to 80 characters that is distinct within the batch. It answers 207 once the request is read, with each entry in `successes` or `failures`. A missing, invalid or repeated id, or more than 500 entries, refuses the whole batch with 400.

A record holds at most 8 MiB once decoded (key, value and header names and values), and a request body at most 16 MiB. The records of a request share 30 s to be acknowledged: a record not acknowledged by then fails with 50002 and may still be written, so a retry can write it twice. The token, namespace and topic checks answer as on the consumer routes.

| Code | Status | Meaning |
| --- | --- | --- |
| 40001 | 400 | The body is not one JSON object |
| 40002 | 400 | The body holds more than one JSON value |
| 40003 | 400 | A record field is invalid (bad base64, unknown type or field, timestamp without an offset, no key or value) |
| 40004 | 400 | The record needs a schema registry |
| 40005 | 400 | `partition_id` is not 0 |
| 40006 | 400 | The topic name contains a dot |
| 40007 | 400 | The batch's `entries` are missing, empty or more than 500 |
| 40008 | 400 | A batch entry's `id` is missing, invalid or repeated |
| 40401 | 404 | The namespace or topic is not found |
| 41301 | 413 | The request body is over 16 MiB |
| 41302 | 413 | A record is over 8 MiB once decoded |
| 50002 | 500 | Kafka failed to write the record, or did not acknowledge it within 30 s |

In a batch, 40003, 40004, 40005, 41302 and 50002 fail the one entry and the others go on; the other codes refuse the whole request.

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
