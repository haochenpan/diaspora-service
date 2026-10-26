# Produce API

The produce routes take the request syntax of Confluent's REST API v3, with the namespace in place of the cluster. They reply once every in-sync replica has acknowledged the record, so a client that only reaches port 443 can publish as well as consume.

| Route | Body | Success |
| --- | --- | --- |
| `POST /api/v3/{namespace}/topics/{topic}/records` | One record | 200, the record's topic, partition, offset and timestamp |
| `POST /api/v3/{namespace}/topics/{topic}/records:batch` | `{"entries": [...]}`, 1 to 500 records | 207, `successes` and `failures` |

Both routes take the `subject` and `authorization` headers ([Getting `subject` and `authorization`](api.md#getting-subject-and-authorization)). The namespace in the path must be one the user holds, such as the default one, `$NAMESPACE`, and the topic one the namespace holds, named without the namespace; the [management routes](management.md#create-topic) create topics.

The SDK's `Client.produce` and `Client.produce_batch` call these routes. [ConsumerRESTDemo.ipynb](https://github.com/globus-labs/diaspora-event-sdk/blob/main/diaspora_event_sdk/examples/ConsumerRESTDemo.ipynb) produces records with plain HTTP requests and then with the SDK.

## Examples

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

## Records

A produce request needs `Content-Type: application/json`; a `charset` parameter, if present, must be UTF-8. Any other content type is refused with 415.

A record has the optional fields `key`, `value`, `headers`, `partition_id` and `timestamp`, and needs a key or a value:

- `key` and `value` are `{"type": ..., "data": ...}`. `BINARY` data is base64 and is written decoded; `JSON` data is any JSON value, written compactly as UTF-8; `STRING` data is a string, written as UTF-8. Absent or null `data` writes a null key or value. Octopus runs no schema registry, so the `AVRO`, `PROTOBUF` and `JSONSCHEMA` types, the schema fields (`schema`, `schema_id`, `schema_version`, `subject`, `subject_name_strategy`) and a key or value without a `type` are refused with 422.
- A record whose key and value are both null or absent is refused with 400: kafka-python, which the service sends with, cannot send one.
- `headers` is a list of `{"name": string, "value": base64 string or null}`; a null value is written as an empty one, because kafka-python cannot send a null header value.
- `partition_id` may only be 0: every topic has one partition, and any other partition is not found (404).
- `timestamp` is ISO-8601 with a UTC offset (for example `2026-10-26T15:04:05.123Z`) and becomes the record's timestamp; without it, the record takes the time it is produced. The reply gives the record's timestamp in UTC.

## Batches

The single-record route takes exactly one record: an empty body is refused with 422 and a body that holds more than one JSON value with 400. The batch route takes 1 to 500 entries, each a record with an `id` of 1 to 80 letters, digits, dashes or underscores that is distinct within the batch. It answers 207 once every entry has been acknowledged or has failed, within the 30 s, with each entry in `successes` or `failures`. An empty body, missing or empty `entries`, more than 500 entries, or a missing, invalid or repeated id refuses the whole batch with 422.

## Limits

A record holds at most 8 MiB (8,388,608 bytes), including about 100 bytes of Kafka's per-record overhead: its key, value and header names and values, once decoded, must fit in the rest. A request body holds at most 16 MiB, and so do a batch's records once encoded: JSON data is written compactly, but a number is written in full (`1e15` as `1000000000000000.0`), so a batch can grow past its body and is then refused with 413.

A body whose JSON would take too much memory to parse is refused with 413 before it is parsed. The service estimates the cost from the body's bytes and refuses an estimate over 192 MiB. The estimate counts 4 MiB for any body; `w` bytes for each byte of the body, where `w` is 1, 2 or 4 by the widest character the body holds or escapes (`\u4e2d` counts 2, `\ud83d\ude00` counts 4); `2w + 2` bytes for each byte of JSON it may write, which can be longer than the body by up to 14 bytes for each number with an exponent and 3 for each decimal point; and 160 bytes for each `{` or `[`, 32 for each `"`, 40 for each `,` and 48 for each `:`. So a body of long ASCII strings or base64 passes up to the 16 MiB, a JSON array of four-digit numbers (`[1000,1000,...]`) up to about 14.5 MiB, a string of emoji up to about 13.4 MiB, an object with unique keys (`{"1":1,"2":1,...}`) up to about 9.9 MiB, an array of single digits or of numbers like `1e15` up to about 7.5 MiB, and one of empty objects or arrays (`[{},{},...]`) up to about 2.6 MiB.

The service holds at most 48 MiB of produce bodies and records at once, and at most 16 MiB for any one user (`subject`). A request holds the bytes of its body that have arrived, whatever its `Content-Length` declares, and once parsed the size of its records if that is larger. Once its body has arrived, a request takes one of the 8 places the service has for parsing and sending; a request still sending its body takes none. A user may have at most 2 requests in flight, at any stage. A request past a limit is refused with 429 and `Retry-After: 1`, as soon as it passes it: when it arrives, while its body arrives, or once it is read or parsed. When it arrives, a request is also refused before its body is sent if every place is taken or its `Content-Length` is more than the bytes free, for the service or for its user; nothing is held for it until its bytes arrive. A request's body must arrive within 20 s of its admission, which needs about 7 Mbit/s for a body of 16 MiB; a slower body is refused with 408. The service closes the connection after a 408, a 413 or a 429.

## Acknowledgement

The records' 30 s start once the body is read and parsed, and the records of a request share them to be acknowledged. A record not sent by then fails with 50003 and is not written; a record sent but not acknowledged by then fails with 50003 and may still be written. A 200 or a success in a 207 does not rule out a second copy either: the producer is not idempotent and retries a failed write within the 30 s, so a write that reached the cluster but whose acknowledgement was lost can be written twice. Consumers that need each record once should carry an id in the record and drop repeats.

Every topic has replication factor 2 on the cluster's two brokers, and the cluster's `min.insync.replicas` is 2, so a write needs both brokers to acknowledge it. While one broker restarts, for a cluster update or after a failure, the cluster refuses writes as `NOT_ENOUGH_REPLICAS`. The producer retries such a write until the record's 30 s are spent, and the record then fails with 50003 and the message "The cluster did not acknowledge the record in time; it may still be written": a single-record request answers 500, and a batch answers 207 with the entry in `failures`. Clients should retry such records with backoff, for example from 1 s doubling to 30 s, for at least as long as a broker takes to restart. Since the record may still be written, a retry can write it twice.

## Errors

While the service cannot reach the Kafka cluster, it answers 503 for 5 s before it tries again, with `Retry-After` set to the seconds left. A topic that the namespace holds but the cluster does not list answers 404 once the cluster has answered twice without it and 2 s have passed (a topic just created can take a moment to reach every broker). A failed token check answers 401 with `{"status": "error", "reason": ...}`, the body the [management routes](management.md#replies) give for it, and a namespace or topic not found answers 404 with 40401. A request without the `subject` or `authorization` header answers 422 with FastAPI's `{"detail": [...]}`.

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

## Differences from Confluent's REST API

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
| Token check failed | 401 with `status` and `reason` | 401 from the web server's HTTP authentication, without a Confluent error code; Confluent's 40101 is for a failed Kafka authentication | The same body as the management routes |
| `partition_id` above 0 | 404, 40402, at once | Negative ids: 404, 40402. Ids past the partition count: the Kafka producer waits `max.block.ms`, then fails with "not present in metadata", which Confluent answers with 404, 40401 | Every topic has one partition, so the answer is known at once |
| No key and no value | Refused, 400 | Sent | kafka-python cannot send it |
| Null header value | Written empty | Written null | kafka-python cannot send a null header value |
| Too many requests or bytes | 429, 42901 | 429, 429 for its own rate limit | Confluent's code for a quota, which this is |
| Topic missing from the cluster | 404, 40401 after 2 s | 404, 40401 after `max.block.ms` (60 s by default) | Same reply, sooner |
| Body slower than 20 s | 408, 40801 | No limit of its own; Confluent sends 408, 40801 for a serialization error | A stalled upload would hold its share; the code is Confluent's for a 408 |
| Parse cost, records over 16 MiB | 413 with 41303, 41304 | No such limits | They bound the service's memory |
