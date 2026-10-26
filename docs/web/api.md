# API Reference

- [Live docs](https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/#/)
- [Live openapi.json](https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/openapi.json)

The web service has three REST APIs. Each has a reference page that gives every route with a curl example, its request, its replies and its limits.

| API | Routes | Does |
| --- | --- | --- |
| [Management](management.md) | `/api/v3/user`, `/api/v3/key`, `/api/v3/namespace`, `/api/v3/{namespace}/{topic}` | Creates and deletes the user, its access key and its topics, and lists its namespaces and topics |
| [Produce](produce.md) | `/api/v3/{namespace}/topics/{topic}/records` and `records:batch` | Writes records to a topic, in the request syntax of Confluent's REST API v3 |
| [Consume](consume.md) | `/api/v2/consumers` | Reads records through consumer instances, in the paths and bodies of Confluent's REST Proxy v2 |

All three run over HTTPS on port 443 with a Globus token. A client starts with [`POST /api/v3/key`](management.md#create-key), which creates the user and its default namespace, or [`POST /api/v3/user`](management.md#create-user), which creates them without a key; then it [creates a topic](management.md#create-topic), produces to it and consumes from it. The access key that `POST /api/v3/key` returns lets a Kafka client connect to the cluster directly instead; the produce and consume routes need no key.

## Getting `subject` and `authorization`

Every endpoint requires a `subject` header (your Globus OpenID `sub` claim) and an `authorization` header (a Globus OAuth2 Bearer token). The [diaspora-event-sdk](https://github.com/globus-labs/diaspora-event-sdk) gets both:

```bash
pip install diaspora-event-sdk
```

```python
from diaspora_event_sdk import Client as GlobusClient

c = GlobusClient()  # triggers Globus login flow on first use

subject = c.subject_openid
authorizer = c.authorizer or c.app.get_authorizer(c.DIASPORA_RESOURCE_SERVER)
authorization = authorizer.get_authorization_header()
namespace = c.namespace

print(f'export SUBJECT="{subject}"')
print(f'export AUTHORIZATION="{authorization}"')
print(f'export NAMESPACE="{namespace}"')
```

Use these values in the `subject` and `authorization` headers when you call the routes of each API's page.

`$NAMESPACE` is the user's default namespace: `ns-` followed by the last 12 characters of the subject without dashes. The default namespace exists once `POST /api/v3/user` or `POST /api/v3/key` has run.

## Try it out

Once you have your `subject` and `authorization`, you can interact with the live API directly from the [Swagger UI](https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/#/). Click "Try it out" and fill in the header fields on each endpoint. Each API's page gives a `curl` example for every route.

## Errors

The management routes and the produce and consume routes report errors in two shapes.

The management routes answer HTTP 200, for a failure as for a success, with `status` in the body: `"success"` or `"failure"` with a `message`, or `"error"` with a `reason` when the token check fails ([Replies](management.md#replies)):

```json
{"status": "failure", "message": "Topic my-topic not found in ns-0123456789ab"}
```

The produce and consume routes answer an HTTP status that says what happened, and a failure's body is `{"error_code": <int>, "message": <str>}`, with Confluent's five-digit codes ([produce errors](produce.md#errors), [consume errors](consume.md#errors)):

```json
{"error_code": 40401, "message": "Topics not found in namespace ns-0123456789ab: my-topic"}
```

These replies differ from the two shapes:

- A failed token check on a produce route answers 401 with the management routes' body, `{"status": "error", "reason": ...}`. On a consume route it answers 401 with `{"error_code": 40101, "message": ...}`.
- A request without the `subject` or `authorization` header answers 422. A management or produce route gives FastAPI's `{"detail": [...]}`, which names each missing header; a consume route gives `{"error_code": 422, "message": ...}`.
- A path that matches no route answers 404, and a method a route does not take 405, both with FastAPI's `{"detail": <str>}`.
- An error the service does not handle answers 500: with `{"error_code": 500, "message": "Internal server error"}` on a consume route, and with the text `Internal Server Error` on the others.

## Examples

The [Diaspora Event SDK](https://github.com/globus-labs/diaspora-event-sdk) calls these routes from Python; its README shows how. Its examples run against the live service:

- [DiasporaDemo.ipynb](https://github.com/globus-labs/diaspora-event-sdk/blob/main/diaspora_event_sdk/examples/DiasporaDemo.ipynb): logging in, the management routes through the SDK, and producing and consuming with a Kafka client and the access key.
- [ConsumerRESTDemo.ipynb](https://github.com/globus-labs/diaspora-event-sdk/blob/main/diaspora_event_sdk/examples/ConsumerRESTDemo.ipynb): producing and consuming over the produce and consume routes, first with plain HTTP requests and then with the SDK.
