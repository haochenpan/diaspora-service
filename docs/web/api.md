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
namespace = f"ns-{subject.replace('-', '')[-12:]}"

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

### Consumer

```bash
# Create consumer instance
curl -X POST "https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com/api/v3/$NAMESPACE/consumers/my-group" \
  -H "subject: $SUBJECT" \
  -H "authorization: $AUTHORIZATION" \
  -H "Content-Type: application/json" \
  -d '{"format": "json", "auto.offset.reset": "earliest"}'

# Subscribe to topics
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
