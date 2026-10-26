"""The OpenAPI summary and description of each route, and of the app.

The reference pages under docs/web/ say the same at more length; the
numbers here are the service's defaults.
"""

from __future__ import annotations

from typing import NamedTuple

DOCS_URL = 'https://haochenpan.github.io/diaspora-service/latest/web/api/'


class RouteText(NamedTuple):
    """A route's OpenAPI summary and description."""

    summary: str
    description: str | None = None


APP_DESCRIPTION = (
    'Manage Diaspora users, access keys and topics, and produce and '
    'consume records over HTTPS.\n\n'
    'Every route takes two headers: `subject`, your Globus identity ID, '
    'and `authorization`, `Bearer` followed by a Globus access token for '
    'this service. A request without either answers 422.\n\n'
    'The management routes (User, Authentication, Namespace, Topic) '
    'answer HTTP 200 with `status` in the body: `success` or `failure` '
    'with a `message`, or `error` with a `reason` when the token check '
    'fails. The Producer and Consumer routes answer an HTTP status that '
    'says what happened, and a failure\'s body is `{"error_code", '
    '"message"}`, except a produce route\'s failed token check, which '
    'answers 401 with `status` and `reason`.\n\n'
    f'Reference: {DOCS_URL}'
)

TAGS = [
    {
        'name': 'User',
        'description': (
            'The IAM user and the default namespace, `ns-` and the last 12 '
            'characters of the subject without dashes.'
        ),
    },
    {
        'name': 'Authentication',
        'description': (
            'The AWS access key a Kafka client uses to connect to the '
            'cluster with MSK IAM authentication. The produce and consume '
            'routes need no key.'
        ),
    },
    {
        'name': 'Namespace',
        'description': "The user's namespaces and their topics.",
    },
    {
        'name': 'Topic',
        'description': (
            'Topics of a namespace the user holds, named without the '
            'namespace. A topic name is 3 to 32 letters, digits, `-` or '
            '`_`, not starting or ending with `-` or `_`; a namespace '
            'holds at most 50 topics. Every topic has one partition.'
        ),
    },
    {
        'name': 'Producer',
        'description': (
            "Writes records in the request syntax of Confluent's REST API "
            'v3, with the namespace in place of the cluster.'
        ),
    },
    {
        'name': 'Consumer',
        'description': (
            "Consumer instances in the paths and bodies of Confluent's "
            "REST Proxy v2, in the caller's default namespace. Every "
            'route answers 401 with 40101 for a failed token check, 404 '
            'with 40403 for an instance not found, 409 with 40904 when '
            'another request holds the instance for more than 10 s, 413 '
            'for a body over 64 KiB and 415 for a content type that is not '
            'a v2 type or `application/json`. A user may hold 4 instances '
            'and the service 12; an instance idle for 5 minutes, and every '
            'instance at a restart, is closed.'
        ),
    },
]

# --- Management routes ---

_FAILS_WITH_STATUS = (
    ' Answers HTTP 200 for a failure too: read `status`, which is '
    '`failure` with a `message`, or `error` with a `reason` for a failed '
    'token check.'
)

CREATE_USER = RouteText(
    'Create the user and its default namespace',
    'Creates an IAM user named by the subject, with a policy that lets it '
    'use only Kafka topics, consumer groups and transactional IDs that '
    'start with the default namespace and a dot, and creates the default '
    'namespace. What already exists is kept, so a repeat succeeds. '
    'Creates no access key: `POST /api/v3/key` does. Returns `subject` '
    'and `namespace`. Fails if IAM refuses a call, or if another user '
    'holds the namespace name.' + _FAILS_WITH_STATUS,
)

DELETE_USER = RouteText(
    'Delete the user, its topics and its key',
    'Deletes each topic of the default namespace from the cluster, with '
    'its records, then the namespace, the stored access key and the IAM '
    'user with its keys and policy. Deleting a user that does not exist '
    'succeeds. Fails, deleting nothing, if the user holds namespaces '
    'other than the default one; stops if the cluster does not delete a '
    'topic, after 3 attempts, or if IAM refuses a call.' + _FAILS_WITH_STATUS,
)

CREATE_KEY = RouteText(
    'Get the access key, creating it if needed',
    'If the service holds a key for the user, returns that key with '
    '`fresh: false` and creates nothing; it does not check the key with '
    'IAM. Otherwise creates the user and the default namespace as `POST '
    '/api/v3/user` does, deletes every IAM access key of the user, creates '
    'one and returns it with `fresh: true`. To replace a key, delete it '
    'first. Returns `access_key`, `secret_key`, `create_date` and '
    "`endpoint`, the cluster's bootstrap servers for IAM authentication. "
    'Fails if creating the user fails or IAM refuses a call.'
    + _FAILS_WITH_STATUS,
)

DELETE_KEY = RouteText(
    'Delete the access keys',
    'Deletes every IAM access key of the user, then the stored key. The '
    'user, its namespace and its topics are kept. Deleting a key that '
    'does not exist succeeds. Fails, keeping the stored key, if IAM '
    'refuses to list or delete the keys.' + _FAILS_WITH_STATUS,
)

LIST_NAMESPACES = RouteText(
    'List namespaces and their topics',
    "Returns `namespaces`, which maps each of the user's namespaces to "
    'its topics, named without the namespace and sorted. The topics are '
    'those the service records; the cluster is not asked. A user not yet '
    'created has none.' + _FAILS_WITH_STATUS,
)

CREATE_TOPIC = RouteText(
    'Create a topic',
    'Adds the topic to the namespace and creates the Kafka topic '
    '`{namespace}.{topic}`, with one partition and replication factor 2. '
    "Returns the namespace's `topics`. A topic name is 3 to 32 letters, "
    'digits, `-` or `_`, not starting or ending with `-` or `_`. A '
    'namespace holds at most 50 topics: a new topic past that fails, and '
    'a create that leaves the namespace with 45 or more adds a `warning`. '
    'Creating a topic the namespace holds succeeds, is not counted, and '
    'creates the Kafka topic if the cluster lacks it. Fails for a name '
    'that breaks the rule, a namespace the user does not hold, or a topic '
    'the cluster does not create after 3 attempts, which is then removed '
    'from the namespace if this create added it.' + _FAILS_WITH_STATUS,
)

DELETE_TOPIC = RouteText(
    'Delete a topic and its records',
    'Deletes the Kafka topic, with every record in it, then removes the '
    "topic from the namespace and returns the namespace's `topics`. A "
    'topic the cluster or the namespace does not have counts as deleted. '
    'Fails for a namespace the user does not hold, a topic name with a '
    'dot, or a topic the cluster does not delete after 3 attempts, which '
    'the namespace then keeps.' + _FAILS_WITH_STATUS,
)

RECREATE_TOPIC = RouteText(
    'Empty a topic by deleting and creating it',
    'Deletes the Kafka topic and creates it again with the same name, so '
    "the topic loses every record; the namespace's topics do not change. "
    'Waits 5 s between the delete and the create, and retries the create '
    'after 2 s while the cluster still holds the old topic, 3 times in '
    'all. Fails for a namespace the user does not hold, a topic the '
    'namespace does not hold, or a delete or create the cluster does not '
    'complete; after a failed create, retry the recreate or create the '
    'topic.' + _FAILS_WITH_STATUS,
)

# --- Produce routes ---
# Their descriptions are the handlers' docstrings, which state the
# limits and every error code.

PRODUCE_RECORD = RouteText('Produce one record')
PRODUCE_BATCH = RouteText('Produce a batch of up to 500 records')

# --- Consumer routes ---

CONSUMER_CREATE = RouteText(
    'Create a consumer instance',
    'Creates an instance in the group `{group_name}`, the Kafka consumer '
    "group `<namespace>.{group_name}`. The body's `name`, `format` "
    '(`binary`, the default, or `json`), `auto.offset.reset` (`earliest`, '
    '`latest`, the default, or `none`), `auto.commit.enable`, '
    '`fetch.min.bytes` and `consumer.request.timeout.ms` (the default '
    'fetch wait, 5000 ms) are optional; no body takes every default. The '
    'group and the name are 1 to 128 letters, digits, dots, dashes or '
    'underscores, starting with a letter or digit; without a name the '
    'service picks one. Returns `instance_id` and `base_uri`, the '
    "instance's absolute URL. Errors: 409 with 40902 for a name the group "
    'has, 422 with 42204 for a group, name or setting that breaks its '
    'rule, 422 for the format `avro`, `jsonschema` or `protobuf`, 429 '
    'with 42901 once the user holds 4 instances or the service 12, 503 '
    'while the service shuts down.',
)

CONSUMER_DELETE = RouteText(
    'Delete a consumer instance',
    'Closes the instance, which leaves its group. An instance created '
    'with `auto.commit.enable` commits its positions first, waiting at '
    'most 5 s. Answers 204.',
)

CONSUMER_SUBSCRIBE = RouteText(
    'Subscribe to topics',
    'Subscribes the instance to `{"topics": [...]}`, topics of the '
    'namespace named without it; the group assigns their partitions at '
    'a later fetch. Topic patterns are not supported. Answers 204. '
    'Errors: 404 with 40401 for a topic the namespace does not hold, 409 '
    'with 40903 while partitions are assigned, 422 with 42204 for an '
    'empty list or a `topic_pattern`, 422 for more than 50 topics.',
)

CONSUMER_GET_SUBSCRIPTION = RouteText(
    'Get the subscription',
    'Returns `{"topics": [...]}`, the topics the instance subscribes to, '
    'named without the namespace; empty when it subscribes to none.',
)

CONSUMER_UNSUBSCRIBE = RouteText(
    'Unsubscribe from every topic',
    'Ends the subscription, so the instance can be assigned partitions. '
    'An instance created with `auto.commit.enable` commits its positions '
    'first. Answers 204.',
)

CONSUMER_FETCH = RouteText(
    'Fetch records',
    'Returns the records of the subscribed topics or assigned partitions '
    "after the instance's positions, each with `topic`, `key`, `value`, "
    '`partition`, `offset` and `headers`. The `Accept` header chooses the '
    "format, as in Confluent's REST Proxy: "
    '`application/vnd.kafka.binary.v2+json` (also for '
    '`application/vnd.kafka.v2+json`, `*/*` or none) or '
    '`application/vnd.kafka.json.v2+json`. A format other than the '
    "instance's is refused with 406 and 40601, and an `Accept` that names "
    'no v2 type with 406 and 406. A json instance whose next record is '
    'not UTF-8 is refused with 408 and 40801.\n\n'
    'The fetch waits up to `timeout` for records, at most 30000 ms, and '
    'returns `[]` if none came. A reply holds at most 2 MiB of JSON and '
    'stays under `max_bytes`, but always holds its first record; the '
    'records left out come first in the next fetch. A fetch to an '
    'instance another request holds is refused at once with 409 and '
    '40904. A user may have 2 fetches in progress; the service polls for '
    '3 at once, in turns of at most 500 ms, which fetches take in the '
    'order they ask. A third fetch of a user, or a fetch that gets no '
    'turn within its wait, is refused with 429, 42901 and `Retry-After: '
    '1`. A reply must be sent within 30 s.',
)

CONSUMER_COMMIT = RouteText(
    'Commit offsets',
    'With `{"offsets": [{"topic", "partition", "offset", "metadata"}]}`, '
    'naming for each partition the offset of the last record processed, '
    'the group commits the offset after it. Without a body, commits the '
    'position of every assigned partition, the record after the last one '
    'fetched. Returns `[]`. Errors: 404 with 40402 for a partition other '
    'than 0, 409 with 40903 for a partition not assigned to the instance '
    'or a commit the group refused after a rebalance, 422 for more than '
    '50 offsets.',
)

CONSUMER_GET_OFFSETS = RouteText(
    'Get committed offsets',
    'Takes `{"partitions": [{"topic", "partition"}]}` in the body of the '
    "GET, as Confluent's REST Proxy does, and returns `offsets`: for each "
    'partition the group has committed, the committed offset, the next '
    'record to read, and its `metadata`. A partition never committed is '
    'left out. Errors: 404 with 40402 without a body or for a partition '
    'other than 0, 422 for more than 50 partitions.',
)

CONSUMER_ASSIGN = RouteText(
    'Assign partitions',
    'Assigns `{"partitions": [{"topic", "partition"}]}` to the instance, '
    'in place of a subscription; the group does not assign them. Every '
    'topic has one partition, 0. Answers 204. Errors: 404 with 40401 for '
    'a topic the namespace does not hold, 404 with 40402 for a partition '
    'other than 0, 409 with 40903 while the instance subscribes, 422 for '
    'more than 50 partitions.',
)

CONSUMER_GET_ASSIGNMENTS = RouteText(
    'Get the assigned partitions',
    'Returns `{"partitions": [{"topic", "partition"}]}`, the partitions '
    'assigned to the instance by `assignments` or by its group, topics '
    'named without the namespace.',
)

CONSUMER_SEEK = RouteText(
    'Seek to offsets',
    'Sets the position of each partition in `{"offsets": [{"topic", '
    '"partition", "offset"}]}`, so the next fetch starts at that offset. '
    'Answers 204. Errors: 404 with 40402 for a partition other than 0, '
    '409 with 40903 for a partition not assigned to the instance, 422 '
    'for more than 50 offsets.',
)

CONSUMER_SEEK_BEGINNING = RouteText(
    'Seek to the beginning',
    'Moves each partition in `{"partitions": [...]}`, or every assigned '
    'partition for an empty list, to its first record. Answers 204. '
    'Errors: 404 with 40402 for a partition other than 0, 409 with 40903 '
    'for a partition not assigned or an empty list when none is, 422 for '
    'more than 50 partitions.',
)

CONSUMER_SEEK_END = RouteText(
    'Seek to the end',
    'Moves each partition in `{"partitions": [...]}`, or every assigned '
    'partition for an empty list, past its last record, so the next '
    'fetch returns only records produced after the seek. Answers 204. '
    'Errors: 404 with 40402 for a partition other than 0, 409 with 40903 '
    'for a partition not assigned or an empty list when none is, 422 for '
    'more than 50 partitions.',
)
