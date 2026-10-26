"""Live test of what a user's own access key may do on the cluster.

Creates a user, its key and a topic through the service, then connects to
the cluster as the user, with the key, the way a Kafka client outside the
service does. A new IAM key takes seconds to start working, so the first
connection is retried until it does.
"""

from __future__ import annotations

import contextlib
import os
import time
from collections.abc import Iterator

import pytest
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from botocore.credentials import CredentialProvider
from botocore.credentials import Credentials
from kafka import KafkaConsumer
from kafka.errors import GroupAuthorizationFailedError
from kafka.net.sasl.oauth import AbstractTokenProvider
from kafka.structs import OffsetAndMetadata
from kafka.structs import TopicPartition

from tests.live_identity import live_namespace
from tests.live_identity import permissions_boundary_arn
from web_service.services import DynamoDBService
from web_service.services import IAMService
from web_service.services import KafkaService
from web_service.services import NamespaceService
from web_service.services import WebService

pytestmark = pytest.mark.integration

REQUIRED_VARS = (
    'AWS_ACCOUNT_ID',
    'AWS_ACCOUNT_REGION',
    'MSK_CLUSTER_NAME',
    'DEFAULT_SERVERS',
)
# How long a new key may take to start working on the cluster
KEY_READY_TIMEOUT_S = 120
# How long a consumer may take to join its group, or to be refused
JOIN_TIMEOUT_S = 60


class _KeyCredentials(CredentialProvider):
    """Credentials of one access key, for the MSK token signer."""

    METHOD = 'user-access-key'

    def __init__(self, access_key: str, secret_key: str) -> None:
        super().__init__()
        self.access_key = access_key
        self.secret_key = secret_key

    def load(self) -> Credentials:
        return Credentials(
            self.access_key,
            self.secret_key,
            method=self.METHOD,
        )


class _KeyTokenProvider(AbstractTokenProvider):
    """MSK auth tokens signed with a user's access key."""

    def __init__(self, region: str, access_key: str, secret_key: str) -> None:
        self.region = region
        self.credentials = _KeyCredentials(access_key, secret_key)

    def token(self) -> str:
        token, _ = (
            MSKAuthTokenProvider.generate_auth_token_from_credentials_provider(
                self.region,
                self.credentials,
            )
        )
        return token


@pytest.fixture
def web_service() -> WebService:
    """A WebService on the test tables and the real cluster."""
    missing = [var for var in REQUIRED_VARS if not os.getenv(var)]
    if missing:
        pytest.skip(
            f'Missing required environment variables: {", ".join(missing)}',
        )
    region = os.getenv('AWS_ACCOUNT_REGION') or ''
    db_service = DynamoDBService(
        region=region,
        keys_table_name='test-diaspora-keys-table',
        users_table_name='test-diaspora-users-table',
        namespace_table_name='test-diaspora-namespace-table',
    )
    return WebService(
        iam_service=IAMService(
            account_id=os.getenv('AWS_ACCOUNT_ID') or '',
            region=region,
            cluster_name=os.getenv('MSK_CLUSTER_NAME') or '',
            permissions_boundary_arn=permissions_boundary_arn(),
        ),
        kafka_service=KafkaService(
            bootstrap_servers=os.getenv('DEFAULT_SERVERS'),
            region=region,
        ),
        namespace_service=NamespaceService(dynamodb_service=db_service),
    )


@pytest.fixture
def user_with_key(
    web_service: WebService,
    random_subject: str,
) -> Iterator[tuple[str, str, _KeyTokenProvider]]:
    """A user with a key and a topic; deleted with the user afterwards."""
    subject = random_subject
    try:
        key = web_service.create_key(subject)
        assert key['status'] == 'success', key['message']
        namespace = web_service.namespace_service.generate_default(subject)
        topic = f'test-topic-{os.urandom(4).hex()}'
        result = web_service.create_topic(subject, namespace, topic)
        assert result['status'] == 'success', result
        tokens = _KeyTokenProvider(
            os.getenv('AWS_ACCOUNT_REGION') or '',
            key['access_key'],
            key['secret_key'],
        )
        yield namespace, f'{namespace}.{topic}', tokens
    finally:
        with contextlib.suppress(Exception):
            web_service.delete_user(subject)


def _join_and_commit(
    group: str,
    kafka_topic: str,
    tokens: _KeyTokenProvider,
) -> None:
    """Join the group as the user, then commit offset 0 for the topic.

    Raises GroupAuthorizationFailedError if the cluster refuses the group.
    """
    consumer = KafkaConsumer(
        bootstrap_servers=os.getenv('DEFAULT_SERVERS'),
        security_protocol='SASL_SSL',
        sasl_mechanism='OAUTHBEARER',
        sasl_oauth_token_provider=tokens,
        group_id=group,
        enable_auto_commit=False,
        auto_offset_reset='earliest',
    )
    try:
        consumer.subscribe([kafka_topic])
        deadline = time.monotonic() + JOIN_TIMEOUT_S
        while not consumer.assignment():
            assert time.monotonic() < deadline, f'never joined {group}'
            consumer.poll(timeout_ms=1000)
        consumer.commit(
            {TopicPartition(kafka_topic, 0): OffsetAndMetadata(0, '', -1)},
        )
    finally:
        consumer.close(autocommit=False, timeout_ms=10_000)


def _join_once_the_key_works(
    group: str,
    kafka_topic: str,
    tokens: _KeyTokenProvider,
) -> None:
    """Retry _join_and_commit until the new key is accepted."""
    deadline = time.monotonic() + KEY_READY_TIMEOUT_S
    while True:
        try:
            _join_and_commit(group, kafka_topic, tokens)
            return
        except Exception as e:
            if time.monotonic() > deadline:
                raise
            print(f'\n  Key not usable yet: {type(e).__name__}: {e}')
            time.sleep(5)


def test_user_key_uses_only_groups_in_its_namespace(
    user_with_key: tuple[str, str, _KeyTokenProvider],
) -> None:
    """The key joins <namespace>.workers and is refused any other group."""
    namespace, kafka_topic, tokens = user_with_key

    _join_once_the_key_works(f'{namespace}.workers', kafka_topic, tokens)

    # Another test namespace is inside the live tests' boundary, so only
    # the user's own policy refuses its group
    for group in ('workers', f'{live_namespace()}.workers'):
        with pytest.raises(GroupAuthorizationFailedError):
            _join_and_commit(group, kafka_topic, tokens)
