"""Diaspora Web Service entry point."""

from __future__ import annotations

import asyncio
import contextlib
import functools
import importlib.metadata as importlib_metadata
import logging
import os
import time
from collections import deque
from collections.abc import AsyncIterator
from collections.abc import Callable
from contextlib import asynccontextmanager
from contextlib import AsyncExitStack
from typing import Any
from typing import TypeVar

import anyio
import anyio.lowlevel
import uvicorn
from anyio.abc import TaskGroup
from fastapi import APIRouter
from fastapi import Body
from fastapi import FastAPI
from fastapi import Header
from fastapi import Query
from fastapi import Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from fastapi.responses import Response
from starlette.requests import ClientDisconnect
from starlette.types import Receive
from starlette.types import Scope
from starlette.types import Send

from web_service.consumer_models import AssignmentRequest
from web_service.consumer_models import CreateConsumerRequest
from web_service.consumer_models import OffsetsCommitRequest
from web_service.consumer_models import OffsetsGetRequest
from web_service.consumer_models import SeekPartitionsRequest
from web_service.consumer_models import SeekRequest
from web_service.consumer_models import SubscriptionRequest
from web_service.consumer_records import FetchReply
from web_service.consumer_routes import ConsumerRoute
from web_service.consumer_routes import error_response
from web_service.consumer_routes import INVALID_REQUEST
from web_service.consumer_service import ConsumerService
from web_service.consumer_service import DEFAULT_FETCH_WAIT_MS
from web_service.consumer_service import MAX_FETCH_WAIT_MS
from web_service.consumer_service import REPLY_MAX_BYTES
from web_service.consumer_service import run_holding
from web_service.consumer_service import SERVER_ERROR
from web_service.consumer_service import TOO_MANY_REQUESTS
from web_service.producer_models import BatchResult
from web_service.producer_models import PRODUCE_BATCH_REQUEST_SCHEMA
from web_service.producer_models import PRODUCE_REQUEST_SCHEMA
from web_service.producer_models import ProduceError
from web_service.producer_models import ProduceResult
from web_service.producer_service import BAD_TOPIC_NAME
from web_service.producer_service import batch_reply
from web_service.producer_service import BODY_TIMEOUT
from web_service.producer_service import BODY_TOO_LARGE
from web_service.producer_service import encoded_size
from web_service.producer_service import MAX_BODY_BYTES
from web_service.producer_service import OutgoingRecord
from web_service.producer_service import prepare_batch
from web_service.producer_service import prepare_record
from web_service.producer_service import PRODUCE_MAX_REQUESTS
from web_service.producer_service import ProduceAdmission
from web_service.producer_service import ProducerService
from web_service.producer_service import READ_TIMEOUT_S
from web_service.producer_service import record_reply
from web_service.producer_service import RecordError
from web_service.producer_service import Reservation
from web_service.producer_service import SEND_TIMEOUT_S
from web_service.producer_service import UNSUPPORTED_MEDIA_TYPE
from web_service.producer_service import without_traceback
from web_service.utils import AuthManager
from web_service.utils import EnvironmentChecker

from .services import DynamoDBService
from .services import IAMService
from .services import KafkaService
from .services import NamespaceService
from .services import parse_max_topics_per_namespace
from .services import WebService

_Prepared = TypeVar('_Prepared')

logger = logging.getLogger(__name__)

# Fetches that hold a slot at once: from a poll that found records until
# the reply is sent, or for one slice of a poll that finds none. A slot
# whose poll returned an 8 MiB record from each of two partitions took
# 71 MiB in a test, so 3 slots hold about 220 MiB; see the consumer
# routes' memory in docs/architecture.md.
FETCH_MAX_CONCURRENT = 3
# Fetches one subject may have in progress at once, and the longest a
# fetch's reply may take to send before its connection is closed: both
# keep one user's fetches from holding every slot
FETCH_MAX_PER_SUBJECT = 2
FETCH_SEND_TIMEOUT_S = 30
# Seconds a fetch refused for want of a slot is told to wait
FETCH_RETRY_AFTER_S = 1
# Longest a fetch polls under one slot before it gives the slot back and
# takes its turn again, unless the poll found records
FETCH_SLICE_MS = 500
# Threads of the pool that every other blocking call shares: the token
# check, DynamoDB, the Kafka admin calls and the consumer calls other than
# fetches. Produce parsing and sends and fetches have limiters of their
# own, which bound their memory, so this bounds only how many calls may
# wait at once; 40 is anyio's default, set here so an upgrade cannot
# change it.
SHARED_THREADS = 40
# Code of a failed token check on the consumer routes: Confluent's code
# for a failed authentication
AUTHENTICATION_FAILED = 40101


def fetch_wait_ms(timeout: int | None, default_wait_ms: int | None) -> int:
    """How long a fetch waits for records.

    As in Confluent's REST Proxy, a timeout of 0 or less, or none, takes
    the instance's consumer.request.timeout.ms; unlike there, a positive
    timeout is not capped by it. Either is capped at MAX_FETCH_WAIT_MS.
    """
    if timeout is not None and timeout > 0:
        return min(timeout, MAX_FETCH_WAIT_MS)
    if default_wait_ms is not None:
        return min(default_wait_ms, MAX_FETCH_WAIT_MS)
    return DEFAULT_FETCH_WAIT_MS


class FetchSlots:
    """A fixed number of fetch slots, handed out in the order asked for.

    A slot given back goes straight to the fetch that has waited longest,
    so a fetch that gives its slot back and asks again at once waits
    behind the fetches already waiting. anyio's CapacityLimiter lets such
    a fetch take the slot first in some versions (4.13.0 among them),
    which lets more fetches hold a slot than there are slots. Used only
    on the event loop.
    """

    def __init__(self, total: int) -> None:
        """Make total slots, all free."""
        self.total_tokens = total
        self._held = 0
        self._waiters: deque[asyncio.Future[None]] = deque()

    @property
    def borrowed_tokens(self) -> int:
        """Slots held."""
        return self._held

    @property
    def available_tokens(self) -> int:
        """Slots free."""
        return self.total_tokens - self._held

    async def take(self, timeout: float) -> bool:
        """Take a slot, waiting at most timeout seconds behind earlier askers.

        Returns:
            Whether a slot was taken
        """
        if self._held < self.total_tokens and not self._waiters:
            self._held += 1
            return True
        waiter: asyncio.Future[None] = (
            asyncio.get_running_loop().create_future()
        )
        self._waiters.append(waiter)
        try:
            with anyio.move_on_after(timeout):
                await waiter
                return True
        except BaseException:
            self._abandon(waiter)
            raise
        self._abandon(waiter)
        return False

    def _abandon(self, waiter: asyncio.Future[None]) -> None:
        if waiter.done() and not waiter.cancelled():
            # Handed a slot just as the wait ended: pass it on
            self.give_back()
            return
        waiter.cancel()
        with contextlib.suppress(ValueError):
            self._waiters.remove(waiter)

    def give_back(self) -> None:
        """Give a slot back, to the longest waiter if there is one."""
        while self._waiters:
            waiter = self._waiters.popleft()
            if not waiter.done():
                waiter.set_result(None)
                return
        self._held -= 1


class FetchResponse(Response):
    """A fetch's reply, which holds the fetch until the reply is sent.

    The reply is encoded and sent inside __call__, under the fetch's
    release (the instance's lock, the subject's count and the slot), so
    the release runs however the send ends, a failure to encode included.
    A reply of up to REPLY_MAX_BYTES is written whole, with its
    Content-Length; a larger one, a single record, is written a piece at
    a time, so its JSON is never held whole.

    If the client leaves before the whole reply is handed to the server,
    the send fails, or it takes longer than FETCH_SEND_TIMEOUT_S, unfetch
    runs before the release, so the next fetch returns the reply's
    records again. A reply handed whole to the server counts as received,
    since the server cannot tell whether the client read it.
    """

    def __init__(
        self,
        reply: FetchReply,
        *,
        release: AsyncExitStack,
        unfetch: Callable[[], None],
        threads: anyio.CapacityLimiter,
    ) -> None:
        """Make the response; nothing is encoded until it is sent."""
        super().__init__(status_code=200, media_type='application/json')
        self._reply = reply
        self._release = release
        self._unfetch = unfetch
        self._threads = threads
        self._started = False
        self._complete = False

    async def __call__(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
    ) -> None:
        """Send the reply, then release the fetch."""
        async with self._release:
            delivered = failed = False
            try:
                with anyio.move_on_after(FETCH_SEND_TIMEOUT_S):
                    async with anyio.create_task_group() as tasks:
                        tasks.start_soon(self._watch, receive, tasks)
                        try:
                            await self._write(send)
                        except OSError:
                            # A server on ASGI 2.4 or later reports a closed
                            # connection this way: the client left
                            pass
                        except Exception:
                            logger.exception('Failed to send a fetch reply')
                            failed = True
                        else:
                            self._complete = delivered = True
                        tasks.cancel_scope.cancel()
            finally:
                if not delivered:
                    self._unfetch()
            if failed and not self._started:
                refusal = error_response(
                    500,
                    SERVER_ERROR,
                    'Internal server error',
                )
                await refusal(scope, receive, send)

    async def _watch(
        self,
        receive: Receive,
        tasks: TaskGroup,
    ) -> None:
        """Stop the send if the client leaves before it is complete."""
        while (await receive())['type'] != 'http.disconnect':
            pass
        if not self._complete:
            tasks.cancel_scope.cancel()

    async def _write(self, send: Send) -> None:
        reply = self._reply
        start: dict[str, Any] = {
            'type': 'http.response.start',
            'status': 200,
            'headers': [(b'content-type', b'application/json')],
        }
        if reply.size <= REPLY_MAX_BYTES:
            body = await anyio.to_thread.run_sync(
                reply.body,
                limiter=self._threads,
            )
            start['headers'].append(
                (b'content-length', str(len(body)).encode()),
            )
            # Let _watch see a disconnect the server has already noticed:
            # once disconnected, uvicorn drops what is sent without an
            # error, and the reply would count as received
            await anyio.lowlevel.checkpoint()
            self._started = True
            await send(start)
            await send({'type': 'http.response.body', 'body': body})
            return
        chunks = reply.chunks()
        self._started = True
        await send(start)
        while True:
            chunk = await anyio.to_thread.run_sync(
                next,
                chunks,
                None,
                limiter=self._threads,
            )
            if chunk is None:
                break
            await send(
                {
                    'type': 'http.response.body',
                    'body': chunk,
                    'more_body': True,
                },
            )
        await anyio.lowlevel.checkpoint()
        await send({'type': 'http.response.body', 'body': b''})


class DiasporaService:
    """Service for managing Diaspora web service."""

    app: FastAPI

    def __init__(self) -> None:
        """Initialize the service by checking env vars and setting up deps."""
        EnvironmentChecker.check_env_variables(
            'AWS_ACCESS_KEY_ID',
            'AWS_SECRET_ACCESS_KEY',
            'SERVER_CLIENT_ID',
            'SERVER_SECRET',
            'AWS_ACCOUNT_ID',
            'AWS_ACCOUNT_REGION',
            'MSK_CLUSTER_NAME',
        )

        self.auth = AuthManager(
            os.getenv('SERVER_CLIENT_ID'),
            os.getenv('SERVER_SECRET'),
            'c5d4fab4-7f0d-422e-b0c8-5c74329b52fe',
        )

        # Initialize services
        account_id = os.getenv('AWS_ACCOUNT_ID') or ''
        region = os.getenv('AWS_ACCOUNT_REGION') or ''
        cluster_name = os.getenv('MSK_CLUSTER_NAME') or ''

        iam_service = IAMService(
            account_id=account_id,
            region=region,
            cluster_name=cluster_name,
            permissions_boundary_arn=os.getenv(
                'IAM_USER_PERMISSIONS_BOUNDARY_ARN',
            ),
        )

        kafka_service = KafkaService(
            bootstrap_servers=os.getenv('DEFAULT_SERVERS'),
            region=region,
        )

        db_service = DynamoDBService(
            region=region,
            keys_table_name=os.getenv(
                'KEYS_TABLE_NAME',
                'diaspora-keys-table',
            ),
            users_table_name=os.getenv(
                'USERS_TABLE_NAME',
                'diaspora-users-table',
            ),
            namespace_table_name=os.getenv(
                'NAMESPACE_TABLE_NAME',
                'diaspora-namespace-table',
            ),
        )

        # The most topics a namespace holds, and so the most topics or
        # partitions a consumer request may list
        self.max_topics = parse_max_topics_per_namespace(
            os.getenv('MAX_TOPICS_PER_NAMESPACE'),
        )
        namespace_service = NamespaceService(
            dynamodb_service=db_service,
            max_topics_per_namespace=self.max_topics,
        )

        self.web_service = WebService(
            iam_service=iam_service,
            kafka_service=kafka_service,
            namespace_service=namespace_service,
        )

        self.consumer_service = ConsumerService(
            bootstrap_servers=os.getenv('DEFAULT_SERVERS'),
            region=region,
        )

        self.producer_service = ProducerService(
            bootstrap_servers=os.getenv('DEFAULT_SERVERS'),
            region=region,
        )
        # Produce requests in flight, by count and body size; parsing, one
        # body at a time; and the threads that wait for the cluster, apart
        # from the threadpool every other route shares
        self.produce_admission = ProduceAdmission()
        self.parse_limiter = anyio.CapacityLimiter(1)
        self.send_limiter = anyio.CapacityLimiter(PRODUCE_MAX_REQUESTS)
        # Fetches in progress, on threads apart from the shared pool
        # Slots a fetch takes before it polls, and the threads it polls and
        # writes its reply on; a fetch holds a slot whenever it holds one
        # of these threads, so the threads are never short
        self.fetch_slots = FetchSlots(FETCH_MAX_CONCURRENT)
        self.fetch_threads = anyio.CapacityLimiter(FETCH_MAX_CONCURRENT)
        # Fetches in progress by subject; changed only on the event loop
        self.fetching: dict[str, int] = {}

        self.app = FastAPI(
            title='Diaspora Web Service V3',
            docs_url='/',
            version=importlib_metadata.version('diaspora_service'),
            lifespan=self._lifespan,
        )
        self.add_routes()
        self.add_consumer_routes()
        self.add_producer_routes()

    def add_routes(self) -> None:
        """Add routes to the FastAPI app."""
        # User management routes
        self.app.post('/api/v3/user', tags=['User'])(
            self.create_user,
        )
        self.app.delete('/api/v3/user', tags=['User'])(
            self.delete_user,
        )
        # Key management routes
        self.app.post('/api/v3/key', tags=['Authentication'])(
            self.create_key,
        )
        self.app.delete('/api/v3/key', tags=['Authentication'])(
            self.delete_key,
        )
        # List namespaces and topics
        self.app.get('/api/v3/namespace', tags=['Namespace'])(
            self.list_namespace_and_topics,
        )
        # Topic management routes
        self.app.post('/api/v3/{namespace}/{topic}', tags=['Topic'])(
            self.create_topic,
        )
        self.app.delete('/api/v3/{namespace}/{topic}', tags=['Topic'])(
            self.delete_topic,
        )
        self.app.put('/api/v3/{namespace}/{topic}/recreate', tags=['Topic'])(
            self.recreate_topic,
        )

    async def create_user(
        self,
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> dict[str, Any]:
        """Create an IAM user for the authenticated subject."""
        return await run_in_threadpool(
            self._authorized,
            subject,
            token,
            self.web_service.create_user,
        )

    async def delete_user(
        self,
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> dict[str, Any]:
        """Delete an IAM user for the authenticated subject."""
        return await run_in_threadpool(
            self._authorized,
            subject,
            token,
            self.web_service.delete_user,
        )

    async def create_key(
        self,
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> dict[str, Any]:
        """Create an access key for an existing IAM user."""
        return await run_in_threadpool(
            self._authorized,
            subject,
            token,
            self.web_service.create_key,
        )

    async def delete_key(
        self,
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> dict[str, Any]:
        """Delete access keys for a user."""
        return await run_in_threadpool(
            self._authorized,
            subject,
            token,
            self.web_service.delete_key,
        )

    async def create_topic(
        self,
        namespace: str,
        topic: str,
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> dict[str, Any]:
        """Create a topic under a namespace.

        A namespace holds at most `MAX_TOPICS_PER_NAMESPACE` topics (50
        unless the service sets it). Creating a new topic in a namespace
        that already holds that many fails with a message that says to
        delete a topic first; existing topics are kept. A create that
        leaves the namespace with at least 90% of the cap (45 of 50)
        succeeds and appends a warning to `message`, also returned in a
        `warning` field, which is absent otherwise. Repeating the create
        of a topic the namespace already holds is neither counted nor
        refused by the cap.

        The Kafka admin call blocks, so it runs in the threadpool: on the
        event loop it would stall every other request and the worker's
        heartbeat to gunicorn.
        """
        if err := await run_in_threadpool(
            self.auth.validate_access_token,
            subject,
            token,
        ):
            return err
        return await run_in_threadpool(
            self.web_service.create_topic,
            subject,
            namespace,
            topic,
        )

    async def delete_topic(
        self,
        namespace: str,
        topic: str,
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> dict[str, Any]:
        """Delete a topic from a namespace.

        The Kafka admin call blocks, so it runs in the threadpool: on the
        event loop it would stall every other request and the worker's
        heartbeat to gunicorn.
        """
        if err := await run_in_threadpool(
            self.auth.validate_access_token,
            subject,
            token,
        ):
            return err
        return await run_in_threadpool(
            self.web_service.delete_topic,
            subject,
            namespace,
            topic,
        )

    async def recreate_topic(
        self,
        namespace: str,
        topic: str,
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> dict[str, Any]:
        """Recreate a topic by deleting and recreating it.

        Authenticates the client with namespace and topic access, then:
        1. Verifies the subject owns the namespace
        2. Deletes the Kafka topic and DynamoDB entry
        3. Waits 5 seconds
        4. Recreates the Kafka topic and DynamoDB entry
        """
        if err := await run_in_threadpool(
            self.auth.validate_access_token,
            subject,
            token,
        ):
            return err
        return await run_in_threadpool(
            self.web_service.recreate_topic,
            subject,
            namespace,
            topic,
        )

    async def list_namespace_and_topics(
        self,
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> dict[str, Any]:
        """List all namespaces owned by a user and their topics."""
        return await run_in_threadpool(
            self._authorized,
            subject,
            token,
            self.web_service.namespace_service.list_namespace_and_topics,
        )

    def _authorized(
        self,
        subject: str,
        token: str,
        action: Callable[[str], dict[str, Any]],
    ) -> dict[str, Any]:
        """Run action(subject) if the token belongs to the subject.

        Token introspection and the action both block, so route handlers
        call this in the threadpool: on the event loop they would stall
        every other request and the worker's heartbeat to gunicorn.
        """
        if err := self.auth.validate_access_token(subject, token):
            return err
        return action(subject)

    # --- Lifespan ---

    @asynccontextmanager
    async def _lifespan(
        self,
        app: FastAPI,
    ) -> AsyncIterator[None]:
        """Application lifespan: size the threadpool; close consumers."""
        anyio.to_thread.current_default_thread_limiter().total_tokens = (
            SHARED_THREADS
        )
        yield
        await run_in_threadpool(self.consumer_service.shutdown_all)
        await run_in_threadpool(self.producer_service.close)

    # --- Consumer helpers ---

    def _validate_namespace(
        self,
        subject: str,
        namespace: str,
    ) -> dict[str, Any] | None:
        """Validate user owns the namespace.

        Returns:
            Error dict if validation fails, None if OK
        """
        user_ns = (
            self.web_service.namespace_service.dynamodb.get_user_namespaces(
                subject,
            )
        )
        if namespace not in user_ns:
            return {
                'error_code': 40401,
                'message': (f'Namespace {namespace} not found for user'),
            }
        return None

    def _validate_topics_in_namespace(
        self,
        namespace: str,
        topics: list[str],
    ) -> dict[str, Any] | None:
        """Validate topics exist in the namespace.

        Returns:
            Error dict if any topic not found, None if OK
        """
        existing = (
            self.web_service.namespace_service.dynamodb.get_namespace_topics(
                namespace,
            )
        )
        invalid = [t for t in topics if t not in existing]
        if invalid:
            return {
                'error_code': 40401,
                'message': (
                    f'Topics not found in namespace '
                    f'{namespace}: {", ".join(invalid)}'
                ),
            }
        return None

    def _check_access(
        self,
        subject: str,
        token: str,
        namespace: str,
    ) -> Response | None:
        """Check the token and the namespace for a consumer request.

        Returns:
            The 401 or 404 response if a check fails, None if both pass
        """
        if err := self.auth.validate_access_token(subject, token):
            return JSONResponse(content=err, status_code=401)
        if err := self._validate_namespace(subject, namespace):
            return JSONResponse(content=err, status_code=404)
        return None

    def _check_consumer_access(
        self,
        subject: str,
        token: str,
        namespace: str,
    ) -> Response | None:
        """Check the token and the namespace, in Confluent's error shape.

        Returns:
            The 401 or 404 response if a check fails, None if both pass
        """
        if err := self.auth.validate_access_token(subject, token):
            reason = err.get('reason')
            # The audience check's reason names the service's client id
            if not isinstance(reason, str) or reason.startswith(
                'Not in the audience set',
            ):
                reason = 'Token is not for this service.'
            return error_response(
                401,
                AUTHENTICATION_FAILED,
                f'Authentication failed: {reason}',
            )
        if err := self._validate_namespace(subject, namespace):
            return JSONResponse(content=err, status_code=404)
        return None

    def _serve_consumer(
        self,
        subject: str,
        token: str,
        namespace: str,
        serve: Callable[[], Response],
    ) -> Response:
        """Check access to a consumer request, then serve it.

        Token introspection, the namespace lookup and the consumer calls
        all block, so async route handlers call this in the threadpool: on
        the event loop they would stall every other request and the
        worker's heartbeat to gunicorn.
        """
        if err := self._check_consumer_access(subject, token, namespace):
            return err
        return serve()

    async def _serve_instance(
        self,
        subject: str,
        token: str,
        key: tuple[str, str, str, str],
        serve: Callable[[], Response],
    ) -> Response:
        """Check access to a request on an instance, then serve it.

        The access check and serve run in the shared threadpool. Between
        them the request takes the instance's lock on the event loop,
        waiting up to 10 s for another request on the instance without
        holding a thread, and serve runs as the lock's holder.
        """
        namespace = key[1]
        if err := await run_in_threadpool(
            self._check_consumer_access,
            subject,
            token,
            namespace,
        ):
            return err
        async with self.consumer_service.hold(key) as held:
            if isinstance(held, tuple):
                return self._consumer_response(*held)
            return await run_in_threadpool(run_holding, held, serve)

    def _too_many(self, items: list[Any], what: str) -> Response | None:
        """The 422 reply if a request lists more items than a namespace holds.

        A namespace holds at most max_topics topics, each of one partition,
        so a longer list of topics, partitions or offsets is a mistake.
        """
        if len(items) <= self.max_topics:
            return None
        return error_response(
            422,
            INVALID_REQUEST,
            f'{len(items)} {what} listed; a request may list at most '
            f'{self.max_topics}, the most topics a namespace holds',
        )

    _NO_CONTENT = 204

    @staticmethod
    def _consumer_response(
        body: Any,
        status_code: int,
    ) -> Response:
        """Build an HTTP response for consumer endpoints."""
        if status_code == DiasporaService._NO_CONTENT:
            return Response(status_code=status_code)
        return JSONResponse(content=body, status_code=status_code)

    # --- Consumer routes ---

    def add_consumer_routes(self) -> None:
        """Add consumer REST API routes (Confluent v2 compatible).

        Their route class answers every failure in Confluent's shape.
        """
        base = '/api/v3/{namespace}/consumers'
        router = APIRouter(route_class=ConsumerRoute)

        # 1. Create consumer instance
        router.post(
            f'{base}/{{group_name}}',
            tags=['Consumer'],
        )(self.create_consumer_instance)

        # 2. Delete consumer instance
        router.delete(
            f'{base}/{{group_name}}/instances/{{instance_id}}',
            tags=['Consumer'],
        )(self.delete_consumer_instance)

        # 3. Subscribe
        router.post(
            f'{base}/{{group_name}}/instances/{{instance_id}}/subscription',
            tags=['Consumer'],
        )(self.subscribe_consumer)

        # 4. Get subscription
        router.get(
            f'{base}/{{group_name}}/instances/{{instance_id}}/subscription',
            tags=['Consumer'],
        )(self.get_consumer_subscription)

        # 5. Unsubscribe
        router.delete(
            f'{base}/{{group_name}}/instances/{{instance_id}}/subscription',
            tags=['Consumer'],
        )(self.unsubscribe_consumer)

        # 6. Fetch records
        router.get(
            f'{base}/{{group_name}}/instances/{{instance_id}}/records',
            tags=['Consumer'],
        )(self.fetch_consumer_records)

        # 7. Commit offsets
        router.post(
            f'{base}/{{group_name}}/instances/{{instance_id}}/offsets',
            tags=['Consumer'],
        )(self.commit_consumer_offsets)

        # 8. Get committed offsets
        router.get(
            f'{base}/{{group_name}}/instances/{{instance_id}}/offsets',
            tags=['Consumer'],
        )(self.get_committed_offsets)

        # 9. Assign partitions
        router.post(
            f'{base}/{{group_name}}/instances/{{instance_id}}/assignments',
            tags=['Consumer'],
        )(self.assign_consumer_partitions)

        # 10. Get assignments
        router.get(
            f'{base}/{{group_name}}/instances/{{instance_id}}/assignments',
            tags=['Consumer'],
        )(self.get_consumer_assignments)

        # 11. Seek to offset
        router.post(
            f'{base}/{{group_name}}/instances/{{instance_id}}/positions',
            tags=['Consumer'],
        )(self.seek_consumer_positions)

        # 12. Seek to beginning
        router.post(
            f'{base}/{{group_name}}/instances'
            f'/{{instance_id}}/positions/beginning',
            tags=['Consumer'],
        )(self.seek_consumer_beginning)

        # 13. Seek to end
        router.post(
            f'{base}/{{group_name}}/instances/{{instance_id}}/positions/end',
            tags=['Consumer'],
        )(self.seek_consumer_end)

        self.app.include_router(router)

    # --- Consumer endpoint handlers ---

    async def create_consumer_instance(
        self,
        namespace: str,
        group_name: str,
        body: CreateConsumerRequest = Body(...),
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> Response:
        """Create a consumer instance in a consumer group."""

        def serve() -> Response:
            resp_body, status = self.consumer_service.create_consumer(
                subject=subject,
                namespace=namespace,
                group_name=group_name,
                name=body.name,
                format_type=body.format,
                auto_offset_reset=body.auto_offset_reset,
                auto_commit_enable=body.auto_commit_enable,
                fetch_min_bytes=body.fetch_min_bytes,
                consumer_request_timeout_ms=(body.consumer_request_timeout_ms),
            )
            return self._consumer_response(resp_body, status)

        return await run_in_threadpool(
            self._serve_consumer,
            subject,
            token,
            namespace,
            serve,
        )

    async def delete_consumer_instance(
        self,
        namespace: str,
        group_name: str,
        instance_id: str,
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> Response:
        """Destroy a consumer instance."""

        def serve() -> Response:
            resp_body, status = self.consumer_service.delete_consumer(
                subject=subject,
                namespace=namespace,
                group_name=group_name,
                instance_id=instance_id,
            )
            return self._consumer_response(resp_body, status)

        return await self._serve_instance(
            subject,
            token,
            (subject, namespace, group_name, instance_id),
            serve,
        )

    async def subscribe_consumer(
        self,
        namespace: str,
        group_name: str,
        instance_id: str,
        *,
        body: SubscriptionRequest = Body(...),
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> Response:
        """Subscribe consumer to a list of topics."""

        def serve() -> Response:
            if body.topic_pattern is not None:
                return JSONResponse(
                    content={
                        'error_code': 42204,
                        'message': (
                            'topic_pattern is not supported; '
                            'subscribe with a list of topics'
                        ),
                    },
                    status_code=422,
                )
            if not body.topics:
                return JSONResponse(
                    content={
                        'error_code': 42204,
                        'message': 'Must provide a non-empty list of topics',
                    },
                    status_code=422,
                )
            if too_many := self._too_many(body.topics, 'topics'):
                return too_many
            if err := self._validate_topics_in_namespace(
                namespace,
                body.topics,
            ):
                return JSONResponse(content=err, status_code=404)
            resp_body, status = self.consumer_service.subscribe(
                subject=subject,
                namespace=namespace,
                group_name=group_name,
                instance_id=instance_id,
                topics=body.topics,
            )
            return self._consumer_response(resp_body, status)

        return await self._serve_instance(
            subject,
            token,
            (subject, namespace, group_name, instance_id),
            serve,
        )

    async def get_consumer_subscription(
        self,
        namespace: str,
        group_name: str,
        instance_id: str,
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> Response:
        """Get the current subscribed list of topics."""

        def serve() -> Response:
            resp_body, status = self.consumer_service.get_subscription(
                subject=subject,
                namespace=namespace,
                group_name=group_name,
                instance_id=instance_id,
            )
            return self._consumer_response(resp_body, status)

        return await self._serve_instance(
            subject,
            token,
            (subject, namespace, group_name, instance_id),
            serve,
        )

    async def unsubscribe_consumer(
        self,
        namespace: str,
        group_name: str,
        instance_id: str,
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> Response:
        """Unsubscribe from topics currently subscribed."""

        def serve() -> Response:
            resp_body, status = self.consumer_service.unsubscribe(
                subject=subject,
                namespace=namespace,
                group_name=group_name,
                instance_id=instance_id,
            )
            return self._consumer_response(resp_body, status)

        return await self._serve_instance(
            subject,
            token,
            (subject, namespace, group_name, instance_id),
            serve,
        )

    async def fetch_consumer_records(
        self,
        namespace: str,
        group_name: str,
        instance_id: str,
        *,
        request: Request,
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
        timeout: int | None = Query(
            None,
            description=(
                'Milliseconds to wait for records. 0, a negative value or '
                "none takes the instance's consumer.request.timeout.ms, "
                'else 5000. Values above 30000 are treated as 30000.'
            ),
        ),
        max_bytes: int | None = Query(
            None,
            description=(
                'Most records to return; 0, a negative value or none takes '
                'the default of 500.'
            ),
        ),
    ) -> Response:
        """Fetch data for subscribed topics or assigned partitions.

        A fetch to an instance another request holds is refused at once,
        and a subject may have FETCH_MAX_PER_SUBJECT fetches in progress.
        The fetch polls in slices of at most FETCH_SLICE_MS, each under
        one of FETCH_MAX_CONCURRENT slots. Fetches take the slots in the
        order they ask, and a slice that finds nothing gives its slot
        back, so quiet long polls cannot keep the slots from fetches with
        records waiting. The slices and the waits for a slot all fall
        within the fetch's wait for records. A fetch that got no slot in
        that time is refused with 429 and polls nothing; one that polled
        and found nothing returns []. A slice that finds records keeps
        its slot, the instance and the subject's count until the reply is
        sent.
        """
        started = time.monotonic()
        if err := await run_in_threadpool(
            self._check_consumer_access,
            subject,
            token,
            namespace,
        ):
            return err
        key = (subject, namespace, group_name, instance_id)
        async with AsyncExitStack() as stack:
            held = await stack.enter_async_context(
                self.consumer_service.hold(key, wait=False),
            )
            if isinstance(held, tuple):
                return self._consumer_response(*held)
            if self.fetching.get(subject, 0) >= FETCH_MAX_PER_SUBJECT:
                return self._fetch_refused(
                    f'You have {FETCH_MAX_PER_SUBJECT} fetches in progress, '
                    'the most one user may; retry when one finishes',
                )
            self.fetching[subject] = self.fetching.get(subject, 0) + 1
            stack.callback(self._fetch_done, subject)
            wait_ms = fetch_wait_ms(timeout, held.default_wait_ms)
            deadline = started + wait_ms / 1000
            fetch = functools.partial(
                self.consumer_service.fetch_records,
                subject,
                namespace,
                group_name,
                instance_id,
                max_records=max_bytes if max_bytes and max_bytes > 0 else None,
            )
            reply = await self._poll_for_records(
                stack,
                held,
                fetch,
                deadline,
                request,
            )
            if not isinstance(reply, FetchReply):
                return reply
            unfetch = functools.partial(
                self.consumer_service.unfetch,
                held,
                reply,
            )
            # The client may have left while the slice polled; nothing has
            # committed the records since, so they can be fetched again
            if await request.is_disconnected():
                unfetch()
                return Response(status_code=400)
            release = stack.pop_all()
            try:
                return FetchResponse(
                    reply,
                    release=release,
                    unfetch=unfetch,
                    threads=self.fetch_threads,
                )
            except BaseException:
                unfetch()
                await release.aclose()
                raise

    async def _poll_for_records(
        self,
        stack: AsyncExitStack,
        held: Any,
        fetch: Callable[..., tuple[Any, int]],
        deadline: float,
        request: Request,
    ) -> FetchReply | Response:
        """Poll in slices, each under a slot, until records come or time ends.

        Returns:
            The reply of the slice that found records, whose slot stack
            now releases; or the response to give instead: [] once the
            fetch polled and its wait ended, 429 if it never got a slot,
            or the error a slice met
        """
        polled = False
        while True:
            if not await self._take_fetch_slot(deadline):
                if polled:
                    return self._consumer_response([], 200)
                return self._fetch_refused(
                    'The service is serving the most fetches it serves at '
                    'once; retry',
                )
            polled = True
            found = await self._poll_slice(
                held,
                fetch,
                deadline,
                request,
            )
            if not isinstance(found, FetchReply):
                return found
            if found.records:
                stack.callback(self.fetch_slots.give_back)
                return found
            if time.monotonic() >= deadline:
                return self._consumer_response([], 200)

    async def _poll_slice(
        self,
        held: Any,
        fetch: Callable[..., tuple[Any, int]],
        deadline: float,
        request: Request,
    ) -> FetchReply | Response:
        """Poll once, for at most FETCH_SLICE_MS, under a slot it holds.

        The slot is given back unless the poll found records.
        """
        keep = False
        try:
            # A client that left would lose the records polled
            if await request.is_disconnected():
                return Response(status_code=400)
            remaining_ms = max(0, int((deadline - time.monotonic()) * 1000))
            reply, status = await anyio.to_thread.run_sync(
                run_holding,
                held,
                functools.partial(
                    fetch,
                    timeout_ms=min(remaining_ms, FETCH_SLICE_MS),
                ),
                limiter=self.fetch_threads,
            )
            if not isinstance(reply, FetchReply):
                return self._consumer_response(reply, status)
            keep = bool(reply.records)
            return reply
        finally:
            if not keep:
                self.fetch_slots.give_back()

    def _fetch_done(self, subject: str) -> None:
        left = self.fetching.get(subject, 0) - 1
        if left > 0:
            self.fetching[subject] = left
        else:
            self.fetching.pop(subject, None)

    async def _take_fetch_slot(self, deadline: float) -> bool:
        """Take a fetch slot by the deadline (time.monotonic()).

        Returns:
            Whether a slot came free in time; the caller gives it back
        """
        return await self.fetch_slots.take(
            max(0.0, deadline - time.monotonic()),
        )

    @staticmethod
    def _fetch_refused(message: str) -> Response:
        response = error_response(429, TOO_MANY_REQUESTS, message)
        response.headers['Retry-After'] = str(FETCH_RETRY_AFTER_S)
        return response

    async def commit_consumer_offsets(
        self,
        namespace: str,
        group_name: str,
        instance_id: str,
        *,
        body: OffsetsCommitRequest | None = Body(None),
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> Response:
        """Commit offsets for the consumer."""

        def serve() -> Response:
            offsets = None
            if body and body.offsets:
                if too_many := self._too_many(body.offsets, 'offsets'):
                    return too_many
                offsets = [o.model_dump() for o in body.offsets]
            resp_body, status = self.consumer_service.commit_offsets(
                subject=subject,
                namespace=namespace,
                group_name=group_name,
                instance_id=instance_id,
                offsets=offsets,
            )
            return self._consumer_response(resp_body, status)

        return await self._serve_instance(
            subject,
            token,
            (subject, namespace, group_name, instance_id),
            serve,
        )

    async def get_committed_offsets(
        self,
        namespace: str,
        group_name: str,
        instance_id: str,
        *,
        request: Request,
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> Response:
        """Get last committed offsets for given partitions."""
        if err := await run_in_threadpool(
            self._check_consumer_access,
            subject,
            token,
            namespace,
        ):
            return err
        # Confluent sends partitions in the GET request body; a body that
        # is not JSON or fails validation raises, and the route class
        # answers it
        req = OffsetsGetRequest.model_validate_json(await request.body())
        if too_many := self._too_many(req.partitions, 'partitions'):
            return too_many
        partitions = [p.model_dump() for p in req.partitions]
        lookup = functools.partial(
            self.consumer_service.get_committed,
            subject=subject,
            namespace=namespace,
            group_name=group_name,
            instance_id=instance_id,
            partitions=partitions,
        )
        key = (subject, namespace, group_name, instance_id)
        async with self.consumer_service.hold(key) as held:
            if isinstance(held, tuple):
                return self._consumer_response(*held)
            resp_body, status = await run_in_threadpool(
                run_holding,
                held,
                lookup,
            )
        return self._consumer_response(resp_body, status)

    async def assign_consumer_partitions(
        self,
        namespace: str,
        group_name: str,
        instance_id: str,
        *,
        body: AssignmentRequest = Body(...),
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> Response:
        """Manually assign partitions to this consumer."""

        def serve() -> Response:
            if too_many := self._too_many(body.partitions, 'partitions'):
                return too_many
            # Validate topics exist in namespace
            topics = list({p.topic for p in body.partitions})
            if err := self._validate_topics_in_namespace(
                namespace,
                topics,
            ):
                return JSONResponse(content=err, status_code=404)
            partitions = [p.model_dump() for p in body.partitions]
            resp_body, status = self.consumer_service.assign_partitions(
                subject=subject,
                namespace=namespace,
                group_name=group_name,
                instance_id=instance_id,
                partitions=partitions,
            )
            return self._consumer_response(resp_body, status)

        return await self._serve_instance(
            subject,
            token,
            (subject, namespace, group_name, instance_id),
            serve,
        )

    async def get_consumer_assignments(
        self,
        namespace: str,
        group_name: str,
        instance_id: str,
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> Response:
        """Get the list of partitions assigned to this consumer."""

        def serve() -> Response:
            resp_body, status = self.consumer_service.get_assignments(
                subject=subject,
                namespace=namespace,
                group_name=group_name,
                instance_id=instance_id,
            )
            return self._consumer_response(resp_body, status)

        return await self._serve_instance(
            subject,
            token,
            (subject, namespace, group_name, instance_id),
            serve,
        )

    async def seek_consumer_positions(
        self,
        namespace: str,
        group_name: str,
        instance_id: str,
        *,
        body: SeekRequest = Body(...),
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> Response:
        """Override fetch offsets for the next set of records."""

        def serve() -> Response:
            if too_many := self._too_many(body.offsets, 'offsets'):
                return too_many
            offsets = [o.model_dump() for o in body.offsets]
            resp_body, status = self.consumer_service.seek(
                subject=subject,
                namespace=namespace,
                group_name=group_name,
                instance_id=instance_id,
                offsets=offsets,
            )
            return self._consumer_response(resp_body, status)

        return await self._serve_instance(
            subject,
            token,
            (subject, namespace, group_name, instance_id),
            serve,
        )

    async def seek_consumer_beginning(
        self,
        namespace: str,
        group_name: str,
        instance_id: str,
        *,
        body: SeekPartitionsRequest = Body(...),
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> Response:
        """Seek to the first offset for given partitions."""

        def serve() -> Response:
            if too_many := self._too_many(body.partitions, 'partitions'):
                return too_many
            partitions = [p.model_dump() for p in body.partitions]
            resp_body, status = self.consumer_service.seek_to_beginning(
                subject=subject,
                namespace=namespace,
                group_name=group_name,
                instance_id=instance_id,
                partitions=partitions,
            )
            return self._consumer_response(resp_body, status)

        return await self._serve_instance(
            subject,
            token,
            (subject, namespace, group_name, instance_id),
            serve,
        )

    async def seek_consumer_end(
        self,
        namespace: str,
        group_name: str,
        instance_id: str,
        *,
        body: SeekPartitionsRequest = Body(...),
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> Response:
        """Seek to the last offset for given partitions."""

        def serve() -> Response:
            if too_many := self._too_many(body.partitions, 'partitions'):
                return too_many
            partitions = [p.model_dump() for p in body.partitions]
            resp_body, status = self.consumer_service.seek_to_end(
                subject=subject,
                namespace=namespace,
                group_name=group_name,
                instance_id=instance_id,
                partitions=partitions,
            )
            return self._consumer_response(resp_body, status)

        return await self._serve_instance(
            subject,
            token,
            (subject, namespace, group_name, instance_id),
            serve,
        )

    # --- Producer routes ---

    def add_producer_routes(self) -> None:
        """Add produce REST API routes (Confluent v3 syntax)."""
        base = '/api/v3/{namespace}/topics/{topic}'

        def error(description: str) -> dict[str, Any]:
            return {'model': ProduceError, 'description': description}

        refusals: dict[int | str, dict[str, Any]] = {
            400: error('Body not JSON, or invalid field or topic name'),
            401: {'description': 'The token check failed'},
            404: error('Namespace, topic or partition not found'),
            408: error('The body did not arrive within 20 s'),
            413: error('Body over 16 MiB, or too costly to parse'),
            415: error('Content-Type is not application/json'),
            422: error('Payload error: needs a schema registry, or empty'),
            429: error('Too many produce requests or bytes in flight'),
        }

        def with_schema(schema: dict[str, Any]) -> dict[str, Any]:
            return {
                'requestBody': {
                    'required': True,
                    'content': {'application/json': {'schema': schema}},
                },
            }

        self.app.post(
            f'{base}/records',
            tags=['Producer'],
            responses={
                200: {
                    'model': ProduceResult,
                    'description': 'The cluster wrote the record',
                },
                **refusals,
                403: error('The service may not write to the topic'),
                413: error(
                    'Body over 16 MiB, record over 8 MiB, or body too '
                    'costly to parse',
                ),
                500: error('Kafka failed to write the record'),
                503: error('The Kafka cluster is not reachable'),
            },
            openapi_extra=with_schema(PRODUCE_REQUEST_SCHEMA),
        )(self.produce_record)
        self.app.post(
            f'{base}/records:batch',
            tags=['Producer'],
            status_code=207,
            responses={
                207: {
                    'model': BatchResult,
                    'description': 'The outcome of each entry',
                },
                **refusals,
                413: error(
                    'Body over 16 MiB, too costly to parse, or records '
                    'over 16 MiB once encoded',
                ),
                422: error(
                    'The batch is empty, has over 500 entries, or an '
                    'entry id is missing, invalid or repeated',
                ),
            },
            openapi_extra=with_schema(PRODUCE_BATCH_REQUEST_SCHEMA),
        )(self.produce_records_batch)

    async def produce_record(
        self,
        namespace: str,
        topic: str,
        *,
        request: Request,
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> Response:
        """Produce one record to a topic of the namespace.

        The body (`Content-Type: application/json`) is one record in the
        syntax of Confluent's REST API v3 (`partition_id`, `headers`,
        `key`, `value`, `timestamp`); a body holding more than one JSON
        value is refused, so send several records to `records:batch`. A
        key or value is `{"type": "BINARY" | "JSON" | "STRING", "data":
        ...}`: BINARY data is base64, JSON data any JSON value, STRING
        data a string. AVRO, PROTOBUF, JSONSCHEMA, schema fields and a key
        or value without a type are refused, since Octopus runs no schema
        registry. `partition_id` may only be 0, the topic's one partition.

        Replies 200 once every in-sync replica has acknowledged the
        record, with its partition, offset and timestamp (ISO-8601 UTC).
        The body must arrive within 20 s, and the record then has 30 s to
        be acknowledged: if it is not, the reply is 500 with 50003 and the
        record may still be written. The producer is not idempotent: its
        retries within the 30 s can write a record twice, even when the
        reply is 200.

        A record may be 8 MiB (8388608 bytes), counting its key, value,
        headers and about 100 bytes of Kafka's overhead; a body 16 MiB.
        The service holds at most 48 MiB of produce bodies and records at
        once, counting only the bytes that have arrived, and one user at
        most 16 MiB in at most 2 requests; once read, a body takes one of
        8 places for parsing and sending. Past a limit it answers 429 with
        `Retry-After`, before the body is sent if every place is taken or
        the Content-Length is more than the bytes free.

        Error codes: 40002 dot in the topic name, 40003 body not one JSON
        object, 40004 more than one JSON value, 40005 invalid record
        field, 40301 the service may not write to the topic, 40401
        namespace or topic not found, 40402 partition other than 0, 40801
        body not received within 20 s, 41301 body over 16 MiB, 41302
        record over 8 MiB, 41303 body too costly to parse, 41501 body not
        JSON, 42206 payload that needs a schema registry or empty body,
        42901 too many produce requests, 50002 Kafka error, 50003
        retriable Kafka error or timeout, 50302 cluster not reachable.
        """
        return await self._serve_producer(
            request,
            subject=subject,
            token=token,
            namespace=namespace,
            topic=topic,
            batch=False,
        )

    async def produce_records_batch(
        self,
        namespace: str,
        topic: str,
        *,
        request: Request,
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> Response:
        """Produce a batch of records to a topic of the namespace.

        The body is `{"entries": [...]}` with 1 to 500 records, each in
        the syntax of the single-record route plus an `id` of 1 to 80
        letters, digits, dashes or underscores, distinct within the
        batch. A missing, invalid or repeated id, or an empty batch or one
        over 500 entries, refuses the whole request with 422 and 42208,
        and records that total over 16 MiB once encoded with 413 and
        41304. The token, namespace and topic checks, the Content-Type,
        the body limits, the 20 s to send the body and the 429 answer as
        on the single-record route.

        Otherwise the reply is 207, once every entry is acknowledged or
        has failed, within 30 s of the body's arrival: `successes` lists
        each entry the cluster acknowledged with its partition, offset
        and timestamp, and `failures` each other entry with its error
        code and message (40005, 40301, 40401, 40402, 41302, 42206,
        50002, 50003, 50302, as on the single-record route). Entries are
        sent in order.
        """
        return await self._serve_producer(
            request,
            subject=subject,
            token=token,
            namespace=namespace,
            topic=topic,
            batch=True,
        )

    async def _serve_producer(
        self,
        request: Request,
        *,
        subject: str,
        token: str,
        namespace: str,
        topic: str,
        batch: bool,
    ) -> Response:
        """Check a produce request, admit it, read its body and produce it.

        The Content-Type and a Content-Length over the limit are refused
        before anything else, and the body is read only once the access
        checks pass and the request is admitted. Parsing runs one body at
        a time, and the sends on threads of their own, apart from the
        threadpool every other route shares.
        """
        if err := self._check_content_type(request):
            return err
        declared = self._declared_length(request)
        if declared is not None and declared > MAX_BODY_BYTES:
            return self._body_too_large()
        if err := await run_in_threadpool(
            self._check_topic_access,
            subject,
            token,
            namespace,
            topic,
        ):
            return err
        # A request that could not be served now is refused before its body
        # is sent; the body's bytes are reserved as they arrive, not as
        # declared, so a stalled upload holds only what it has sent
        try:
            reservation = self.produce_admission.admit(subject, declared)
        except RecordError as e:
            return self._produce_error(e)
        try:
            return await self._produce(
                request,
                reservation=reservation,
                namespace=namespace,
                topic=topic,
                batch=batch,
            )
        finally:
            reservation.release()

    async def _produce(
        self,
        request: Request,
        *,
        reservation: Reservation,
        namespace: str,
        topic: str,
        batch: bool,
    ) -> Response:
        """Read, parse and send an admitted produce request."""
        try:
            raw = await self._read_body(request, reservation)
            reservation.start()
            prepared, records = await self._parse(raw, batch=batch)
            del raw
            # The body is gone; hold the records' bytes instead, which
            # JSON data can make larger
            reservation.grow_to(sum(encoded_size(r) for r in records))
        except RecordError as e:
            return self._produce_error(e)
        except ClientDisconnect:
            # Nobody is left to read a reply
            return Response(status_code=400)
        # The records' time starts once they are parsed, so a slow upload
        # or a wait to parse does not count against it
        deadline = time.monotonic() + SEND_TIMEOUT_S
        results = await anyio.to_thread.run_sync(
            self.producer_service.send,
            self.producer_service.kafka_topic_name(namespace, topic),
            records,
            deadline,
            limiter=self.send_limiter,
        )
        if batch:
            resp_body, status = batch_reply(topic, prepared, results)
            return JSONResponse(content=resp_body, status_code=status)
        result = results[0]
        if isinstance(result, RecordError):
            return self._produce_error(result)
        resp_body, status = record_reply(topic, result)
        return JSONResponse(content=resp_body, status_code=status)

    async def _parse(
        self,
        raw: bytearray,
        *,
        batch: bool,
    ) -> tuple[
        list[tuple[str, OutgoingRecord | RecordError]],
        list[OutgoingRecord],
    ]:
        """Parse a body, one at a time, into its entries and records.

        Raises:
            RecordError: For a body the route refuses as a whole
        """
        if batch:
            prepared = await anyio.to_thread.run_sync(
                _refusal_returned,
                prepare_batch,
                raw,
                limiter=self.parse_limiter,
            )
            if isinstance(prepared, RecordError):
                raise prepared
            return prepared, [
                r for _, r in prepared if isinstance(r, OutgoingRecord)
            ]
        record = await anyio.to_thread.run_sync(
            _refusal_returned,
            prepare_record,
            raw,
            limiter=self.parse_limiter,
        )
        if isinstance(record, RecordError):
            raise record
        return [], [record]

    def _check_topic_access(
        self,
        subject: str,
        token: str,
        namespace: str,
        topic: str,
    ) -> Response | None:
        """Check the token, the namespace and the topic of a request.

        Returns:
            The 401, 404 or 400 response if a check fails, None if all pass
        """
        if err := self._check_access(subject, token, namespace):
            return err
        # Topic names never contain a dot, so the Kafka name stays inside
        # the namespace
        if '.' in topic:
            return JSONResponse(
                content={
                    'error_code': BAD_TOPIC_NAME,
                    'message': (
                        f'Invalid topic name {topic}: topic names never '
                        'contain a dot'
                    ),
                },
                status_code=400,
            )
        if missing := self._validate_topics_in_namespace(namespace, [topic]):
            return JSONResponse(content=missing, status_code=404)
        return None

    @staticmethod
    def _check_content_type(request: Request) -> Response | None:
        """Refuse a body that is not JSON (in UTF-8, if a charset is named)."""
        media_type, *params = request.headers.get('content-type', '').split(
            ';',
        )
        charsets = [
            value.strip().strip('"').lower()
            for name, _, value in (p.partition('=') for p in params)
            if name.strip().lower() == 'charset'
        ]
        if media_type.strip().lower() == 'application/json' and all(
            c in ('utf-8', 'utf8') for c in charsets
        ):
            return None
        return JSONResponse(
            content={
                'error_code': UNSUPPORTED_MEDIA_TYPE,
                'message': (
                    'The body must be Content-Type: application/json, in UTF-8'
                ),
            },
            status_code=415,
        )

    @staticmethod
    def _declared_length(request: Request) -> int | None:
        """The request's Content-Length, or None if it declares none."""
        length = request.headers.get('content-length')
        try:
            return None if length is None else int(length)
        except ValueError:
            return None

    @staticmethod
    async def _read_body(
        request: Request,
        reservation: Reservation,
    ) -> bytearray:
        """Read an admitted request's body within READ_TIMEOUT_S.

        The reservation grows with the bytes as they arrive, whatever the
        Content-Length.

        Raises:
            RecordError: 408 if the body takes too long, 413 if it is too
                large, 429 if the produce path cannot hold its bytes
            ClientDisconnect: If the client goes away first
        """
        body = bytearray()
        with anyio.move_on_after(READ_TIMEOUT_S) as read:
            async for chunk in request.stream():
                size = len(body) + len(chunk)
                if size > MAX_BODY_BYTES:
                    raise _body_too_large_error()
                reservation.grow_to(size)
                body += chunk
        if read.cancelled_caught:
            raise RecordError(
                408,
                BODY_TIMEOUT,
                f'The request body did not arrive within {READ_TIMEOUT_S} s',
            )
        return body

    @staticmethod
    def _produce_error(error: RecordError) -> Response:
        headers = {}
        if error.retry_after_s is not None:
            headers['Retry-After'] = str(error.retry_after_s)
        if error.status in (408, 413, 429):
            # The rest of the body, if any, is not read
            headers['Connection'] = 'close'
        return JSONResponse(
            content=error.body(),
            status_code=error.status,
            headers=headers or None,
        )

    def _body_too_large(self) -> Response:
        return self._produce_error(_body_too_large_error())


def _refusal_returned(
    prepare: Callable[[bytearray], _Prepared],
    raw: bytearray,
) -> _Prepared | RecordError:
    """Prepare a body, returning its refusal instead of raising it.

    A refusal raised out of the parse thread would keep the frames that
    raised it, and with them the parsed body, in a reference cycle
    through anyio's thread call, until the garbage collector ran.
    """
    try:
        return prepare(raw)
    except RecordError as e:
        return without_traceback(e)


def _body_too_large_error() -> RecordError:
    return RecordError(
        413,
        BODY_TOO_LARGE,
        f'The request body is over {MAX_BODY_BYTES} bytes, the most a '
        'produce request may send',
    )


service = DiasporaService()
app: FastAPI = service.app

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
