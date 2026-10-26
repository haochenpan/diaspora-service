"""Diaspora Web Service entry point."""

from __future__ import annotations

import importlib.metadata as importlib_metadata
import os
from collections.abc import AsyncIterator
from collections.abc import Callable
from contextlib import asynccontextmanager
from typing import Any

import uvicorn
from fastapi import Body
from fastapi import FastAPI
from fastapi import Header
from fastapi import Query
from fastapi import Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from fastapi.responses import Response

from web_service.consumer_models import AssignmentRequest
from web_service.consumer_models import CreateConsumerRequest
from web_service.consumer_models import OffsetsCommitRequest
from web_service.consumer_models import OffsetsGetRequest
from web_service.consumer_models import SeekPartitionsRequest
from web_service.consumer_models import SeekRequest
from web_service.consumer_models import SubscriptionRequest
from web_service.consumer_service import ConsumerService
from web_service.producer_models import BatchResult
from web_service.producer_models import PRODUCE_BATCH_REQUEST_SCHEMA
from web_service.producer_models import PRODUCE_REQUEST_SCHEMA
from web_service.producer_models import ProduceError
from web_service.producer_models import ProduceResult
from web_service.producer_service import BAD_TOPIC_NAME
from web_service.producer_service import BODY_TOO_LARGE
from web_service.producer_service import MAX_BODY_BYTES
from web_service.producer_service import ProducerService
from web_service.utils import AuthManager
from web_service.utils import EnvironmentChecker

from .services import DynamoDBService
from .services import IAMService
from .services import KafkaService
from .services import NamespaceService
from .services import parse_max_topics_per_namespace
from .services import WebService

# Fetch wait when the request names none, and the longest it may name
DEFAULT_FETCH_TIMEOUT_MS = 5_000
MAX_FETCH_TIMEOUT_MS = 30_000


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

        namespace_service = NamespaceService(
            dynamodb_service=db_service,
            max_topics_per_namespace=parse_max_topics_per_namespace(
                os.getenv('MAX_TOPICS_PER_NAMESPACE'),
            ),
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
        """Application lifespan: close consumers and the producer."""
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
        if err := self._check_access(subject, token, namespace):
            return err
        return serve()

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
        """Add consumer REST API routes (Confluent v2 compatible)."""
        base = '/api/v3/{namespace}/consumers'

        # 1. Create consumer instance
        self.app.post(
            f'{base}/{{group_name}}',
            tags=['Consumer'],
        )(self.create_consumer_instance)

        # 2. Delete consumer instance
        self.app.delete(
            f'{base}/{{group_name}}/instances/{{instance_id}}',
            tags=['Consumer'],
        )(self.delete_consumer_instance)

        # 3. Subscribe
        self.app.post(
            f'{base}/{{group_name}}/instances/{{instance_id}}/subscription',
            tags=['Consumer'],
        )(self.subscribe_consumer)

        # 4. Get subscription
        self.app.get(
            f'{base}/{{group_name}}/instances/{{instance_id}}/subscription',
            tags=['Consumer'],
        )(self.get_consumer_subscription)

        # 5. Unsubscribe
        self.app.delete(
            f'{base}/{{group_name}}/instances/{{instance_id}}/subscription',
            tags=['Consumer'],
        )(self.unsubscribe_consumer)

        # 6. Fetch records
        self.app.get(
            f'{base}/{{group_name}}/instances/{{instance_id}}/records',
            tags=['Consumer'],
        )(self.fetch_consumer_records)

        # 7. Commit offsets
        self.app.post(
            f'{base}/{{group_name}}/instances/{{instance_id}}/offsets',
            tags=['Consumer'],
        )(self.commit_consumer_offsets)

        # 8. Get committed offsets
        self.app.get(
            f'{base}/{{group_name}}/instances/{{instance_id}}/offsets',
            tags=['Consumer'],
        )(self.get_committed_offsets)

        # 9. Assign partitions
        self.app.post(
            f'{base}/{{group_name}}/instances/{{instance_id}}/assignments',
            tags=['Consumer'],
        )(self.assign_consumer_partitions)

        # 10. Get assignments
        self.app.get(
            f'{base}/{{group_name}}/instances/{{instance_id}}/assignments',
            tags=['Consumer'],
        )(self.get_consumer_assignments)

        # 11. Seek to offset
        self.app.post(
            f'{base}/{{group_name}}/instances/{{instance_id}}/positions',
            tags=['Consumer'],
        )(self.seek_consumer_positions)

        # 12. Seek to beginning
        self.app.post(
            f'{base}/{{group_name}}/instances'
            f'/{{instance_id}}/positions/beginning',
            tags=['Consumer'],
        )(self.seek_consumer_beginning)

        # 13. Seek to end
        self.app.post(
            f'{base}/{{group_name}}/instances/{{instance_id}}/positions/end',
            tags=['Consumer'],
        )(self.seek_consumer_end)

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

        return await run_in_threadpool(
            self._serve_consumer,
            subject,
            token,
            namespace,
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

        return await run_in_threadpool(
            self._serve_consumer,
            subject,
            token,
            namespace,
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

        return await run_in_threadpool(
            self._serve_consumer,
            subject,
            token,
            namespace,
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

        return await run_in_threadpool(
            self._serve_consumer,
            subject,
            token,
            namespace,
            serve,
        )

    def fetch_consumer_records(
        self,
        namespace: str,
        group_name: str,
        instance_id: str,
        *,
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
        timeout: int | None = Query(
            None,
            description=(
                'Milliseconds to wait for records (default 5000). Values '
                'above 30000 are treated as 30000.'
            ),
        ),
        max_bytes: int | None = Query(None),
    ) -> Response:
        """Fetch data for subscribed topics or assigned partitions.

        Uses def (not async def) so FastAPI runs it in a threadpool,
        avoiding blocking the event loop during consumer.poll().
        """

        def serve() -> Response:
            resp_body, status = self.consumer_service.poll_records(
                subject=subject,
                namespace=namespace,
                group_name=group_name,
                instance_id=instance_id,
                timeout_ms=min(
                    timeout or DEFAULT_FETCH_TIMEOUT_MS,
                    MAX_FETCH_TIMEOUT_MS,
                ),
                max_bytes=max_bytes,
            )
            return self._consumer_response(resp_body, status)

        return self._serve_consumer(subject, token, namespace, serve)

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
                offsets = [o.model_dump() for o in body.offsets]
            resp_body, status = self.consumer_service.commit_offsets(
                subject=subject,
                namespace=namespace,
                group_name=group_name,
                instance_id=instance_id,
                offsets=offsets,
            )
            return self._consumer_response(resp_body, status)

        return await run_in_threadpool(
            self._serve_consumer,
            subject,
            token,
            namespace,
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
            self._check_access,
            subject,
            token,
            namespace,
        ):
            return err
        # Confluent sends partitions in the GET request body
        raw_body = await request.json()
        req = OffsetsGetRequest(**raw_body)
        partitions = [p.model_dump() for p in req.partitions]
        resp_body, status = await run_in_threadpool(
            self.consumer_service.get_committed,
            subject=subject,
            namespace=namespace,
            group_name=group_name,
            instance_id=instance_id,
            partitions=partitions,
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

        return await run_in_threadpool(
            self._serve_consumer,
            subject,
            token,
            namespace,
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

        return await run_in_threadpool(
            self._serve_consumer,
            subject,
            token,
            namespace,
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
            offsets = [o.model_dump() for o in body.offsets]
            resp_body, status = self.consumer_service.seek(
                subject=subject,
                namespace=namespace,
                group_name=group_name,
                instance_id=instance_id,
                offsets=offsets,
            )
            return self._consumer_response(resp_body, status)

        return await run_in_threadpool(
            self._serve_consumer,
            subject,
            token,
            namespace,
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
            partitions = [p.model_dump() for p in body.partitions]
            resp_body, status = self.consumer_service.seek_to_beginning(
                subject=subject,
                namespace=namespace,
                group_name=group_name,
                instance_id=instance_id,
                partitions=partitions,
            )
            return self._consumer_response(resp_body, status)

        return await run_in_threadpool(
            self._serve_consumer,
            subject,
            token,
            namespace,
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
            partitions = [p.model_dump() for p in body.partitions]
            resp_body, status = self.consumer_service.seek_to_end(
                subject=subject,
                namespace=namespace,
                group_name=group_name,
                instance_id=instance_id,
                partitions=partitions,
            )
            return self._consumer_response(resp_body, status)

        return await run_in_threadpool(
            self._serve_consumer,
            subject,
            token,
            namespace,
            serve,
        )

    # --- Producer routes ---

    def add_producer_routes(self) -> None:
        """Add produce REST API routes (Confluent v3 syntax)."""
        base = '/api/v3/{namespace}/topics/{topic}'
        refusals: dict[int | str, dict[str, Any]] = {
            400: {'model': ProduceError, 'description': 'Refused request'},
            401: {'description': 'The token check failed'},
            404: {
                'model': ProduceError,
                'description': 'Namespace or topic not found',
            },
            413: {
                'model': ProduceError,
                'description': 'Request body over 16 MiB',
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
                413: {
                    'model': ProduceError,
                    'description': (
                        'Request body over 16 MiB, or record over 8 MiB'
                    ),
                },
                500: {
                    'model': ProduceError,
                    'description': 'Kafka failed to write the record',
                },
            },
            openapi_extra={
                'requestBody': {
                    'required': True,
                    'content': {
                        'application/json': {
                            'schema': PRODUCE_REQUEST_SCHEMA,
                        },
                    },
                },
            },
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
            },
            openapi_extra={
                'requestBody': {
                    'required': True,
                    'content': {
                        'application/json': {
                            'schema': PRODUCE_BATCH_REQUEST_SCHEMA,
                        },
                    },
                },
            },
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

        The body is one record in the syntax of Confluent's REST API v3
        (`partition_id`, `headers`, `key`, `value`, `timestamp`); a body
        holding more than one JSON value is refused, so send several
        records to `records:batch`. A key or value is
        `{"type": "BINARY" | "JSON" | "STRING", "data": ...}`: BINARY data
        is base64, JSON data any JSON value, STRING data a string. AVRO,
        PROTOBUF, JSONSCHEMA and schema fields are refused, since Octopus
        runs no schema registry. `partition_id` may only be 0, the topic's
        one partition.

        Replies 200 once the cluster has acknowledged the record (acks
        from all in-sync replicas), with its partition, offset and
        timestamp (ISO-8601 UTC). A body over 16 MiB or a record over
        8 MiB once decoded (key, value and headers) answers 413; a record
        the cluster did not acknowledge within 30 s answers 500, and it
        may still be written.

        Error codes: 40001 body not one JSON object, 40002 more than one
        JSON value, 40003 invalid record field, 40004 format or field that
        needs a schema registry, 40005 partition other than 0, 40006 dot
        in the topic name, 40401 namespace or topic not found, 41301 body
        over 16 MiB, 41302 record over 8 MiB, 50002 Kafka failure.
        """
        return await self._serve_producer(
            request,
            subject=subject,
            token=token,
            namespace=namespace,
            topic=topic,
            produce=self.producer_service.produce_record,
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
        the syntax of the single-record route plus an `id` (a string of 1
        to 80 characters, distinct within the batch). A missing,
        duplicated or invalid id, or more than 500 entries, refuses the
        whole request with 400; a failed token, namespace or topic check
        or a body over 16 MiB answers as on the single-record route.

        Otherwise the reply is 207: `successes` lists each entry the
        cluster acknowledged with its partition, offset and timestamp, and
        `failures` lists each other entry with its error code and message
        (the codes of the single-record route; an entry over 8 MiB fails
        with 41302). Entries are sent in order, and the request waits at
        most 30 s for all of them.

        Error codes of the whole request: 40001 body not one JSON object,
        40002 more than one JSON value, 40006 dot in the topic name, 40007
        entries missing, empty or over 500, 40008 entry id missing,
        invalid or duplicated, 40401 namespace or topic not found, 41301
        body over 16 MiB.
        """
        return await self._serve_producer(
            request,
            subject=subject,
            token=token,
            namespace=namespace,
            topic=topic,
            produce=self.producer_service.produce_batch,
        )

    async def _serve_producer(
        self,
        request: Request,
        *,
        subject: str,
        token: str,
        namespace: str,
        topic: str,
        produce: Callable[[str, str, bytes], tuple[dict[str, Any], int]],
    ) -> Response:
        """Check a produce request, read its body, then produce it.

        A Content-Length over the limit is refused before anything else,
        and the body is read only once the access checks pass. The checks
        and the produce block, so they run in the threadpool.
        """
        if self._declared_too_large(request):
            return self._body_too_large()
        if err := await run_in_threadpool(
            self._check_topic_access,
            subject,
            token,
            namespace,
            topic,
        ):
            return err
        raw = await self._read_body(request)
        if raw is None:
            return self._body_too_large()
        resp_body, status = await run_in_threadpool(
            produce,
            namespace,
            topic,
            raw,
        )
        return JSONResponse(content=resp_body, status_code=status)

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
        if err := self._validate_topics_in_namespace(namespace, [topic]):
            return JSONResponse(content=err, status_code=404)
        return None

    @staticmethod
    def _declared_too_large(request: Request) -> bool:
        """Whether the request's Content-Length is over the body limit."""
        length = request.headers.get('content-length')
        try:
            return length is not None and int(length) > MAX_BODY_BYTES
        except ValueError:
            return False

    @staticmethod
    async def _read_body(request: Request) -> bytes | None:
        """Read the request body, or None once it passes the limit."""
        chunks: list[bytes] = []
        size = 0
        async for chunk in request.stream():
            size += len(chunk)
            if size > MAX_BODY_BYTES:
                return None
            chunks.append(chunk)
        return b''.join(chunks)

    @staticmethod
    def _body_too_large() -> Response:
        return JSONResponse(
            content={
                'error_code': BODY_TOO_LARGE,
                'message': (
                    f'The request body is over {MAX_BODY_BYTES} bytes, '
                    'the most a produce request may send'
                ),
            },
            status_code=413,
        )


service = DiasporaService()
app: FastAPI = service.app

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
