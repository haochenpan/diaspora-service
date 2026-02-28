"""Diaspora Web Service entry point."""

from __future__ import annotations

import importlib.metadata as importlib_metadata
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import uvicorn
from fastapi import Body
from fastapi import FastAPI
from fastapi import Header
from fastapi import Query
from fastapi import Request
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
from web_service.utils import AuthManager
from web_service.utils import EnvironmentChecker

from .services import DynamoDBService
from .services import IAMService
from .services import KafkaService
from .services import NamespaceService
from .services import WebService


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

        namespace_service = NamespaceService(dynamodb_service=db_service)

        self.web_service = WebService(
            iam_service=iam_service,
            kafka_service=kafka_service,
            namespace_service=namespace_service,
        )

        self.consumer_service = ConsumerService(
            bootstrap_servers=os.getenv('DEFAULT_SERVERS'),
            region=region,
        )

        self.app = FastAPI(
            title='Diaspora Web Service V3',
            docs_url='/',
            version=importlib_metadata.version('diaspora_service'),
            lifespan=self._lifespan,
        )
        self.add_consumer_routes()
        self.add_routes()

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
        # List namespaces and topics
        self.app.get('/api/v3/namespace', tags=['Namespace'])(
            self.list_namespace_and_topics,
        )

    async def create_user(
        self,
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> dict[str, Any]:
        """Create an IAM user for the authenticated subject."""
        if err := self.auth.validate_access_token(subject, token):
            return err
        return self.web_service.create_user(subject)

    async def delete_user(
        self,
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> dict[str, Any]:
        """Delete an IAM user for the authenticated subject."""
        if err := self.auth.validate_access_token(subject, token):
            return err
        return self.web_service.delete_user(subject)

    async def create_key(
        self,
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> dict[str, Any]:
        """Create an access key for an existing IAM user."""
        if err := self.auth.validate_access_token(subject, token):
            return err
        return self.web_service.create_key(subject)

    async def delete_key(
        self,
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> dict[str, Any]:
        """Delete access keys for a user."""
        if err := self.auth.validate_access_token(subject, token):
            return err
        return self.web_service.delete_key(subject)

    async def create_topic(
        self,
        namespace: str,
        topic: str,
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> dict[str, Any]:
        """Create a topic under a namespace."""
        if err := self.auth.validate_access_token(subject, token):
            return err
        return self.web_service.create_topic(subject, namespace, topic)

    async def delete_topic(
        self,
        namespace: str,
        topic: str,
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> dict[str, Any]:
        """Delete a topic from a namespace."""
        if err := self.auth.validate_access_token(subject, token):
            return err
        return self.web_service.delete_topic(subject, namespace, topic)

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
        if err := self.auth.validate_access_token(subject, token):
            return err
        return self.web_service.recreate_topic(subject, namespace, topic)

    async def list_namespace_and_topics(
        self,
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> dict[str, Any]:
        """List all namespaces owned by a user and their topics."""
        if err := self.auth.validate_access_token(subject, token):
            return err
        return self.web_service.namespace_service.list_namespace_and_topics(
            subject,
        )

    # --- Lifespan ---

    @asynccontextmanager
    async def _lifespan(
        self,
        app: FastAPI,
    ) -> AsyncIterator[None]:
        """Application lifespan: clean up consumers on shutdown."""
        yield
        self.consumer_service.shutdown_all()

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
        if err := self.auth.validate_access_token(subject, token):
            return JSONResponse(content=err, status_code=401)
        if err := self._validate_namespace(subject, namespace):
            return JSONResponse(content=err, status_code=404)
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

    async def delete_consumer_instance(
        self,
        namespace: str,
        group_name: str,
        instance_id: str,
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> Response:
        """Destroy a consumer instance."""
        if err := self.auth.validate_access_token(subject, token):
            return JSONResponse(content=err, status_code=401)
        if err := self._validate_namespace(subject, namespace):
            return JSONResponse(content=err, status_code=404)
        resp_body, status = self.consumer_service.delete_consumer(
            subject=subject,
            namespace=namespace,
            group_name=group_name,
            instance_id=instance_id,
        )
        return self._consumer_response(resp_body, status)

    async def subscribe_consumer(
        self,
        namespace: str,
        group_name: str,
        instance_id: str,
        body: SubscriptionRequest = Body(...),
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> Response:
        """Subscribe consumer to topics or a topic pattern."""
        if err := self.auth.validate_access_token(subject, token):
            return JSONResponse(content=err, status_code=401)
        if err := self._validate_namespace(subject, namespace):
            return JSONResponse(content=err, status_code=404)
        if body.topics and body.topic_pattern:
            return JSONResponse(
                content={
                    'error_code': 40903,
                    'message': (
                        'Subscription to topics, partitions '
                        'and pattern are mutually exclusive.'
                    ),
                },
                status_code=409,
            )
        if body.topics and (
            err := self._validate_topics_in_namespace(
                namespace,
                body.topics,
            )
        ):
            return JSONResponse(content=err, status_code=404)
        resp_body, status = self.consumer_service.subscribe(
            subject=subject,
            namespace=namespace,
            group_name=group_name,
            instance_id=instance_id,
            topics=body.topics,
            topic_pattern=body.topic_pattern,
        )
        return self._consumer_response(resp_body, status)

    async def get_consumer_subscription(
        self,
        namespace: str,
        group_name: str,
        instance_id: str,
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> Response:
        """Get the current subscribed list of topics."""
        if err := self.auth.validate_access_token(subject, token):
            return JSONResponse(content=err, status_code=401)
        if err := self._validate_namespace(subject, namespace):
            return JSONResponse(content=err, status_code=404)
        resp_body, status = self.consumer_service.get_subscription(
            subject=subject,
            namespace=namespace,
            group_name=group_name,
            instance_id=instance_id,
        )
        return self._consumer_response(resp_body, status)

    async def unsubscribe_consumer(
        self,
        namespace: str,
        group_name: str,
        instance_id: str,
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> Response:
        """Unsubscribe from topics currently subscribed."""
        if err := self.auth.validate_access_token(subject, token):
            return JSONResponse(content=err, status_code=401)
        if err := self._validate_namespace(subject, namespace):
            return JSONResponse(content=err, status_code=404)
        resp_body, status = self.consumer_service.unsubscribe(
            subject=subject,
            namespace=namespace,
            group_name=group_name,
            instance_id=instance_id,
        )
        return self._consumer_response(resp_body, status)

    def fetch_consumer_records(
        self,
        namespace: str,
        group_name: str,
        instance_id: str,
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
        timeout: int | None = Query(None),
        max_bytes: int | None = Query(None),
    ) -> Response:
        """Fetch data for subscribed topics or assigned partitions.

        Uses def (not async def) so FastAPI runs it in a threadpool,
        avoiding blocking the event loop during consumer.poll().
        """
        if err := self.auth.validate_access_token(subject, token):
            return JSONResponse(content=err, status_code=401)
        if err := self._validate_namespace(subject, namespace):
            return JSONResponse(content=err, status_code=404)
        resp_body, status = self.consumer_service.poll_records(
            subject=subject,
            namespace=namespace,
            group_name=group_name,
            instance_id=instance_id,
            timeout_ms=timeout or 5000,
            max_bytes=max_bytes,
        )
        return self._consumer_response(resp_body, status)

    async def commit_consumer_offsets(
        self,
        namespace: str,
        group_name: str,
        instance_id: str,
        body: OffsetsCommitRequest | None = Body(None),
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> Response:
        """Commit offsets for the consumer."""
        if err := self.auth.validate_access_token(subject, token):
            return JSONResponse(content=err, status_code=401)
        if err := self._validate_namespace(subject, namespace):
            return JSONResponse(content=err, status_code=404)
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

    async def get_committed_offsets(
        self,
        namespace: str,
        group_name: str,
        instance_id: str,
        request: Request,
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> Response:
        """Get last committed offsets for given partitions."""
        if err := self.auth.validate_access_token(subject, token):
            return JSONResponse(content=err, status_code=401)
        if err := self._validate_namespace(subject, namespace):
            return JSONResponse(content=err, status_code=404)
        # Confluent sends partitions in the GET request body
        raw_body = await request.json()
        req = OffsetsGetRequest(**raw_body)
        partitions = [p.model_dump() for p in req.partitions]
        resp_body, status = self.consumer_service.get_committed(
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
        body: AssignmentRequest = Body(...),
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> Response:
        """Manually assign partitions to this consumer."""
        if err := self.auth.validate_access_token(subject, token):
            return JSONResponse(content=err, status_code=401)
        if err := self._validate_namespace(subject, namespace):
            return JSONResponse(content=err, status_code=404)
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

    async def get_consumer_assignments(
        self,
        namespace: str,
        group_name: str,
        instance_id: str,
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> Response:
        """Get the list of partitions assigned to this consumer."""
        if err := self.auth.validate_access_token(subject, token):
            return JSONResponse(content=err, status_code=401)
        if err := self._validate_namespace(subject, namespace):
            return JSONResponse(content=err, status_code=404)
        resp_body, status = self.consumer_service.get_assignments(
            subject=subject,
            namespace=namespace,
            group_name=group_name,
            instance_id=instance_id,
        )
        return self._consumer_response(resp_body, status)

    async def seek_consumer_positions(
        self,
        namespace: str,
        group_name: str,
        instance_id: str,
        body: SeekRequest = Body(...),
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> Response:
        """Override fetch offsets for the next set of records."""
        if err := self.auth.validate_access_token(subject, token):
            return JSONResponse(content=err, status_code=401)
        if err := self._validate_namespace(subject, namespace):
            return JSONResponse(content=err, status_code=404)
        offsets = [o.model_dump() for o in body.offsets]
        resp_body, status = self.consumer_service.seek(
            subject=subject,
            namespace=namespace,
            group_name=group_name,
            instance_id=instance_id,
            offsets=offsets,
        )
        return self._consumer_response(resp_body, status)

    async def seek_consumer_beginning(
        self,
        namespace: str,
        group_name: str,
        instance_id: str,
        body: SeekPartitionsRequest = Body(...),
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> Response:
        """Seek to the first offset for given partitions."""
        if err := self.auth.validate_access_token(subject, token):
            return JSONResponse(content=err, status_code=401)
        if err := self._validate_namespace(subject, namespace):
            return JSONResponse(content=err, status_code=404)
        partitions = [p.model_dump() for p in body.partitions]
        resp_body, status = self.consumer_service.seek_to_beginning(
            subject=subject,
            namespace=namespace,
            group_name=group_name,
            instance_id=instance_id,
            partitions=partitions,
        )
        return self._consumer_response(resp_body, status)

    async def seek_consumer_end(
        self,
        namespace: str,
        group_name: str,
        instance_id: str,
        body: SeekPartitionsRequest = Body(...),
        subject: str = Header(..., alias='subject'),
        token: str = Header(..., alias='authorization'),
    ) -> Response:
        """Seek to the last offset for given partitions."""
        if err := self.auth.validate_access_token(subject, token):
            return JSONResponse(content=err, status_code=401)
        if err := self._validate_namespace(subject, namespace):
            return JSONResponse(content=err, status_code=404)
        partitions = [p.model_dump() for p in body.partitions]
        resp_body, status = self.consumer_service.seek_to_end(
            subject=subject,
            namespace=namespace,
            group_name=group_name,
            instance_id=instance_id,
            partitions=partitions,
        )
        return self._consumer_response(resp_body, status)


service = DiasporaService()
app: FastAPI = service.app

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
