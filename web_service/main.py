"""Diaspora Web Service entry point."""

from __future__ import annotations

import importlib.metadata as importlib_metadata
import os
from typing import Any
from typing import Callable

import uvicorn
from fastapi import Body
from fastapi import Depends
from fastapi import FastAPI
from fastapi import Header
from fastapi import HTTPException

from web_service.utils import AuthManager
from web_service.utils import EnvironmentChecker
from web_service.utils import WEB_SERVICE_DESC
from web_service.utils import WEB_SERVICE_TAGS_METADATA

from .services import DynamoDBService
from .services import IAMService
from .services import KafkaService
from .services import NamespaceService
from .services import WebService


def extract_val(alias: str) -> Callable[..., Any]:
    """Extract value from header or body."""

    async def extract_from_header_or_body(
        header: str | None = Header(None, alias=alias),
        body: str | None = Body(None, alias=alias),
    ) -> str:
        val = header or body
        if val is None:
            raise HTTPException(
                status_code=400,
                detail=(
                    f'{alias.capitalize()} must be provided'
                    ' either in header or body'
                ),
            )
        return val

    return extract_from_header_or_body


class DiasporaService:
    """Service for managing Diaspora web service."""

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

        self.app = FastAPI(
            title='Diaspora Web Service V3',
            docs_url='/',
            version=importlib_metadata.version('diaspora_service'),
            description=WEB_SERVICE_DESC,
            openapi_tags=WEB_SERVICE_TAGS_METADATA,
        )
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
        subject: str = Depends(extract_val('subject')),
        token: str = Depends(extract_val('authorization')),
    ) -> dict[str, Any]:
        """Create an IAM user for the authenticated subject."""
        if err := self.auth.validate_access_token(subject, token):
            return err
        return self.web_service.create_user(subject)

    async def delete_user(
        self,
        subject: str = Depends(extract_val('subject')),
        token: str = Depends(extract_val('authorization')),
    ) -> dict[str, Any]:
        """Delete an IAM user for the authenticated subject."""
        if err := self.auth.validate_access_token(subject, token):
            return err
        return self.web_service.delete_user(subject)

    async def create_key(
        self,
        subject: str = Depends(extract_val('subject')),
        token: str = Depends(extract_val('authorization')),
    ) -> dict[str, Any]:
        """Create an access key for an existing IAM user."""
        if err := self.auth.validate_access_token(subject, token):
            return err
        return self.web_service.create_key(subject)

    async def delete_key(
        self,
        subject: str = Depends(extract_val('subject')),
        token: str = Depends(extract_val('authorization')),
    ) -> dict[str, Any]:
        """Delete access keys for a user."""
        if err := self.auth.validate_access_token(subject, token):
            return err
        return self.web_service.delete_key(subject)

    async def create_topic(
        self,
        namespace: str,
        topic: str,
        subject: str = Depends(extract_val('subject')),
        token: str = Depends(extract_val('authorization')),
    ) -> dict[str, Any]:
        """Create a topic under a namespace."""
        if err := self.auth.validate_access_token(subject, token):
            return err
        return self.web_service.create_topic(subject, namespace, topic)

    async def delete_topic(
        self,
        namespace: str,
        topic: str,
        subject: str = Depends(extract_val('subject')),
        token: str = Depends(extract_val('authorization')),
    ) -> dict[str, Any]:
        """Delete a topic from a namespace."""
        if err := self.auth.validate_access_token(subject, token):
            return err
        return self.web_service.delete_topic(subject, namespace, topic)

    async def recreate_topic(
        self,
        namespace: str,
        topic: str,
        subject: str = Depends(extract_val('subject')),
        token: str = Depends(extract_val('authorization')),
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
        subject: str = Depends(extract_val('subject')),
        token: str = Depends(extract_val('authorization')),
    ) -> dict[str, Any]:
        """List all namespaces owned by a user and their topics."""
        if err := self.auth.validate_access_token(subject, token):
            return err
        return self.web_service.namespace_service.list_namespace_and_topics(
            subject,
        )


service = DiasporaService()
app = service.app

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
