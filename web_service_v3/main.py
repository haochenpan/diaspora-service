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

from .utils import AWSManagerV3


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

        self.aws = AWSManagerV3(
            os.getenv('AWS_ACCOUNT_ID') or '',
            os.getenv('AWS_ACCOUNT_REGION') or '',
            os.getenv('MSK_CLUSTER_NAME') or '',
            os.getenv(
                'DEFAULT_SERVERS',
            ),  # iam_public: for endpoint in API response
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
        self.app.get('/api/v3/key', tags=['Authentication'])(
            self.get_key,
        )
        self.app.delete('/api/v3/key', tags=['Authentication'])(
            self.delete_key,
        )

        # Namespace management routes
        self.app.post('/api/v3/namespace', tags=['Namespace'])(
            self.create_namespace,
        )
        self.app.delete('/api/v3/namespace', tags=['Namespace'])(
            self.delete_namespace,
        )

        # Topic management routes
        self.app.post('/api/v3/{namespace}/{topic}', tags=['Topic'])(
            self.create_topic,
        )
        self.app.delete('/api/v3/{namespace}/{topic}', tags=['Topic'])(
            self.delete_topic,
        )

    async def create_user(
        self,
        subject: str = Depends(extract_val('subject')),
        token: str = Depends(extract_val('authorization')),
    ) -> dict[str, Any]:
        """Create an IAM user for the authenticated subject."""
        if err := self.auth.validate_access_token(subject, token):
            return err
        return self.aws.create_user(subject)

    async def delete_user(
        self,
        subject: str = Depends(extract_val('subject')),
        token: str = Depends(extract_val('authorization')),
    ) -> dict[str, Any]:
        """Delete an IAM user for the authenticated subject."""
        if err := self.auth.validate_access_token(subject, token):
            return err
        return self.aws.delete_user(subject)

    async def create_key(
        self,
        subject: str = Depends(extract_val('subject')),
        token: str = Depends(extract_val('authorization')),
    ) -> dict[str, Any]:
        """Create an access key for an existing IAM user."""
        if err := self.auth.validate_access_token(subject, token):
            return err
        return self.aws.create_key(subject)

    async def get_key(
        self,
        subject: str = Depends(extract_val('subject')),
        token: str = Depends(extract_val('authorization')),
    ) -> dict[str, Any]:
        """Get access key for a user, creating one if it doesn't exist."""
        if err := self.auth.validate_access_token(subject, token):
            return err
        return self.aws.get_key(subject)

    async def delete_key(
        self,
        subject: str = Depends(extract_val('subject')),
        token: str = Depends(extract_val('authorization')),
    ) -> dict[str, Any]:
        """Delete access keys for a user."""
        if err := self.auth.validate_access_token(subject, token):
            return err
        return self.aws.delete_key(subject)

    async def create_namespace(
        self,
        subject: str = Depends(extract_val('subject')),
        token: str = Depends(extract_val('authorization')),
        namespace: str = Body(...),
    ) -> dict[str, Any]:
        """Create a namespace for a user."""
        if err := self.auth.validate_access_token(subject, token):
            return err
        try:
            return self.aws.create_namespace(subject, namespace)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

    async def delete_namespace(
        self,
        subject: str = Depends(extract_val('subject')),
        token: str = Depends(extract_val('authorization')),
        namespace: str = Body(...),
    ) -> dict[str, Any]:
        """Delete a namespace for a user."""
        if err := self.auth.validate_access_token(subject, token):
            return err
        try:
            return self.aws.delete_namespace(subject, namespace)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

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
        try:
            return self.aws.create_topic(subject, namespace, topic)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

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
        try:
            return self.aws.delete_topic(subject, namespace, topic)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e


service = DiasporaService()
app = service.app

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
