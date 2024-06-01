"""Diaspora Web Service entry point."""

from __future__ import annotations

import base64
import importlib.metadata as importlib_metadata
import os

import uvicorn
from fastapi import Body
from fastapi import Depends
from fastapi import FastAPI
from fastapi import Header
from fastapi import HTTPException
from fastapi import Request

from web_service.utils import AuthManager
from web_service.utils import AWSManager
from web_service.utils import EnvironmentChecker
from web_service.utils import WEB_SERVICE_DESC
from web_service.utils import WEB_SERVICE_LAMBDA_CONFIGS
from web_service.utils import WEB_SERVICE_TAGS_METADATA
from web_service.utils import WEB_SERVICE_TRIGGER_CONFIGS


def extract_val(alias):
    """Extract value from header or body."""

    async def extract_from_header_or_body(
        header=Header(None, alias=alias),  # noqa: B008
        body=Body(None, alias=alias),  # noqa: B008
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

    PARTITION_MIN = 1
    PARTITION_MAX = 4

    def __init__(self):
        """Initialize the service by checking env vars and setting up deps."""
        EnvironmentChecker.check_env_variables(
            'AWS_ACCESS_KEY_ID',
            'AWS_SECRET_ACCESS_KEY',
            'SERVER_CLIENT_ID',
            'SERVER_SECRET',
        )

        self.auth = AuthManager(
            os.getenv('SERVER_CLIENT_ID'),
            os.getenv('SERVER_SECRET'),
            'c5d4fab4-7f0d-422e-b0c8-5c74329b52fe',
        )

        self.aws = AWSManager(
            '845889416464',
            'us-east-1',
            'diaspora',
            '0b48e9a3-c32b-4783-9993-30798cdda646-9',
            'b-1-public.diaspora.fy49oq.c9.kafka.us-east-1.amazonaws.com:9198,b-2-public.diaspora.fy49oq.c9.kafka.us-east-1.amazonaws.com:9198',
            'b-1-public.diaspora.fy49oq.c9.kafka.us-east-1.amazonaws.com:9198,b-2-public.diaspora.fy49oq.c9.kafka.us-east-1.amazonaws.com:9198',
        )

        self.app = FastAPI(
            title='Diaspora Web Service',
            docs_url='/',
            version=importlib_metadata.version('diaspora_service'),
            description=WEB_SERVICE_DESC,
            openapi_tags=WEB_SERVICE_TAGS_METADATA,
        )
        self.add_routes()

    def validate_topic_access(self, subject, topic):
        """Validate if the subject has access to the topic."""
        if topic not in self.aws.list_topics_for_principal(subject):
            return {
                'status': 'error',
                'message': f'Principal {subject} has no access.',
            }

    def add_routes(self):
        """Add routes to the FastAPI app."""
        # Authentication
        self.app.get('/api/v2/create_key', tags=['Authentication'])(
            self.create_key,
        )

        # Topic Management
        self.app.get('/api/v2/topics', tags=['Topic'])(self.list_topics)
        self.app.put('/api/v2/topic/{topic}', tags=['Topic'])(
            self.register_or_unregister_topic,
        )
        self.app.get('/api/v2/topic/{topic}', tags=['Topic'])(
            self.get_topic_configs,
        )
        self.app.post('/api/v2/topic/{topic}', tags=['Topic'])(
            self.update_topic_configs,
        )
        self.app.post('/api/v2/topic/{topic}/partitions', tags=['Topic'])(
            self.update_topic_partitions,
        )
        self.app.post('/api/v2/topic/{topic}/reset', tags=['Topic'])(
            self.reset_topic,
        )
        self.app.get('/api/v2/topic/{topic}/users', tags=['Topic'])(
            self.list_topic_users,
        )
        self.app.post('/api/v2/topic/{topic}/user', tags=['Topic'])(
            self.grant_or_revoke_user_access,
        )

        # Trigger Management
        self.app.get('/api/v2/triggers', tags=['Trigger'])(
            self.list_triggers,
        )
        self.app.put('/api/v2/trigger', tags=['Trigger'])(
            self.create_or_delete_trigger,
        )
        self.app.post('/api/v2/triggers/{trigger_id}', tags=['Trigger'])(
            self.update_trigger,
        )
        self.app.get('/api/v2/logs', tags=['Trigger'])(
            self.list_log_streams,
        )
        self.app.get('/api/v2/log', tags=['Trigger'])(
            self.get_log_events,
        )

        # Legacy
        self.app.post('/v1/create_key', tags=['Legacy'])(
            self.create_key,
        )
        self.app.post('/v1/register_topic', tags=['Legacy'])(
            self.register_topic,
        )
        self.app.post('/v1/unregister_topic', tags=['Legacy'])(
            self.unregister_topic,
        )
        self.app.get('/v1/list_topics', tags=['Legacy'])(
            self.list_topics,
        )

    async def create_key(
        self,
        subject: str = Depends(extract_val('subject')),
        token: str = Depends(extract_val('authorization')),
    ):
        """Create a key for the given subject."""
        if err := self.auth.validate_access_token(subject, token):
            return err
        return self.aws.create_user_and_key(subject)

    async def list_topics(
        self,
        subject: str = Depends(extract_val('subject')),
        token: str = Depends(extract_val('authorization')),
    ):
        """List topics for the given subject."""
        if err := self.auth.validate_access_token(subject, token):
            return err
        return self.aws.topic_listing_route(subject)

    async def register_or_unregister_topic(
        self,
        topic: str,
        subject: str = Depends(extract_val('subject')),
        token: str = Depends(extract_val('authorization')),
        action: str = Depends(extract_val('action')),
    ):
        """Register or unregister a topic based on the action."""
        if err := (
            self.auth.validate_access_token(subject, token)
            or self.auth.validate_name(topic)
        ):
            return err
        return self.aws.topic_registration_route(subject, topic, action)

    async def get_topic_configs(
        self,
        topic: str,
        subject: str = Depends(extract_val('subject')),
        token: str = Depends(extract_val('authorization')),
    ):
        """Get configurations for a specific topic."""
        if err := (
            self.auth.validate_access_token(subject, token)
            or self.auth.validate_name(topic)
            or self.validate_topic_access(subject, topic)
        ):
            return err
        return self.aws.topic_configs_get_route(topic, {})

    async def update_topic_configs(
        self,
        request: Request,
        topic: str,
        subject: str = Depends(extract_val('subject')),
        token: str = Depends(extract_val('authorization')),
    ):
        """Update configurations for a specific topic."""
        if err := (
            self.auth.validate_access_token(subject, token)
            or self.auth.validate_name(topic)
            or self.validate_topic_access(subject, topic)
        ):
            return err

        try:
            body = await request.json()
            configs = {key: body[key] for key in body}
            return self.aws.topic_configs_update_route(topic, configs)
        except Exception as e:
            return {
                'status': 'error',
                'reason': f'Error decoding the function body: {e}',
            }

    async def reset_topic(
        self,
        request: Request,
        topic: str,
        subject: str = Depends(extract_val('subject')),
        token: str = Depends(extract_val('authorization')),
    ):
        """Reset a topic by deleting all messages and configurations."""
        if err := (
            self.auth.validate_access_token(subject, token)
            or self.auth.validate_name(topic)
            or self.validate_topic_access(subject, topic)
        ):
            return err

        try:
            return self.aws.reset_topic_route(topic)
        except Exception as e:
            return {'status': 'error', 'reason': f'Error resetting topic: {e}'}

    async def list_topic_users(
        self,
        request: Request,
        topic: str,
        subject: str = Depends(extract_val('subject')),
        token: str = Depends(extract_val('authorization')),
    ):
        """List users with access to a specific topic."""
        if err := (
            self.auth.validate_access_token(subject, token)
            or self.auth.validate_name(topic)
            or self.validate_topic_access(subject, topic)
        ):
            return err
        return self.aws.list_topic_users_route(topic)

    async def update_topic_partitions(
        self,
        topic: str,
        subject: str = Depends(extract_val('subject')),
        token: str = Depends(extract_val('authorization')),
        newpartitions: str = Depends(extract_val('newpartitions')),
    ):
        """Update the number of partitions for a specific topic."""
        if err := (
            self.auth.validate_access_token(subject, token)
            or self.auth.validate_name(topic)
            or self.validate_topic_access(subject, topic)
        ):
            return err

        try:
            newpartitions_int = int(newpartitions)
            if (
                newpartitions_int > DiasporaService.PARTITION_MAX
                or newpartitions_int < DiasporaService.PARTITION_MIN
            ):
                return {
                    'status': 'error',
                    'reason': 'Invalid number of partitions: must be [1,4].',
                }
            return self.aws.topic_partitions_update_route(
                topic,
                newpartitions_int,
            )
        except ValueError:
            return {
                'status': 'error',
                'reason': 'newpartitions must be an integer in string format.',
            }

    async def grant_or_revoke_user_access(  # noqa: PLR0913
        self,
        topic: str,
        subject: str = Depends(extract_val('subject')),
        token: str = Depends(extract_val('authorization')),
        action: str = Depends(extract_val('action')),
        user: str = Depends(extract_val('user')),
    ):
        """Grant or revoke access to a topic for a specific user."""
        if err := (
            self.auth.validate_access_token(subject, token)
            or self.auth.validate_name(topic)
            or self.validate_topic_access(subject, topic)
        ):
            return err
        return self.aws.topic_user_access_route(subject, topic, user, action)

    async def register_topic(
        self,
        topic: str = Depends(extract_val('topic')),
        subject: str = Depends(extract_val('subject')),
        token: str = Depends(extract_val('authorization')),
    ):
        """Register a topic for the given subject."""
        if err := (
            self.auth.validate_access_token(subject, token)
            or self.auth.validate_name(topic)
        ):
            return err
        return self.aws.topic_registration_route(subject, topic, 'register')

    async def unregister_topic(
        self,
        topic: str = Depends(extract_val('topic')),
        subject: str = Depends(extract_val('subject')),
        token: str = Depends(extract_val('authorization')),
    ):
        """Unregister a topic for the given subject."""
        if err := (
            self.auth.validate_access_token(subject, token)
            or self.auth.validate_name(topic)
        ):
            return err
        return self.aws.topic_registration_route(subject, topic, 'unregister')

    async def list_triggers(
        self,
        subject: str = Depends(extract_val('subject')),
        token: str = Depends(extract_val('authorization')),
    ):
        """List triggers for the given subject."""
        if err := self.auth.validate_access_token(subject, token):
            return err
        return self.aws.trigger_listing_route(subject)

    async def create_or_delete_trigger(  # noqa: PLR0913
        self,
        request: Request,
        subject: str = Depends(extract_val('subject')),
        token: str = Depends(extract_val('authorization')),
        action: str = Depends(extract_val('action')),
        topic: str = Depends(extract_val('topic')),
        trigger: str = Depends(extract_val('trigger')),
    ):
        """Create or delete a trigger for a specific topic."""
        if err := (
            self.auth.validate_access_token(subject, token)
            or self.auth.validate_name(topic)
            or self.auth.validate_name(trigger, 'trigger')
            or self.validate_topic_access(subject, topic)
        ):
            return err

        try:
            function_configs, trigger_configs = {}, {}
            body = await request.json()
            assert (
                'function' in body
            ), 'Function configuration must be included in the request.'

            assert (
                'trigger' in body
            ), 'Trigger configuration must be included in the request.'

            if 'Code' in body['function']:
                if 'ZipFile' not in body['function']['Code']:
                    function_configs['Code'] = body['function']['Code']
                else:
                    function_configs['Code'] = {
                        'ZipFile': base64.b64decode(
                            body['function']['Code']['ZipFile'],
                        ),
                    }

            for key in WEB_SERVICE_LAMBDA_CONFIGS:
                if key in body['function']:
                    function_configs[key] = body['function'][key]

            for key in WEB_SERVICE_TRIGGER_CONFIGS:
                if key in body['trigger']:
                    trigger_configs[key] = body['trigger'][key]

            return self.aws.trigger_creation_route(
                subject,
                topic,
                trigger,
                action,
                function_configs,
                trigger_configs,
            )
        except Exception as e:
            return {
                'status': 'error',
                'reason': f'Error processing the request: {e}',
            }

    async def update_trigger(
        self,
        request: Request,
        trigger_id: str,
        subject: str = Depends(extract_val('subject')),
        token: str = Depends(extract_val('authorization')),
    ):
        """Update a trigger configuration."""
        if err := self.auth.validate_access_token(subject, token):
            return err

        try:
            body = await request.json()
            trigger_configs = {}
            for key in WEB_SERVICE_TRIGGER_CONFIGS:
                if key in body:
                    trigger_configs[key] = body[key]

            return self.aws.event_source_mapping_update_route(
                subject,
                trigger_id,
                trigger_configs,
            )
        except Exception as e:
            return {
                'status': 'error',
                'reason': f'Error decoding the function body: {e}',
            }

    async def list_log_streams(
        self,
        subject: str = Depends(extract_val('subject')),
        token: str = Depends(extract_val('authorization')),
        trigger: str = Depends(extract_val('trigger')),
    ):
        """List log streams for the given trigger."""
        if err := self.auth.validate_access_token(subject, token):
            return err
        return self.aws.list_log_streams(subject, trigger)

    async def get_log_events(
        self,
        subject: str = Depends(extract_val('subject')),
        token: str = Depends(extract_val('authorization')),
        trigger: str = Depends(extract_val('trigger')),
        stream: str = Depends(extract_val('stream')),
    ):
        """Get log events for the given trigger and stream."""
        if err := self.auth.validate_access_token(subject, token):
            return err
        return self.aws.get_log_events(subject, trigger, stream)


service = DiasporaService()
app = service.app

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
