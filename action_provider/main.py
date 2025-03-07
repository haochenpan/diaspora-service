"""Flask application for the Diaspora Action Provider."""

from __future__ import annotations

import importlib.metadata as importlib_metadata
import os

from flask import Flask
from globus_action_provider_tools import ActionProviderDescription
from globus_action_provider_tools import ActionRequest
from globus_action_provider_tools import ActionStatus
from globus_action_provider_tools import ActionStatusValue
from globus_action_provider_tools import AuthState
from globus_action_provider_tools.authorization import (
    authorize_action_access_or_404,
)
from globus_action_provider_tools.authorization import (
    authorize_action_management_or_404,
)
from globus_action_provider_tools.flask import ActionProviderBlueprint
from globus_action_provider_tools.flask.exceptions import ActionConflict
from globus_action_provider_tools.flask.helpers import assign_json_provider
from globus_action_provider_tools.flask.types import ActionCallbackReturn

from action_provider.action_consume import action_consume
from action_provider.action_produce import action_produce
from action_provider.utils import _delete_action
from action_provider.utils import _delete_request
from action_provider.utils import _get_request_from_dynamo
from action_provider.utils import _get_status_request
from action_provider.utils import _insert_into_action_table
from action_provider.utils import _insert_into_request_table
from action_provider.utils import load_schema
from action_provider.utils import setup_logging
from common.utils import EnvironmentChecker

logger = setup_logging(__name__)


CLIENT_ID = os.environ['CLIENT_ID']
CLIENT_SECRET = os.environ['CLIENT_SECRET']
CLIENT_SCOPE = os.environ['CLIENT_SCOPE']
DEFAULT_SERVERS = os.environ['DEFAULT_SERVERS']


def perform_action(
    full_request_id: str,
    request: ActionRequest,
    auth: AuthState,
) -> ActionStatus:
    """Perform a produce or consume action."""
    action = request.body['action']
    if action == 'produce':
        action_status = action_produce(request, auth)
    else:
        action_status = action_consume(request, auth)

    _insert_into_request_table(
        full_request_id,
        request,
        action_status.action_id,
    )
    _insert_into_action_table(action_status, request)
    return action_status


def action_run(
    request: ActionRequest,
    auth: AuthState,
) -> ActionCallbackReturn:
    """Handle the action request to produce or consume events.

    Processes the action specified in the request by calling either
    `action_produce` or `action_consume` based on the action type.

    Parameters:
    ----------
    request : ActionRequest
        The action request object containing the details of the request.
    auth : AuthState
        The authentication state object containing identity and
        authentication information of the requester.

    Returns:
    -------
    ActionCallbackReturn
        The result of the action.
    """
    caller_id = auth.effective_identity
    request_id = request.request_id
    full_request_id = f'{caller_id}:{request_id}'
    logger.info(f'Run endpoint is called with request_id = {full_request_id}')
    logger.debug(f'new request = {request}')

    prev_request = _get_request_from_dynamo(full_request_id)
    if not prev_request:
        # New action
        action_status = perform_action(full_request_id, request, auth)
        return action_status

    logger.debug(f'old request = {prev_request}')
    if prev_request['request'] != request.json():
        raise ActionConflict(
            f'Request {request_id} already present with different param. ',
        )

    prev_action_id = prev_request['action_id']
    prev_status, _ = _get_status_request(prev_action_id)
    if prev_status.status in (
        ActionStatusValue.SUCCEEDED,
        ActionStatusValue.FAILED,
    ):
        return prev_status

    # Unfinished action
    action_status = perform_action(full_request_id, request, auth)
    return action_status


def action_status(action_id: str, auth: AuthState) -> ActionCallbackReturn:
    """Action status endpoint."""
    logger.info(f'Status endpoint called with action_id = {action_id}')
    status, request = _get_status_request(action_id)
    authorize_action_access_or_404(status, auth)

    # If action is already completed, return it
    logger.debug(f'old status = {status}')
    if status.status in (
        ActionStatusValue.SUCCEEDED,
        ActionStatusValue.FAILED,
    ):
        return status

    # Otherwise, perform the action
    caller_id = auth.effective_identity
    request_id = request.request_id
    full_request_id = f'{caller_id}:{request_id}'
    action_status = perform_action(full_request_id, request, auth)
    return action_status, 200


def action_cancel(action_id: str, auth: AuthState) -> ActionCallbackReturn:
    """Action cancel endpoint."""
    logger.info(f'Cancel endpoint called with action_id = {action_id}')
    status, request = _get_status_request(action_id)
    authorize_action_management_or_404(status, auth)

    # If action is already completed, return it
    logger.debug(f'old status = {status}')
    if status.status in (
        ActionStatusValue.SUCCEEDED,
        ActionStatusValue.FAILED,
    ):
        return status

    # Otherwise, cancel the action
    status.status = ActionStatusValue.FAILED
    status.display_status = 'Canceled by user request'
    _insert_into_action_table(status, request)
    return status


def action_release(action_id: str, auth: AuthState) -> ActionCallbackReturn:
    """Action release endpoint."""
    logger.info(f'Release endpoint called with action_id = {action_id}')
    status, request = _get_status_request(action_id)
    authorize_action_management_or_404(status, auth)

    # If action is already completed, raise an error
    if status.status not in (
        ActionStatusValue.SUCCEEDED,
        ActionStatusValue.FAILED,
    ):
        raise ActionConflict('Action is not complete')

    # Otherwise, release the action and request
    _delete_action(action_id)

    caller_id = auth.effective_identity
    request_id = request.request_id
    full_request_id = f'{caller_id}:{request_id}'
    _delete_request(full_request_id)
    return status, 200


def create_app() -> Flask:
    """Create the Flask application."""
    app = Flask(__name__)
    assign_json_provider(app)
    app.url_map.strict_slashes = False

    provider_description = ActionProviderDescription(
        globus_auth_scope=CLIENT_SCOPE,
        title='Diaspora Action Provider',
        admin_contact='haochenpan@uchicago.edu',
        administered_by=['Diaspora Team', 'Globus Labs'],
        api_version=importlib_metadata.version('diaspora_service'),
        synchronous=True,
        input_schema=load_schema(),
        log_supported=False,
        visible_to=['public'],
    )

    app.config['DIASPORA_CLIENT_ID'] = CLIENT_ID
    app.config['DIASPORA_CLIENT_SECRET'] = CLIENT_SECRET

    ap_blueprint = ActionProviderBlueprint(
        name='diaspora',
        import_name=__name__,
        provider_description=provider_description,
    )

    # Contributor: Stephen Rosen
    # https://github.com/haochenpan/diaspora-service/pull/42
    # register routes
    ap_blueprint.action_run(action_run)
    ap_blueprint.action_status(action_status)
    ap_blueprint.action_cancel(action_cancel)
    ap_blueprint.action_release(action_release)

    app.register_blueprint(ap_blueprint)

    return app


EnvironmentChecker.check_env_variables(
    'AWS_ACCESS_KEY_ID',
    'AWS_SECRET_ACCESS_KEY',
    'SERVER_CLIENT_ID',
    'SERVER_SECRET',
    'AWS_ACCOUNT_ID',
    'AWS_ACCOUNT_REGION',
    'MSK_CLUSTER_NAME',
    'MSK_CLUSTER_ARN_SUFFIX',
)
app = create_app()  # required by Dockerfile


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)
