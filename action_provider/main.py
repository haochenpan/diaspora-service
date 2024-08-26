"""Flask application for the Diaspora Action Provider."""

from __future__ import annotations

import importlib.metadata as importlib_metadata
import os

from cachetools import TTLCache
from flask import Blueprint
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
from globus_action_provider_tools.flask import add_action_routes_to_blueprint
from globus_action_provider_tools.flask.exceptions import ActionConflict
from globus_action_provider_tools.flask.exceptions import ActionNotFound
from globus_action_provider_tools.flask.helpers import assign_json_provider
from globus_action_provider_tools.flask.types import ActionCallbackReturn

from action_provider.action_consume import action_consume
from action_provider.action_produce import action_produce
from action_provider.utils import load_schema
from common.utils import EnvironmentChecker

# from action_provider.utils import build_action_status


CLIENT_ID = os.environ['CLIENT_ID']
CLIENT_SECRET = os.environ['CLIENT_SECRET']
CLIENT_SCOPE = os.environ['CLIENT_SCOPE']
DEFAULT_SERVERS = os.environ['DEFAULT_SERVERS']
CACHE_SIZE = 1000
CACHE_TTL = 24 * 60  # in seconds

_fake_request_db: TTLCache[str, tuple[ActionRequest, str]] = TTLCache(
    maxsize=CACHE_SIZE,
    ttl=CACHE_TTL,
)
_fake_action_db: TTLCache[str, ActionStatus] = TTLCache(
    maxsize=CACHE_SIZE,
    ttl=CACHE_TTL,
)


def _retrieve_action_status(action_id: str) -> ActionStatus:
    status = _fake_action_db.get(action_id)
    if status is None:
        raise ActionNotFound(f'No Action with id {action_id}')
    return status


def action_run(
    request: ActionRequest,
    auth: AuthState,
) -> ActionCallbackReturn:
    """Handle the action request to produce or consume events.

    This function processes the action specified in the request, either
    producing events (publishing) or consuming events (retrieving). The
    action type determines which internal function is called to handle the
    request.

    Parameters
    ----------

    request : ActionRequest
        The action request object containing the details of the request.

        The request body must include the following fields:

        - action (str): The action to perform. Must be either 'produce' or
          'consume'.

            - 'produce': Publish events to a topic.

            - 'consume': Retrieve recent events from a topic.

        - topic (str): The topic to publish or retrieve the events.

        Depending on the action type, additional fields are required:

        - For 'produce' action:

            - msgs (list of dict): List of events, each formatted as a JSON
            object.

            - keys (str or list of str, optional): Optional single event key or
            list of event keys.

        - For 'consume' action:

            - ts (int, optional): Timestamp in milliseconds since the epoch to
            start retrieving messages from. If not provided, the messages since
            the earliest available messages will be returned.

            - group_id (str, optional): Kafka consumer group ID for
            managing session state across multiple consumers.

            - filters (list of dict, optional): Criteria to filter messages
            based on patterns within their content. See notebook examples.

        Optional field:
        - servers (str): Comma-separated list of diaspora servers (for
          development use).

    auth : AuthState
        The authentication state object containing identity and
        authentication information of the requester.

    Returns:
    -------
    ActionCallbackReturn
        The result of the action, either from the `action_produce` or
        `action_consume` function based on the action type.

    Examples:
    --------
    The function can handle both 'produce' and 'consume' actions based on
    the request body:

    Example request for 'produce' action:

    ```python
    request_body = {
        "action": "produce",
        "topic": "example_topic",
        "msgs": [{"key1": "value1"}, {"key2": "value2"}],
        "keys": ["key1", "key2"]
    }
    ```

    Example request for 'consume' action:

    ```python
    request_body = {
        "action": "consume",
        "topic": "example_topic",
        "ts": 1620000000000  # Optional: if not provided, consume from
                             # the earliest messages.
    }
    ```

    See the notebooks for more filter examples.

    The function will call the appropriate internal function to handle the
    request:
    - `action_produce` for publishing events.
    - `action_consume` for retrieving events.
    """
    caller_id = auth.effective_identity
    request_id = request.request_id
    full_request_id = f'{caller_id}:{request_id}'
    print('Run endpoint is called with full_request_id =', full_request_id)
    print(request)

    prev_request = _fake_request_db.get(full_request_id)
    if prev_request is not None:
        print('Previous request is found =', prev_request)
        print(prev_request)

        action_id = prev_request[1]
        action_status = _fake_action_db[action_id]
        if action_status.status in ['SUCCEEDED', 'FAILED']:
            print('here1 - do nothing', action_status)
            return action_status
        elif prev_request[0] != request:
            print('here2 - conflict', action_status)
            raise ActionConflict(
                f'Request {request_id} already present with different param. ',
            )
        else:
            print('here3 - status = ', action_status.status)

    action = request.body['action']
    if action == 'produce':
        action_status = action_produce(request, auth)
    else:
        action_status = action_consume(request, auth)

    print('here 4 - action status', action_status)
    _fake_request_db[full_request_id] = (request, action_status.action_id)
    _fake_action_db[action_status.action_id] = action_status

    return action_status


def action_status(action_id: str, auth: AuthState) -> ActionCallbackReturn:
    """Action status endpoint."""
    print('Status endpoint is called with action_id =', action_id)
    status = _retrieve_action_status(action_id)
    authorize_action_access_or_404(status, auth)
    return status, 200


def action_cancel(action_id: str, auth: AuthState) -> ActionCallbackReturn:
    """Action cancel endpoint."""
    print('Cancel endpoint is called with action_id =', action_id)
    status = _retrieve_action_status(action_id)
    authorize_action_management_or_404(status, auth)

    # If action is already in complete state, return completion details
    if status.status in (
        ActionStatusValue.SUCCEEDED,
        ActionStatusValue.FAILED,
    ):
        return status

    # Process Action cancellation
    status.status = ActionStatusValue.FAILED
    status.display_status = 'Canceled by user request'
    return status


def action_release(action_id: str, auth: AuthState) -> ActionCallbackReturn:
    """Action release endpoint."""
    print('Release endpoint is called with action_id =', action_id)
    status = _retrieve_action_status(action_id)
    authorize_action_management_or_404(status, auth)

    # Error if attempt to release an active Action
    if status.status not in (
        ActionStatusValue.SUCCEEDED,
        ActionStatusValue.FAILED,
    ):
        raise ActionConflict('Action is not complete')

    _fake_action_db.pop(action_id)
    # Both fake and badly inefficient
    # remove_req_id: str | None = None
    # for req_id, req_and_action_id in _fake_request_db.items():
    #     if req_and_action_id[1] == action_id:
    #         remove_req_id = req_id
    #         break
    # if remove_req_id is not None:
    #     _fake_request_db.pop(remove_req_id)
    return status, 200


def create_app() -> Flask:
    """Create the Flask application."""
    app = Flask(__name__)
    assign_json_provider(app)
    app.url_map.strict_slashes = False

    skeleton_blueprint = Blueprint('diaspora', __name__)

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

    add_action_routes_to_blueprint(
        blueprint=skeleton_blueprint,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        client_name=None,
        provider_description=provider_description,
        action_run_callback=action_run,
        action_status_callback=action_status,
        action_cancel_callback=action_cancel,
        action_release_callback=action_release,
    )

    app.register_blueprint(skeleton_blueprint)

    return app


EnvironmentChecker.check_env_variables(
    'AWS_ACCESS_KEY_ID',
    'AWS_SECRET_ACCESS_KEY',
    'CLIENT_ID',
    'CLIENT_SECRET',
    'CLIENT_SCOPE',
    'DEFAULT_SERVERS',
)
app = create_app()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)
