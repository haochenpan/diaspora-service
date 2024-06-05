"""Flask application for the Diaspora Action Provider."""

from __future__ import annotations

import importlib.metadata as importlib_metadata
import os

from flask import Blueprint
from flask import Flask
from globus_action_provider_tools import ActionProviderDescription
from globus_action_provider_tools import ActionRequest
from globus_action_provider_tools import AuthState
from globus_action_provider_tools.flask import add_action_routes_to_blueprint
from globus_action_provider_tools.flask.helpers import assign_json_provider
from globus_action_provider_tools.flask.types import ActionCallbackReturn

from action_provider.action_consume import action_consume
from action_provider.action_produce import action_produce
from action_provider.utils import build_action_status
from action_provider.utils import load_schema
from common.utils import EnvironmentChecker

CLIENT_ID = os.environ['CLIENT_ID']
CLIENT_SECRET = os.environ['CLIENT_SECRET']
CLIENT_SCOPE = os.environ['CLIENT_SCOPE']
DEFAULT_SERVERS = os.environ['DEFAULT_SERVERS']


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
    action = request.body['action']
    if action == 'produce':
        return action_produce(request, auth)
    else:
        return action_consume(request, auth)


def action_status(action_id: str, auth: AuthState) -> ActionCallbackReturn:
    """Placeholder status endpoint."""
    status = build_action_status(auth)
    return status, 200


def action_cancel(action_id: str, auth: AuthState) -> ActionCallbackReturn:
    """Placeholder cancel endpoint."""
    status = build_action_status(auth)
    return status


def action_release(action_id: str, auth: AuthState) -> ActionCallbackReturn:
    """Placeholder release endpoint."""
    status = build_action_status(auth)
    return status


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
