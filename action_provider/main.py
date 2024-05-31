"""Flask application for the Diaspora Action Provider."""

from __future__ import annotations

import os

from flask import Blueprint
from flask import Flask
from globus_action_provider_tools import ActionProviderDescription
from globus_action_provider_tools import ActionRequest
from globus_action_provider_tools import AuthState
from globus_action_provider_tools.flask import add_action_routes_to_blueprint
from globus_action_provider_tools.flask.helpers import assign_json_provider
from globus_action_provider_tools.flask.types import ActionCallbackReturn

from action_provider import __version__
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
    """Produce or consume events."""
    status = build_action_status(auth)
    return status, 200
    # action = request.body['action']
    # if action == 'produce':
    #     return action_produce(request, auth)
    # else:
    #     return action_consume(request, auth)


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
        api_version=__version__,
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


if __name__ == '__main__':
    EnvironmentChecker.check_env_variables(
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY',
        'CLIENT_ID',
        'CLIENT_SECRET',
        'CLIENT_SCOPE',
        'DEFAULT_SERVERS',
    )
    app = create_app()
    app.run(host='0.0.0.0', port=8000, debug=True)
