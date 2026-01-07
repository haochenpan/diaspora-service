"""Response types for AWSManagerV3 operations."""

from __future__ import annotations

from typing import Any


def combine_user_creation_result(
    iam_result: dict[str, bool],
    namespace_result: dict[str, Any],
) -> dict[str, Any]:
    """Combine IAM and namespace creation results."""
    if namespace_result.get('status') != 'success':
        return {
            'status': 'success',
            'message': 'IAM user created',
            'user_created': str(iam_result.get('user_created', False)),
            'policy_created': str(iam_result.get('policy_created', False)),
            'policy_attached': str(iam_result.get('policy_attached', False)),
            'namespace_created': 'false',
            'namespace_error': namespace_result.get(
                'message',
                'Unknown error',
            ),
        }
    result: dict[str, Any] = {
        'status': 'success',
        'message': 'IAM user created',
        'user_created': str(iam_result.get('user_created', False)),
        'policy_created': str(iam_result.get('policy_created', False)),
        'policy_attached': str(iam_result.get('policy_attached', False)),
        'namespace_created': 'true',
    }
    namespace = namespace_result.get('namespace')
    if namespace:
        result['namespace'] = namespace
    return result


def combine_user_deletion_result(
    namespace_result: dict[str, bool],
    iam_result: dict[str, Any],
) -> dict[str, Any]:
    """Combine namespace and IAM deletion results."""
    return {
        'status': iam_result.get('status', 'success'),
        'message': iam_result.get('message', ''),
        'namespaces_deleted': str(
            namespace_result.get('namespaces_deleted', False),
        ),
        'keys_deleted': str(iam_result.get('keys_deleted', False)),
        'policy_detached': str(iam_result.get('policy_detached', False)),
        'policy_deleted': str(iam_result.get('policy_deleted', False)),
        'user_deleted': str(iam_result.get('user_deleted', False)),
    }


def create_success_result(message: str) -> dict[str, Any]:
    """Create a success result dictionary."""
    return {'status': 'success', 'message': message}


def create_failure_result(
    message: str,
    error: str | None = None,
) -> dict[str, Any]:
    """Create a failure result dictionary."""
    result: dict[str, Any] = {'status': 'failure', 'message': message}
    if error:
        result['error'] = error
    return result
