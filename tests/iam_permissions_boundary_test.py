"""Tests for the optional permissions boundary on created IAM users."""

from __future__ import annotations

from datetime import datetime
from datetime import UTC
from unittest.mock import patch

import pytest
from botocore.stub import ANY
from botocore.stub import Stubber

from web_service.main import DiasporaService
from web_service.services import IAMService

ACCOUNT_ID = '123456789012'
SUBJECT = 'test-subject-testabcd1234'
USER_ARN = f'arn:aws:iam::{ACCOUNT_ID}:user/msk-users/{SUBJECT}'
BOUNDARY_ARN = (
    f'arn:aws:iam::{ACCOUNT_ID}:policy/diaspora-test/test-user-boundary'
)


def _iam_service(boundary: str | None) -> IAMService:
    return IAMService(
        account_id=ACCOUNT_ID,
        region='us-east-1',
        cluster_name='test-cluster',
        permissions_boundary_arn=boundary,
    )


def _create_with_stubbed_iam(
    iam_service: IAMService,
    expected_create_user: dict[str, str],
) -> dict[str, str]:
    """Run create_user_and_policy against a stubbed IAM client.

    The stubber fails the call if create_user gets any other parameters.
    """
    with Stubber(iam_service.iam) as stubber:
        stubber.add_response(
            'create_user',
            {
                'User': {
                    'Path': '/msk-users/',
                    'UserName': SUBJECT,
                    'UserId': 'AIDAEXAMPLEEXAMPLE01',
                    'Arn': USER_ARN,
                    'CreateDate': datetime(2026, 1, 1, tzinfo=UTC),
                },
            },
            expected_create_user,
        )
        stubber.add_response(
            'create_policy',
            {},
            {
                'PolicyName': SUBJECT,
                'Path': '/msk-policy/',
                'PolicyDocument': ANY,
            },
        )
        stubber.add_response(
            'attach_user_policy',
            {},
            {
                'UserName': SUBJECT,
                'PolicyArn': (
                    f'arn:aws:iam::{ACCOUNT_ID}:policy/msk-policy/{SUBJECT}'
                ),
            },
        )
        result = iam_service.create_user_and_policy(SUBJECT)
        stubber.assert_no_pending_responses()
    return result


@pytest.mark.parametrize('boundary', (None, ''))
def test_create_user_without_boundary(boundary: str | None) -> None:
    """Without a boundary, the user is created as before, with none set."""
    result = _create_with_stubbed_iam(
        _iam_service(boundary),
        {'Path': '/msk-users/', 'UserName': SUBJECT},
    )

    assert result['status'] == 'success'


def test_create_user_with_boundary() -> None:
    """With a boundary, every created user gets it."""
    result = _create_with_stubbed_iam(
        _iam_service(BOUNDARY_ARN),
        {
            'Path': '/msk-users/',
            'UserName': SUBJECT,
            'PermissionsBoundary': BOUNDARY_ARN,
        },
    )

    assert result['status'] == 'success'


@pytest.mark.parametrize('boundary', (None, BOUNDARY_ARN))
def test_service_reads_boundary_from_environment(
    boundary: str | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The service passes IAM_USER_PERMISSIONS_BOUNDARY_ARN to IAMService."""
    if boundary is None:
        monkeypatch.delenv('IAM_USER_PERMISSIONS_BOUNDARY_ARN', raising=False)
    else:
        monkeypatch.setenv('IAM_USER_PERMISSIONS_BOUNDARY_ARN', boundary)

    with (
        patch('web_service.main.AuthManager'),
        patch('web_service.main.IAMService') as iam_service_class,
        patch('web_service.main.KafkaService'),
        patch('web_service.main.DynamoDBService'),
        patch('web_service.main.NamespaceService'),
        patch('web_service.main.WebService'),
        patch('web_service.main.ConsumerService'),
    ):
        DiasporaService()

    kwargs = iam_service_class.call_args.kwargs
    assert kwargs['permissions_boundary_arn'] == boundary
