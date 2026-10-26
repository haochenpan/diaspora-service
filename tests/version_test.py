"""Unit tests for the version the service reports."""

from __future__ import annotations

import importlib.metadata as importlib_metadata
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from web_service.main import DiasporaService
from web_service.main import service_version

PACKAGE_VERSION = importlib_metadata.version('diaspora_service')
COMMIT = '0123456789abcdef0123456789abcdef01234567'  # pragma: allowlist secret


def _service() -> DiasporaService:
    with (
        patch('web_service.main.AuthManager'),
        patch('web_service.main.IAMService'),
        patch('web_service.main.KafkaService'),
        patch('web_service.main.DynamoDBService'),
        patch('web_service.main.NamespaceService'),
        patch('web_service.main.WebService'),
        patch('web_service.main.ConsumerService'),
    ):
        return DiasporaService()


@pytest.mark.parametrize('value', (None, '', '  '))
def test_version_without_a_commit_is_the_package_version(
    monkeypatch: pytest.MonkeyPatch,
    value: str | None,
) -> None:
    if value is None:
        monkeypatch.delenv('GIT_SHA', raising=False)
    else:
        monkeypatch.setenv('GIT_SHA', value)
    assert service_version() == PACKAGE_VERSION


def test_version_names_the_first_12_characters_of_the_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('GIT_SHA', COMMIT)
    assert service_version() == f'{PACKAGE_VERSION}+0123456789ab'


def test_openapi_and_the_docs_page_report_the_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('GIT_SHA', COMMIT)
    client = TestClient(_service().app)

    spec = client.get('/openapi.json')
    assert spec.status_code == 200
    assert spec.json()['info']['version'] == f'{PACKAGE_VERSION}+0123456789ab'

    # The page at / renders the spec it loads from /openapi.json, version
    # included; Lightsail's health check requests it
    page = client.get('/')
    assert page.status_code == 200
    assert page.headers['content-type'].startswith('text/html')
    assert '/openapi.json' in page.text
