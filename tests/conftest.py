"""Pytest configuration for web_service tests."""

from __future__ import annotations

import socket
import warnings
from collections.abc import Iterator
from typing import Any

import pytest

from tests.live_identity import live_subject

# Every test in these modules builds real IAM, DynamoDB or Kafka clients
# and talks to AWS, so all of them run only in the integration set.
LIVE_TEST_MODULES = frozenset(
    {
        'db_service_test.py',
        'iam_service_test.py',
        'kafka_service_test.py',
        'namespace_service_test.py',
        'web_service_test.py',
    },
)

_NETWORK_FAMILIES = (socket.AF_INET, socket.AF_INET6)
_LOOKUPS = (
    'getaddrinfo',
    'gethostbyname',
    'gethostbyname_ex',
    'gethostbyaddr',
    'getnameinfo',
)


# Configure pytest to ignore deprecation warnings from external libraries
def pytest_configure(config: pytest.Config) -> None:
    """Configure pytest to suppress deprecation warnings."""
    # Suppress all deprecation warnings
    config.addinivalue_line(
        'filterwarnings',
        'ignore::DeprecationWarning',
    )
    # Specifically suppress SSL deprecation warnings from kafka library
    config.addinivalue_line(
        'filterwarnings',
        'ignore:ssl.PROTOCOL_TLS is deprecated:DeprecationWarning',
    )


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(
    config: pytest.Config,
    items: list[pytest.Item],
) -> None:
    """Mark every test in a live module as an integration test."""
    for item in items:
        if item.path.name in LIVE_TEST_MODULES:
            item.add_marker(pytest.mark.integration)


@pytest.fixture
def random_subject() -> str:
    """Return a new subject named the way the live identity allows."""
    return live_subject()


class NetworkBlockedError(RuntimeError):
    """A test outside the integration set tried to use the network."""


@pytest.fixture(autouse=True)
def _block_network_outside_integration(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[None]:
    """Fail any test not marked integration that opens a network socket.

    The test fails even when the code under test swallows the error, so a
    test that needs AWS or Kafka cannot pass in the fast set by accident.
    """
    if request.node.get_closest_marker('integration'):
        yield
        return

    attempts: list[str] = []
    real_connect = socket.socket.connect
    real_connect_ex = socket.socket.connect_ex

    def _refuse(what: str) -> NetworkBlockedError:
        attempts.append(what)
        return NetworkBlockedError(
            f'{what} attempted outside an integration test',
        )

    def _connect(sock: socket.socket, address: Any) -> None:
        if sock.family in _NETWORK_FAMILIES:
            raise _refuse(f'connect to {address!r}')
        real_connect(sock, address)

    def _connect_ex(sock: socket.socket, address: Any) -> int:
        if sock.family in _NETWORK_FAMILIES:
            raise _refuse(f'connect to {address!r}')
        return real_connect_ex(sock, address)

    def _lookup(host: Any, *args: Any, **kwargs: Any) -> Any:
        raise _refuse(f'DNS lookup of {host!r}')

    monkeypatch.setattr(socket.socket, 'connect', _connect)
    monkeypatch.setattr(socket.socket, 'connect_ex', _connect_ex)
    for name in _LOOKUPS:
        monkeypatch.setattr(socket, name, _lookup)
    yield
    if attempts:
        pytest.fail(
            'Network use in a test not marked integration: '
            + '; '.join(attempts),
        )


# Also set up warnings filter at module level
warnings.filterwarnings('ignore', category=DeprecationWarning)
warnings.filterwarnings(
    'ignore',
    message='.*ssl.PROTOCOL_TLS.*',
    category=DeprecationWarning,
)
