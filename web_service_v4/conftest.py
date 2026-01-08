"""Pytest configuration for web_service_v4 tests."""

import warnings

import pytest


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


# Also set up warnings filter at module level
warnings.filterwarnings('ignore', category=DeprecationWarning)
warnings.filterwarnings(
    'ignore',
    message='.*ssl.PROTOCOL_TLS.*',
    category=DeprecationWarning,
)

