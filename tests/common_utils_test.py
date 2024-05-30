from __future__ import annotations

import pytest

from common.utils import EnvironmentChecker


def test_check_env_variables_all_set(monkeypatch):
    # Set all environment variables
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'fake_access_key_id')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'fake_secret_access_key')
    monkeypatch.setenv('CLIENT_ID', 'fake_client_id')
    monkeypatch.setenv('CLIENT_SECRET', 'fake_client_secret')
    monkeypatch.setenv('CLIENT_SCOPE', 'fake_client_scope')
    monkeypatch.setenv('DEFAULT_SERVERS', 'fake_default_servers')

    # No exception should be raised if all variables are set
    EnvironmentChecker.check_env_variables(
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY',
        'CLIENT_ID',
        'CLIENT_SECRET',
        'CLIENT_SCOPE',
        'DEFAULT_SERVERS',
    )


def test_check_one_env_variable_missing_1(monkeypatch):
    # Set some environment variables, but leave out CLIENT_SECRET
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'fake_access_key_id')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'fake_secret_access_key')
    monkeypatch.setenv('CLIENT_ID', 'fake_client_id')
    monkeypatch.delenv('CLIENT_SECRET', raising=False)
    monkeypatch.setenv('CLIENT_SCOPE', 'fake_client_scope')
    monkeypatch.setenv('DEFAULT_SERVERS', 'fake_default_servers')

    # Expect an assertion error because CLIENT_SECRET is not set
    with pytest.raises(AssertionError) as excinfo:
        EnvironmentChecker.check_env_variables(
            'AWS_ACCESS_KEY_ID',
            'AWS_SECRET_ACCESS_KEY',
            'CLIENT_ID',
            'CLIENT_SECRET',  # This one is missing
            'CLIENT_SCOPE',
            'DEFAULT_SERVERS',
        )

    assert (
        str(excinfo.value) == 'CLIENT_SECRET environment variable is not set.'
    )


def test_check_one_env_variable_missing_2(monkeypatch):
    # Set some environment variables, but leave out DEFAULT_SERVERS
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'fake_access_key_id')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'fake_secret_access_key')
    monkeypatch.setenv('CLIENT_ID', 'fake_client_id')
    monkeypatch.setenv('CLIENT_SECRET', 'fake_client_secret')
    monkeypatch.setenv('CLIENT_SCOPE', 'fake_client_scope')
    monkeypatch.delenv('DEFAULT_SERVERS', raising=False)

    # Expect an assertion error because DEFAULT_SERVERS is not set
    with pytest.raises(AssertionError) as excinfo:
        EnvironmentChecker.check_env_variables(
            'AWS_ACCESS_KEY_ID',
            'AWS_SECRET_ACCESS_KEY',
            'CLIENT_ID',
            'CLIENT_SECRET',
            'CLIENT_SCOPE',
            'DEFAULT_SERVERS',  # This one is missing
        )

    assert (
        str(excinfo.value)
        == 'DEFAULT_SERVERS environment variable is not set.'
    )


def test_check_no_env_variables_set(monkeypatch):
    # Ensure no environment variables are set
    monkeypatch.delenv('AWS_ACCESS_KEY_ID', raising=False)
    monkeypatch.delenv('AWS_SECRET_ACCESS_KEY', raising=False)
    monkeypatch.delenv('CLIENT_ID', raising=False)
    monkeypatch.delenv('CLIENT_SECRET', raising=False)
    monkeypatch.delenv('CLIENT_SCOPE', raising=False)
    monkeypatch.delenv('DEFAULT_SERVERS', raising=False)

    # Expect an assertion error because no environment variables are set
    with pytest.raises(AssertionError):
        EnvironmentChecker.check_env_variables(
            'AWS_ACCESS_KEY_ID',
            'AWS_SECRET_ACCESS_KEY',
            'CLIENT_ID',
            'CLIENT_SECRET',
            'CLIENT_SCOPE',
            'DEFAULT_SERVERS',
        )


def test_check_only_one_env_variable_set(monkeypatch):
    # Set only one environment variable
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'fake_access_key_id')
    monkeypatch.delenv('AWS_SECRET_ACCESS_KEY', raising=False)
    monkeypatch.delenv('CLIENT_ID', raising=False)
    monkeypatch.delenv('CLIENT_SECRET', raising=False)
    monkeypatch.delenv('CLIENT_SCOPE', raising=False)
    monkeypatch.delenv('DEFAULT_SERVERS', raising=False)

    # Expect an assertion error because not all environment variables are set
    with pytest.raises(AssertionError) as excinfo:
        EnvironmentChecker.check_env_variables(
            'AWS_ACCESS_KEY_ID',
            'AWS_SECRET_ACCESS_KEY',
            'CLIENT_ID',
            'CLIENT_SECRET',
            'CLIENT_SCOPE',
            'DEFAULT_SERVERS',
        )

    assert (
        str(excinfo.value)
        == 'AWS_SECRET_ACCESS_KEY environment variable is not set.'
    )
