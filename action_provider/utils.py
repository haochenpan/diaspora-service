"""Utility functions for the Action Provider."""

from __future__ import annotations

import os


class EnvironmentChecker:
    """Check if environment variables are set."""

    @staticmethod
    def check_env_variables(*variables):
        """Check if each environment variable in vars is set."""
        for var in variables:
            assert os.getenv(var), f'{var} environment variable is not set.'


if __name__ == '__main__':
    EnvironmentChecker.check_env_variables(
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY',
        'CLIENT_ID',
        'CLIENT_SECRET',
        'CLIENT_SCOPE',
        'DEFAULT_SERVERS',
    )
