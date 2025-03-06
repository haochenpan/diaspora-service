"""Common utility functions."""

from __future__ import annotations

import os


class EnvironmentChecker:
    """Check if environment variables are set."""

    @staticmethod
    def check_env_variables(*variables: str) -> None:
        """Check if each environment variable in vars is set."""
        for var in variables:
            assert os.getenv(var), f'{var} environment variable is not set.'


if __name__ == '__main__':
    EnvironmentChecker.check_env_variables(
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY',
        'SERVER_CLIENT_ID',
        'SERVER_SECRET',
        'AWS_ACCOUNT_ID',
        'AWS_ACCOUNT_REGION',
        'MSK_CLUSTER_NAME',
        'MSK_CLUSTER_ARN_SUFFIX',
    )
