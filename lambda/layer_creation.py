"""Lambda function layer creator.

a Lambda function that creates lambda layer for Diaspora Trigger
and CICD pipeline as needed.
"""

from __future__ import annotations

import os
import subprocess
import zipfile
from typing import Any

import boto3


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Lambda function."""
    # Initial setup: package name(s) and S3 bucket details
    try:
        # for Diaspora Trigger
        # packages = ['globus_sdk', 'diaspora-event-sdk[kafka-python]']
        
        # for CICD monitor
        packages = ['requests']
        bucket_name = 'fabric-lambda-layers-bucket'
        object_key = 'python_package_requests.zip'

        # Target directory for package installation
        package_dir = '/tmp/python/lib/python3.12/site-packages/'
        os.makedirs(package_dir, exist_ok=True)

        # Install the package(s) using pip
        for package in packages:
            subprocess.run(
                ['pip', 'install', package, '--target', package_dir],
                check=True,
            )

        # Create a zip archive of the installed package(s)
        with zipfile.ZipFile(
            '/tmp/python_package.zip',
            'w',
            zipfile.ZIP_DEFLATED,
        ) as zipf:
            for root, _, files in os.walk('/tmp/python'):
                for file in files:
                    file_path = os.path.join(root, file)
                    zipf.write(file_path, os.path.relpath(file_path, '/tmp'))

        # Upload the zip file to S3
        s3_client = boto3.client('s3')
        s3_client.upload_file(
            '/tmp/python_package.zip',
            bucket_name,
            object_key,
        )

        return {
            'statusCode': 200,
            'body': f'Successfully installed, zipped, and uploaded package to s3://{bucket_name}/{object_key}',
        }

    except Exception as e:
        print(e)
        return {
            'statusCode': 500,
            'body': 'An error occurred during the process.',
        }
