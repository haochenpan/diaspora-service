"""Lambda function that monitors the AP and reboots the container if needed."""

from __future__ import annotations

from typing import Any

import boto3
import requests

url = 'https://diaspora-action-provider.ml22sevubfnks.us-east-1.cs.amazonlightsail.com'
service_name = 'diaspora-action-provider'
power = 'micro'
normal_status_code = 200


def lambda_handler(event: dict[str, Any] | None, context: Any) -> str | None:
    """The entry point of the monitor function."""
    action = ''
    try:
        client = boto3.client('lightsail')
        response = client.get_container_services(serviceName=service_name)
        aws_status = response['containerServices'][0]['state']
        action = f'Status = {aws_status} '
    except Exception as e:
        print('Boto API error, do nothing.')
        print('ERROR', e)
        return None

    try:
        response = requests.get(url, timeout=7)
        http_code = response.status_code
        action += f'Code = {http_code} '
    except Exception as e:
        print('ERROR', e)
        http_code = 0

    if aws_status == 'RUNNING':
        if http_code == normal_status_code:
            action += 'Service operational.'
        else:
            client.update_container_service(
                serviceName=service_name,
                power=power,
                scale=1,
                isDisabled=True,
            )
            action += 'Service was running and has now been disabled.'

    elif aws_status == 'DISABLED':
        client.update_container_service(
            serviceName=service_name,
            power=power,
            scale=1,
            isDisabled=False,
        )
        action += 'Service was disabled and has now been enabled.'

    else:
        action += 'Service in reconfiguration thus no action.'

    print(action)
    return action


if __name__ == '__main__':
    lambda_handler(None, None)
