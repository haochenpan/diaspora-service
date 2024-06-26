"""Lambda function monitor.

a Lambda function that monitors the action provider
and web services and reboots the containers if needed.
"""

from __future__ import annotations

from typing import Any

import boto3
import requests  # type:ignore

ap_url = 'https://diaspora-action-provider.ml22sevubfnks.us-east-1.cs.amazonlightsail.com'
ap_service_name = 'diaspora-action-provider'
web_url = 'https://diaspora-web-service.ml22sevubfnks.us-east-1.cs.amazonlightsail.com'
web_service_name = 'diaspora-web-service'
power = 'micro'
normal_status_code = 200


def check_service_status(service_name: str, url: str) -> str:
    """Check the container status and reboot if needed."""
    client = boto3.client('lightsail')
    action = ''
    try:
        response = client.get_container_services(serviceName=service_name)
        aws_status = response['containerServices'][0]['state']
        action = f'{service_name} Status = {aws_status} '
    except Exception as e:
        print('Boto API error for', service_name)
        print('ERROR', e)

    try:
        response = requests.get(url, timeout=7)
        http_code = response.status_code
        action += f'{service_name} Code = {http_code} '
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


def lambda_handler(event: dict[str, Any] | None, context: Any) -> str | None:
    """The entry point of the monitor function."""
    action = ''
    action += check_service_status(ap_service_name, ap_url)
    action += check_service_status(web_service_name, web_url)
    return action


if __name__ == '__main__':
    lambda_handler(None, None)
