"""Utility function for the web service."""

from __future__ import annotations

import contextlib
import json
import os
import re
import threading
import time
import uuid
from collections import defaultdict

import boto3
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from globus_sdk import ConfidentialAppAuthClient
from kafka import KafkaConsumer
from kafka import TopicPartition
from kafka.admin import ACL
from kafka.admin import ACLFilter
from kafka.admin import ACLOperation
from kafka.admin import ACLPermissionType
from kafka.admin import ACLResourcePatternType
from kafka.admin import ConfigResource
from kafka.admin import ConfigResourceType
from kafka.admin import KafkaAdminClient
from kafka.admin import NewPartitions
from kafka.admin import NewTopic
from kafka.admin import ResourcePattern
from kafka.admin import ResourcePatternFilter
from kafka.admin import ResourceType
from kafka.errors import TopicAlreadyExistsError

WEB_SERVICE_DESC = (
    '[Docs and Demo](https://haochenpan.github.io/diaspora-service/)'
)

WEB_SERVICE_TAGS_METADATA = [
    {
        'name': 'Authentication',
        'description': 'Cluster authentication and authorization',
    },
    {'name': 'Topic', 'description': 'Topic namespace management'},
    {'name': 'Trigger', 'description': 'Trigger management'},
    {'name': 'Legacy', 'description': 'Legacy APIs'},
    {'name': 'Proposed', 'description': 'Volatile APIs'},
]

WEB_SERVICE_LAMBDA_CONFIGS = [
    'Runtime',
    'Handler',
    'Timeout',
    'MemorySize',
    'Environment',
    'EphemeralStorage',
    'Layers',
]

WEB_SERVICE_TRIGGER_CONFIGS = [
    'Enabled',
    'BatchSize',
    'FilterCriteria',
    'MaximumBatchingWindowInSeconds',
    'StartingPosition',
]


class EnvironmentChecker:
    """Check if environment variables are set."""

    @staticmethod
    def check_env_variables(*variables):
        """Check if each environment variable in vars is set."""
        for var in variables:
            assert os.getenv(var), f'{var} environment variable is not set.'


class AuthManager:
    """Manage authentication and validation of access tokens."""

    TOKEN_MIN = 32
    TOKEN_MAX = 255
    NAME_MIN = 2
    NAME_MAX = 55

    def __init__(self, server_client_id, server_secret, client_id):
        """Initialize the AuthManager with client credentials."""
        self.client = ConfidentialAppAuthClient(
            server_client_id,
            server_secret,
        )
        self.action_scope = (
            f'https://auth.globus.org/scopes/{server_client_id}/action_all'
        )
        self.scopes = f'openid email profile {self.action_scope}'
        self.client_id = client_id  # SDK's internal_auth_client
        self.server_client_id = server_client_id

    def validate_access_token(self, user_id, token):  # noqa: PLR0911
        """Validate the access token for the given user ID."""
        print('validate_access_token')
        print(user_id)
        print(token)

        if not self.is_uuid(user_id):
            return self.error_response('Invalid user ID; expected UUID.')

        if not token.startswith('Bearer '):
            return self.error_response('Auth token must start with "Bearer ".')

        token = token.split(' ')[1]  # Extract the token part
        if not self.is_valid_token(token):
            return self.error_response('Invalid token format.')

        introspection_response = self.client.oauth2_token_introspect(token)
        if not introspection_response.get('active'):
            return self.error_response('Token is inactive.')

        if introspection_response.get('sub') != user_id:
            return self.error_response('Token does not belong to the user.')

        print(introspection_response)

        # if introspection_response.get('client_id') != self.client_id:
        #     return self.error_response(
        #         (
        #             "Token's client ID does not match. ",
        #             f"introspected = {introspection_response.get('client_id')}",  # noqa: E501
        #             f'client_id = {self.client_id}',
        #         ),
        #     )

        if self.server_client_id not in introspection_response.get('aud'):
            return self.error_response(
                (
                    'Not in the audience set. ',
                    f"introspected = {introspection_response.get('aud')}",
                    f'server_client_id = {self.server_client_id}',
                ),
            )

        if self.action_scope not in introspection_response.get('scope', ''):
            return self.error_response('Token lacks the scope for action.')

        # If all checks pass, return None to indicate success
        return None

    @staticmethod
    def error_response(reason):
        """Return an error response with the given reason."""
        return {'status': 'error', 'reason': reason}

    @staticmethod
    def is_uuid(value):
        """Check if the given value is a valid UUID."""
        try:
            uuid.UUID(value)
            return True
        except ValueError:
            return False

    @staticmethod
    def is_valid_token(token):
        """Check if the given token is valid."""
        return (
            len(token) > AuthManager.TOKEN_MIN
            and len(token) < AuthManager.TOKEN_MAX
            and re.match(r'^[\w]+$', token)
        )

    @staticmethod
    def validate_name(name, type_name='topic'):
        """Validate the given name for the specified type."""
        if not (
            AuthManager.NAME_MIN < len(name) < AuthManager.NAME_MAX
            and re.match(r'^[^\W_\-][\w\-]*$', name)
        ):
            return {
                'status': 'error',
                'reason': (
                    f'Invalid {type_name} name. Must be 3-64 characters, '
                    'including letters, digits, underscores, and hyphens, '
                    'and must not start with a hyphen or underscore.'
                ),
            }


class MSKTokenProvider:
    """Provide tokens for MSK authentication."""

    def token(self):
        """Generate and return an MSK auth token."""
        token, _ = MSKAuthTokenProvider.generate_auth_token('us-east-1')
        return token


class MSKTokenProviderFromRole:
    """Provide tokens for MSK authentication using a role ARN."""

    def __init__(self, open_id):
        """Initialize with the given open ID."""
        self.open_id = open_id

    def token(self):
        """Generate and return an MSK auth token using a role ARN."""
        token, _ = MSKAuthTokenProvider.generate_auth_token_from_role_arn(
            'us-east-1',
            f'arn:aws:iam::845889416464:role/ap/{self.open_id}-role',
        )
        return token


class AWSManager:
    """Manage AWS resource naming and policies."""

    class NamingManager:
        """Handle naming conventions for AWS resources."""

        def __init__(self, prefix, account_id, region, cluster_name):
            """Initialize with the given parameters."""
            self.prefix = prefix
            self.account_id = account_id
            self.region = region
            self.cluster_name = cluster_name

        def policy_name(self, open_id, lambda_name):
            """Generate a policy name based on the open ID and lambda name."""
            return f'{self.prefix}-policy-{open_id[-12:]}-{lambda_name}'

        def role_name(self, open_id, lambda_name):
            """Generate a role name based on the open ID and lambda name."""
            return f'{self.prefix}-role-{open_id[-12:]}-{lambda_name}'

        def function_name(self, open_id, lambda_name):
            """Generate a function name based on the open ID and lambda name."""  # noqa: E501
            return f'{self.prefix}-func-{open_id[-12:]}-{lambda_name}'

        def policy_arn(self, open_id, lambda_name):
            """Generate a policy ARN based on the open ID and lambda name."""
            policy_name = self.policy_name(open_id, lambda_name)
            return (
                f'arn:aws:iam::{self.account_id}:'
                f'policy/{self.prefix}/{policy_name}'
            )

        def role_arn(self, open_id, lambda_name):
            """Generate a role ARN based on the open ID and lambda name."""
            role_name = self.role_name(open_id, lambda_name)
            return (
                f'arn:aws:iam::{self.account_id}:'
                f'role/{self.prefix}/{role_name}'
            )

        def function_arn(self, open_id, lambda_name):
            """Generate a function ARN based on the open ID and lambda name."""
            function_name = self.function_name(open_id, lambda_name)
            return (
                f'arn:aws:lambda:{self.region}:'
                f'{self.account_id}:function:{function_name}'
            )

        def function_log_name(self, open_id, lambda_name):
            """Generate a function log name based on the open ID and lambda name."""  # noqa: E501
            return f'/aws/lambda/{self.function_name(open_id, lambda_name)}'

        def iam_user_policy(self):
            """Generate an IAM user policy for Kafka access."""
            return {
                'Version': '2012-10-17',
                'Statement': [
                    {
                        'Effect': 'Allow',
                        'Action': [
                            'kafka-cluster:Connect',
                            'kafka-cluster:DescribeTopic',
                            'kafka-cluster:ReadData',
                            'kafka-cluster:WriteData',
                            'kafka-cluster:DescribeGroup',
                            'kafka-cluster:AlterGroup',
                        ],
                        'Resource': [
                            f'arn:aws:kafka:us-east-1:{self.account_id}:group/{self.cluster_name}/*',
                            f'arn:aws:kafka:us-east-1:{self.account_id}:cluster/{self.cluster_name}/*',
                            f'arn:aws:kafka:us-east-1:{self.account_id}:transactional-id/{self.cluster_name}/*',
                            f'arn:aws:kafka:us-east-1:{self.account_id}:topic/{self.cluster_name}/*/__connection_test',
                        ],
                    },
                ],
            }

        def lambda_role_trust_policy(self):
            """Generate a trust policy for a Lambda role."""
            return {
                'Version': '2012-10-17',
                'Statement': [
                    {
                        'Effect': 'Allow',
                        'Principal': {'Service': 'lambda.amazonaws.com'},
                        'Action': 'sts:AssumeRole',
                    },
                ],
            }

        def lambda_policy(self, open_id, lambda_name, topic_name):
            """Generate a policy for Lambda based on open ID, lambda name, and topic name."""  # noqa: E501
            return {
                'Version': '2012-10-17',
                'Statement': [
                    {
                        'Effect': 'Allow',
                        'Action': [
                            'kafka:DescribeCluster',
                            'kafka:DescribeClusterV2',
                            'kafka:GetBootstrapBrokers',
                            'ec2:CreateNetworkInterface',
                            'ec2:DescribeNetworkInterfaces',
                            'ec2:DescribeVpcs',
                            'ec2:DeleteNetworkInterface',
                            'ec2:DescribeSubnets',
                            'ec2:DescribeSecurityGroups',
                            'logs:CreateLogGroup',
                            'logs:CreateLogStream',
                            'logs:PutLogEvents',
                        ],
                        'Resource': '*',
                    },
                    {
                        'Effect': 'Allow',
                        'Action': [
                            'kafka-cluster:Connect',
                            'kafka-cluster:DescribeGroup',
                            'kafka-cluster:AlterGroup',
                            'kafka-cluster:DescribeTopic',
                            'kafka-cluster:ReadData',
                            'kafka-cluster:DescribeClusterDynamicConfiguration',
                        ],
                        'Resource': [
                            f'arn:aws:kafka:us-east-1:{self.account_id}:group/{self.cluster_name}/*',
                            f'arn:aws:kafka:us-east-1:{self.account_id}:cluster/{self.cluster_name}/*',
                            f'arn:aws:kafka:us-east-1:{self.account_id}:topic/{self.cluster_name}/*/{topic_name}',
                        ],
                    },
                ],
            }

        def ap_role_name(self, open_id):
            """Generate a role name for an application based on the open ID."""
            return f'{open_id}-role'

        def ap_trust_policy(self):
            """Generate a trust policy for an application role."""
            return {
                'Version': '2012-10-17',
                'Statement': [
                    {
                        'Effect': 'Allow',
                        'Principal': {'AWS': 'arn:aws:iam::845889416464:root'},
                        'Action': 'sts:AssumeRole',
                        'Condition': {},
                    },
                ],
            }

    def __init__(  # noqa: PLR0913
        self,
        account_id,
        region,
        cluster_name,
        cluster_arn_suffix,
        iam_vpc,
        iam_public,
    ):
        """Init AWS Manager."""
        self.account_id = account_id
        self.region = region
        self.cluster_name = cluster_name
        self.cluster_arn = (
            f'arn:aws:kafka:{region}:{account_id}:'
            f'cluster/{cluster_name}/{cluster_arn_suffix}'
        )
        self.iam_vpc = iam_vpc
        self.iam_public = iam_public
        self.topic_arn_prefix = (
            f'arn:aws:kafka:{region}:{account_id}:topic/{cluster_name}/*/'
        )
        self.iam_policy_arn_prefix = (
            f'arn:aws:iam::{account_id}:policy/msk-policy/'
        )

        self.maning = self.NamingManager('t', account_id, region, cluster_name)
        self.iam = boto3.client('iam')
        self.kafka = boto3.client('kafka', region_name='us-east-1')
        self.lamb = boto3.client(
            'lambda',
            region_name='us-east-1',
        )
        self.logs = boto3.client(
            'logs',
            region_name='us-east-1',
        )
        self.admin = KafkaAdminClient(
            bootstrap_servers=iam_vpc,
            security_protocol='SASL_SSL',
            sasl_mechanism='OAUTHBEARER',
            sasl_oauth_token_provider=MSKTokenProvider(),
        )
        self.lock = threading.Lock()  # TODO

    def create_user_and_key(self, subject):
        """Create an IAM user if not exists and user credential."""
        with self.lock:
            # 1. create IAM user
            try:  # noqa: SIM105
                self.iam.create_user(Path='/msk-users/', UserName=subject)
            except self.iam.exceptions.EntityAlreadyExistsException:
                pass  # user already created

            # 2. create IAM policy
            try:  # noqa: SIM105
                self.iam.create_policy(
                    PolicyName=subject,
                    Path='/msk-policy/',
                    PolicyDocument=json.dumps(self.maning.iam_user_policy()),
                )
            except self.iam.exceptions.EntityAlreadyExistsException:
                pass  # policy already created

            # 3. attach the created policy to user
            self.iam.attach_user_policy(
                UserName=subject,
                PolicyArn=self.iam_policy_arn_prefix + subject,
            )

            self.create_user_role_if_not_exists(subject)  # for diaspora AP

            # 4. delete existing keys from IAM if there are any
            existing_keys = self.iam.list_access_keys(UserName=subject)
            for key in existing_keys['AccessKeyMetadata']:
                self.iam.delete_access_key(
                    UserName=subject,
                    AccessKeyId=key['AccessKeyId'],
                )

            # 5. create new key
            new_key = self.iam.create_access_key(UserName=subject)
            access_key = new_key['AccessKey']['AccessKeyId']
            secret_key = new_key['AccessKey']['SecretAccessKey']

            return {
                'status': 'success',
                'username': subject,
                'access_key': access_key,
                'secret_key': secret_key,
                'endpoint': self.iam_public,
            }

    def create_user_role_if_not_exists(self, subject):  # for AP
        """Create an IAM role that attach to the same user policy."""
        try:
            response = self.iam.create_role(
                Path='/ap/',
                RoleName=self.maning.ap_role_name(subject),
                AssumeRolePolicyDocument=json.dumps(
                    self.maning.ap_trust_policy(),
                ),
            )
            print(100, response)
        except Exception as e:
            print(f'Error creating IAM role for AP: {e}')

        try:
            response = self.iam.attach_role_policy(
                RoleName=self.maning.ap_role_name(subject),
                PolicyArn=self.iam_policy_arn_prefix + subject,
            )
            print(200, response)
        except Exception as e:
            print(f'Error attaching IAM policy for AP: {e}')

    def delete_user_role_if_exists(self, subject):  # for AP
        """Delete the IAM role that belong to the user."""
        try:
            response = self.iam.detach_role_policy(
                RoleName=self.maning.ap_role_name(subject),
                PolicyArn=self.iam_policy_arn_prefix + subject,
            )
            print(100, response)
        except Exception as e:
            print(f'Error detaching IAM policy for AP: {e}')

        try:
            response = self.iam.delete_role(
                RoleName=self.maning.ap_role_name(subject),
            )
            print(200, response)
        except Exception as e:
            print(f'Error deleting IAM role for AP: {e}')

    def _get_iam_policy(self, policy_arn):
        try:
            existing_policies = self.iam.list_policy_versions(
                PolicyArn=policy_arn,
            )
            policy = ''
            for ver in existing_policies['Versions']:
                # 1. delete non-default policies to avoid reaching the quota
                if not ver['IsDefaultVersion']:
                    self.iam.delete_policy_version(
                        PolicyArn=policy_arn,
                        VersionId=ver['VersionId'],
                    )
                else:
                    # 2. prepare the default (newest) policy to return
                    policy = self.iam.get_policy_version(
                        PolicyArn=policy_arn,
                        VersionId=ver['VersionId'],
                    )['PolicyVersion']['Document']
            return policy

        except self.iam.exceptions.NoSuchEntityException:
            pass  # policy already deleted

    def delete_user_and_key(self, subject):
        """Delete the IAM user and their credential."""
        with self.lock:
            # 1. delete existing keys from IAM if there are any
            try:
                existing_keys = self.iam.list_access_keys(UserName=subject)
                for key in existing_keys['AccessKeyMetadata']:
                    self.iam.delete_access_key(
                        UserName=subject,
                        AccessKeyId=key['AccessKeyId'],
                    )
            except self.iam.exceptions.NoSuchEntityException:
                pass

            self.delete_user_role_if_exists(subject)

            # 2. detach IAM policy
            try:
                policy_arn = self.iam_policy_arn_prefix + subject
                self.iam.detach_user_policy(
                    UserName=subject,
                    PolicyArn=policy_arn,
                )
            except self.iam.exceptions.NoSuchEntityException:
                pass

            # 3. delete IAM policy
            try:
                # delete non-default policies
                self._get_iam_policy(self.iam_policy_arn_prefix + subject)
                # delete default policy
                self.iam.delete_policy(
                    PolicyArn=self.iam_policy_arn_prefix + subject,
                )
            except self.iam.exceptions.NoSuchEntityException:
                pass

            # 4. delete IAM user
            with contextlib.suppress(
                self.iam.exceptions.NoSuchEntityException,
            ):
                self.iam.delete_user(UserName=subject)

    def _add_topic_to_policy(self, subject, topic):
        policy_arn = self.iam_policy_arn_prefix + subject
        policy = self._get_iam_policy(policy_arn)
        resources = policy['Statement'][0]['Resource']
        topic_arn = self.topic_arn_prefix + topic
        if topic_arn in resources:
            return False  # not updated

        resources.append(topic_arn)
        self.iam.create_policy_version(
            PolicyArn=policy_arn,
            PolicyDocument=json.dumps(policy, default=str),
            SetAsDefault=True,
        )
        return True  # updated

    def _remove_topic_from_policy(self, subject, topic):
        policy_arn = self.iam_policy_arn_prefix + subject
        policy = self._get_iam_policy(policy_arn)
        resources = policy['Statement'][0]['Resource']
        topic_arn = self.topic_arn_prefix + topic
        if topic_arn not in resources:
            return False  # not updated

        resources.remove(topic_arn)
        self.iam.create_policy_version(
            PolicyArn=policy_arn,
            PolicyDocument=json.dumps(policy, default=str),
            SetAsDefault=True,
        )
        return True  # updated

    def list_topics_for_principal(self, subject):
        """List topics that a subject has access to."""
        acl_filter = ACLFilter(
            principal=f'User:{subject}',
            host='*',
            operation=ACLOperation.ANY,
            permission_type=ACLPermissionType.ALLOW,
            resource_pattern=ResourcePatternFilter(
                resource_type=ResourceType.TOPIC,
                resource_name=None,
                pattern_type=ACLResourcePatternType.ANY,
            ),
        )

        result, error = self.admin.describe_acls(acl_filter)
        topics = [acl.resource_pattern.resource_name for acl in result]
        return sorted(set(topics))

    def list_principals_for_topic(self, topic):
        """List subjects that have access to a topic."""
        acl_filter = ACLFilter(
            principal=None,
            host='*',
            operation=ACLOperation.ANY,
            permission_type=ACLPermissionType.ALLOW,
            resource_pattern=ResourcePatternFilter(
                resource_type=ResourceType.TOPIC,
                resource_name=topic,
                pattern_type=ACLResourcePatternType.ANY,
            ),
        )
        result, error = self.admin.describe_acls(acl_filter)

        p_ops = defaultdict(lambda: set())
        for acl in result:
            principal, operation = (
                str(acl.principal)[len('User:') :],
                # str(acl.operation)[len('ACLOperation.') :],
                acl.operation.name,  # due to change of python version?
            )
            p_ops[principal].add(operation)
        return dict(p_ops)

    def create_topic_and_acl(self, subject, topic):
        """Default policy for creating a topic."""
        try:
            topic_list = [
                NewTopic(name=topic, num_partitions=1, replication_factor=2),
            ]
            self.admin.create_topics(new_topics=topic_list)
        except TopicAlreadyExistsError:
            pass

        acl0 = ACL(
            principal=f'User:{subject}',
            host='*',
            operation=ACLOperation.READ,
            permission_type=ACLPermissionType.ALLOW,
            resource_pattern=ResourcePattern(ResourceType.GROUP, '*'),
        )
        acl1 = ACL(
            principal=f'User:{subject}',
            host='*',
            operation=ACLOperation.READ,
            permission_type=ACLPermissionType.ALLOW,
            resource_pattern=ResourcePattern(ResourceType.TOPIC, topic),
        )
        acl2 = ACL(
            principal=f'User:{subject}',
            host='*',
            operation=ACLOperation.DESCRIBE,
            permission_type=ACLPermissionType.ALLOW,
            resource_pattern=ResourcePattern(ResourceType.TOPIC, topic),
        )
        acl3 = ACL(
            principal=f'User:{subject}',
            host='*',
            operation=ACLOperation.WRITE,
            permission_type=ACLPermissionType.ALLOW,
            resource_pattern=ResourcePattern(ResourceType.TOPIC, topic),
        )
        self.admin.create_acls([acl0, acl1, acl2, acl3])

    def delete_topic_acl(self, subject, topic):
        """Delete ACLs for a specific topic for the given subject."""
        acl = ACLFilter(
            principal=f'User:{subject}',
            host='*',
            operation=ACLOperation.ANY,
            permission_type=ACLPermissionType.ANY,
            resource_pattern=ResourcePattern(ResourceType.TOPIC, topic),
        )
        self.admin.delete_acls([acl])

    def delete_group_acl(self, subject):
        """Delete ACLs for all groups for the given subject."""
        acl = ACLFilter(
            principal=f'User:{subject}',
            host='*',
            operation=ACLOperation.ANY,
            permission_type=ACLPermissionType.ANY,
            resource_pattern=ResourcePattern(ResourceType.GROUP, '*'),
        )
        self.admin.delete_acls([acl])

    def register_topic(self, subject, topic):
        """Register a topic and grant access to the given subject."""
        with self.lock:
            p_ops = self.list_principals_for_topic(topic)
            if len(p_ops) > 0 and subject not in p_ops:
                return {
                    'status': 'error',
                    'message': 'Topic already has ACLs.',
                }
            if (
                len(p_ops) > 0
                and subject in p_ops
                and p_ops[subject] == {'READ', 'DESCRIBE', 'WRITE'}
            ):
                self._add_topic_to_policy(subject, topic)  # idempotent
                return {
                    'status': 'no-op',
                    'message': f'Principal {subject} already has access.',
                }

            # update ACLs and then IAM policy to be idempotent
            self.create_topic_and_acl(subject, topic)
            self._add_topic_to_policy(subject, topic)

            return {
                'status': 'success',
                'message': f'Access granted to principal {subject}.',
            }

    def unregister_topic(self, subject, topic):
        """Unregister a topic and revoke access from the given subject."""
        with self.lock:
            p_ops = self.list_principals_for_topic(topic)
            if subject not in p_ops:
                self._remove_topic_from_policy(subject, topic)  # idempotent
                return {
                    'status': 'no-op',
                    'message': f'Principal {subject} has no access.',
                }

            # update ACLs and then IAM policy to be idempotent
            self.delete_topic_acl(subject, topic)
            self._remove_topic_from_policy(subject, topic)

            return {
                'status': 'success',
                'message': f'Access removed from principal {subject}.',
            }

    def topic_registration_route(self, subject, topic, action):
        """Route the request to register/unregister a topic."""
        if action == 'register':
            return self.register_topic(subject, topic)
        else:
            return self.unregister_topic(subject, topic)

    def topic_listing_route(self, subject):
        """Return the list of topics for the given subject."""
        return {
            'status': 'success',
            'topics': self.list_topics_for_principal(subject),
        }

    def topic_configs_get_route(self, topic, configs):
        """Get configuration for a specific topic."""
        try:
            conf = ConfigResource(ConfigResourceType.TOPIC, topic, configs)
            configs = self.admin.describe_configs([conf])[0]
            configs = configs.resources[0][4]
            configs = {config[0]: config[1] for config in configs}
            return {'status': 'success', 'configs': configs}
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    def topic_configs_update_route(self, topic, configs):
        """Update configuration for a specific topic."""
        try:
            conf = ConfigResource(ConfigResourceType.TOPIC, topic, configs)
            configs_before = self.admin.describe_configs([conf])[0]
            configs_before = configs_before.resources[0][4]
            configs_before = {
                config[0]: config[1] for config in configs_before
            }

            self.admin.alter_configs([conf])
            configs_after = self.admin.describe_configs([conf])[0]
            configs_after = configs_after.resources[0][4]
            configs_after = {config[0]: config[1] for config in configs_after}
            return {
                'status': 'success',
                'before': configs_before,
                'after': configs_after,
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    def topic_partitions_update_route(self, topic, new_partition):
        """Update the number of partitions for a specific topic."""
        try:
            self.admin.create_partitions({topic: NewPartitions(new_partition)})
            return {'status': 'success'}
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    def reset_topic_route(self, topic):
        """Reset a topic by deleting and recreating it with default configs."""
        with contextlib.suppress(Exception):
            self.admin.delete_topics([topic])

        try:
            topic_list = [
                NewTopic(name=topic, num_partitions=1, replication_factor=2),
            ]
            self.admin.create_topics(new_topics=topic_list)
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

        return {
            'status': 'success',
            'message': 'Topic deleted and re-created with default configs',
        }

    def topic_user_access_route(self, subject, topic, user, action):
        """Grant or revoke access to a topic for a specific user."""
        if action == 'grant':
            return self.register_topic_for_user(subject, topic, user)
        else:
            return self.unregister_topic_for_user(subject, topic, user)

    def register_topic_for_user(self, subject, topic, user):
        """Register a topic and grant access to a specific user."""
        with self.lock:
            p_ops = self.list_principals_for_topic(topic)
            if len(p_ops) > 0 and subject not in p_ops:
                return {
                    'status': 'error',
                    'message': 'Topic already has ACLs.',
                }
            if (
                len(p_ops) > 0
                and subject in p_ops
                and p_ops[subject] == {'READ', 'DESCRIBE', 'WRITE'}
            ):
                try:
                    self.iam.get_user(UserName=user)
                except self.iam.exceptions.NoSuchEntityException:
                    return {
                        'status': 'error',
                        'message': f'User {user} does not exist.',
                    }

                if user in p_ops and p_ops[user] == {
                    'READ',
                    'DESCRIBE',
                    'WRITE',
                }:
                    self._add_topic_to_policy(user, topic)
                    return {
                        'status': 'no-op',
                        'message': f'User {user} already has access.',
                    }

                self.create_topic_and_acl(user, topic)
                self._add_topic_to_policy(user, topic)

                return {
                    'status': 'success',
                    'message': f'Access granted to user {user}.',
                }

    def unregister_topic_for_user(self, subject, topic, user):
        """Unregister a topic and revoke access from a specific user."""
        with self.lock:
            p_ops = self.list_principals_for_topic(topic)
            if subject not in p_ops:
                self._remove_topic_from_policy(subject, topic)
                return {
                    'status': 'no-op',
                    'message': f'Principal {subject} has no access.',
                }

            if user not in p_ops:
                self._remove_topic_from_policy(user, topic)
                return {
                    'status': 'no-op',
                    'message': f'User {user} has no access.',
                }

            self.delete_topic_acl(user, topic)
            self._remove_topic_from_policy(user, topic)

            return {
                'status': 'success',
                'message': f'Access removed from user {user}.',
            }

    def list_topic_users_route(self, topic):
        """List users with access to a specific topic."""
        try:
            p_ops = self.list_principals_for_topic(topic)
            return {'status': 'success', 'users': list(p_ops.keys())}
        except Exception as e:
            return {'status': 'error', 'reason': f'Error listing users: {e}'}

    def trigger_creation_route(  # noqa: PLR0913
        self,
        open_id,
        topic_name,
        lambda_name,
        action,
        function_configs,
        trigger_configs,
    ):
        """Delete or create a trigger."""
        if action == 'create':
            return self.create_trigger(
                open_id,
                topic_name,
                lambda_name,
                function_configs,
                trigger_configs,
            )
        else:
            return self.delete_trigger(open_id, lambda_name)

    def create_trigger(  # noqa: PLR0913
        self,
        open_id,
        topic_name,
        lambda_name,
        function_configs,
        trigger_configs,
        sleep=8,
    ):
        """Create a trigger."""
        # 1. create lambda IAM policy
        try:
            response = self.iam.create_policy(
                Path=f'/{self.maning.prefix}/',
                PolicyName=self.maning.policy_name(open_id, lambda_name),
                PolicyDocument=json.dumps(
                    self.maning.lambda_policy(
                        open_id,
                        lambda_name,
                        topic_name,
                    ),
                ),
            )
            print(100, response)
        except Exception as e:
            print(f'Error creating lambda IAM policy: {e}')

        # 2. create lambda IAM role
        try:
            response = self.iam.create_role(
                Path=f'/{self.maning.prefix}/',
                RoleName=self.maning.role_name(open_id, lambda_name),
                AssumeRolePolicyDocument=json.dumps(
                    self.maning.lambda_role_trust_policy(),
                ),
            )
            print(200, response)
        except Exception as e:
            print(f'Error creating lambda IAM role: {e}')

        # 3. attach the created IAM policy
        try:
            response = self.iam.attach_role_policy(
                RoleName=self.maning.role_name(open_id, lambda_name),
                PolicyArn=self.maning.policy_arn(open_id, lambda_name),
            )
            print(300, response)
        except Exception as e:
            print(f'Error attaching lambda IAM policy: {e}')

        # 4. create lambda log group
        try:
            response = self.logs.create_log_group(
                logGroupName=self.maning.function_log_name(
                    open_id,
                    lambda_name,
                ),
            )
            print(400, response)
        except Exception as e:
            print(f'Error creating lambda log group: {e}')

        base_function_configs = {
            'Runtime': 'python3.11',
            'Handler': 'lambda_function.lambda_handler',
            'Code': {},
            'Timeout': 30,
            'MemorySize': 128,
            'Environment': {},
            'EphemeralStorage': {'Size': 512},
            'Layers': [],
        }
        base_function_configs.update(function_configs)

        # 5. create the lambda function
        print(self.maning.role_arn(open_id, lambda_name))
        time.sleep(sleep)
        try:
            response = self.lamb.create_function(
                FunctionName=self.maning.function_name(open_id, lambda_name),
                Role=self.maning.role_arn(open_id, lambda_name),
                **base_function_configs,
            )
            print(500, response)
        except Exception as e:
            print(f'Error creating lambda: {e}')
            return {
                'status': 'error',
                'message': f'Error creating lambda: {e}',
            }

        # attach MSK trigger
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/lambda/client/create_event_source_mapping.html

        base_trigger_config = {
            'Enabled': True,
            'BatchSize': 1,
            'StartingPosition': 'LATEST',
        }
        base_trigger_config.update(trigger_configs)
        print(base_trigger_config)

        # 6. create the MSK trigger (event source mapping)
        try:
            response = self.lamb.create_event_source_mapping(
                EventSourceArn=self.cluster_arn,
                FunctionName=self.maning.function_name(open_id, lambda_name),
                Topics=[topic_name],
                **base_trigger_config,
            )
            print(600, response)
        except Exception as e:
            print(f'Error attaching the MSK trigger: {e}')
            return {
                'status': 'error',
                'message': f'Error attaching trigger: {e}',
            }

        return {'status': 'success', 'message': 'Trigger creation started.'}

    def delete_trigger(self, open_id, lambda_name):
        """Delete a trigger."""
        # 1. detach the MSK trigger
        try:
            response = self.lamb.list_event_source_mappings(
                EventSourceArn=self.cluster_arn,
                FunctionName=self.maning.function_name(open_id, lambda_name),
            )
            for mapping in response['EventSourceMappings']:
                mapping_uuid = mapping['UUID']
                del_response = self.lamb.delete_event_source_mapping(
                    UUID=mapping_uuid,
                )
                print(100, del_response)
        except Exception as e:
            print(f'Error detaching MSK trigger: {e}')

        # 2. delete lambda
        try:
            response = self.lamb.delete_function(
                FunctionName=self.maning.function_name(open_id, lambda_name),
            )
            print(200, response)
        except Exception as e:
            print(f'Error deleting lambda: {e}')

        # 3. delete lambda log group
        try:
            response = self.logs.delete_log_group(
                logGroupName=self.maning.function_log_name(
                    open_id,
                    lambda_name,
                ),
            )
            print(300, response)
        except Exception as e:
            print(f'Error deleting lambda log group: {e}')

        # 4. detach lambda IAM policy
        try:
            response = self.iam.detach_role_policy(
                RoleName=self.maning.role_name(open_id, lambda_name),
                PolicyArn=self.maning.policy_arn(open_id, lambda_name),
            )
            print(400, response)
        except Exception as e:
            print(f'Error detaching lambda IAM policy: {e}')

        # 5. delete lambda IAM role
        try:
            response = self.iam.delete_role(
                RoleName=self.maning.role_name(open_id, lambda_name),
            )
            print(500, response)
        except Exception as e:
            print(f'Error deleting lambda IAM role: {e}')

        # 6. delete lambda IAM policy
        try:
            response = self.iam.delete_policy(
                PolicyArn=self.maning.policy_arn(open_id, lambda_name),
            )
            print(600, response)
        except Exception as e:
            print(f'Error deleting lambda IAM policy: {e}')

        return {'status': 'success', 'message': 'Trigger deletion started.'}

    # TODO: max return

    def trigger_listing_route(self, open_id):
        """Route to list all triggers of a user."""
        functions = []
        try:
            response = self.lamb.list_functions(
                FunctionVersion='ALL',
            )

            for function in response['Functions']:
                lambda_name = function['FunctionName']
                if not lambda_name.startswith(self.maning.prefix):
                    continue
                print('lambda_name', lambda_name)
                prefix_len = len(f'{self.maning.prefix}-func-')
                open_id_suffix = lambda_name[prefix_len : prefix_len + 12]
                func_name = lambda_name[prefix_len + 13 :]
                print('open_id_suffix', open_id_suffix)
                print('func_name', func_name)

                if open_id[-12:] == open_id_suffix:
                    func_detail = self.lamb.get_function(
                        FunctionName=self.maning.function_name(
                            open_id,
                            func_name,
                        ),
                    )
                    del func_detail['ResponseMetadata']

                    trigger_details = []
                    trigger_response = self.lamb.list_event_source_mappings(
                        EventSourceArn=self.cluster_arn,
                        FunctionName=self.maning.function_name(
                            open_id,
                            func_name,
                        ),
                    )
                    for mapping in trigger_response['EventSourceMappings']:
                        mapping_uuid = mapping['UUID']
                        trigger_detail = self.lamb.get_event_source_mapping(
                            UUID=mapping_uuid,
                        )
                        del trigger_detail['ResponseMetadata']
                        trigger_details.append(trigger_detail)

                    functions.append(
                        {
                            'function_name': func_name,
                            'function_detail': func_detail,
                            'triggers': trigger_details,
                        },
                    )
            print(functions)
            return {
                'status': 'success',
                'triggers': sorted(
                    functions,
                    key=lambda x: x['function_name'],
                ),
            }

        except Exception as e:
            print(f'Error listing lambdas: {e}')

    def event_source_mapping_update_route(
        self,
        open_id,
        mapping_uuid,
        configs,
    ):
        """Route to update the trigger configs."""
        try:
            client_response = {'status': 'success'}
            response = self.lamb.get_event_source_mapping(UUID=mapping_uuid)

            del response['ResponseMetadata']
            client_response['before'] = response

            lambda_name = response['FunctionArn'].split(':')[-1]
            if not lambda_name.startswith(self.maning.prefix):
                return {
                    'status': 'error',
                    'message': (
                        f'trigger associated lambda {lambda_name} '
                        f'does not have prefix {self.maning.prefix}'
                    ),
                }

            print('lambda_name', lambda_name)
            prefix_len = len(f'{self.maning.prefix}-func-')
            open_id_suffix = lambda_name[prefix_len : prefix_len + 12]
            func_name = lambda_name[prefix_len + 13 :]
            print('open_id_suffix', open_id_suffix)
            print('func_name', func_name)

            if open_id[-12:] != open_id_suffix:
                return {
                    'status': 'error',
                    'message': (
                        f'trigger associated lambda {lambda_name} '
                        f'is not owned by user {open_id}'
                    ),
                }

            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/lambda/client/update_event_source_mapping.html
            response = self.lamb.update_event_source_mapping(
                UUID=mapping_uuid,
                **configs,
            )
            del response['ResponseMetadata']
            client_response['after'] = response
            return client_response

        except Exception as e:
            return {
                'status': 'error',
                'message': f'Error updating the trigger: {e}',
            }

    def list_log_streams(self, open_id, lambda_name):
        """List log streams of a trigger."""
        print(open_id, lambda_name, 'open_id, lambda_name')
        try:
            response = self.logs.describe_log_streams(
                logGroupName=self.maning.function_log_name(
                    open_id,
                    lambda_name,
                ),
            )
            # print(response)
            return {'status': 'success', 'streams': response['logStreams']}
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Error describing log streams!: {e}',
            }

    def get_log_events(self, open_id, lambda_name, log_stream_name):
        """Get log events from a specific log stream."""
        # TODO: max return
        try:
            log_events = self.logs.get_log_events(
                logGroupName=self.maning.function_log_name(
                    open_id,
                    lambda_name,
                ),
                logStreamName=log_stream_name,
                limit=10000,
                startFromHead=True,
            )['events']
            return {'status': 'success', 'events': log_events}
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Error getting log events: {e}',
            }

    def list_unused_topics(self):
        """List topics that are not used."""
        acl_filter = ACLFilter(
            principal=None,
            host='*',
            operation=ACLOperation.ANY,
            permission_type=ACLPermissionType.ALLOW,  # the only change
            resource_pattern=ResourcePatternFilter(
                resource_type=ResourceType.ANY,
                resource_name=None,
                pattern_type=ACLResourcePatternType.ANY,
            ),
        )
        result, error = self.admin.describe_acls(acl_filter)
        sorted_result = sorted(
            result,
            key=lambda acl: (
                acl.resource_pattern.resource_type,
                acl.principal,
                acl.resource_pattern.resource_name,
                acl.operation,
            ),
        )

        acls = defaultdict(lambda: set())  # topic:user
        for acl in sorted_result:
            if acl.resource_pattern.resource_type == ResourceType.TOPIC:
                acls[acl.resource_pattern.resource_name].add(acl.principal)

        no_owner_topics = []  # no user & not internal topic
        for topic in self.admin.list_topics():
            if len(acls[topic]) == 0 and not topic.startswith('_'):
                no_owner_topics.append(topic)
        # pprint(no_owner_topics)

        consumer = KafkaConsumer(
            bootstrap_servers=self.iam_vpc,
            security_protocol='SASL_SSL',
            sasl_mechanism='OAUTHBEARER',
            sasl_oauth_token_provider=MSKTokenProvider(),
        )

        zero_message_topics = []
        for topic in no_owner_topics:
            topic_partitions = self.admin.describe_topics([topic])[0][
                'partitions'
            ]
            total_records = 0
            for partition in topic_partitions:
                topic_partition = TopicPartition(topic, partition['partition'])
                beginning_offset = consumer.beginning_offsets(
                    [topic_partition],
                )[topic_partition]
                end_offset = consumer.end_offsets([topic_partition])[
                    topic_partition
                ]
                partition_records = end_offset - beginning_offset
                total_records += partition_records
            if total_records == 0:
                zero_message_topics.append(topic)

        print(zero_message_topics)


if __name__ == '__main__':
    pass
    EnvironmentChecker.check_env_variables(
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY',
        'SERVER_CLIENT_ID',
        'SERVER_SECRET',
    )
    auth_manager = AuthManager(
        os.getenv('SERVER_CLIENT_ID'),
        os.getenv('SERVER_SECRET'),
        'c5d4fab4-7f0d-422e-b0c8-5c74329b52fe',
    )
    aws = AWSManager(
        '845889416464',
        'us-east-1',
        'diaspora',
        '0b48e9a3-c32b-4783-9993-30798cdda646-9',
        'b-1-public.diaspora.fy49oq.c9.kafka.us-east-1.amazonaws.com:9198,b-2-public.diaspora.fy49oq.c9.kafka.us-east-1.amazonaws.com:9198',
        'b-1-public.diaspora.fy49oq.c9.kafka.us-east-1.amazonaws.com:9198,b-2-public.diaspora.fy49oq.c9.kafka.us-east-1.amazonaws.com:9198',
    )
