"""Utility functions for web service v3."""

from __future__ import annotations

import contextlib
import json
import os
import threading
from collections import defaultdict

import boto3
from kafka.admin import ACL
from kafka.admin import ACLFilter
from kafka.admin import ACLOperation
from kafka.admin import ACLPermissionType
from kafka.admin import ACLResourcePatternType
from kafka.admin import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.admin import ResourcePattern
from kafka.admin import ResourcePatternFilter
from kafka.admin import ResourceType
from kafka.errors import TopicAlreadyExistsError

from web_service.utils import MSKTokenProvider


class AWSManagerV3:
    """Manage AWS resources for v3 web service."""

    class NamingManager:
        """Handle naming conventions for AWS resources."""

        def __init__(self, account_id, region, cluster_name):
            """Initialize with the given parameters."""
            self.account_id = account_id
            self.region = region
            self.cluster_name = cluster_name

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
                            'kafka-cluster:WriteDataIdempotently',
                            'kafka-cluster:DescribeTransactionalId',
                            'kafka-cluster:AlterTransactionalId',
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

        def ap_role_name(self, open_id):
            """Generate a role name for an application based on the open ID."""
            return f'{open_id}-role'

        def ap_trust_policy(self):
            """Generate a trust policy for an application role."""
            principal_arn = f'arn:aws:iam::{self.account_id}:root'
            return {
                'Version': '2012-10-17',
                'Statement': [
                    {
                        'Effect': 'Allow',
                        'Principal': {
                            'AWS': principal_arn,
                        },
                        'Action': 'sts:AssumeRole',
                        'Condition': {},
                    },
                ],
            }

    def __init__(
        self,
        account_id,
        region,
        cluster_name,
        iam_vpc=None,
    ):
        """Init AWS Manager V3."""
        self.account_id = account_id
        self.region = region
        self.cluster_name = cluster_name
        self.iam_policy_arn_prefix = (
            f'arn:aws:iam::{account_id}:policy/msk-policy/'
        )
        self.topic_arn_prefix = (
            f'arn:aws:kafka:{region}:{account_id}:topic/{cluster_name}/*/'
        )

        self.naming = self.NamingManager(account_id, region, cluster_name)
        self.iam = boto3.client('iam')
        self.ssm = boto3.client('ssm', region_name=region)
        self.lock = threading.Lock()

        # Initialize Kafka admin client if iam_vpc is provided
        if iam_vpc:
            self.admin = KafkaAdminClient(
                bootstrap_servers=iam_vpc,
                security_protocol='SASL_SSL',
                sasl_mechanism='OAUTHBEARER',
                sasl_oauth_token_provider=MSKTokenProvider(),
            )
        else:
            self.admin = None

    def create_user(self, subject):
        """Create IAM user, policy, and role if not exists."""
        # Note: This method should be called within a locked context
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
                PolicyDocument=json.dumps(self.naming.iam_user_policy()),
            )
        except self.iam.exceptions.EntityAlreadyExistsException:
            pass  # policy already created

        # 3. attach the created policy to user
        self.iam.attach_user_policy(
            UserName=subject,
            PolicyArn=self.iam_policy_arn_prefix + subject,
        )

        # 4. Create IAM role that attach to the same user policy
        self.create_user_role_if_not_exists(subject)

    def delete_user(self, subject):
        """Delete IAM user and all associated resources."""
        with self.lock:
            # 1. Delete IAM access keys
            with contextlib.suppress(
                self.iam.exceptions.NoSuchEntityException,
            ):
                existing_keys = self.iam.list_access_keys(UserName=subject)
                for key in existing_keys['AccessKeyMetadata']:
                    self.iam.delete_access_key(
                        UserName=subject,
                        AccessKeyId=key['AccessKeyId'],
                    )

            # 2. Delete IAM role if exists
            role_name = self.naming.ap_role_name(subject)
            with contextlib.suppress(
                self.iam.exceptions.NoSuchEntityException,
            ):
                self.iam.detach_role_policy(
                    RoleName=role_name,
                    PolicyArn=self.iam_policy_arn_prefix + subject,
                )
            with contextlib.suppress(
                self.iam.exceptions.NoSuchEntityException,
            ):
                self.iam.delete_role(RoleName=role_name)
                print(f'DEBUG - Deleted IAM role: {role_name}')

            # 3. Detach IAM policy from user
            with contextlib.suppress(
                self.iam.exceptions.NoSuchEntityException,
            ):
                self.iam.detach_user_policy(
                    UserName=subject,
                    PolicyArn=self.iam_policy_arn_prefix + subject,
                )

            # 4. Delete IAM policy (delete non-default versions first)
            with contextlib.suppress(
                self.iam.exceptions.NoSuchEntityException,
            ):
                # List and delete non-default policy versions
                existing_policies = self.iam.list_policy_versions(
                    PolicyArn=self.iam_policy_arn_prefix + subject,
                )
                for ver in existing_policies['Versions']:
                    if not ver['IsDefaultVersion']:
                        self.iam.delete_policy_version(
                            PolicyArn=self.iam_policy_arn_prefix + subject,
                            VersionId=ver['VersionId'],
                        )
                # Delete default policy
                self.iam.delete_policy(
                    PolicyArn=self.iam_policy_arn_prefix + subject,
                )

            # 5. Delete IAM user
            with contextlib.suppress(
                self.iam.exceptions.NoSuchEntityException,
            ):
                self.iam.delete_user(UserName=subject)
                print(f'DEBUG - Deleted IAM user: {subject}')

    def create_user_and_key(self, subject):
        """Create an IAM user if not exists and user credential."""
        with self.lock:
            # Create user, policy, and role
            self.create_user(subject)

            # Delete existing keys from IAM if there are any
            existing_keys = self.iam.list_access_keys(UserName=subject)
            for key in existing_keys['AccessKeyMetadata']:
                self.iam.delete_access_key(
                    UserName=subject,
                    AccessKeyId=key['AccessKeyId'],
                )

            # Create new key
            new_key = self.iam.create_access_key(UserName=subject)
            access_key = new_key['AccessKey']['AccessKeyId']
            secret_key = new_key['AccessKey']['SecretAccessKey']
            create_date = new_key['AccessKey']['CreateDate']

            # Print new_key for debug
            print('DEBUG - new_key:', new_key)

            # Store the new_key and subject into AWS Systems Manager
            # Parameter Store
            self.store_key_to_ssm(
                subject, access_key, secret_key, create_date,
            )

            return {
                'status': 'success',
                'username': subject,
                'access_key': access_key,
                'secret_key': secret_key,
                'retrieved_from_ssm': False,
            }

    def create_user_role_if_not_exists(self, subject):
        """Create an IAM role that attach to the same user policy."""
        try:
            response = self.iam.create_role(
                Path='/ap/',
                RoleName=self.naming.ap_role_name(subject),
                AssumeRolePolicyDocument=json.dumps(
                    self.naming.ap_trust_policy(),
                ),
            )
            print('DEBUG - IAM role created:', response)
        except Exception as e:
            print(f'Error creating IAM role for AP: {e}')

        try:
            response = self.iam.attach_role_policy(
                RoleName=self.naming.ap_role_name(subject),
                PolicyArn=self.iam_policy_arn_prefix + subject,
            )
            print('DEBUG - IAM policy attached to role:', response)
        except Exception as e:
            print(f'Error attaching IAM policy for AP: {e}')

    def store_key_to_ssm(
        self, subject, access_key, secret_key, create_date,
    ):
        """Store the key and subject into AWS Systems Manager Parameter Store."""  # noqa: E501
        parameter_name = f'/diaspora/v3/keys/{subject}'
        # Convert datetime to ISO format string for JSON serialization
        if hasattr(create_date, 'isoformat'):
            create_date_str = create_date.isoformat()
        else:
            create_date_str = str(create_date)
        parameter_value = json.dumps({
            'subject': subject,
            'access_key': access_key,
            'secret_key': secret_key,
            'create_date': create_date_str,
        })

        try:
            # Store as SecureString type for encryption
            self.ssm.put_parameter(
                Name=parameter_name,
                Value=parameter_value,
                Type='SecureString',
                Overwrite=True,  # Overwrite if parameter already exists
            )
            print(
                f'DEBUG - Stored key to SSM Parameter Store: {parameter_name}',
            )
        except Exception as e:
            print(f'Error storing key to SSM Parameter Store: {e}')
            raise

    def retrieve_key_from_ssm(self, subject):
        """Retrieve the key from AWS Systems Manager Parameter Store."""
        parameter_name = f'/diaspora/v3/keys/{subject}'
        try:
            response = self.ssm.get_parameter(
                Name=parameter_name,
                WithDecryption=True,  # Decrypt SecureString
            )
            parameter_value = response['Parameter']['Value']
            key_data = json.loads(parameter_value)
            print(
                f'DEBUG - Retrieved key from SSM Parameter Store: '
                f'{parameter_name}',
            )
            return key_data
        except self.ssm.exceptions.ParameterNotFound:
            print(
                f'DEBUG - Key not found in SSM Parameter Store: '
                f'{parameter_name}',
            )
            return None
        except Exception as e:
            print(f'Error retrieving key from SSM Parameter Store: {e}')
            raise

    def retrieve_or_create_key(self, subject):
        """Retrieve key from SSM if exists, otherwise create and return."""
        # Try to retrieve from SSM first
        key_data = self.retrieve_key_from_ssm(subject)
        if key_data:
            # Key exists in SSM, return it
            return {
                'status': 'success',
                'username': key_data['subject'],
                'access_key': key_data['access_key'],
                'secret_key': key_data['secret_key'],
                'create_date': key_data.get('create_date'),
                'retrieved_from_ssm': True,
            }
        else:
            # Key doesn't exist, create a new one
            return self.create_user_and_key(subject)

    def delete_key(self, subject):
        """Delete key from SSM Parameter Store only."""
        parameter_name = f'/diaspora/v3/keys/{subject}'
        try:
            self.ssm.delete_parameter(Name=parameter_name)
            print(
                f'DEBUG - Deleted key from SSM Parameter Store: '
                f'{parameter_name}',
            )
            return {
                'status': 'success',
                'message': f'Key deleted from SSM for {subject}',
            }
        except self.ssm.exceptions.ParameterNotFound:
            print(
                f'DEBUG - Key not found in SSM Parameter Store: '
                f'{parameter_name}',
            )
            return {
                'status': 'success',
                'message': f'Key not found in SSM for {subject}',
            }
        except Exception as e:
            print(f'Error deleting key from SSM Parameter Store: {e}')
            raise

    def _get_iam_policy(self, policy_arn):
        """Get IAM policy and delete non-default versions."""
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

    def _add_topic_to_policy(self, subject, topic):
        """Add topic to IAM policy."""
        policy_arn = self.iam_policy_arn_prefix + subject
        policy = self._get_iam_policy(policy_arn)
        if not policy:
            return False
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
        """Remove topic from IAM policy."""
        policy_arn = self.iam_policy_arn_prefix + subject
        policy = self._get_iam_policy(policy_arn)
        if not policy:
            return False
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
        if not self.admin:
            return []
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

        result, _ = self.admin.describe_acls(acl_filter)
        topics = [acl.resource_pattern.resource_name for acl in result]
        return sorted(set(topics))

    def list_principals_for_topic(self, topic):
        """List subjects that have access to a topic."""
        if not self.admin:
            return {}
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
        result, _ = self.admin.describe_acls(acl_filter)

        p_ops = defaultdict(lambda: set())
        for acl in result:
            principal, operation = (
                str(acl.principal)[len('User:') :],
                acl.operation.name,
            )
            p_ops[principal].add(operation)
        return dict(p_ops)

    def create_topic_and_acl(self, subject, topic):
        """Create topic and ACLs for the subject."""
        if not self.admin:
            raise ValueError('Kafka admin client not initialized')
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
        if not self.admin:
            return
        acl = ACLFilter(
            principal=f'User:{subject}',
            host='*',
            operation=ACLOperation.ANY,
            permission_type=ACLPermissionType.ANY,
            resource_pattern=ResourcePattern(ResourceType.TOPIC, topic),
        )
        self.admin.delete_acls([acl])

    def register_topic(self, subject, topic):
        """Register a topic and grant access to the given subject."""
        if not self.admin:
            return {
                'status': 'error',
                'message': 'Kafka admin client not initialized',
            }
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
        if not self.admin:
            return {
                'status': 'error',
                'message': 'Kafka admin client not initialized',
            }
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

    def topic_listing_route(self, subject):
        """Return the list of topics for the given subject."""
        return {
            'status': 'success',
            'topics': self.list_topics_for_principal(subject),
        }

