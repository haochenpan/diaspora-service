"""Unit tests for web_service_v3 KafkaService."""

from __future__ import annotations

import contextlib
import json
import logging
import os
from typing import Any

import pytest

from web_service_v3.services import KafkaService

# Suppress verbose logging from external libraries
logging.getLogger('kafka').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

# ============================================================================
# Test Constants
# ============================================================================

# ============================================================================
# Test Fixtures
# ============================================================================


@pytest.fixture
def kafka_service() -> KafkaService:
    """Create a KafkaService instance with real AWS services."""
    bootstrap_servers = os.getenv('DEFAULT_SERVERS')
    region = os.getenv('AWS_ACCOUNT_REGION')

    if not region:
        raise ValueError(
            'Missing required environment variable: AWS_ACCOUNT_REGION',
        )

    return KafkaService(
        bootstrap_servers=bootstrap_servers,
        region=region,
    )


@pytest.fixture
def random_namespace() -> str:
    """Generate a random namespace for each test."""
    return f'test-namespace-{os.urandom(8).hex()}'


@pytest.fixture
def random_topic() -> str:
    """Generate a random topic name for each test."""
    return f'test-topic-{os.urandom(8).hex()}'


@pytest.fixture
def cleanup_topics(kafka_service: KafkaService) -> Any:
    """Fixture that provides cleanup function for test topics."""
    created_topics: list[tuple[str, str]] = []

    def _cleanup(namespace: str, topic: str) -> None:
        """Mark a topic for cleanup."""
        topic_key = (namespace, topic)
        if topic_key not in created_topics:
            created_topics.append(topic_key)

    yield _cleanup

    # Cleanup all created topics
    for namespace, topic in created_topics:
        with contextlib.suppress(Exception):
            kafka_service.delete_topic(namespace, topic)


# ============================================================================
# Integration Tests with Real AWS Services
# ============================================================================


@pytest.mark.integration
def test_create_topic(
    kafka_service: KafkaService,
    random_namespace: str,
    random_topic: str,
    cleanup_topics: Any,
) -> None:
    """Test create_topic with real Kafka service."""
    print(
        f'\n[test_create_topic] '
        f'Testing with namespace: {random_namespace}, topic: {random_topic}',
    )

    # Mark for cleanup
    cleanup_topics(random_namespace, random_topic)

    # Create topic
    result = kafka_service.create_topic(
        namespace=random_namespace,
        topic=random_topic,
    )
    print('  Create topic result:')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    if result is None:
        pytest.skip('Kafka bootstrap servers not configured')
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'message' in result
    assert (
        random_namespace in result['message'] or 'Topic' in result['message']
    )


@pytest.mark.integration
def test_create_topic_idempotent(
    kafka_service: KafkaService,
    random_namespace: str,
    random_topic: str,
    cleanup_topics: Any,
) -> None:
    """Test create_topic is idempotent (can be called twice)."""
    print(
        f'\n[test_create_topic_idempotent] '
        f'Testing with namespace: {random_namespace}, topic: {random_topic}',
    )

    # Mark for cleanup
    cleanup_topics(random_namespace, random_topic)

    # Create topic first time
    result1 = kafka_service.create_topic(
        namespace=random_namespace,
        topic=random_topic,
    )
    print('  First create result:')
    print(json.dumps(result1, indent=2, default=str))

    if result1 is None:
        pytest.skip('Kafka bootstrap servers not configured')

    # Create topic second time (should succeed as already exists)
    result2 = kafka_service.create_topic(
        namespace=random_namespace,
        topic=random_topic,
    )
    print('  Second create result:')
    print(json.dumps(result2, indent=2, default=str))

    # Assertions
    assert isinstance(result1, dict)
    assert result1['status'] == 'success'
    assert isinstance(result2, dict)
    assert result2['status'] == 'success'
    assert 'already exists' in result2['message'].lower() or (
        'created' in result2['message'].lower()
    )


@pytest.mark.integration
def test_delete_topic(
    kafka_service: KafkaService,
    random_namespace: str,
    random_topic: str,
    cleanup_topics: Any,
) -> None:
    """Test delete_topic with real Kafka service."""
    print(
        f'\n[test_delete_topic] '
        f'Testing with namespace: {random_namespace}, topic: {random_topic}',
    )

    # Create topic first
    create_result = kafka_service.create_topic(
        namespace=random_namespace,
        topic=random_topic,
    )
    if create_result is None:
        pytest.skip('Kafka bootstrap servers not configured')

    # Mark for cleanup (in case delete fails)
    cleanup_topics(random_namespace, random_topic)

    # Delete topic
    result = kafka_service.delete_topic(
        namespace=random_namespace,
        topic=random_topic,
    )
    print('  Delete topic result:')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'message' in result
    assert (
        random_namespace in result['message'] or 'Topic' in result['message']
    )


@pytest.mark.integration
def test_delete_topic_idempotent(
    kafka_service: KafkaService,
    random_namespace: str,
    random_topic: str,
) -> None:
    """Test delete_topic is idempotent (can delete non-existent topic)."""
    print(
        f'\n[test_delete_topic_idempotent] '
        f'Testing with namespace: {random_namespace}, topic: {random_topic}',
    )

    # Delete non-existent topic (should succeed as idempotent)
    result = kafka_service.delete_topic(
        namespace=random_namespace,
        topic=random_topic,
    )
    print('  Delete result:')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    if result is None:
        pytest.skip('Kafka bootstrap servers not configured')
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'message' in result
    assert (
        'does not exist' in result['message'].lower()
        or 'deleted' in result['message'].lower()
    )


@pytest.mark.integration
def test_recreate_topic(
    kafka_service: KafkaService,
    random_namespace: str,
    random_topic: str,
    cleanup_topics: Any,
) -> None:
    """Test recreate_topic with real Kafka service."""
    print(
        f'\n[test_recreate_topic] '
        f'Testing with namespace: {random_namespace}, topic: {random_topic}',
    )

    # Mark for cleanup
    cleanup_topics(random_namespace, random_topic)

    # Create topic first
    create_result = kafka_service.create_topic(
        namespace=random_namespace,
        topic=random_topic,
    )
    if create_result is None:
        pytest.skip('Kafka bootstrap servers not configured')

    # Recreate topic (delete then create with 5 second sleep)
    result = kafka_service.recreate_topic(
        namespace=random_namespace,
        topic=random_topic,
    )
    print('  Recreate topic result:')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'message' in result
    assert 'recreated' in result['message'].lower()


@pytest.mark.integration
def test_recreate_topic_nonexistent(
    kafka_service: KafkaService,
    random_namespace: str,
    random_topic: str,
    cleanup_topics: Any,
) -> None:
    """Test recreate_topic with non-existent topic (should still work)."""
    print(
        f'\n[test_recreate_topic_nonexistent] '
        f'Testing with namespace: {random_namespace}, topic: {random_topic}',
    )

    # Mark for cleanup
    cleanup_topics(random_namespace, random_topic)

    # Recreate non-existent topic (delete is idempotent, then create)
    result = kafka_service.recreate_topic(
        namespace=random_namespace,
        topic=random_topic,
    )
    print('  Recreate topic result:')
    print(json.dumps(result, indent=2, default=str))

    # Assertions
    if result is None:
        pytest.skip('Kafka bootstrap servers not configured')
    assert isinstance(result, dict)
    assert result['status'] == 'success'
    assert 'message' in result
    assert 'recreated' in result['message'].lower()


@pytest.mark.integration
def test_full_lifecycle(
    kafka_service: KafkaService,
    random_namespace: str,
    random_topic: str,
    cleanup_topics: Any,
) -> None:
    """Test full lifecycle: create, recreate, delete."""
    print(
        f'\n[test_full_lifecycle] '
        f'Testing with namespace: {random_namespace}, topic: {random_topic}',
    )

    # Mark for cleanup
    cleanup_topics(random_namespace, random_topic)

    # 1. Create topic
    create_result = kafka_service.create_topic(
        namespace=random_namespace,
        topic=random_topic,
    )
    if create_result is None:
        pytest.skip('Kafka bootstrap servers not configured')
    assert create_result is not None
    assert create_result['status'] == 'success'

    # 2. Recreate topic (delete + 5s sleep + create)
    recreate_result = kafka_service.recreate_topic(
        namespace=random_namespace,
        topic=random_topic,
    )
    assert recreate_result is not None
    assert recreate_result['status'] == 'success'
    assert 'recreated' in recreate_result['message'].lower()

    # 3. Delete topic
    delete_result = kafka_service.delete_topic(
        namespace=random_namespace,
        topic=random_topic,
    )
    assert delete_result is not None
    assert delete_result['status'] == 'success'

    # 4. Delete again (idempotent)
    delete_result2 = kafka_service.delete_topic(
        namespace=random_namespace,
        topic=random_topic,
    )
    assert delete_result2 is not None
    assert delete_result2['status'] == 'success'

    print('  Full lifecycle test completed successfully')
