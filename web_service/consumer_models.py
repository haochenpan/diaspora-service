"""Pydantic models for consumer REST API endpoints.

Field names match the Confluent Kafka REST Proxy v2 API exactly.
"""

from __future__ import annotations

from pydantic import BaseModel
from pydantic import Field


class CreateConsumerRequest(BaseModel):
    """Request body for POST /consumers/{group_name}."""

    name: str | None = Field(
        None,
        description=(
            'Consumer instance name, used in URLs. '
            'Must be unique. Auto-generated if omitted.'
        ),
    )
    format: str = Field(
        'binary',
        description=(
            'Deserialization format: "binary", "json", "avro", '
            '"jsonschema", or "protobuf". Defaults to "binary".'
        ),
    )
    auto_offset_reset: str = Field(
        'latest',
        alias='auto.offset.reset',
        description='Sets the auto.offset.reset setting.',
    )
    auto_commit_enable: str = Field(
        'false',
        alias='auto.commit.enable',
        description='Sets the auto.commit.enable setting.',
    )
    fetch_min_bytes: str | None = Field(
        None,
        alias='fetch.min.bytes',
        description='Sets the fetch.min.bytes setting.',
    )
    consumer_request_timeout_ms: str | None = Field(
        None,
        alias='consumer.request.timeout.ms',
        description=(
            'Max time to wait for messages for a request '
            'if the max request size has not been reached.'
        ),
    )

    model_config = {'populate_by_name': True}


class SubscriptionRequest(BaseModel):
    """Request body for POST .../subscription."""

    topics: list[str] | None = Field(
        None,
        description='List of topic names to subscribe to.',
    )
    topic_pattern: str | None = Field(
        None,
        description=(
            'A regex pattern. topics and topic_pattern are mutually exclusive.'
        ),
    )


class PartitionOffset(BaseModel):
    """A topic-partition-offset triple."""

    topic: str
    partition: int
    offset: int


class OffsetsCommitRequest(BaseModel):
    """Request body for POST .../offsets."""

    offsets: list[PartitionOffset] | None = Field(
        None,
        description=(
            'Offsets to commit. If null/empty, commits all fetched records.'
        ),
    )


class PartitionInfo(BaseModel):
    """A topic-partition pair."""

    topic: str
    partition: int


class OffsetsGetRequest(BaseModel):
    """Request body for GET .../offsets."""

    partitions: list[PartitionInfo] = Field(
        ...,
        description='Partitions to get committed offsets for.',
    )


class AssignmentRequest(BaseModel):
    """Request body for POST .../assignments."""

    partitions: list[PartitionInfo] = Field(
        ...,
        description='Partitions to manually assign.',
    )


class SeekRequest(BaseModel):
    """Request body for POST .../positions."""

    offsets: list[PartitionOffset] = Field(
        ...,
        description='Offsets to seek to.',
    )


class SeekPartitionsRequest(BaseModel):
    """Request body for POST .../positions/beginning or .../positions/end."""

    partitions: list[PartitionInfo] = Field(
        ...,
        description='Partitions to seek to beginning/end.',
    )
