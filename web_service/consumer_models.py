"""Pydantic models for consumer REST API endpoints.

Field names match the Confluent Kafka REST Proxy v2 API exactly.
"""

from __future__ import annotations

from pydantic import BaseModel
from pydantic import Field
from pydantic import field_validator

# Formats a consumer instance decodes records in
FORMATS = ('binary', 'json')
# Formats Confluent decodes with a schema registry, which Octopus does not run
SCHEMA_FORMATS = ('avro', 'jsonschema', 'protobuf')
# Largest partition (int32) the Kafka protocol encodes, and largest offset
# a request may name: a commit adds 1, which must fit in an int64
MAX_PARTITION = 2**31 - 1
MAX_OFFSET = 2**63 - 2


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
            'Deserialization format, "binary" or "json" in any case; '
            'defaults to "binary". "avro", "jsonschema" and "protobuf" '
            'need a schema registry, which Octopus does not run, and are '
            'refused with 422.'
        ),
    )
    auto_offset_reset: str = Field(
        'latest',
        alias='auto.offset.reset',
        description=(
            'Where a group without a committed offset starts: "earliest", '
            '"latest" (the default) or "none".'
        ),
    )
    auto_commit_enable: str = Field(
        'false',
        alias='auto.commit.enable',
        description='"true" or "false" (the default).',
    )
    fetch_min_bytes: int | str | None = Field(
        None,
        alias='fetch.min.bytes',
        description=(
            "Sets the Kafka consumer's fetch.min.bytes: a whole number of "
            'at least 0.'
        ),
    )
    consumer_request_timeout_ms: int | str | None = Field(
        None,
        alias='consumer.request.timeout.ms',
        description=(
            'Milliseconds a fetch that names no timeout waits for records: '
            'a whole number of at least 0, of which 30000 at most is used. '
            'Defaults to 5000.'
        ),
    )

    model_config = {'populate_by_name': True}

    @field_validator('format')
    @classmethod
    def _known_format(cls, value: str) -> str:
        """Accept binary and json in any case, as Confluent does.

        The refusal does not repeat the value, as no validation message
        of the consumer routes does.
        """
        lowered = value.lower()
        if lowered in FORMATS:
            return lowered
        if lowered in SCHEMA_FORMATS:
            raise ValueError(
                'avro, jsonschema and protobuf need a schema registry, '
                'which Octopus does not run; use binary or json',
            )
        raise ValueError('Invalid format type; use binary or json')


class SubscriptionRequest(BaseModel):
    """Request body for POST .../subscription."""

    topics: list[str] | None = Field(
        None,
        description='Non-empty list of topic names to subscribe to.',
    )
    topic_pattern: str | None = Field(
        None,
        description=(
            'Not supported. A request that sets it is refused; '
            'use topics instead.'
        ),
    )


class PartitionOffset(BaseModel):
    """A topic-partition-offset triple."""

    topic: str = Field(min_length=1)
    partition: int = Field(ge=0, le=MAX_PARTITION)
    offset: int = Field(ge=0, le=MAX_OFFSET)


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

    topic: str = Field(min_length=1)
    partition: int = Field(ge=0, le=MAX_PARTITION)


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
