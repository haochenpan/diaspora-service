"""Schemas of the produce routes for the OpenAPI document.

The routes read and check their bodies themselves (to bound their size
and answer with the routes' own error codes), so the request schemas are
given to OpenAPI as JSON Schema; the replies are pydantic models.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

_DATA_SCHEMA: dict[str, Any] = {
    'type': 'object',
    'properties': {
        'type': {
            'type': 'string',
            'enum': ['BINARY', 'JSON', 'STRING'],
            'description': (
                'How data is encoded. AVRO, PROTOBUF and JSONSCHEMA are '
                'refused: Octopus runs no schema registry.'
            ),
        },
        'data': {
            'description': (
                'BINARY: a base64 string, written decoded. JSON: any JSON '
                'value, written compactly as UTF-8. STRING: a string, '
                'written as UTF-8. Absent or null writes a null key or '
                'value.'
            ),
        },
    },
    'required': ['type'],
    'additionalProperties': False,
}

_RECORD_PROPERTIES: dict[str, Any] = {
    'partition_id': {
        'type': 'integer',
        'enum': [0],
        'description': 'Every topic has one partition, 0.',
    },
    'headers': {
        'type': 'array',
        'items': {
            'type': 'object',
            'properties': {
                'name': {'type': 'string'},
                'value': {
                    'type': ['string', 'null'],
                    'description': (
                        'Base64; null is written as an empty value.'
                    ),
                },
            },
            'required': ['name'],
            'additionalProperties': False,
        },
    },
    'key': _DATA_SCHEMA,
    'value': _DATA_SCHEMA,
    'timestamp': {
        'type': 'string',
        'format': 'date-time',
        'description': (
            "ISO-8601 with a UTC offset; the record's timestamp. Absent "
            'means the time of the request.'
        ),
    },
}

PRODUCE_REQUEST_SCHEMA: dict[str, Any] = {
    'type': 'object',
    'properties': _RECORD_PROPERTIES,
    'additionalProperties': False,
    'example': {
        'key': {'type': 'STRING', 'data': 'job-42'},
        'value': {'type': 'JSON', 'data': {'step': 7, 'loss': 0.31}},
    },
}

PRODUCE_BATCH_REQUEST_SCHEMA: dict[str, Any] = {
    'type': 'object',
    'properties': {
        'entries': {
            'type': 'array',
            'minItems': 1,
            'maxItems': 500,
            'items': {
                'type': 'object',
                'properties': {
                    'id': {
                        'type': 'string',
                        'minLength': 1,
                        'maxLength': 80,
                        'description': 'Distinct within the batch.',
                    },
                    **_RECORD_PROPERTIES,
                },
                'required': ['id'],
                'additionalProperties': False,
            },
        },
    },
    'required': ['entries'],
    'additionalProperties': False,
    'example': {
        'entries': [
            {'id': '1', 'value': {'type': 'JSON', 'data': {'step': 7}}},
            {'id': '2', 'value': {'type': 'STRING', 'data': 'done'}},
        ],
    },
}


class ProduceResult(BaseModel):
    """Where the cluster wrote a record."""

    topic_name: str
    partition_id: int
    offset: int
    timestamp: str


class BatchSuccess(ProduceResult):
    """A batch entry the cluster wrote."""

    id: str


class BatchFailure(BaseModel):
    """A batch entry that was not written."""

    id: str
    error_code: int
    message: str


class BatchResult(BaseModel):
    """The outcome of each entry of a batch."""

    successes: list[BatchSuccess]
    failures: list[BatchFailure]


class ProduceError(BaseModel):
    """The reply to a refused or failed produce request."""

    error_code: int
    message: str
