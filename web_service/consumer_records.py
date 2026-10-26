"""The fetch route's reply: records encoded as JSON, measured before.

Each record is Confluent REST Proxy v2's record, `topic`, `key`, `value`,
`partition` and `offset`, plus `headers` in the produce routes' form, a
list of `{"name": ..., "value": <base64>}`. A key or value is encoded as
the consumer instance's format says: binary as base64, json as the JSON
value it holds, written as it was produced. The reply is measured before
it is written and written a piece at a time, so a record of 8 MiB is never
held whole as JSON.
"""

from __future__ import annotations

import base64
import json
import re
import threading
from collections.abc import Iterator
from dataclasses import dataclass
from dataclasses import field
from typing import Any

from kafka.structs import TopicPartition

from web_service.producer_service import char_width

# Bytes of a key or value encoded at a time; a multiple of 3, so that the
# base64 of each piece joins into the base64 of the whole
PIECE_BYTES = 3 * 21_846
# Encoded bytes gathered before a piece of the reply is yielded
CHUNK_BYTES = 256 * 1024
# Most memory one check that a key or value is JSON may take, by
# json_check_cost; one check runs at a time. A JSON string of ASCII text
# passes up to 15 MiB, more than any record a broker returns, and an array
# of small objects (`[{"a":1},...]`) up to about 1.5 MiB.
JSON_CHECK_MAX_COST = 32 * 1024 * 1024
_JSON_CHECK = threading.Lock()
# Bytes of memory the check holds per byte that opens a list, separates
# two values, delimits a string or follows a key: an upper bound of
# CPython's objects (an empty list is 72 bytes, a short string about 50,
# a list's pointer 8 and its growth), with the decoder's hooks dropping
# each object and number as soon as it is made
_LIST_COST = 80
_COMMA_COST = 16
_QUOTE_COST = 32
_COLON_COST = 64
# Memory any check can take beyond what the counts predict: the decoder's
# own state and the over-allocation of a growing list
_FIXED_COST = 1024 * 1024
_NULL = b'null'
_WHITESPACE = re.compile(r'[ \t\n\r]*')


class NotJSON(ValueError):  # noqa: N818
    """A key or value that a json consumer instance cannot return."""


def json_check_cost(raw: bytes) -> int:
    """An upper bound of the memory that checking raw is JSON costs.

    Counts the decoded text and the strings parsing makes, at the widest
    character raw can decode to, and the lists, strings and keys parsing
    holds, from the bytes that open a list, separate two values, delimit
    a string or follow a key. The counts run at memchr speed.
    """
    return (
        _FIXED_COST
        + 2 * char_width(raw) * len(raw)
        + _LIST_COST * raw.count(b'[')
        + _COMMA_COST * raw.count(b',')
        + _QUOTE_COST * raw.count(b'"')
        + _COLON_COST * raw.count(b':')
    )


def _drop(_: Any) -> None:
    return None


def _no_constant(name: str) -> None:
    raise ValueError(f'{name} is not valid JSON')


def check_json(raw: bytes) -> None:
    """Check that raw is one JSON value in UTF-8, one check at a time.

    Raises:
        NotJSON: If raw is not, or checking it would cost more than
            JSON_CHECK_MAX_COST
    """
    if json_check_cost(raw) > JSON_CHECK_MAX_COST:
        raise NotJSON('is too large to check as JSON in the memory allowed')
    decoder = json.JSONDecoder(
        object_hook=_drop,
        parse_float=_drop,
        parse_int=_drop,
        parse_constant=_no_constant,
    )
    with _JSON_CHECK:
        try:
            text = raw.decode('utf-8')
            start = _WHITESPACE.match(text).end()  # type: ignore[union-attr]
            _, end = decoder.raw_decode(text, start)
        except ValueError, RecursionError:
            raise NotJSON('is not one JSON value in UTF-8') from None
        if _WHITESPACE.match(text, end).end() < len(text):  # type: ignore[union-attr]
            raise NotJSON('is not one JSON value in UTF-8')


def _pieces(value: bytes) -> Iterator[bytes]:
    view = memoryview(value)
    for start in range(0, len(value), PIECE_BYTES):
        yield bytes(view[start : start + PIECE_BYTES])


def _base64_pieces(value: bytes) -> Iterator[bytes]:
    view = memoryview(value)
    for start in range(0, len(value), PIECE_BYTES):
        yield base64.b64encode(view[start : start + PIECE_BYTES])


def _json_text(text: str) -> bytes:
    return json.dumps(text, ensure_ascii=False).encode()


@dataclass(frozen=True)
class EncodedField:
    """A record's key or value, with the size of its JSON encoding."""

    raw: bytes | None
    as_json: bool
    size: int

    @classmethod
    def of(cls, raw: bytes | None, format_type: str) -> EncodedField:
        """Measure raw as the format encodes it.

        Raises:
            NotJSON: In the json format, if raw is not one JSON value
        """
        if raw is None:
            return cls(None, as_json=False, size=len(_NULL))
        if format_type == 'json':
            check_json(raw)
            return cls(raw, as_json=True, size=len(raw))
        return cls(raw, as_json=False, size=4 * -(-len(raw) // 3) + 2)

    def pieces(self) -> Iterator[bytes]:
        """The field's JSON encoding, a piece at a time."""
        if self.raw is None:
            yield _NULL
            return
        if self.as_json:
            yield from _pieces(self.raw)
            return
        yield b'"'
        yield from _base64_pieces(self.raw)
        yield b'"'

    def decoded(self) -> Any:
        """The field as the reply's JSON holds it, as a Python value."""
        if self.raw is None:
            return None
        if self.as_json:
            return json.loads(self.raw)
        return base64.b64encode(self.raw).decode('ascii')


@dataclass(frozen=True)
class EncodedRecord:
    """One record of a fetch reply."""

    topic: str
    key: EncodedField
    value: EncodedField
    partition: int
    offset: int
    headers: tuple[tuple[str, bytes | None], ...] = ()

    @property
    def payload_size(self) -> int:
        """Bytes of the record's key and value, as Confluent counts them."""
        return sum(len(f.raw) for f in (self.key, self.value) if f.raw)

    def _head(self) -> bytes:
        return b'{"topic":' + _json_text(self.topic) + b',"key":'

    def _tail(self) -> bytes:
        headers = b','.join(
            b'{"name":'
            + _json_text(name)
            + b',"value":'
            + (
                _NULL
                if value is None
                else b'"' + base64.b64encode(value) + b'"'
            )
            + b'}'
            for name, value in self.headers
        )
        return b',"partition":%d,"offset":%d,"headers":[%s]}' % (
            self.partition,
            self.offset,
            headers,
        )

    @property
    def size(self) -> int:
        """Bytes of the record's JSON object."""
        return (
            len(self._head())
            + self.key.size
            + len(b',"value":')
            + self.value.size
            + len(self._tail())
        )

    def pieces(self) -> Iterator[bytes]:
        """The record's JSON object, a piece at a time."""
        yield self._head()
        yield from self.key.pieces()
        yield b',"value":'
        yield from self.value.pieces()
        yield self._tail()

    def as_dict(self) -> dict[str, Any]:
        """The record as the reply's JSON holds it."""
        return {
            'topic': self.topic,
            'key': self.key.decoded(),
            'value': self.value.decoded(),
            'partition': self.partition,
            'offset': self.offset,
            'headers': [
                {
                    'name': name,
                    'value': (
                        None
                        if value is None
                        else base64.b64encode(value).decode('ascii')
                    ),
                }
                for name, value in self.headers
            ],
        }


@dataclass(frozen=True)
class FetchReply:
    """The records a fetch returns, and the size of their JSON array.

    starts holds the offset of each partition's first record in the
    reply, to seek back to if the reply does not reach the client.
    undecodable names the first record a json instance could not return,
    as its topic, partition, offset and the reason, if the poll found one.
    """

    records: list[EncodedRecord]
    starts: dict[TopicPartition, int] = field(default_factory=dict)
    undecodable: tuple[str, int, int, str] | None = None

    @property
    def size(self) -> int:
        """Bytes of the JSON array: brackets, commas and records."""
        records = sum(r.size for r in self.records)
        return 2 + records + max(len(self.records) - 1, 0)

    def chunks(self) -> Iterator[bytes]:
        """The JSON array, in chunks of about CHUNK_BYTES."""
        pending: list[bytes] = [b'[']
        held = 1
        for i, record in enumerate(self.records):
            if i:
                pending.append(b',')
                held += 1
            for piece in record.pieces():
                pending.append(piece)
                held += len(piece)
                if held >= CHUNK_BYTES:
                    yield b''.join(pending)
                    pending, held = [], 0
        pending.append(b']')
        yield b''.join(pending)

    def body(self) -> bytes:
        """The whole JSON array."""
        return b''.join(self.chunks())
