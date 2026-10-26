"""The fetch route's reply: records encoded as JSON, measured before.

A record's key and value are encoded as the consumer instance's format
says: binary as base64, json as the UTF-8 text itself, or base64 when it is
not valid UTF-8. The reply is the JSON array Starlette's JSONResponse would
write for the records as dictionaries, byte for byte, but it is measured
before it is written and written a piece at a time: a value of 8 MiB can
take 48 MiB as JSON text, since each control character takes six bytes.
"""

from __future__ import annotations

import base64
import codecs
import json
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

# Bytes of a key or value encoded at a time; a multiple of 3, so that the
# base64 of each piece joins into the base64 of the whole
PIECE_BYTES = 3 * 21_846
# Encoded bytes gathered before a piece of the reply is yielded
CHUNK_BYTES = 256 * 1024
_NULL = b'null'


def _json_text_pieces(value: bytes) -> Iterator[bytes]:
    """The JSON string of value's UTF-8 text, without its quotes.

    Escapes as json.dumps(ensure_ascii=False) does, one character at a
    time, so the pieces join into the escape of the whole text.

    Raises:
        UnicodeDecodeError: If value is not valid UTF-8
    """
    decoder = codecs.getincrementaldecoder('utf-8')()
    view = memoryview(value)
    for start in range(0, len(value), PIECE_BYTES):
        text = decoder.decode(view[start : start + PIECE_BYTES])
        if text:
            yield json.dumps(text, ensure_ascii=False)[1:-1].encode()
    text = decoder.decode(b'', final=True)
    if text:
        yield json.dumps(text, ensure_ascii=False)[1:-1].encode()


def _base64_pieces(value: bytes) -> Iterator[bytes]:
    view = memoryview(value)
    for start in range(0, len(value), PIECE_BYTES):
        yield base64.b64encode(view[start : start + PIECE_BYTES])


@dataclass(frozen=True)
class EncodedField:
    """A record's key or value, with the size of its JSON encoding."""

    raw: bytes | None
    as_text: bool
    size: int

    @classmethod
    def of(cls, raw: bytes | None, format_type: str) -> EncodedField:
        """Measure raw as the format encodes it."""
        if raw is None:
            return cls(None, as_text=False, size=len(_NULL))
        if format_type == 'json':
            try:
                size = sum(len(p) for p in _json_text_pieces(raw))
            except UnicodeDecodeError:
                pass
            else:
                return cls(raw, as_text=True, size=size + 2)
        return cls(raw, as_text=False, size=4 * -(-len(raw) // 3) + 2)

    def pieces(self) -> Iterator[bytes]:
        """The field's JSON encoding, a piece at a time."""
        if self.raw is None:
            yield _NULL
            return
        yield b'"'
        if self.as_text:
            yield from _json_text_pieces(self.raw)
        else:
            yield from _base64_pieces(self.raw)
        yield b'"'

    def decoded(self) -> str | None:
        """The field as the reply's JSON holds it, as a Python string."""
        if self.raw is None:
            return None
        if self.as_text:
            return self.raw.decode()
        return base64.b64encode(self.raw).decode('ascii')


@dataclass(frozen=True)
class EncodedRecord:
    """One record of a fetch reply."""

    topic: str
    key: EncodedField
    value: EncodedField
    partition: int
    offset: int

    def _head(self) -> bytes:
        topic = json.dumps(self.topic, ensure_ascii=False).encode()
        return b'{"topic":' + topic + b',"key":'

    def _tail(self) -> bytes:
        return b',"partition":%d,"offset":%d}' % (self.partition, self.offset)

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
        }


@dataclass(frozen=True)
class FetchReply:
    """The records a fetch returns, and the size of their JSON array."""

    records: list[EncodedRecord]

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
