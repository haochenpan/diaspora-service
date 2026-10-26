"""The route class of the consumer routes: Confluent's errors and types.

Every reply of a consumer route that is not a success is Confluent's
`{"error_code": <int>, "message": <str>}`, including those FastAPI would
make itself: a body that is not JSON, a validation failure, and an
exception no handler caught. A body over MAX_BODY_BYTES is refused before
it is parsed. The other routes keep FastAPI's own replies.

As in Confluent's REST Proxy v2, a request's Content-Type, if it has one,
must be one of the v2 types (or application/json), and a JSON reply is
`application/vnd.kafka.v2+json`; the fetch route chooses its reply's type
from the request's Accept, as negotiate_records does. A deprecated route
also answers with the Deprecation header of RFC 9745.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable
from collections.abc import Coroutine
from collections.abc import Sequence
from typing import Any

from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.responses import Response
from fastapi.routing import APIRoute
from pydantic import ValidationError
from starlette.exceptions import HTTPException
from starlette.requests import ClientDisconnect
from starlette.requests import Request

from web_service.consumer_service import SERVER_ERROR

logger = logging.getLogger(__name__)

# Largest body a consumer route reads. The largest a client needs is a
# list of 50 offsets with topic names of 249 characters, about 16 KiB.
MAX_BODY_BYTES = 64 * 1024

# Codes of the replies this module makes. 40003 and 41301 are the produce
# routes' codes for the same cases; 422 is the code rest-utils'
# ConstraintViolationExceptionMapper gives a failed validation.
BAD_BODY = 40003
BODY_TOO_LARGE = 41301
INVALID_REQUEST = 422

# The type of every JSON reply of a consumer route but a fetch's
V2_JSON = 'application/vnd.kafka.v2+json'
# The types a consumer route's request may declare: those Confluent's
# ConsumersResource consumes, and application/json, which it does not
REQUEST_TYPES = frozenset(
    {
        V2_JSON,
        'application/vnd.kafka.binary.v2+json',
        'application/vnd.kafka.json.v2+json',
        'application/vnd.kafka.avro.v2+json',
        'application/vnd.kafka.jsonschema.v2+json',
        'application/vnd.kafka.protobuf.v2+json',
        'application/json',
    },
)
# The types the fetch route produces, as Confluent's ConsumersResource
# does: each with the format it serves and its weight (qs). Without an
# Accept that names one, the heaviest, binary, is chosen.
RECORDS_TYPES = (
    ('application/vnd.kafka.binary.v2+json', 'binary', 1.0),
    (V2_JSON, 'binary', 0.9),
    ('application/vnd.kafka.json.v2+json', 'json', 0.1),
    ('application/vnd.kafka.avro.v2+json', 'avro', 0.1),
    ('application/vnd.kafka.jsonschema.v2+json', 'jsonschema', 0.1),
    ('application/vnd.kafka.protobuf.v2+json', 'protobuf', 0.1),
)
# The Deprecation header of a deprecated route (RFC 9745): deprecated since
# 2026-10-26T00:00:00Z
DEPRECATION = '@1792972800'
_Q = re.compile(r'\s*q\s*=\s*([0-9.]+)\s*$', re.IGNORECASE)


def error_response(status: int, error_code: int, message: str) -> Response:
    """A Confluent-shaped error reply."""
    return JSONResponse(
        content={'error_code': error_code, 'message': message},
        status_code=status,
    )


def validation_message(errors: Sequence[Any]) -> str:
    """Name each invalid field and what is wrong with it.

    Uses only the field's location and pydantic's description of the
    error, never the value sent.
    """
    parts = []
    for error in errors:
        where = '.'.join(str(p) for p in error.get('loc', ()))
        what = str(error.get('msg', 'invalid'))
        what = what.removeprefix('Value error, ')
        parts.append(f'{where}: {what}' if where else what)
    return '; '.join(parts) or 'Invalid request'


def negotiate_records(accept: str | None) -> tuple[str, str] | None:
    """The format and type a fetch replies in, for the request's Accept.

    Chooses as Jersey does for Confluent's fetch routes: the type the
    client weighs most (q), then the one the server weighs most (qs),
    then the one the client names most exactly. No Accept is */*.

    Returns:
        The format the type serves and the type, or None if the client
        accepts none of RECORDS_TYPES
    """
    ranges: list[tuple[str, float]] = []
    for part in (accept or '*/*').split(','):
        media, *params = part.split(';')
        q = 1.0
        for param in params:
            if found := _Q.match(param):
                try:
                    q = float(found.group(1))
                except ValueError:
                    q = 0.0
        if media.strip():
            ranges.append((media.strip().lower(), q))
    best: tuple[float, float, int] | None = None
    chosen: tuple[str, str] | None = None
    for media_type, format_type, qs in RECORDS_TYPES:
        kind = media_type.split('/')[0]
        matches = [
            (distance, q)
            for accepted, q in ranges
            for distance, pattern in (
                (0, media_type),
                (1, f'{kind}/*'),
                (2, '*/*'),
            )
            if accepted == pattern
        ]
        if not matches:
            continue
        # The most exact range that matches sets the type's q
        distance, q = min(matches)
        rank = (q, qs, -distance)
        if q > 0 and (best is None or rank > best):
            best, chosen = rank, (format_type, media_type)
    return chosen


def _unsupported_type(request: Request) -> Response | None:
    """The 415 reply if the request declares a type no route consumes."""
    declared = request.headers.get('content-type')
    if declared is None:
        return None
    if declared.split(';')[0].strip().lower() in REQUEST_TYPES:
        return None
    # rest-utils' reply for the 415 Jersey raises
    return error_response(415, 415, 'HTTP 415 Unsupported Media Type')


def invalid_request(errors: Sequence[Any]) -> Response:
    """The reply for a request that failed validation."""
    if any(e.get('type') == 'json_invalid' for e in errors):
        return error_response(400, BAD_BODY, 'The body is not valid JSON')
    return error_response(422, INVALID_REQUEST, validation_message(errors))


class ConsumerRoute(APIRoute):
    """An APIRoute whose failures all answer in Confluent's shape."""

    def get_route_handler(
        self,
    ) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        """Wrap FastAPI's handler with the checks, types and error replies."""
        serve = self._serve(super().get_route_handler())

        async def handle(request: Request) -> Response:
            response = await serve(request)
            if response.headers.get('content-type') == 'application/json':
                response.headers['content-type'] = V2_JSON
            if self.deprecated:
                response.headers['Deprecation'] = DEPRECATION
            return response

        return handle

    def _serve(
        self,
        handler: Callable[[Request], Coroutine[Any, Any, Response]],
    ) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        """Wrap handler with the type check, the body limit and errors."""

        async def serve(request: Request) -> Response:  # noqa: PLR0911
            try:
                if refusal := _unsupported_type(request):
                    return refusal
                if refusal := await _read_body(request):
                    return refusal
                return await handler(request)
            except RequestValidationError as e:
                return invalid_request(e.errors())
            except ValidationError as e:
                return invalid_request(e.errors())
            except json.JSONDecodeError:
                return invalid_request([{'type': 'json_invalid'}])
            except HTTPException as e:
                # rest-utils' WebApplicationExceptionMapper gives an HTTP
                # error without a code of its own its status as the code
                return error_response(
                    e.status_code,
                    e.status_code,
                    str(e.detail),
                )
            except ClientDisconnect:
                # Nobody is left to read a reply
                return Response(status_code=400)
            except Exception:
                logger.exception(
                    'Unhandled error in %s %s',
                    request.method,
                    self.path,
                )
                return error_response(
                    500,
                    SERVER_ERROR,
                    'Internal server error',
                )

        return serve


async def _read_body(request: Request) -> Response | None:
    """Read the body up to MAX_BODY_BYTES, or the 413 reply past it.

    The body is kept on the request, where FastAPI reads it from.
    """
    length = request.headers.get('content-length')
    try:
        declared = None if length is None else int(length)
    except ValueError:
        declared = None
    if declared is not None and declared > MAX_BODY_BYTES:
        return _too_large()
    body = bytearray()
    async for chunk in request.stream():
        body += chunk
        if len(body) > MAX_BODY_BYTES:
            return _too_large()
    request._body = bytes(body)
    return None


def _too_large() -> Response:
    response = error_response(
        413,
        BODY_TOO_LARGE,
        f'The request body is over {MAX_BODY_BYTES} bytes, the most a '
        'consumer request may send',
    )
    # The rest of the body is not read
    response.headers['Connection'] = 'close'
    return response
