"""The route class of the consumer routes: Confluent-shaped errors.

Every reply of a consumer route that is not a success is Confluent's
`{"error_code": <int>, "message": <str>}`, including those FastAPI would
make itself: a body that is not JSON, a validation failure, and an
exception no handler caught. A body over MAX_BODY_BYTES is refused before
it is parsed. The other routes keep FastAPI's own replies.
"""

from __future__ import annotations

import json
import logging
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
        """Wrap FastAPI's handler with the body limit and error replies."""
        handler = super().get_route_handler()

        async def handle(request: Request) -> Response:  # noqa: PLR0911
            try:
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

        return handle


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
