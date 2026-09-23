from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

import httpx
from pydantic import BaseModel, ValidationError
from starlette.requests import Request
from starlette.responses import JSONResponse

from .exceptions import MessageValidationError, TransportError
from .message import Message, Status

Handler = Callable[..., Awaitable[Any]]
PayloadModel = type[BaseModel]
logger = logging.getLogger(__name__)
HANDLER_FAILURE_MESSAGE = "The message handler failed"


@dataclass(slots=True)
class HandlerDefinition:
    handler: Handler
    model: PayloadModel | None = None
    schema: dict[str, Any] = field(default_factory=dict)


def json_message(message: Message, status: int) -> JSONResponse:
    return JSONResponse(message.to_dict(), status_code=status)


def error_response(
    sender: str,
    request: Message | None,
    reason: str,
    detail: str,
) -> JSONResponse:
    message = Message.error(sender, request, reason, {"message": detail})
    return json_message(message, status_for(message))


async def request_message(request: Request) -> Message:
    try:
        data = await request.json()
    except (ValueError, UnicodeDecodeError) as exc:
        raise MessageValidationError(f"invalid JSON: {exc}") from exc

    message = Message.from_dict(data)
    if not message.is_request:
        raise MessageValidationError("the messages endpoint only accepts requests")
    return message


def status_for(message: Message) -> int:
    if message.status is not Status.ERROR:
        return 200
    return {
        "malformed_message": 400,
        "handler_timeout": 504,
        "agent_closed": 503,
    }.get(message.reason, 500)


def parse_payload(
    agent_name: str,
    definition: HandlerDefinition,
    message: Message,
) -> tuple[Message | None, BaseModel | None]:
    if definition.model is not None:
        try:
            return None, definition.model.model_validate(message.payload)
        except ValidationError as exc:
            return (
                Message.reject(
                    agent_name,
                    message,
                    "invalid_payload",
                    {
                        "expected_message": {"type": message.type, "schema": definition.schema},
                        "issues": pydantic_issues(exc),
                    },
                ),
                None,
            )

    return None, None


async def dispatch_handler(
    sender: str,
    definition: HandlerDefinition,
    message: Message,
    timeout: float | None,
) -> Message:
    invalid, parsed_payload = parse_payload(sender, definition, message)
    if invalid is not None:
        return invalid

    try:
        invocation = (
            definition.handler(message, parsed_payload)
            if definition.model is not None
            else definition.handler(message)
        )
        result = await invocation if timeout is None else await asyncio.wait_for(invocation, timeout)
    except asyncio.TimeoutError:
        return Message.error(
            sender,
            message,
            "handler_timeout",
            {"message": f"Handler timed out after {timeout:g}s"},
        )
    except Exception:
        logger.exception("Handler %r failed", message.type)
        return Message.error(sender, message, "handler_failure", {"message": HANDLER_FAILURE_MESSAGE})

    return normalize_handler_result(sender, message, result)


def normalize_handler_result(sender: str, request: Message, result: Any) -> Message:
    try:
        if isinstance(result, Message):
            response = normalize_handler_message(sender, request, result)
        elif result is None:
            response = Message.ok(sender, request)
        elif isinstance(result, dict):
            response = Message.ok(sender, request, result)
        else:
            raise MessageValidationError("Handler must return a dict, Message, or None")
    except MessageValidationError as exc:
        return Message.error(sender, request, "invalid_handler_result", {"message": str(exc)})

    return response


def pydantic_issues(exc: ValidationError) -> list[dict[str, Any]]:
    return [
        {
            "field": ".".join(str(part) for part in error["loc"]),
            "reason": error["type"],
            "message": error["msg"],
        }
        for error in exc.errors()
    ]


def normalize_handler_message(sender: str, request: Message, result: Message) -> Message:
    if result.reply_to != request.id or result.type != request.type:
        raise MessageValidationError("handler response must match the request")
    if result.sender != sender:
        return Message(
            sender=sender,
            type=result.type,
            payload=result.payload,
            id=result.id,
            reply_to=result.reply_to,
            status=result.status,
            reason=result.reason,
        )
    return result


def validate_response(address: str, request: Message, response: Message) -> None:
    if response.reply_to != request.id:
        raise TransportError(f"{address} response did not match message {request.id}")
    if response.type != request.type:
        raise TransportError(f"{address} response type {response.type!r} did not match {request.type!r}")
    if response.status is None:
        raise TransportError(f"{address} response did not include status")


def remote_error_payload(response: Message) -> dict[str, Any]:
    return {
        "status": response.status.value if response.status else None,
        "reason": response.reason,
        "payload": response.payload,
    }


def complete_future(future: asyncio.Future[Message] | None, result: Message) -> None:
    if future is not None and not future.done():
        future.set_result(result)


def fail_future(future: asyncio.Future[Message] | None, exc: Exception) -> None:
    if future is not None and not future.done():
        future.set_exception(exc)


def response_message(response: httpx.Response, target: str) -> Message:
    try:
        return Message.from_dict(response.json())
    except Exception as exc:
        raise TransportError(f"{target} returned a malformed response") from exc
