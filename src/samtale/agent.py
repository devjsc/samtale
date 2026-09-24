from __future__ import annotations

"""HTTP agent runtime.

The `Agent` class exposes a small peer-to-peer HTTP message endpoint, a handler
registry, an internal queue, and outbound helpers for sending messages to other
agents. Handler execution is intentionally serialized through the queue.
"""

import asyncio
import inspect
import logging
import math
from contextlib import asynccontextmanager
from typing import Any

import httpx
from pydantic import BaseModel
import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.routing import Route

from .exceptions import (
    AgentClosedError,
    MessageValidationError,
    RemoteError,
    RemoteRejection,
    SendTimeout,
    TransportError,
)
from .helpers import (
    Handler,
    HandlerDefinition,
    complete_future,
    dispatch_handler,
    error_response,
    fail_future,
    json_message,
    remote_error_payload,
    request_message,
    response_message,
    status_for,
    validate_response,
)
from .message import Message, Status

logger = logging.getLogger(__name__)


class Agent:
    """An asynchronous HTTP endpoint that receives and sends `Message` objects.

    Each agent owns a Starlette app at `app`, an inbox queue, a bounded worker
    pool, and a reusable HTTP client for outbound messages.
    """

    def __init__(
        self,
        name: str,
        *,
        request_timeout: float = 5.0,
        handler_timeout: float | None = None,
        max_concurrency: int = 1,
        max_queue_size: int | None = None,
    ):
        """Create an agent.

        Args:
            name: Stable sender name used in outbound and response messages.
            request_timeout: HTTP client timeout for outbound requests.
            handler_timeout: Optional timeout for each handler invocation.
            max_concurrency: Maximum number of handlers that may run at once.
            max_queue_size: Maximum number of requests waiting for a worker.
        """
        if not isinstance(name, str) or not name.strip():
            raise ValueError("name must be a non-empty string")
        if not _valid_timeout(request_timeout):
            raise ValueError("request_timeout must be greater than zero")
        if handler_timeout is not None and not _valid_timeout(handler_timeout):
            raise ValueError("handler_timeout must be greater than zero")
        if (
            not isinstance(max_concurrency, int)
            or isinstance(max_concurrency, bool)
            or max_concurrency <= 0
        ):
            raise ValueError("max_concurrency must be a positive integer")
        if max_queue_size is not None and (
            not isinstance(max_queue_size, int)
            or isinstance(max_queue_size, bool)
            or max_queue_size <= 0
        ):
            raise ValueError("max_queue_size must be a positive integer or None")

        # Keep construction cheap: the HTTP app is ready, but the worker pool
        # starts with the app lifespan or an explicit start() call.
        self.name = name.strip()
        self.request_timeout = request_timeout
        self.handler_timeout = handler_timeout
        self.max_concurrency = max_concurrency
        self.max_queue_size = max_queue_size
        self.handlers: dict[str, HandlerDefinition] = {}
        self.inbox: asyncio.Queue[tuple[Message, asyncio.Future[Message]]] = (
            asyncio.Queue(maxsize=max_queue_size or 0)
        )
        self._workers: list[asyncio.Task[None]] = []
        self._client: httpx.AsyncClient | None = None
        self._active_futures: set[asyncio.Future[Message]] = set()
        self._accepting = False

        @asynccontextmanager
        async def lifespan(app: Starlette):
            # Starlette owns the server lifecycle; the agent owns the inbox worker.
            await self.start()
            try:
                yield
            finally:
                await self.close()

        async def receive(request: Request):
            # HTTP requests stop at the queue boundary. The agent loop does the
            # actual handler lookup and execution.
            try:
                message = await request_message(request)
            except MessageValidationError as exc:
                return error_response(self.name, None, "malformed_message", str(exc))
            except Exception:
                logger.exception("Failed to read inbound message")
                return error_response(
                    self.name,
                    None,
                    "internal_error",
                    "The message could not be processed",
                )

            if not self._accepting:
                return error_response(
                    self.name, message, "agent_closed", "The agent is closed"
                )

            future: asyncio.Future[Message] = asyncio.get_running_loop().create_future()
            try:
                self.inbox.put_nowait((message, future))
            except asyncio.QueueFull:
                response = Message.reject(
                    self.name,
                    message,
                    "agent_busy",
                    {"retryable": True},
                )
                return json_message(response, status_for(response))
            try:
                response = await future
            except asyncio.CancelledError:
                future.cancel()
                raise
            except AgentClosedError:
                response = Message.error(
                    self.name,
                    message,
                    "agent_closed",
                    {"message": "The agent is closed"},
                )

            return json_message(response, status_for(response))

        self.app = Starlette(
            lifespan=lifespan, routes=[Route("/messages", receive, methods=["POST"])]
        )

    def on(
        self,
        message_type: str,
        *,
        model: type[BaseModel] | None = None,
    ):
        """Register an async handler for a message type.

        Args:
            message_type: Domain message type, for example `temperature.read`.
            model: Optional Pydantic model used to validate and parse the payload.

        Returns:
            A decorator that stores the handler and returns it unchanged.
        """
        if not isinstance(message_type, str) or not message_type:
            raise ValueError("message_type must be a non-empty string")
        if model is not None and (
            not isinstance(model, type) or not issubclass(model, BaseModel)
        ):
            raise TypeError("model must be a Pydantic BaseModel subclass")

        # Decorator API: @agent.on("read") registers the async handler.
        def decorator(fn: Handler):
            if not inspect.iscoroutinefunction(fn):
                raise TypeError("handler must be an async function")
            if message_type in self.handlers:
                raise ValueError(f"handler already registered for {message_type!r}")
            self.handlers[message_type] = HandlerDefinition(
                handler=fn,
                model=model,
                schema=model.model_json_schema() if model is not None else {},
            )
            return fn

        return decorator

    def reject(
        self, request: Message, reason: str, payload: dict[str, Any] | None = None
    ) -> Message:
        """Build a correlated domain rejection from inside a handler."""
        return Message.reject(self.name, request, reason, payload)

    def ok(self, request: Message, payload: dict[str, Any] | None = None) -> Message:
        """Build a correlated successful response from inside a handler."""
        return Message.ok(self.name, request, payload)

    async def start(self) -> None:
        """Start the outbound HTTP client and handler workers."""
        # Idempotent so tests or embedding code can call it directly.
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=self.request_timeout)
        self._workers = [worker for worker in self._workers if not worker.done()]
        self._workers.extend(
            asyncio.create_task(self._loop())
            for _ in range(self.max_concurrency - len(self._workers))
        )
        self._accepting = True

    async def close(self) -> None:
        """Stop the worker pool, fail pending requests, and close the client."""
        # Cancel the background worker cleanly during app shutdown.
        self._accepting = False
        error = AgentClosedError(f"agent {self.name!r} is closed")
        for future in tuple(self._active_futures):
            fail_future(future, error)

        for worker in self._workers:
            worker.cancel()
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()
        self._active_futures.clear()

        while not self.inbox.empty():
            _message, future = self.inbox.get_nowait()
            fail_future(future, error)
            self.inbox.task_done()

        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _loop(self) -> None:
        """Dispatch queued messages until cancelled."""
        while True:
            message, future = await self.inbox.get()
            self._active_futures.add(future)
            try:
                if not future.done():
                    complete_future(future, await self._dispatch(message))
            except Exception:
                logger.exception("Handler %r failed unexpectedly", message.type)
                complete_future(
                    future,
                    Message.error(
                        self.name,
                        message,
                        "handler_failure",
                        {"message": "The message handler failed"},
                    ),
                )
            finally:
                self._active_futures.discard(future)
                self.inbox.task_done()

    async def _dispatch(self, message: Message) -> Message:
        """Run the registered handler and normalize its result to a response."""
        definition = self.handlers.get(message.type)
        if definition is None:
            return Message.reject(
                self.name,
                message,
                "unsupported_message",
                {
                    "accepted_messages": [
                        {
                            "type": message_type,
                            "schema": self.handlers[message_type].schema,
                        }
                        for message_type in sorted(self.handlers)
                    ],
                },
            )

        return await dispatch_handler(
            self.name, definition, message, self.handler_timeout
        )

    async def send(self, address: str, message_type: str, **payload: Any) -> Message:
        """Send a request to another agent and return the full response message.

        Raises:
            SendTimeout: The HTTP request timed out.
            TransportError: The transport failed or the response envelope was invalid.
            RemoteError: The peer returned an HTTP error with a valid agent error.
        """
        # Create the wire message, POST it to the peer, and return the complete
        # validated response envelope.
        message = Message(sender=self.name, type=message_type, payload=payload)
        await self.start()
        assert self._client is not None

        try:
            response = await self._client.post(
                f"{address.rstrip('/')}/messages", json=message.to_dict()
            )
            response.raise_for_status()
        except httpx.TimeoutException as exc:
            raise SendTimeout(address, message.id, self.request_timeout) from exc
        except httpx.HTTPStatusError as exc:
            envelope = response_message(exc.response, address)
            validate_response(address, message, envelope)
            if envelope.status is Status.ERROR:
                raise RemoteError(
                    address, message.id, remote_error_payload(envelope)
                ) from exc
            raise TransportError(
                f"{address} returned HTTP {exc.response.status_code}"
            ) from exc
        except httpx.HTTPError as exc:
            raise TransportError(str(exc)) from exc

        envelope = response_message(response, address)
        validate_response(address, message, envelope)
        return envelope

    async def ask(
        self, address: str, message_type: str, **payload: Any
    ) -> dict[str, Any]:
        """Send a request that is expected to succeed and return only payload."""
        response = await self.send(address, message_type, **payload)
        self._require_ok(address, response)
        return response.payload

    async def emit(
        self,
        address: str,
        message_type: str,
        **payload: Any,
    ) -> None:
        """Send a message and discard the successful response payload.

        This is an acknowledgement-based convenience method, not guaranteed
        delivery and not true fire-and-forget.
        """
        response = await self.send(
            address,
            message_type,
            **payload,
        )

        self._require_ok(address, response)

    @staticmethod
    def _require_ok(address: str, response: Message) -> None:
        if response.status is Status.REJECTED:
            raise RemoteRejection(
                address,
                response.reply_to or "",
                response.reason or "rejected",
                response.payload,
            )
        if response.status is Status.ERROR:
            raise RemoteError(
                address, response.reply_to or "", remote_error_payload(response)
            )

    def run(self, *, host: str = "127.0.0.1", port: int = 8000) -> None:
        """Run the agent's Starlette app with uvicorn."""
        # Convenience runner for examples and small local processes.
        uvicorn.run(self.app, host=host, port=port)


def _valid_timeout(value: object) -> bool:
    return (
        isinstance(value, int | float)
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value > 0
    )
