from typing import Any


class ChatterError(Exception):
    """Base exception for chatters."""


class AgentClosedError(ChatterError):
    """Raised when work cannot complete because the agent is closing."""


class MessageValidationError(ChatterError):
    """Raised when a wire message is malformed."""


class RemoteError(ChatterError):
    def __init__(self, target: str, message_id: str, error: dict):
        self.target = target
        self.message_id = message_id
        self.error = error
        super().__init__(f"{target} returned error for {message_id}: {error}")


class RemoteRejection(ChatterError):
    """Raised when a remote agent rejects an otherwise valid request."""

    def __init__(
        self,
        target: str,
        message_id: str,
        reason: str,
        payload: dict[str, Any],
    ):
        self.target = target
        self.message_id = message_id
        self.reason = reason
        self.payload = payload
        super().__init__(f"{target} rejected message {message_id}: {reason}")


class SendTimeout(ChatterError):
    def __init__(self, target: str, message_id: str, timeout: float):
        self.target = target
        self.message_id = message_id
        self.timeout = timeout
        super().__init__(f"message {message_id} to {target} timed out after {timeout:g}s")


class TransportError(ChatterError):
    """Raised when the HTTP transport fails or returns a malformed response."""
