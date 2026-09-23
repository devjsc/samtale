from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any
from uuid import uuid4

from .exceptions import MessageValidationError


class Status(StrEnum):
    OK = "ok"
    REJECTED = "rejected"
    ERROR = "error"


@dataclass(slots=True)
class Message:
    sender: str
    type: str
    payload: dict[str, Any] = field(default_factory=dict)
    id: str = field(default_factory=lambda: str(uuid4()))
    reply_to: str | None = None
    status: Status | None = None
    reason: str | None = None

    def __post_init__(self) -> None:
        _require_text(self.id, "id")
        _require_text(self.sender, "sender")
        _require_text(self.type, "type")
        if not isinstance(self.payload, dict):
            raise MessageValidationError("payload must be an object")
        try:
            json.dumps(self.payload, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise MessageValidationError("payload must be JSON-compatible") from exc
        if self.status is not None and not isinstance(self.status, Status):
            try:
                self.status = Status(self.status)
            except (TypeError, ValueError) as exc:
                raise MessageValidationError(f"invalid status: {self.status!r}") from exc

        if self.reply_to is None:
            self._validate_uncorrelated()
            return

        _require_text(self.reply_to, "reply_to")
        if self.status is None:
            raise MessageValidationError("responses require status")
        if self.status is Status.OK:
            if self.reason is not None:
                raise MessageValidationError("ok responses cannot have reason")
            return
        _require_text(self.reason, "reason")

    def _validate_uncorrelated(self) -> None:
        if self.type == "_agent.error":
            if self.status is not Status.ERROR:
                raise MessageValidationError("uncorrelated framework errors require error status")
            _require_text(self.reason, "reason")
            return
        if self.status is not None:
            raise MessageValidationError("requests cannot have status")
        if self.reason is not None:
            raise MessageValidationError("requests cannot have reason")

    @classmethod
    def from_dict(cls, data: object) -> "Message":
        if not isinstance(data, dict):
            raise MessageValidationError("message must be a JSON object")

        allowed = {"id", "sender", "type", "payload", "reply_to", "status", "reason"}
        extra = set(data) - allowed
        if extra:
            raise MessageValidationError(f"unknown message field: {sorted(extra)[0]}")
        for field_name in ("id", "sender", "type"):
            if field_name not in data:
                raise MessageValidationError(f"missing required field: {field_name}")
        if "reply_to" in data and data["reply_to"] is None:
            raise MessageValidationError("reply_to must be a non-empty string")
        if "status" in data and data["status"] is None:
            raise MessageValidationError("status must be one of: ok, rejected, error")
        if "reason" in data and data["reason"] is None:
            raise MessageValidationError("reason must be a non-empty string")

        try:
            status = Status(data["status"]) if "status" in data else None
        except (TypeError, ValueError) as exc:
            raise MessageValidationError(f"invalid status: {data.get('status')!r}") from exc

        return cls(
            id=data["id"],
            sender=data["sender"],
            type=data["type"],
            payload=data.get("payload", {}),
            reply_to=data.get("reply_to"),
            status=status,
            reason=data.get("reason"),
        )

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "id": self.id,
            "sender": self.sender,
            "type": self.type,
            "payload": self.payload,
        }
        if self.reply_to is not None:
            data["reply_to"] = self.reply_to
        if self.status is not None:
            data["status"] = self.status.value
        if self.reason is not None:
            data["reason"] = self.reason
        return data

    @classmethod
    def ok(cls, sender: str, request: "Message", payload: dict[str, Any] | None = None) -> "Message":
        return cls(sender=sender, type=request.type, payload=payload or {}, reply_to=request.id, status=Status.OK)

    @classmethod
    def reject(
        cls,
        sender: str,
        request: "Message",
        reason: str,
        payload: dict[str, Any] | None = None,
    ) -> "Message":
        return cls(
            sender=sender,
            type=request.type,
            payload=payload or {},
            reply_to=request.id,
            status=Status.REJECTED,
            reason=reason,
        )

    @classmethod
    def error(
        cls,
        sender: str,
        request: "Message" | None,
        reason: str,
        payload: dict[str, Any] | None = None,
    ) -> "Message":
        return cls(
            sender=sender,
            type=request.type if request else "_agent.error",
            payload=payload or {},
            reply_to=request.id if request else None,
            status=Status.ERROR,
            reason=reason,
        )

    @property
    def is_request(self) -> bool:
        return self.reply_to is None and self.status is None

    @property
    def is_response(self) -> bool:
        return self.reply_to is not None


def _require_text(value: object, field: str) -> None:
    if not isinstance(value, str) or not value:
        raise MessageValidationError(f"{field} must be a non-empty string")
