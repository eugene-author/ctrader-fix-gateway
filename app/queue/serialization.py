"""The AMQP message envelope."""

import json
from datetime import UTC, datetime
from uuid import uuid4

from pydantic import BaseModel


class Envelope(BaseModel):
    """Every command and event is included here."""

    message_id: str
    message_type: str
    version: int
    occurred_at: datetime
    correlation_id: str
    causation_id: str
    account_id: str
    payload: dict[str, object]


def build_envelope(
    *,
    message_type: str,
    payload: dict[str, object],
    correlation_id: str,
    causation_id: str,
    account_id: str,
    message_id: str | None = None,
    version: int = 1,
) -> Envelope:
    """Build an Envelope, minting a message_id if one isn't supplied."""
    return Envelope(
        message_id=message_id or str(uuid4()),
        message_type=message_type,
        version=version,
        occurred_at=datetime.now(UTC),
        correlation_id=correlation_id,
        causation_id=causation_id,
        account_id=account_id,
        payload=payload,
    )


def encode_envelope(envelope: Envelope) -> bytes:
    """Serialize an Envelope to the JSON bytes that go on the payload."""
    return envelope.model_dump_json().encode("utf-8")


def decode_envelope(body: bytes) -> Envelope:
    """Parse payload JSON bytes back into an Envelope."""
    return Envelope.model_validate(json.loads(body))
