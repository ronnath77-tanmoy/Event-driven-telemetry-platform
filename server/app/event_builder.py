from datetime import datetime, timezone
from uuid import uuid4

from .schemas import IncomingTelemetryRequest, TelemetryEvent


def _current_utc_iso8601() -> str:
    """
    Return current UTC time in ISO-8601 format with 'Z' suffix.
    Example: '2025-01-01T12:34:56.789123Z'
    """
    now = datetime.now(timezone.utc)
    # Pydantic and most consumers accept either '+00:00' or 'Z'; we normalize to 'Z'.
    return now.isoformat().replace("+00:00", "Z")


def build_telemetry_event(
    request: IncomingTelemetryRequest,
) -> TelemetryEvent:
    """
    Construct a canonical TelemetryEvent from validated request data.

    Responsibilities:
    - Assume `request` has already passed schema validation.
    - Generate `event_id` (UUID v4).
    - Generate server-side ingestion `timestamp` (UTC, ISO-8601).
    - Preserve client-provided fields (`event_type`, `user_id`, `metadata`).

    This function has no side effects other than calling uuid/datetime from
    the standard library and returns a pure data object.
    """
    return TelemetryEvent(
        event_id=str(uuid4()),
        event_type=request.event_type,
        user_id=request.user_id,
        timestamp=_current_utc_iso8601(),
        metadata=request.metadata,
    )