from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class IncomingTelemetryRequest(BaseModel):
    """
    Client-provided payload for POST /track.

    This model is strict ingress validation:
    - Clients cannot supply event_id or timestamp
    - event_type is required and non-empty
    - metadata is flexible and optional
    """

    event_type: str = Field(..., min_length=1)
    user_id: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class TelemetryEvent(BaseModel):
    """
    Canonical telemetry event schema sent to Kafka.

    Values are assumed to be already trusted and enriched elsewhere.
    This model performs shape validation only.
    """

    event_id: str  # UUID v4, generated elsewhere
    event_type: str
    user_id: Optional[str]
    timestamp: str  # ISO-8601 UTC
    metadata: Optional[Dict[str, Any]] = None