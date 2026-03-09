"""
ingestion.py – Telemetry ingestion route.

Defines the /track endpoint: validate body → auth → build event → publish to Kafka → 202.
"""

from fastapi import APIRouter, Depends

from .. import kafka_producer
from ..auth import get_current_role
from ..event_builder import build_telemetry_event
from ..schemas import IncomingTelemetryRequest

router = APIRouter()


@router.post("/track", status_code=202)
async def track(
    body: IncomingTelemetryRequest,
    role: str = Depends(get_current_role),
) -> None:
    """
    Ingest a telemetry event.

    Flow: validate body → auth → build event → publish (fire-and-forget) → 202.
    Does not imply persistence, consumer processing, or delivery guarantees.
    """
    event = build_telemetry_event(body)
    await kafka_producer.publish_event(event)
    return None
