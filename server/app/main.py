"""
main.py – Application Entry Point and Orchestrator (Phase 1)

Wires together all layers: HTTP (FastAPI), auth, schema validation,
event construction, and Kafka transport.

This module answers: "How does a telemetry request flow through the system end-to-end?"

Responsibilities:
- Create and configure the FastAPI application
- Define HTTP route(s)
- Register startup and shutdown lifecycle hooks
- Call Kafka producer start/stop
- Orchestrate request flow across layers

Does NOT: business logic, manual schema validation, Kafka implementation
details, event ID/timestamp generation, or authorization rules.
"""

import logging
from fastapi import Depends, FastAPI, Request
from . import kafka_producer
from .auth import get_current_role
from .config import config
from .event_builder import build_telemetry_event
from .schemas import IncomingTelemetryRequest

logger = logging.getLogger(__name__)

app = FastAPI(
    title=config.SERVICE_NAME,
)

@app.exception_handler(Exception)
async def log_unhandled_exception(request: Request, exc: Exception):
    """Log unexpected internal errors; re-raise so FastAPI returns 500."""
    logger.exception("Unhandled exception handling request %s: %s", request.url, exc)
    raise exc

@app.on_event("startup")
async def startup() -> None:
    """Initialize Kafka producer using config; ready before accepting traffic."""
    logger.info("Application startup: initializing Kafka producer")
    await kafka_producer.start_producer(config.KAFKA_BOOTSTRAP_SERVERS)
    logger.info("Kafka producer initialized")

@app.on_event("shutdown")
async def shutdown() -> None:
    """Gracefully stop Kafka producer and flush pending messages."""
    logger.inof("Application shutdown: stopping Kafka producer and flushing pending messages")
    await kafka_producer.stop_producer()
    logger.info("Application shutdown complete")

@app.post("/track", status_code=202)
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

