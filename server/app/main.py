"""
main.py – Application Entry Point and Orchestrator (Phase 1)

Wires together the FastAPI app, routers, and Kafka producer lifecycle.

Responsibilities:
- Create and configure the FastAPI application
- Register routers (route definitions live in app.routers)
- Register startup and shutdown lifecycle hooks (Kafka producer start/stop)
- Global exception handler

Does NOT: define route handlers, business logic, or Kafka implementation details.
"""

import logging
from fastapi import FastAPI, Request

from . import kafka_producer
from .config import config
from .routers import ingestion, metrics

logger = logging.getLogger(__name__)

app = FastAPI(
    title=config.SERVICE_NAME,
)

app.include_router(ingestion.router)
app.include_router(metrics.router)


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
    logger.info("Application shutdown: stopping Kafka producer and flushing pending messages")
    await kafka_producer.stop_producer()
    logger.info("Application shutdown complete")

