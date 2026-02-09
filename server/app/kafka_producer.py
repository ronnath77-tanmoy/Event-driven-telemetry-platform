"""
kafka_producer.py – Kafka Transport Layer

Responsibilities:
- Manage Kafka producer lifecycle (startup & shutdown)
- Publish events asynchronously to Kafka (fire-and-forget)
- Serialize events to JSON bytes
- Select partition key based on user_id
- Encapsulate all Kafka client details

This module answers: "How do we send an event to Kafka safely and asynchronously?"
"""

import json
import logging
import asyncio
from typing import Optional, Union
from aiokafka import AIOKafkaProducer
from .schemas import TelemetryEvent

logger = logging.getLogger(__name__)
TELEMETRY_TOPIC = "telemetry.events"
_producer:Optional[AIOKafkaProducer] = None

async def start_producer(bootsarp_servers: str) -> None:
    """
    Initialize and start the Kafka producer.

    Must be called once at application startup.
    Connection setup happens here, not per-request.

    Args:
        bootstrap_servers: Kafka bootstrap server address (e.g., "localhost:9092")
    """
    global _producer

    if _producer is not None:
        logger.warning("Kafka producer already started; ignoring duplicate start call")
        return
    
    _producer = AIOKafkaProducer(
        bootstrap_servers=bootsarp_servers,
        # Serialize values as UTF-8 encoded JSON bytes
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # Serialize keys as UTF-8 bytes (for partition key)
        key_serializer=lambda k: k.encode("utf-8") if k else None
        )

    await _producer.start()
    logger.info(f"Kafka producer started (bootstrap servers: {bootsarp_servers})" )

async def stop_producer() -> None:
    """
    Gracefully stop the Kafka producer.

    Must be called at application shutdown.
    Flushes pending messages before closing.
    """
    global _producer

    if _producer is None:
        logger.warning("Kafka producer not running; ignoring stop call")
        return

    await _producer.stop()
    _producer = None
    logger.info("Kafka producer stopped")


async def _send_and_log(event_data: dict, key: Optional[str]) -> None:
    """Send event to Kafka; log errors without raising (fire-and-forget)."""
    try:
        await _producer.send(TELEMETRY_TOPIC, value=event_data, key=key)
    except Exception:
        logger.exception("Failed to publish event to Kafka")


async def publish_event(event: Union[TelemetryEvent, dict]) -> None:
    """
    Publish a telemetry event to Kafka asynchronously.

    This is a fire-and-forget operation:
    - Non-blocking
    - Does not wait for Kafka acknowledgment
    - Does not retry on failure
    - Logs errors but does not raise exceptions

    Partition key strategy:
    - If user_id exists → use as key (ensures ordering per user)
    - If user_id absent → no key (enables parallelism)

    Args:
        event: Fully-built telemetry event (TelemetryEvent model or dict)
    """
    if _producer is None:
        logger.error("Cannot publish event: Kafka producer not started")
        return

    if isinstance(event, TelemetryEvent):
        event_data = event.model_dump()
    else:
        event_data = event

    # Partition key: user_id if present (ordering per user), else None (parallelism)
    key = event_data.get("user_id")

    # Fire-and-forget: send in background, log errors without raising
    asyncio.create_task(_send_and_log(event_data, key))

    
