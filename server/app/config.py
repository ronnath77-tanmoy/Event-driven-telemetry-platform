"""
config.py – Configuration (Phase 1)

Centralizes all configuration and environment-dependent values for the
telemetry ingestion service. Single source of truth for runtime configuration.

This module answers: "What values does the application need to run, and where do they come from?"

Responsibilities:
- Define Kafka-related configuration (bootstrap servers, topic name)
- Define application-level constants
- Read configuration from environment variables with safe defaults
- Expose configuration in a clean, importable way

Does NOT: business logic, Kafka producer logic, FastAPI route logic,
I/O beyond reading env vars, or dependency on application runtime state.
"""

import os
from types import SimpleNamespace


def _load_config() -> SimpleNamespace:
    """Load configuration from environment once at import time."""
    return SimpleNamespace(
        # Kafka
        KAFKA_BOOTSTRAP_SERVERS=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        KAFKA_TELEMETRY_TOPIC=os.getenv("KAFKA_TELEMETRY_TOPIC", "telemetry.events"),
        # Application (informational for Phase 1; useful later)
        SERVICE_NAME=os.getenv("SERVICE_NAME", "telemetry-ingestion-service"),
        ENVIRONMENT=os.getenv("ENVIRONMENT", "local"),
    )


# Loaded once at import; other modules import config instead of hardcoding
config = _load_config()