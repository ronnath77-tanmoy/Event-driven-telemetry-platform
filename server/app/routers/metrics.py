"""
metrics.py – Read-only HTTP interface for real-time telemetry metrics from Redis.

Exposes telemetry insights for admin users. This is a READ layer only;
it does not write to Redis, Kafka, or MongoDB.
"""

import logging
import os
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, status
from redis import Redis

from ..auth import get_current_role

logger = logging.getLogger(__name__)

# Redis config (same as redis_consumer: env with safe defaults)
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

# Key patterns used by redis_consumer (single source of naming)
EVENTS_COUNT_PREFIX = "events:count:"
ACTIVE_USERS_KEY = "active_users"
USER_LAST_SEEN_PREFIX = "user:last_seen:"

router = APIRouter(prefix="/metrics", tags=["metrics"])

_redis_client: Redis | None = None


def get_redis() -> Redis:
    """Return a Redis client; create lazily and reuse. Uses same config as redis_consumer."""
    global _redis_client
    if _redis_client is None:
        _redis_client = Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=True,
        )
    return _redis_client


def require_admin(role: str = Depends(get_current_role)) -> str:
    """Dependency: require role to be 'admin'; otherwise raise 403."""
    if role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin role required",
        )
    return role


def _log_access(request: Request, role: str) -> None:
    """Log role, endpoint, and timestamp. Do not log returned data."""
    logger.info(
        "Metrics access: role=%s endpoint=%s at %s",
        role,
        request.url.path,
        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    )


@router.get("/events")
async def get_events_counts(
    request: Request,
    role: str = Depends(require_admin),
) -> dict[str, int]:
    """Return counts for all event types (keys matching events:count:*)."""
    _log_access(request, role)
    redis_client = get_redis()
    try:
        keys = redis_client.keys(f"{EVENTS_COUNT_PREFIX}*")
    except Exception as e:
        logger.exception("Redis error in GET /metrics/events: %s", e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Metrics unavailable") from e
    result: dict[str, int] = {}
    for key in keys or []:
        if key.startswith(EVENTS_COUNT_PREFIX):
            event_type = key[len(EVENTS_COUNT_PREFIX) :]
            try:
                val = redis_client.get(key)
                result[event_type] = int(val) if val is not None else 0
            except (ValueError, TypeError):
                result[event_type] = 0
    return result


@router.get("/events/{event_type}")
async def get_event_count(
    event_type: str,
    request: Request,
    role: str = Depends(require_admin),
) -> dict[str, Any]:
    """Return count for a specific event type."""
    _log_access(request, role)
    key = f"{EVENTS_COUNT_PREFIX}{event_type}"
    redis_client = get_redis()
    try:
        val = redis_client.get(key)
    except Exception as e:
        logger.exception("Redis error in GET /metrics/events/{%s}: %s", event_type, e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Metrics unavailable") from e
    count = int(val) if val is not None else 0
    return {"event_type": event_type, "count": count}


@router.get("/active-users")
async def get_active_users(
    request: Request,
    role: str = Depends(require_admin),
) -> dict[str, Any]:
    """Return currently active users (set active_users)."""
    _log_access(request, role)
    redis_client = get_redis()
    try:
        users = redis_client.smembers(ACTIVE_USERS_KEY)
    except Exception as e:
        logger.exception("Redis error in GET /metrics/active-users: %s", e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Metrics unavailable") from e
    user_list = list(users) if users else []
    return {"count": len(user_list), "users": user_list}


@router.get("/user/{user_id}")
async def get_user_last_seen(
    user_id: str,
    request: Request,
    role: str = Depends(require_admin),
) -> dict[str, Any]:
    """Return last seen timestamp for a user."""
    _log_access(request, role)
    key = f"{USER_LAST_SEEN_PREFIX}{user_id}"
    redis_client = get_redis()
    try:
        val = redis_client.get(key)
    except Exception as e:
        logger.exception("Redis error in GET /metrics/user/{%s}: %s", user_id, e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Metrics unavailable") from e
    return {"user_id": user_id, "last_seen": val}
