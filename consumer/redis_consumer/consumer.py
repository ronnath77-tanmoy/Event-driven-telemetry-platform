from ensurepip import bootstrap
import json
import logging
import signal
import sys

from kafka import KafkaConsumer
from redis import Redis

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_CONSUMER_GROUP,
    REDIS_HOST,
    REDIS_PORT,
    REDIS_DB,
    ACTIVE_USERS_TTL,
    USER_LAST_SEEN_TTL,
)


# ===== Logging =====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)

_shutdown_requested = False

def _handle_shutdown_signal(signum, frame): # signum, frame
    global _shutdown_requested
    logger.info("Shutdown signal received (%s). Stopping consumer loop...", signum)
    _shutdown_requested = True


signal.signal(signal.SIGINT, _handle_shutdown_signal)
signal.signal(signal.SIGTERM, _handle_shutdown_signal)  


# ===== Redis =====

def create_redis_client() -> Redis:
    """Create Redis client and validate connection with ping."""
    client = Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True) #decode_reposnses ??
    client.ping() # ping??
    logger.info("Redis connected (host=%s, port=%s, db=%s).", REDIS_HOST, REDIS_PORT, REDIS_DB)
    return client
    

# ===== Kafka =====

def create_kafka_consumer() -> KafkaConsumer:
    """Create KafkaConsumer from config; manual commit only."""
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    consumer.subscribe([KAFKA_TOPIC])
    logger.info(
        "Kafka consumer subscribed to topic=%s, group=%s.",
        KAFKA_TOPIC,
        KAFKA_CONSUMER_GROUP,
    )
    return consumer


# ===== Metric processing =====

def process_event(redis_client: Redis, event: dict) -> None:
    """
    Update Redis metrics from one telemetry event.
    - Event type counter: INCR events:count:<event_type>
    - Active users (rolling): SADD active_users <user_id>, EXPIRE ACTIVE_USERS_TTL
    - Per-user last seen: SET user:last_seen:<user_id> <timestamp> EX USER_LAST_SEEN_TTL
    """
    event_type = event.get('event_type') or "unknown"
    redis_client.incr(f"events:count:{event_type}")

    user_id = event.get('user_id')
    if user_id is not None:
        redis_client.sadd("active_users", str(user_id)) #sadd??
        redis_client.expire("active_users", ACTIVE_USERS_TTL)

        ts = event.get("timestamp")
        if ts is not None:
            redis_client.set(
                f"user:last_seen:{user_id}",
                str(ts),
                ex=USER_LAST_SEEN_TTL,
            )


def run_consumer() -> None:
    redis_client = create_redis_client()
    consumer = create_kafka_consumer()

    logger.info("Redis metrics consumer started. Polling for messages.")

    try:
        for message in consumer:
            if _shutdown_requested:
                break

            raw = message.value
            try:
                text = raw.decode("utf-8")
            except (AttributeError, UnicodeDecodeError) as e:
                logger.warning("Messsage decode error(offset=%s): %s", message.offset, e)
                consumer.commit()
                continue

            try:
                event = json.loads(text)
            except json.JSONDecodeError as e:
                logger.exception("JSON decode error(offset=%s): %s", message.offset, e)
                consumer.commit()
                continue

            try:
                process_event(redis_client, event)
            except Exception as e:
                logger.warning("Metric update error(offset=%s): %s", message.offset, e)
                # Do not commit; allow retry
                continue

            consumer.commit()

    finally:
        consumer.close()
        redis_client.close()
        logger.info("Consumer stopped.")


def main() -> None:
    try:
        run_consumer()
    except Exception as e:
        logger.exception("Fatal startup error: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()