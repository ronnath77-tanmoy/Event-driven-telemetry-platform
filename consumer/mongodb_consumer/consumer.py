import json
import logging
import signal
import sys
from typing import Any, Dict

from kafka import KafkaConsumer
from pymongo import MongoClient, errors as pymongo_errors

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_CONSUMER_GROUP,
    MONGODB_URI,
    MONGODB_DATABASE,
    MONGODB_COLLECTION,
)

# ===== Logging configuration =====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

_shutdown_requested = False

def _handle_shutdown_signal(signum, frame):
    global _shutdown_requested
    logger.info("Shutdown signal received (%s). Stopping consumer loop...", signum)
    _shutdown_requested = True

# Register signal handlers for graceful shutdown
signal.signal(signal.SIGINT, _handle_shutdown_signal)
signal.signal(signal.SIGTERM, _handle_shutdown_signal)

# ===== MongoDB setup =====

def create_mongo_collection():
    """
    Create and return a MongoDB collection object.

    - Connects using MONGODB_URI
    - Selects database + collection from config
    - Ensures a UNIQUE index exists on event_id
    """
    try:
        client = MongoClient(MONGODB_URI)
        db = client[MONGODB_DATABASE]
        collection = db[MONGODB_COLLECTION]

        # Ensure unique index on event_id for idempotency
        collection.create_index("event_id", unique=True)
        logger.info(
            "MongoDB connected (db=%s, collection=%s) and unique index on 'event_id' ensured.",
            MONGODB_DATABASE,
            MONGODB_COLLECTION,
        )
        return collection
    except Exception as exc:
        logger.exception("Fatal error connecting to MongoDB: %s", exc)
        # Startup must fail loudly if we cannot talk to MongoDB
        raise


# ===== Kafka setup =====

def create_kafka_consumer() -> KafkaConsumer:
    """
    Create and return a configured KafkaConsumer.

    Uses configuration from config.py, with:
    - auto_offset_reset='earliest'
    - enable_auto_commit=False  (manual commit only after successful write)
    """
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_CONSUMER_GROUP,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda v: v,  # raw bytes; we'll JSON-decode ourselves
        )
        logger.info(
            "Kafka consumer created and subscribed (topic=%s, group=%s, bootstrap_servers=%s).",
            KAFKA_TOPIC,
            KAFKA_CONSUMER_GROUP,
            KAFKA_BOOTSTRAP_SERVERS,
        )
        return consumer
    except Exception as exc:
        logger.exception("Fatal error creating Kafka consumer: %s", exc)
        # Startup must fail loudly if we cannot talk to Kafka
        raise

# ===== Main processing logic =====

def process_message_value(raw_value: bytes) -> Dict[str, Any]:
    """
    Deserialize a Kafka message value.

    - Expects JSON-encoded bytes
    - Returns dict on success
    - Raises ValueError on JSON decoding issues
    """
    try:
         # Decode bytes and parse JSON
         text = raw_value.decode("utf-8")
         payload = json.loads(text)
         return payload
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Invalid JSON payload: {exc}") from exc

def run_consumer_loop():
    """
    Blocking loop consuming from Kafka and writing to MongoDB.

    Uses poll() for responsive shutdown and checks _shutdown_requested
    at both start and end of message processing to ensure:
    - Responsive shutdown (checks flag every second even when no messages)
    - Current work completes before shutdown (checks at end of processing)

    For each message:
    - Deserialize JSON
    - Insert into MongoDB
    - If insert succeeded OR duplicate key: commit offset
    - If insert failed for other reasons: do NOT commit offset
    """
    collection = create_mongo_collection()
    consumer = create_kafka_consumer()

    logger.info("Starting main consumption loop.")

    try:
        while not _shutdown_requested:
            # Poll with timeout - allows checking shutdown flag frequently
            # Returns empty dict if no messages within timeout_ms
            message_pack = consumer.poll(timeout_ms=1000)  # 1 second timeout
            
            if not message_pack:
                # No messages received, check shutdown flag again
                continue
            
            # Process all messages in this batch
            for topic_partition, messages in message_pack.items():
                for message in messages:
                    # Check at START - exit immediately if shutdown requested
                    if _shutdown_requested:
                        logger.info("Shutdown requested. Breaking consumption loop.")
                        break

                    try:
                        event = process_message_value(message.value)
                    except ValueError as exc:
                        # JSON decoding error: log and skip; commit offset so we don't reprocess bad data
                        logger.error(
                            "Skipping invalid JSON message at offset %s: %s",
                            message.offset,
                            exc,
                        )
                        consumer.commit()
                        # Check at END - don't start next iteration if shutdown requested
                        if _shutdown_requested:
                            logger.info("Shutdown requested after processing message. Breaking loop.")
                            break
                        continue

                    # Optional: avoid logging full payloads
                    event_id = event.get("event_id")
                    if event_id is None:
                        # Depending on your upstream contract, you might:
                        # - treat this as a hard failure (do not commit)
                        # - or log & skip (but that breaks idempotency guarantee).
                        logger.error(
                            "Missing 'event_id' in event at offset %s; not committing offset.",
                            message.offset,
                        )
                        # Do NOT commit; message will be retried.
                        # Check at END
                        if _shutdown_requested:
                            logger.info("Shutdown requested after processing message. Breaking loop.")
                            break
                        continue

                    try:
                        collection.insert_one(event)
                        # Insert success
                        logger.debug("Inserted event_id=%s into MongoDB.", event_id)
                        consumer.commit()
                    except pymongo_errors.DuplicateKeyError:
                        # Duplicate event_id: treat as success, commit offset
                        logger.info("Duplicate event detected (event_id=%s). Committing offset.", event_id)
                        consumer.commit()
                    except Exception as exc:
                        # MongoDB failure: log, DO NOT commit offset
                        logger.exception(
                            "MongoDB insert failed for event_id=%s at offset %s. Offset not committed.",
                            event_id,
                            message.offset,
                        )
                        # Optionally sleep/backoff here for transient errors
                        # For now, just continue; Kafka will redeliver since we didn't commit.
                        # Check at END
                        if _shutdown_requested:
                            logger.info("Shutdown requested after processing message. Breaking loop.")
                            break
                        continue
                    
                    # Check at END of successful processing - ensures we finish current work before shutdown
                    if _shutdown_requested:
                        logger.info("Shutdown requested after processing message. Breaking loop.")
                        break
                
                # Break outer loop if shutdown requested
                if _shutdown_requested:
                    break
    
    finally:
        logger.info("Closing Kafka consumer.")
        try:
            consumer.close()
        except Exception:
            logger.exception("Error while closing Kafka consumer.")


def main():
    logger.info("MongoDB writer consumer starting up.")
    try:
        run_consumer_loop()
    except Exception as exc:
        # Only fatal startup errors or unexpected crashes should reach here
        logger.exception("Consumer terminated due to fatal error: %s", exc)
        sys.exit(1)

    logger.info("Consumer shut down cleanly.")
    sys.exit(0)


if __name__ == "__main__":
    main()