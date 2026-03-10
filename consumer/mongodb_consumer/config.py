import os

"""
MongoDB Kafka consumer configuration.

This module answers one question:
    "What infrastructure does this consumer talk to?"

It centralizes:
- Kafka connection details
- MongoDB connection details
- Consumer group settings

No consumer logic, no MongoDB client logic, no business rules.
Only reads environment variables via the standard library.
"""

# ===== Kafka configuration =====

KAFKA_BOOTSTRAP_SERVERS: str = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

KAFKA_TOPIC: str = os.getenv(
    "KAFKA_TOPIC",
    "telemetry.events",
)

KAFKA_CONSUMER_GROUP: str = os.getenv(
    "KAFKA_CONSUMER_GROUP",
    "mongodb-writer",
)

# ===== MongoDB configuration =====

MONGODB_URI: str = os.getenv(
    "MONGODB_URI",
    "mongodb://localhost:27017",
)

MONGODB_DATABASE: str = os.getenv(
    "MONGODB_DATABASE",
    "telemetry",
)

MONGODB_COLLECTION: str = os.getenv(
    "MONGODB_COLLECTION",
    "events",
)

# Optional: simple settings object for structured access/extensibility

class Settings:
    # Kafka
    kafka_bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS
    kafka_topic: str = KAFKA_TOPIC
    kafka_consumer_group: str = KAFKA_CONSUMER_GROUP

    # MongoDB
    mongodb_uri: str = MONGODB_URI
    mongodb_database: str = MONGODB_DATABASE
    mongodb_collection: str = MONGODB_COLLECTION


settings = Settings()