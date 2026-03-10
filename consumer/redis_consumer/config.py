import os

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'telemetry.events')
KAFKA_CONSUMER_GROUP = os.getenv('KAFKA_CONSUMER_GROUP', 'redis-metrics')

# Redis Configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
REDIS_DB = int(os.getenv('REDIS_DB', '0'))

# Metric TTLs (in seconds)
ACTIVE_USERS_TTL = int(os.getenv('ACTIVE_USERS_TTL', '300'))
USER_LAST_SEEN_TTL = int(os.getenv('USER_LAST_SEEN_TTL', '300'))