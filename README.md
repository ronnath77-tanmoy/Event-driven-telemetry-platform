# Event-driven-telemetry-platform
Real-time telemetry pipeline made using FastAPI, Kafka, Redis, and MongoDB with Dockerized deployment and New Relic monitoring.

Tech: FastAPI, Kafka, Redis, MongoDB, Docker, New Relic

Problem statement - Modern applications need to track user activity such as clicks, page visits, and interactions for analytics and monitoring. 
However, processing this data synchronously can slow down the main application and impact user experience.

This project demonstrates how to design an event-driven telemetry system that collects and processes user activity data asynchronously, ensuring the main application remains fast and responsive while still enabling real-time insights.



Architecture Overview - The system follows an event-driven architecture to decouple data ingestion from processing.

1. The client sends event data (e.g., user actions) to a FastAPI-based ingestion service.
2. The ingestion service validates the request and publishes the event to Kafka.
3. Kafka acts as a message broker, allowing events to be processed asynchronously.
4. Consumers read events from Kafka and store raw event data in MongoDB.
5. Redis is used to maintain real-time metrics such as active users and event counts.
6. REST APIs expose aggregated insights and metrics to clients or dashboards.

This design ensures scalability, fault tolerance, and non-blocking performance by separating ingestion from processing.
