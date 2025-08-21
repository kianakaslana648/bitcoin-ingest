# Kafka Docker Compose Setup

This Docker Compose configuration sets up a local Kafka environment with Zookeeper and a Kafka UI for monitoring.

---

## **1. Overview of Services**

The `docker-compose.yml` defines three main services:

| Service | Image | Purpose | Ports |
|---------|-------|---------|-------|
| **zookeeper** | `confluentinc/cp-zookeeper:latest` | Coordinates Kafka brokers and manages metadata. | `2181:2181` |
| **kafka** | `confluentinc/cp-kafka:latest` | Kafka broker that handles message production and consumption. | `9092:9092` |
| **kafka-ui** | `provectuslabs/kafka-ui:latest` | Web UI to monitor topics, consumers, and messages. | `9000:8080` |

---
