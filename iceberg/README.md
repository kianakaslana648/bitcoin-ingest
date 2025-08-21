# Iceberg Docker Compose Setup

This Docker Compose configuration sets up a local environment for **Apache Iceberg** with Hive Metastore, MinIO as the object store, Trino for querying, and Python test/initialization containers.

---

## **1. Overview of Services**

The `docker-compose.yml` defines the following services:

| Service | Image | Purpose | Ports |
|---------|-------|---------|-------|
| **hive-metastore-db** | `postgres:latest` | PostgreSQL database for Hive Metastore metadata storage | `5432:5432` |
| **hive-metastore** | `apache/hive:4.0.0` | Hive Metastore service providing metadata for Iceberg tables | `9083:9083` |
| **minio** | `minio/minio` | S3-compatible object store for storing Iceberg table data | `9100:9000`, `9101:9001` (console) |
| **trino** | `trinodb/trino:476` | Query engine to run SQL queries on Iceberg tables | `8080:8080` |
| **minio-setup** | `minio/mc` | Initializes MinIO bucket(s) and sets permissions | N/A |
| **python-test** | `python:3.11-slim` | Test Hive Metastore and MinIO integration with Python scripts | N/A |
| **iceberg-init** | `python:3.11-slim` | Initializes Iceberg tables via Python scripts | N/A |

---
