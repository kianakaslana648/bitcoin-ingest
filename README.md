# Bitcoin Ingest

Test project mocking end-to-end process for real-time stream ingestion with **Kafka + Flink + Iceberg (Minio + Hive Metastore) + Trino**

Bitcoin Ingest is a pipeline to fetch and stream Bitcoin blockchain data in real-time using Kafka.

## Features

- Real-time Bitcoin data ingestion  
- Kafka producer integration  
- Supports APIs like mempool.space  
- Dockerized deployment for local testing

## Architecture
[Bitcoin APIs] --> [Kafka Producer (bitcoin-ingest)] --> [Kafka Topics] --> [Flink Streaming Jobs] --> [Iceberg Tables (Trino + Minio + Hive Metastore)]


## Prerequisites

- Docker & Docker Compose  
- Maven

## Installation (Local Testing)
### Git Clone
```bash
git clone https://github.com/yourusername/bitcoin-ingest.git
cd bitcoin-ingest
```

### Create Docker Networks
```bash
docker network create kafka-net
docker network create iceberg-net
```

### Kafka Cluster Configuration
```bash
docker network create --driver bridge --scope local kafka-network
cd kafka
docker-compose -f kafka-cluster-gui.yaml up -d
cd ..
```
Kafka UI will be default on http://localhost:9000/ if configured locally

### Flink Cluster Configuration
```bash
cd flink
docker-compose -f flink-gui.yaml up -d
cd ..
```
Flink UI will be default on http://localhost:8081/ if configured locally

### Iceberg Configuration (Minio + Hive Metastore + Trino)
```bash
cd iceberg
docker-compose -f iceberg.yaml up -d
cd ..
```
Minio UI will be default on http://localhost:9101/ if configured locally  
Trino UI will be default on http://localhost:8080/ if configured locally

### Kafka Producer Cronjob (bitcoin-cronjob)
```bash
cd kafka-producer-cronjob/jobs/bitcoin-api
mvn clean package
docker-compose -f bitcoin-cronjob.yaml up -d
cd ../../../
```

### Flink Consumer Streaming Job
```bash
cd flink/jobs/bitcoin_price_streaming
mvn clean package
cd ../../../
```
  Go to Flink UI http://localhost:8081/ and submit the streaming job jar with class being **BitcoinMempoolIngestion**
