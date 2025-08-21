# Flink Job: Bitcoin Mempool Kafka -> Iceberg

This Flink streaming job consumes Bitcoin mempool data from a Kafka topic and writes it into **Iceberg tables** via Hive Metastore. It handles **price, block tip, and fee data**.

---

## 1. Overview

**Data Flow:**  

Kafka (mempool topic)  
│  
Flink Streaming Job  
│  
├── Price Data ──> Iceberg Table: mempool_price  
├── Block Tip ──> Iceberg Table: mempool_blocktip  
└── Fees ──> Iceberg Table: mempool_fee  

- Kafka acts as the source of mempool data.
- Flink processes and transforms the JSON events.
- Iceberg tables provide versioned storage with schema evolution support.
- Hive Metastore stores table metadata.
  
---

## 2. Kafka Source

- **Topic:** `mempool`  
- **Consumer Group:** `flink-iceberg-consumer`  
- **Bootstrap Servers:** `kafka:9092`  

**Data format:** JSON with `source` field to distinguish event type (`price`, `blocktip`, `fees`) and `data` containing the raw API response.

---

## 3. Flink Streaming Job

- **Checkpointing:** Enabled every 10 seconds for fault tolerance.  
- **Environment:** `StreamExecutionEnvironment` with `StreamTableEnvironment` for Flink Table API integration.  
- **Transformations:**  
  - JSON events parsed using Jackson `ObjectMapper`.  
  - Hourly timestamp extracted in the format `YYYYMMDDHH`.  
  - Case classes defined for each type:  
    - `PriceRow` → `mempool_price`  
    - `BlocktipRow` → `mempool_blocktip`  
    - `FeeRow` → `mempool_fee`  

- **Mapping to RowData:** Flink Iceberg sink requires `RowData`. Helper functions convert case classes to `GenericRowData`.

---
