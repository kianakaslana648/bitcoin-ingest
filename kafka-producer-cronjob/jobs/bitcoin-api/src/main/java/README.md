# Bitcoin Kafka Cron Job

Fetches real-time Bitcoin data from the [Mempool.space API](https://mempool.space/docs/api/) and publishes it to a Kafka topic. It runs as a scheduled job every 5 minutes.

---

## **1. Overview**

The cron job fetches the following data:

| Data Type | API Endpoint | Description |
|-----------|--------------|-------------|
| Bitcoin Price | `https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd` | Current Bitcoin price (USD). |
| Block Tip | `https://mempool.space/api/blocks/tip/height` & `https://mempool.space/api/blocks/tip/hash` | Latest Bitcoin block height and hash. |
| Recommended Fees | `https://mempool.space/api/v1/fees/recommended` | Current recommended transaction fees (fastest, half-hour, hour). |

All data is wrapped with metadata and sent to Kafka.

---

## **2. Kafka Producer Configuration**

```java
Properties props = new Properties();
props.put("bootstrap.servers", kafkaServers);
props.put("acks", "all");
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("enable.idempotence", "true"); // ensures no duplicate messages
