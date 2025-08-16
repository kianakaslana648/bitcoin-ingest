java -jar lib/trino-cli-403-executable.jar --server localhost:8080 --catalog iceberg

USE iceberg.default;
CREATE SCHEMA iceberg.bitcoin
WITH (
    location = 's3a://bitcoin-ingest/warehouse/mempool.db/'
);

CREATE TABLE iceberg.bitcoin.mempool_price (
    source VARCHAR,
    timestamp_ms BIGINT,
    bitcoin_usd DOUBLE
)
WITH (
    format = 'PARQUET'
);