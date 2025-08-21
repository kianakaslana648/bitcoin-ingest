import pyiceberg
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, IntegerType, TimestampType, LongType
from pyiceberg.partitioning import PartitionSpec

print(f"iceberg versions {pyiceberg.__version__}")

hive_metastore_url = "thrift://hive-metastore:9083"
warehouse_location = "s3a://bitcoin-ingest/warehouse/"

# Load Hive Metastore catalog (you can also use Glue or Nessie)
catalog = load_catalog("hive", **{
    "uri": hive_metastore_url,
    "warehouse": warehouse_location,
    "s3.endpoint": "http://minio:9000",
    "s3.access-key": "minioadmin",
    "s3.secret-key": "minioadmin"
})


###################################################
# Create a new schema (database in Hive terms)
print("Creating database: mempool")
print(f"Existing namespaces: {catalog.list_namespaces()}")
if "mempool" not in [tuple[0] for tuple in catalog.list_namespaces()]:
    catalog.create_namespace("mempool")
else:
    print("Namespace 'mempool' already exists")

###################################################
print("Creating tables: mempool_price, mempool_blocktip, mempool_fee")
# Create mempool tables
# Schema for mempool_price table
price_schema = Schema(
    NestedField(1, "source", StringType(), required=True),
    NestedField(2, "event_time", TimestampType(), required=True),
    NestedField(3, "event_hour", IntegerType(), required=True),
    NestedField(4, "usd_price", IntegerType(), required=False)
)

# Schema for mempool_blocktip table
blocktip_schema = Schema(
    NestedField(1, "source", StringType(), required=True),          # "blocktip"
    NestedField(2, "event_time", TimestampType(), required=True),   # converted from timestamp ms
    NestedField(3, "event_hour", IntegerType(), required=True),
    NestedField(4, "block_height", LongType(), required=False),     # blockchain height
    NestedField(5, "block_hash", StringType(), required=False),     # current block hash
)

# Schema for mempool_fee table
fee_schema = Schema(
    NestedField(1, "source", StringType(), required=True),          # "fees"
    NestedField(2, "event_time", TimestampType(), required=True),   # converted from timestamp
    NestedField(3, "event_hour", IntegerType(), required=True),
    NestedField(4, "fastest_fee", IntegerType(), required=False),
    NestedField(5, "half_hour_fee", IntegerType(), required=False),
    NestedField(6, "hour_fee", IntegerType(), required=False),
    NestedField(7, "economy_fee", IntegerType(), required=False),
    NestedField(8, "minimum_fee", IntegerType(), required=False)
)


###################################################
# Create Iceberg tables

## create mempool price table
if not catalog.table_exists(("mempool", "mempool_price")):
    price_table = catalog.create_table(
        identifier=("mempool", "mempool_price"),
        schema=price_schema,
        location="s3a://bitcoin-ingest/warehouse/mempool.db/mempool_price",
        # partition_spec=price_spec,
        properties={"write.format.default": "parquet"},
    )
else:
    price_table = catalog.load_table(("mempool", "mempool_price"))
print(f"Created table: {price_table.name}")

## create mempool blocktip table
if not catalog.table_exists(("mempool", "mempool_blocktip")):
    blocktip_table = catalog.create_table(
        identifier=("mempool", "mempool_blocktip"),
        schema=blocktip_schema,
        location="s3a://bitcoin-ingest/warehouse/mempool.db/mempool_blocktip",
        # partition_spec=blocktip_spec,
        properties={"write.format.default": "parquet"},
    )
else:
    blocktip_table = catalog.load_table(("mempool", "mempool_blocktip"))
print(f"Created table: {blocktip_table.name}")


## create mempool fee table
if not catalog.table_exists(("mempool", "mempool_fee")):
    fee_table = catalog.create_table(
        identifier=("mempool", "mempool_fee"),
        schema=fee_schema,
        location="s3a://bitcoin-ingest/warehouse/mempool.db/mempool_fee",
        # partition_spec=fee_spec,
        properties={"write.format.default": "parquet"},
    )
else:
    fee_table = catalog.load_table(("mempool", "mempool_fee"))
print(f"Created table: {fee_table.name}")
