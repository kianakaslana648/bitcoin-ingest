from hive_metastore_client import HiveMetastoreClient
from hive_metastore_client.builders import DatabaseBuilder, TableBuilder, ColumnBuilder
from thrift_files.libraries.thrift_hive_metastore_client.ttypes import StorageDescriptor
from thrift_files.libraries.thrift_hive_metastore_client.ttypes import SerDeInfo




# Hive Metastore host and port (container name + Thrift port)
host = 'hive-metastore'
port = 9083

database_name = 'test_db'
table_name = 'example_table'

columns = [
    ColumnBuilder(name='id', type='int').build(),
    ColumnBuilder(name='name', type='string').build(),
    ColumnBuilder(name='price', type='double').build()
]

serde = SerDeInfo(
    name=table_name,
    serializationLib='org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
    parameters={'field.delim': ','}  # optional
)

sd = sd = StorageDescriptor(
    cols=columns,
    serdeInfo=serde,
)
print("testing hive metastore connection")
# Connect to Hive Metastore
with HiveMetastoreClient(host=host, port=port) as client:
    # List all databases
    databases = client.get_all_databases()
    print("Databases in Hive Metastore:", databases)
    
    # Create database
    database = DatabaseBuilder(name='test_db').build()
    try:
        client.create_database_if_not_exists(database)
        print("Database created in S3A (MinIO) successfully")
    except Exception as e:
        print("Failed to create database in S3A:", e)
        
    # Create tables
    table = TableBuilder(table_name=table_name, db_name=database_name,table_type='EXTERNAL_TABLE', storage_descriptor=sd).build()
    
    client.create_table(table)
    print(f"Table '{table_name}' created successfully in database '{database_name}'")
