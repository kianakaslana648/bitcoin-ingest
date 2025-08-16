import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.flink.data.FlinkStruct;
import org.apache.flink.types.Row;
import org.apache.flink.api.common.typeinfo.Types as FlinkTypes;

import java.util.Properties;

public class BitcoinMempoolIngestion {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "kafka:9092");
        props.setProperty("group.id", "mempool-flink-iceberg");
        props.setProperty("auto.offset.reset", "earliest");

        FlinkKafkaConsumer<Tuple2<String, String>> consumer = new FlinkKafkaConsumer<>(
            "mempool",
            new KafkaKeyValueSchema(),
            props
        );

        DataStream<Tuple2<String, String>> kafkaStream = env.addSource(consumer);

        // Separate streams for price, blocktip, fees
        DataStream<Row> priceStream = kafkaStream
            .filter(r -> "price".equals(r.f0))
            .map(r -> parsePriceToRow(r.f1))
            .returns(FlinkTypes.ROW(FlinkTypes.STRING, FlinkTypes.DOUBLE));  // example schema: (currency, price)

        DataStream<Row> blocktipStream = kafkaStream
            .filter(r -> "blocktip".equals(r.f0))
            .map(r -> parseBlocktipToRow(r.f1))
            .returns(FlinkTypes.ROW(FlinkTypes.LONG, FlinkTypes.LONG)); // example schema: (blockHeight, timestamp)

        DataStream<Row> feesStream = kafkaStream
            .filter(r -> "fees".equals(r.f0))
            .map(r -> parseFeesToRow(r.f1))
            .returns(FlinkTypes.ROW(FlinkTypes.STRING, FlinkTypes.DOUBLE)); // example schema: (feeType, feeValue)

        // Load Iceberg tables
        TableLoader priceTableLoader = TableLoader.fromHiveCatalog(
            "hive",                 // catalog name, must match catalog config
            "my_db.mempool_price"   // fully qualified table name: database.table
        );
        TableLoader blocktipTableLoader = TableLoader.fromHadoopTable("hdfs://path/to/mempool_blocktip");
        TableLoader feesTableLoader = TableLoader.fromHadoopTable("hdfs://path/to/mempool_fees");

        // Sink to Iceberg for price
        FlinkSink.forRow(priceStream)
            .tableLoader(priceTableLoader)
            .build();

        // Sink to Iceberg for blocktip
        FlinkSink.forRow(blocktipStream)
            .tableLoader(blocktipTableLoader)
            .build();

        // Sink to Iceberg for fees
        FlinkSink.forRow(feesStream)
            .tableLoader(feesTableLoader)
            .build();

        env.execute("Mempool Flink Kafka to Iceberg Job");
    }

    // Example parse functions from JSON string to Row objects
    // (Use your preferred JSON lib, e.g. Jackson or Gson)
    private static Row parsePriceToRow(String json) {
        // parse json to extract currency and price
        // dummy example:
        return Row.of("BTC", 26000.5);
    }

    private static Row parseBlocktipToRow(String json) {
        // parse json to extract block height and timestamp
        return Row.of(800_000L, System.currentTimeMillis());
    }

    private static Row parseFeesToRow(String json) {
        // parse json to extract feeType and feeValue
        return Row.of("priority", 1.2);
    }
}
