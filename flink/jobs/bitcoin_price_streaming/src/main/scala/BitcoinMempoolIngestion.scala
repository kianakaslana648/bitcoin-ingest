import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.data.{GenericRowData, RowData}

import org.apache.iceberg.flink.sink.FlinkSink
import org.apache.iceberg.flink.{CatalogLoader, TableLoader}
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.hadoop.conf.Configuration

import com.fasterxml.jackson.databind.ObjectMapper
import java.time.{Instant, ZoneOffset, ZonedDateTime}
import java.util.{Collections, Properties}

// Case classes
case class PriceRow(source: String, event_time: java.sql.Timestamp, event_hour: Int, usd_price: java.lang.Integer)
case class BlocktipRow(source: String, event_time: java.sql.Timestamp, event_hour: Int, block_height: java.lang.Long, block_hash: String)
case class FeeRow(source: String, event_time: java.sql.Timestamp, event_hour: Int, fastest_fee: java.lang.Integer, half_hour_fee: java.lang.Integer, hour_fee: java.lang.Integer, economy_fee: java.lang.Integer, minimum_fee: java.lang.Integer)

object BitcoinMempoolIngestion {

  // --- Transformation helpers ---
  def priceRowToRowData(row: PriceRow): RowData = {
    val r = new GenericRowData(4)
    r.setField(0, row.source)
    r.setField(1, row.event_time.getTime)
    r.setField(2, row.event_hour)
    r.setField(3, row.usd_price)
    r
  }

  def blocktipRowToRowData(row: BlocktipRow): RowData = {
    val r = new GenericRowData(5)
    r.setField(0, row.source)
    r.setField(1, row.event_time.getTime)
    r.setField(2, row.event_hour)
    r.setField(3, row.block_height)
    r.setField(4, row.block_hash)
    r
  }

  def feesRowToRowData(row: FeeRow): RowData = {
    val r = new GenericRowData(8)
    r.setField(0, row.source)
    r.setField(1, row.event_time.getTime)
    r.setField(2, row.event_hour)
    r.setField(3, row.fastest_fee)
    r.setField(4, row.half_hour_fee)
    r.setField(5, row.hour_fee)
    r.setField(6, row.economy_fee)
    r.setField(7, row.minimum_fee)
    r
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(10000)
    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    val hadoopConf = new Configuration()
    hadoopConf.addResource(new org.apache.hadoop.fs.Path("/opt/hive/conf/hive-site.xml"))

    val catalogLoader = CatalogLoader.hive("hive_catalog", hadoopConf, Collections.emptyMap[String, String]())

    implicit val typeInfo = createTypeInformation[String]

    // Kafka consumer
    val props = new Properties()
    props.setProperty("bootstrap.servers", "kafka:9092")
    props.setProperty("group.id", "flink-iceberg-consumer")

    val kafkaStream = env.addSource(new FlinkKafkaConsumer[String]("mempool", new SimpleStringSchema(), props))
    val mapper = new ObjectMapper()

    def extractHour(tsMillis: Long): Int = {
      val zdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(tsMillis), ZoneOffset.UTC)
      zdt.getYear * 1000000 + zdt.getMonthValue * 10000 + zdt.getDayOfMonth * 100 + zdt.getHour
    }

    val parsedStream = kafkaStream.map(jsonStr => mapper.readTree(jsonStr))

    // --- PRICE ---
    val priceStream = parsedStream.filter(_.get("source").asText() == "price").map { node =>
      val ts = node.get("timestamp").asLong()
      val usd = mapper.readTree(node.get("data").asText()).path("bitcoin").path("usd").asInt()
      PriceRow(node.get("source").asText(), new java.sql.Timestamp(ts), extractHour(ts), usd)
    }

    // --- BLOCKTIP ---
    val blocktipStream = parsedStream.filter(_.get("source").asText() == "blocktip").map { node =>
      val ts = node.get("timestamp").asLong()
      val dataNode = mapper.readTree(node.get("data").asText())
      BlocktipRow(node.get("source").asText(), new java.sql.Timestamp(ts), extractHour(ts), dataNode.path("height").asLong(), dataNode.path("hash").asText())
    }

    // --- FEES ---
    val feesStream = parsedStream.filter(_.get("source").asText() == "fees").map { node =>
      val ts = node.get("timestamp").asLong()
      val dataNode = mapper.readTree(node.get("data").asText())
      FeeRow(node.get("source").asText(), new java.sql.Timestamp(ts), extractHour(ts),
        dataNode.path("fastestFee").asInt(),
        dataNode.path("halfHourFee").asInt(),
        dataNode.path("hourFee").asInt(),
        dataNode.path("economyFee").asInt(),
        dataNode.path("minimumFee").asInt())
    }



    // Iceberg table loaders
    val priceTable = TableLoader.fromCatalog(catalogLoader, TableIdentifier.of("mempool", "mempool_price"))
    val blocktipTable = TableLoader.fromCatalog(catalogLoader, TableIdentifier.of("mempool", "mempool_blocktip"))
    val feesTable = TableLoader.fromCatalog(catalogLoader, TableIdentifier.of("mempool", "mempool_fee"))

    // Sink to Iceberg
    FlinkSink.forRowData(priceStream.map(priceRowToRowData _).javaStream)
      .tableLoader(priceTable)
      .overwrite(false)
      .append()

    FlinkSink.forRowData(blocktipStream.map(blocktipRowToRowData _).javaStream)
      .tableLoader(blocktipTable)
      .overwrite(false)
      .append()

    FlinkSink.forRowData(feesStream.map(feesRowToRowData _).javaStream)
      .tableLoader(feesTable)
      .overwrite(false)
      .append()

    env.execute("Kafka -> Flink -> Iceberg 1.7.2")
  }
}
