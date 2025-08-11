import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.*;

public class BitcoinKafkaCronJob {

    private static final String TOPIC_PRICE = "bitcoin-price";
    private static final String TOPIC_BLOCKTIP = "bitcoin-blocktip";
    private static final String TOPIC_FEES = "bitcoin-fees";

    private static final String BTC_PRICE_API = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd";
    private static final String BLOCKS_TIP_API = "https://mempool.space/api/v1/blocks/tip";
    private static final String FEES_RECOMMENDED_API = "https://mempool.space/api/v1/fees/recommended";

    private static final int MAX_RETRIES = 5;
    private static final int INITIAL_BACKOFF_MS = 1000; // 1 second
    private static final int MAX_BACKOFF_MS = 16000;    // 16 seconds max backoff

    private final KafkaProducer<String, String> producer;
    private final String kafkaServers;

    public BitcoinKafkaCronJob(String kafkaServers) {
        this.kafkaServers = kafkaServers;
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaServers);
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("enable.idempotence", "true");
        this.producer = new KafkaProducer<>(props);
    }

    private String fetchDataWithRetry(String urlString) throws Exception {
        int retries = 0;
        int backoff = INITIAL_BACKOFF_MS;

        while (true) {
            HttpURLConnection con = null;
            try {
                con = (HttpURLConnection) new URL(urlString).openConnection();
                con.setRequestMethod("GET");
                con.setConnectTimeout(5000);
                con.setReadTimeout(5000);

                int status = con.getResponseCode();
                if (status == 429) {
                    // Too Many Requests, retry with backoff
                    if (retries >= MAX_RETRIES) {
                        throw new RuntimeException("Max retries reached for 429");
                    }
                    System.err.printf("Received 429, backing off %d ms before retry #%d%n", backoff, retries + 1);
                    Thread.sleep(backoff);
                    retries++;
                    backoff = Math.min(backoff * 2, MAX_BACKOFF_MS);
                    continue;
                }
                if (status != 200) {
                    throw new RuntimeException("HTTP error code: " + status);
                }

                BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
                StringBuilder content = new StringBuilder();
                String inputLine;
                while ((inputLine = in.readLine()) != null) {
                    content.append(inputLine);
                }
                in.close();
                return content.toString();

            } finally {
                if (con != null) {
                    con.disconnect();
                }
            }
        }
    }

    private void sendToKafka(String topic, String message) throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, message);
        Future<RecordMetadata> future = producer.send(record);
        RecordMetadata metadata = future.get();
        System.out.printf("Sent record to topic %s partition %d offset %d%n",
                metadata.topic(), metadata.partition(), metadata.offset());
    }

    private void sendBitcoinPrice() throws Exception {
        String priceJson = fetchDataWithRetry(BTC_PRICE_API);
        sendToKafka(TOPIC_PRICE, priceJson);
    }

    private void sendBlockTip() throws Exception {
        String blockTipJson = fetchDataWithRetry(BLOCKS_TIP_API);
        sendToKafka(TOPIC_BLOCKTIP, blockTipJson);
    }

    private void sendFeesRecommended() throws Exception {
        String feesJson = fetchDataWithRetry(FEES_RECOMMENDED_API);
        sendToKafka(TOPIC_FEES, feesJson);
    }

    public void runAll() {
        try {
            sendBitcoinPrice();
            sendBlockTip();
            sendFeesRecommended();
        } catch (Exception e) {
            System.err.println("Error in scheduled task: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void shutdown() {
        if (producer != null) {
            producer.close();
        }
    }

    public static void main(String[] args) {
        String kafkaServers = "kafka:9092";
        BitcoinKafkaCronJob job = new BitcoinKafkaCronJob(kafkaServers);

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(job::runAll, 0, 5, TimeUnit.MINUTES);

        // Add shutdown hook to close Kafka producer cleanly on JVM exit
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            job.shutdown();
            scheduler.shutdown();
        }));
    }
}
