import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.*;

public class BitcoinKafkaCronJob {

    private static final String TOPIC = "mempool";

    private static final String BTC_PRICE_API = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd";
    private static final String BLOCKS_TIP_API = "https://mempool.space/api/v1/blocks/tip";
    private static final String FEES_RECOMMENDED_API = "https://mempool.space/api/v1/fees/recommended";

    private static final int MAX_RETRIES = 5;
    private static final int INITIAL_BACKOFF_MS = 1000;
    private static final int MAX_BACKOFF_MS = 16000;

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper mapper = new ObjectMapper();

    public BitcoinKafkaCronJob(String kafkaServers) {
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

    private void sendToKafka(String sourceType, String rawData) throws Exception {
        System.out.printf("Sending data to kafka");
        ObjectNode wrapper = mapper.createObjectNode();
        wrapper.put("source", sourceType);
        wrapper.put("timestamp", System.currentTimeMillis());
        wrapper.put("data", rawData); // store original API response as string

        String message = mapper.writeValueAsString(wrapper);

        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, sourceType, message);
        RecordMetadata metadata = producer.send(record).get();
        System.out.printf("Sent %s data to topic %s partition %d offset %d%n",
                sourceType, metadata.topic(), metadata.partition(), metadata.offset());
    }

    private void sendBitcoinPrice() throws Exception {
        String priceJson = fetchDataWithRetry(BTC_PRICE_API);
        sendToKafka("price", priceJson);
    }

    private void sendBlockTip() throws Exception {
        String blockTipJson = fetchDataWithRetry(BLOCKS_TIP_API);
        sendToKafka("blocktip", blockTipJson);
    }

    private void sendFeesRecommended() throws Exception {
        String feesJson = fetchDataWithRetry(FEES_RECOMMENDED_API);
        sendToKafka("fees", feesJson);
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

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            job.shutdown();
            scheduler.shutdown();
        }));
    }
}
