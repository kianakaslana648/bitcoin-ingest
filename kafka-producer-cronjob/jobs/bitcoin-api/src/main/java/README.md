1. **Java Ingestion Service**:
   - Fetches data from the APIs.
   - Wraps each response in a JSON object:
     ```json
     {
       "source": "<sourceType>",
       "timestamp": 1692547200000,
       "data": "<raw API response>"
     }
     ```
   - Sends the JSON message to the configured Kafka topic.
