# Next Pipeline Step

These files continue the current Smart Energy project.

Current real dataset:
D:\smart-energy\data\CC_LCL-FullData.csv (~8.54 GB)

1. Replace/create:
   docker/producer/producer.py
   docker/producer/Dockerfile
   spark/kafka_to_clickhouse.py

2. Build producer:
   docker compose build producer

3. Run a 10,000-row test from the real CSV:
   $env:MAX_ROWS="10000"
   docker compose --profile manual run --rm producer

4. Verify Kafka:
   docker exec ntibigdata-kafka /opt/kafka/bin/kafka-console-consumer.sh `
     --bootstrap-server localhost:9092 `
     --topic smart_energy `
     --from-beginning `
     --max-messages 5

5. Run Spark -> ClickHouse:
   docker exec ntibigdata-spark-master /opt/spark/bin/spark-submit `
     --master spark://spark-master:7077 `
     --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 `
     /opt/spark-apps/kafka_to_clickhouse.py

6. Verify ClickHouse:
   docker exec ntibigdata-clickhouse clickhouse-client `
     --user default `
     --password clickhouse `
     --query "SELECT count(), min(reading_datetime), max(reading_datetime) FROM analytics.raw_smart_meter_events"

Only after the 10k real-row test succeeds should MAX_ROWS be changed to 0 for the full dataset.

Important:
- Producer is streaming the CSV line-by-line; it does not load 8.54 GB into RAM.
- Do not use collect() for full-scale Spark processing.
