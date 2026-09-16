#!/usr/bin/env bash
# ==============================================================================
# Kafka Topic Initialization Script
# Prepares the required 'smart_energy' topic with configurable partitions.
# ==============================================================================

set -euo pipefail

BOOTSTRAP_SERVER="${1:-localhost:9092}"
TOPIC_NAME="${KAFKA_TOPIC:-smart_energy}"
PARTITIONS="${KAFKA_PARTITIONS:-4}"
REPLICATION_FACTOR="${KAFKA_REPLICATION_FACTOR:-1}"

echo "[Kafka Init] Checking readiness of Kafka broker at ${BOOTSTRAP_SERVER}..."
until /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVER}" --list > /dev/null 2>&1; do
  sleep 2
  echo "[Kafka Init] Waiting for Kafka broker..."
done

echo "[Kafka Init] Kafka broker is ready. Checking topic '${TOPIC_NAME}'..."
if /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVER}" --describe --topic "${TOPIC_NAME}" > /dev/null 2>&1; then
    echo "[Kafka Init] Topic '${TOPIC_NAME}' already exists."
else
    echo "[Kafka Init] Creating topic '${TOPIC_NAME}' with ${PARTITIONS} partitions and replication factor ${REPLICATION_FACTOR}..."
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVER}" \
      --create \
      --topic "${TOPIC_NAME}" \
      --partitions "${PARTITIONS}" \
      --replication-factor "${REPLICATION_FACTOR}"
    echo "[Kafka Init] Topic '${TOPIC_NAME}' created successfully."
fi

echo "[Kafka Init] Current topics list:"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVER}" --list
