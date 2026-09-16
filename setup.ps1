# setup.ps1
# Run this once from D:\smart-energy in PowerShell:
#   .\setup.ps1
# It creates every folder and file needed, overwriting anything old.

$root = Get-Location
Write-Host "Setting up in: $root"

# --- Create folders ---
$folders = @(
    "docker\kafka", "docker\kafka-ui", "docker\spark",
    "docker\postgres", "docker\redis", "docker\clickhouse", "docker\airflow",
    "clickhouse", "data", "airflow\dags", "airflow\logs", "airflow\plugins", "pipeline"
)
foreach ($f in $folders) {
    New-Item -ItemType Directory -Force -Path $f | Out-Null
}
Write-Host "Folders created."

# --- docker-compose.yml ---
@'
name: ntibigdata

services:
  kafka:
    build:
      context: ./docker/kafka
    image: ntibigdata/kafka:3.9.2
    container_name: ntibigdata-kafka
    restart: unless-stopped
    ports:
      - "${KAFKA_EXTERNAL_PORT:-9094}:9094"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: controller,broker
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_LISTENERS: CONTROLLER://:9093,INTERNAL://:9092,EXTERNAL://:9094
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:9092,EXTERNAL://${KAFKA_EXTERNAL_HOST:-localhost}:${KAFKA_EXTERNAL_PORT:-9094}
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_NUM_PARTITIONS: ${KAFKA_DEFAULT_PARTITIONS:-4}
      KAFKA_DEFAULT_REPLICATION_FACTOR: 1
      KAFKA_MIN_INSYNC_REPLICAS: 1
      KAFKA_LOG_DIRS: /var/lib/kafka/data
      CLUSTER_ID: ${KAFKA_CLUSTER_ID:-MkU3OEVBNTcwNTJENDM2Qk}
    volumes:
      - kafka_data:/var/lib/kafka/data
    networks:
      - data
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1"]
      interval: 10s
      timeout: 5s
      retries: 30
      start_period: 30s

  kafka-topic-init:
    image: ntibigdata/kafka:3.9.2
    container_name: ntibigdata-kafka-topic-init
    restart: "no"
    entrypoint: ["/bin/sh", "-c"]
    command:
      - |
        /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
          --create --if-not-exists \
          --topic ${KAFKA_TOPIC:-smart_energy} \
          --partitions ${KAFKA_DEFAULT_PARTITIONS:-4} \
          --replication-factor 1
        echo "Topic ${KAFKA_TOPIC:-smart_energy} ready."
    depends_on:
      kafka:
        condition: service_healthy
    networks:
      - data

  kafka-ui:
    build:
      context: ./docker/kafka-ui
    image: ntibigdata/kafka-ui:latest
    container_name: ntibigdata-kafka-ui
    restart: unless-stopped
    ports:
      - "${KAFKA_UI_PORT:-8080}:8080"
    environment:
      DYNAMIC_CONFIG_ENABLED: "false"
      KAFKA_CLUSTERS_0_NAME: ${KAFKA_CLUSTER_NAME:-local}
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
      KAFKA_CLUSTERS_0_READONLY: "false"
      KAFKA_CLUSTERS_0_PROPERTIES_SECURITY_PROTOCOL: PLAINTEXT
    depends_on:
      kafka:
        condition: service_healthy
    networks:
      - data

  producer:
    build:
      context: .
      dockerfile: docker/producer/Dockerfile
    image: ntibigdata/producer:latest
    container_name: ntibigdata-producer
    profiles: ["manual"]
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      KAFKA_TOPIC: ${KAFKA_TOPIC:-smart_energy}
      SOURCE_FILE: /opt/data/CC_LCL-FullData.csv
      BATCH_SIZE: ${PRODUCER_BATCH_SIZE:-1000}
      BATCH_DELAY_SECONDS: ${PRODUCER_BATCH_DELAY_SECONDS:-0.2}
    volumes:
      - ${DATA_PATH:-./data}:/opt/data:ro
    depends_on:
      kafka:
        condition: service_healthy
    networks:
      - data

  postgres:
    build:
      context: ./docker/postgres
    image: ntibigdata/postgres:16-alpine
    container_name: ntibigdata-postgres
    restart: unless-stopped
    environment:
      POSTGRES_USER: ${POSTGRES_USER:-airflow}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-airflow}
      POSTGRES_DB: ${POSTGRES_DB:-airflow}
    volumes:
      - postgres_data:/var/lib/postgresql/data
    networks:
      - orchestration
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER:-airflow} -d ${POSTGRES_DB:-airflow}"]
      interval: 10s
      timeout: 5s
      retries: 10

  redis:
    build:
      context: ./docker/redis
    image: ntibigdata/redis:7.2-alpine
    container_name: ntibigdata-redis
    restart: unless-stopped
    command: redis-server --appendonly yes
    volumes:
      - redis_data:/data
    networks:
      - orchestration
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 10

  airflow-init:
    build:
      context: .
      dockerfile: docker/airflow/Dockerfile
    image: ntibigdata/airflow:2.9.2
    container_name: ntibigdata-airflow-init
    user: "${AIRFLOW_UID:-50000}:0"
    environment: &airflow-common-env
      AIRFLOW__CORE__EXECUTOR: CeleryExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${POSTGRES_USER:-airflow}:${POSTGRES_PASSWORD:-airflow}@postgres/${POSTGRES_DB:-airflow}
      AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://${POSTGRES_USER:-airflow}:${POSTGRES_PASSWORD:-airflow}@postgres/${POSTGRES_DB:-airflow}
      AIRFLOW__CELERY__BROKER_URL: redis://:@redis:6379/0
      AIRFLOW__CORE__FERNET_KEY: ${AIRFLOW_FERNET_KEY:-}
      AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "true"
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      AIRFLOW__API__AUTH_BACKENDS: airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session
      AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK: "true"
      AIRFLOW__WEBSERVER__EXPOSE_CONFIG: "false"
      _PIP_ADDITIONAL_REQUIREMENTS: ${AIRFLOW_PIP_ADDITIONAL_REQUIREMENTS:-}
      CLICKHOUSE_HOST: clickhouse
      CLICKHOUSE_HTTP_PORT: "8123"
      CLICKHOUSE_DB: ${CLICKHOUSE_DB:-analytics}
      CLICKHOUSE_USER: ${CLICKHOUSE_USER:-default}
      CLICKHOUSE_PASSWORD: ${CLICKHOUSE_PASSWORD:-clickhouse}
    volumes: &airflow-common-volumes
      - ./airflow/dags:/opt/airflow/dags
      - ./airflow/logs:/opt/airflow/logs
      - ./airflow/plugins:/opt/airflow/plugins
      - ${DATA_PATH:-./data}:/opt/data:ro
      - ./pipeline:/opt/pipeline:ro
    entrypoint: /bin/bash
    command:
      - -c
      - |
        airflow db migrate
        airflow users create \
          --username "${AIRFLOW_ADMIN_USERNAME:-admin}" \
          --password "${AIRFLOW_ADMIN_PASSWORD:-admin}" \
          --firstname Data \
          --lastname Admin \
          --role Admin \
          --email "${AIRFLOW_ADMIN_EMAIL:-admin@example.com}" || true
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    networks:
      - orchestration

  airflow-webserver:
    image: ntibigdata/airflow:2.9.2
    container_name: ntibigdata-airflow-webserver
    restart: unless-stopped
    user: "${AIRFLOW_UID:-50000}:0"
    command: webserver
    ports:
      - "${AIRFLOW_PORT:-8088}:8080"
    environment: *airflow-common-env
    volumes: *airflow-common-volumes
    depends_on:
      airflow-init:
        condition: service_completed_successfully
    networks:
      - orchestration
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 15s
      timeout: 10s
      retries: 10
      start_period: 30s

  airflow-scheduler:
    image: ntibigdata/airflow:2.9.2
    container_name: ntibigdata-airflow-scheduler
    restart: unless-stopped
    user: "${AIRFLOW_UID:-50000}:0"
    command: scheduler
    environment: *airflow-common-env
    volumes: *airflow-common-volumes
    depends_on:
      airflow-init:
        condition: service_completed_successfully
    networks:
      - orchestration
      - compute
      - data

  airflow-worker:
    image: ntibigdata/airflow:2.9.2
    container_name: ntibigdata-airflow-worker
    restart: unless-stopped
    user: "${AIRFLOW_UID:-50000}:0"
    command: celery worker
    environment: *airflow-common-env
    volumes: *airflow-common-volumes
    depends_on:
      airflow-init:
        condition: service_completed_successfully
    networks:
      - orchestration
      - compute
      - data

  airflow-triggerer:
    image: ntibigdata/airflow:2.9.2
    container_name: ntibigdata-airflow-triggerer
    restart: unless-stopped
    user: "${AIRFLOW_UID:-50000}:0"
    command: triggerer
    environment: *airflow-common-env
    volumes: *airflow-common-volumes
    depends_on:
      airflow-init:
        condition: service_completed_successfully
    networks:
      - orchestration

  airflow-flower:
    image: ntibigdata/airflow:2.9.2
    container_name: ntibigdata-airflow-flower
    restart: unless-stopped
    user: "${AIRFLOW_UID:-50000}:0"
    command: celery flower
    ports:
      - "${FLOWER_PORT:-5555}:5555"
    environment: *airflow-common-env
    volumes: *airflow-common-volumes
    depends_on:
      airflow-init:
        condition: service_completed_successfully
    networks:
      - orchestration

  spark-master:
    build:
      context: ./docker/spark
    image: ntibigdata/spark:4.0.1-java21-scala
    container_name: ntibigdata-spark-master
    restart: unless-stopped
    environment:
      SPARK_NO_DAEMONIZE: "true"
    command: /opt/spark/sbin/start-master.sh
    ports:
      - "${SPARK_MASTER_UI_PORT:-8090}:8080"
      - "${SPARK_MASTER_PORT:-7077}:7077"
    volumes:
      - spark_data:/opt/spark/work
      - spark_ivy:/opt/ivy
      - ./pipeline:/opt/pipeline:ro
    networks:
      - compute
      - data
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://localhost:8080 >/dev/null"]
      interval: 15s
      timeout: 5s
      retries: 10

  spark-worker:
    image: ntibigdata/spark:4.0.1-java21-scala
    container_name: ntibigdata-spark-worker
    restart: unless-stopped
    environment:
      SPARK_NO_DAEMONIZE: "true"
      SPARK_WORKER_MEMORY: ${SPARK_WORKER_MEMORY:-2G}
      SPARK_WORKER_CORES: ${SPARK_WORKER_CORES:-2}
    command: /opt/spark/sbin/start-worker.sh spark://spark-master:7077
    ports:
      - "${SPARK_WORKER_UI_PORT:-8091}:8081"
    depends_on:
      spark-master:
        condition: service_healthy
    volumes:
      - spark_data:/opt/spark/work
      - spark_ivy:/opt/ivy
      - ./pipeline:/opt/pipeline:ro
    networks:
      - compute
      - data

  spark-history-server:
    image: ntibigdata/spark:4.0.1-java21-scala
    container_name: ntibigdata-spark-history-server
    restart: unless-stopped
    environment:
      SPARK_HISTORY_OPTS: -Dspark.history.fs.logDirectory=/tmp/spark-events
      SPARK_NO_DAEMONIZE: "true"
    command: /opt/spark/sbin/start-history-server.sh
    ports:
      - "${SPARK_HISTORY_UI_PORT:-18080}:18080"
    volumes:
      - spark_events:/tmp/spark-events
    networks:
      - compute

  clickhouse:
    build:
      context: ./docker/clickhouse
    image: ntibigdata/clickhouse:26.8
    container_name: ntibigdata-clickhouse
    restart: unless-stopped
    environment:
      CLICKHOUSE_DB: ${CLICKHOUSE_DB:-analytics}
      CLICKHOUSE_USER: ${CLICKHOUSE_USER:-default}
      CLICKHOUSE_PASSWORD: ${CLICKHOUSE_PASSWORD:-clickhouse}
      CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: 1
    ports:
      - "${CLICKHOUSE_HTTP_PORT:-8123}:8123"
      - "${CLICKHOUSE_NATIVE_PORT:-9000}:9000"
    volumes:
      - clickhouse_data:/var/lib/clickhouse
      - clickhouse_logs:/var/log/clickhouse-server
      - ./clickhouse/init.sql:/docker-entrypoint-initdb.d/01-init.sql:ro
    networks:
      - data
      - compute
      - orchestration
    healthcheck:
      test: ["CMD-SHELL", "wget --no-verbose --tries=1 --spider http://localhost:8123/ping || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 12

networks:
  data:
    name: ntibigdata-data
  orchestration:
    name: ntibigdata-orchestration
  compute:
    name: ntibigdata-compute

volumes:
  kafka_data:
  postgres_data:
  redis_data:
  spark_data:
  spark_ivy:
  spark_events:
  clickhouse_data:
  clickhouse_logs:
'@ | Set-Content -Path "docker-compose.yml" -Encoding utf8
Write-Host "docker-compose.yml written."

# --- docker/kafka/Dockerfile ---
@'
FROM apache/kafka:3.9.2
USER root
RUN mkdir -p /var/lib/kafka/data && chmod -R 777 /var/lib/kafka/data
'@ | Set-Content -Path "docker\kafka\Dockerfile" -Encoding utf8

# --- docker/kafka-ui/Dockerfile ---
@'
FROM provectuslabs/kafka-ui:latest
'@ | Set-Content -Path "docker\kafka-ui\Dockerfile" -Encoding utf8

# --- docker/spark/Dockerfile ---
@'
FROM apache/spark:4.0.1-java21-scala
USER root
RUN mkdir -p /opt/ivy/cache /opt/ivy/local && chmod -R 777 /opt/ivy
RUN echo "spark.jars.ivy /opt/ivy" >> /opt/spark/conf/spark-defaults.conf
ENV SPARK_SUBMIT_OPTS="-Dspark.jars.ivy=/opt/ivy -Divy.home=/opt/ivy"
USER spark
'@ | Set-Content -Path "docker\spark\Dockerfile" -Encoding utf8

# --- docker/postgres/Dockerfile ---
@'
FROM postgres:16-alpine
'@ | Set-Content -Path "docker\postgres\Dockerfile" -Encoding utf8

# --- docker/redis/Dockerfile ---
@'
FROM redis:7.2-alpine
'@ | Set-Content -Path "docker\redis\Dockerfile" -Encoding utf8

# --- docker/clickhouse/Dockerfile ---
@'
FROM clickhouse/clickhouse-server:26.8
'@ | Set-Content -Path "docker\clickhouse\Dockerfile" -Encoding utf8

# --- docker/airflow/Dockerfile ---
@'
FROM apache/airflow:2.9.2
USER airflow
RUN pip install --no-cache-dir \
    "apache-airflow-providers-apache-spark>=4.8.0" \
    "apache-airflow-providers-apache-kafka" \
    "apache-airflow-providers-celery" \
    "apache-airflow-providers-redis" \
    "clickhouse-connect"
'@ | Set-Content -Path "docker\airflow\Dockerfile" -Encoding utf8

# --- clickhouse/init.sql ---
@'
CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.raw_smart_meter_events
(
    lcl_id String,
    reading_datetime DateTime,
    kwh Float64,
    ingested_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(reading_datetime)
ORDER BY (lcl_id, reading_datetime);
'@ | Set-Content -Path "clickhouse\init.sql" -Encoding utf8

Write-Host ""
Write-Host "All files written successfully." -ForegroundColor Green
Write-Host "Next: docker compose down --remove-orphans" -ForegroundColor Yellow
Write-Host "Then: docker compose up -d --build" -ForegroundColor Yellow
