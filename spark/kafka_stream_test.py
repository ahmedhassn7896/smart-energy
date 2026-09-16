from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("SmartEnergyKafkaStreamTest")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "smart_energy")
    .option("startingOffsets", "earliest")
    .load()
)

messages = df.select(
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp"),
    col("value").cast("string").alias("message")
)

query = (
    messages.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("numRows", 20)
    .option("checkpointLocation", "/opt/spark/work/checkpoints/kafka_test")
    .start()
)

query.awaitTermination()
