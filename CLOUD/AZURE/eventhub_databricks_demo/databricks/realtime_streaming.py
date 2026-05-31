# Databricks notebook source
# MAGIC %md
# MAGIC # Real-Time Event Hub Streaming

# COMMAND ----------

# Configuration
# TODO: Replace with your Event Hub connection string
EVENTHUB_NAMESPACE = "YOUR-EVENTHUB-NAMESPACE"
EVENTHUB_NAME = "demo-transactions"
EVENTHUB_CONN_STRING = "Endpoint=sb://YOUR-NAMESPACE.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=YOUR-KEY-HERE"

KAFKA_BOOTSTRAP_SERVERS = f"{EVENTHUB_NAMESPACE}.servicebus.windows.net:9093"
KAFKA_SASL_CONFIG = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{EVENTHUB_CONN_STRING}";'

print("✅ Ready to stream")

# COMMAND ----------

from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, BooleanType

transaction_schema = StructType() \
    .add("transaction_id", StringType()) \
    .add("customer_id", StringType()) \
    .add("amount", DoubleType()) \
    .add("merchant", StringType()) \
    .add("category", StringType()) \
    .add("timestamp", StringType()) \
    .add("location", StringType()) \
    .add("is_fraud", BooleanType()) \
    .add("payment_method", StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔴 LIVE Streaming

# COMMAND ----------

print("🔴 LIVE - Streaming from Event Hub...")

stream_df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", EVENTHUB_NAME)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", KAFKA_SASL_CONFIG)
    .option("startingOffsets", "latest")
    .load()
)

parsed_stream = (stream_df
    .selectExpr("CAST(value AS STRING) as json_string", "timestamp as kafka_timestamp")
    .select(
        from_json(col("json_string"), transaction_schema).alias("data"),
        col("kafka_timestamp")
    )
    .select("data.*", "kafka_timestamp")
    .withColumn("processed_at", current_timestamp())
)

display(parsed_stream)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Live Statistics

# COMMAND ----------

from pyspark.sql.functions import count, window

live_stats = (parsed_stream
    .groupBy(
        window(col("processed_at"), "30 seconds"),
        col("category")
    )
    .agg(count("*").alias("count"))
    .orderBy("window", ascending=False)
)

display(live_stats)
