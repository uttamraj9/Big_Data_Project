# Databricks notebook source
# MAGIC %md
# MAGIC # Read from Event Hub using Kafka Endpoint
# MAGIC
# MAGIC This notebook demonstrates reading streaming data from Azure Event Hub using Spark's built-in Kafka connector.
# MAGIC
# MAGIC **No library installation required!**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Event Hub connection details
# TODO: Replace these with your actual values or use Databricks Secrets
EVENTHUB_NAMESPACE = "YOUR-EVENTHUB-NAMESPACE"  # e.g., "ehubnamespacemay2026"
EVENTHUB_NAME = "demo-transactions"
EVENTHUB_CONNECTION_STRING = "Endpoint=sb://YOUR-NAMESPACE.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=YOUR-KEY-HERE"

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = f"{EVENTHUB_NAMESPACE}.servicebus.windows.net:9093"
KAFKA_SASL_JAAS_CONFIG = f'org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{EVENTHUB_CONNECTION_STRING}";'

print("✅ Configuration loaded")
print(f"   Event Hub: {EVENTHUB_NAME}")
print(f"   Kafka Endpoint: {KAFKA_BOOTSTRAP_SERVERS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Stream from Event Hub (Kafka)

# COMMAND ----------

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType, BooleanType, TimestampType

# Define schema for transaction data
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

# Read from Event Hub via Kafka
raw_stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", EVENTHUB_NAME)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", KAFKA_SASL_JAAS_CONFIG)
    .option("startingOffsets", "earliest")  # Start from beginning
    .option("failOnDataLoss", "false")
    .load()
)

print("✅ Stream created from Event Hub")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse JSON and Display Data

# COMMAND ----------

# Event Hub messages are in the 'value' column (binary)
# Convert to string and parse JSON
parsed_stream = (raw_stream
    .selectExpr("CAST(value AS STRING) as json_string")
    .select(from_json(col("json_string"), transaction_schema).alias("data"))
    .select("data.*")
)

print("✅ Schema applied")
parsed_stream.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Streaming Data

# COMMAND ----------

# Display the stream (this will continuously update)
display(parsed_stream)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream Statistics

# COMMAND ----------

# Count by category
category_counts = (parsed_stream
    .groupBy("category")
    .count()
)

display(category_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fraud Detection

# COMMAND ----------

# Filter fraudulent transactions
fraud_transactions = parsed_stream.filter(col("is_fraud") == True)

display(fraud_transactions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Console (for testing)

# COMMAND ----------

# Write stream to console for debugging
query = (parsed_stream
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

# Let it run for 30 seconds
import time
time.sleep(30)

# Stop the query
query.stop()

print("✅ Console output complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ✅ Successfully connected to Event Hub via Kafka endpoint
# MAGIC ✅ Read streaming transaction data
# MAGIC ✅ Parsed JSON messages
# MAGIC ✅ Displayed real-time data
# MAGIC
# MAGIC **Next:** Run `eventhub_to_cosmos.py` to write this data to Cosmos DB
