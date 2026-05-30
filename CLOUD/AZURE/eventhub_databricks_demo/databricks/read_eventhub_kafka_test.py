# Databricks notebook source
# MAGIC %md
# MAGIC # Read from Event Hub using Kafka Endpoint - TEST RUN
# MAGIC
# MAGIC This notebook demonstrates reading streaming data from Azure Event Hub using Spark's built-in Kafka connector.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Event Hub connection details - REPLACE WITH YOUR VALUES
EVENTHUB_NAMESPACE = "YOUR-EVENTHUB-NAMESPACE"
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

# Read from Event Hub via Kafka (batch mode for testing)
raw_df = (spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", EVENTHUB_NAME)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", KAFKA_SASL_JAAS_CONFIG)
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)

print("✅ Connected to Event Hub")
print(f"   Messages read: {raw_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse JSON and Display Data

# COMMAND ----------

# Event Hub messages are in the 'value' column (binary)
# Convert to string and parse JSON
parsed_df = (raw_df
    .selectExpr("CAST(value AS STRING) as json_string")
    .select(from_json(col("json_string"), transaction_schema).alias("data"))
    .select("data.*")
)

print("✅ Schema applied")
parsed_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Sample Data

# COMMAND ----------

# Show first 10 transactions
print(f"Total transactions: {parsed_df.count()}")
display(parsed_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Statistics

# COMMAND ----------

# Count by category
from pyspark.sql.functions import count, sum as _sum, avg

print("=== Transactions by Category ===")
category_stats = (parsed_df
    .groupBy("category")
    .agg(
        count("*").alias("count"),
        _sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount")
    )
    .orderBy("count", ascending=False)
)
display(category_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fraud Detection

# COMMAND ----------

# Filter fraudulent transactions
fraud_df = parsed_df.filter(col("is_fraud") == True)
fraud_count = fraud_df.count()

print(f"⚠️  Fraudulent transactions: {fraud_count} ({fraud_count/parsed_df.count()*100:.1f}%)")
display(fraud_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("TEST RESULTS")
print("=" * 60)
print(f"✅ Successfully connected to Event Hub via Kafka endpoint")
print(f"✅ Total messages read: {parsed_df.count()}")
print(f"✅ Fraudulent transactions: {fraud_count}")
print(f"✅ Unique merchants: {parsed_df.select('merchant').distinct().count()}")
print(f"✅ Date range: {parsed_df.selectExpr('min(timestamp)', 'max(timestamp)').collect()[0]}")
print("=" * 60)
print("\n✅ TEST PASSED - Event Hub → Databricks integration working!")
