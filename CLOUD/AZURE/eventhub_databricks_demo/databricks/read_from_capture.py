# Databricks notebook source
# MAGIC %md
# MAGIC # Read Event Hub Data from ADLS Capture
# MAGIC
# MAGIC Event Hubs Capture automatically writes data to ADLS in Avro format.
# MAGIC This approach bypasses network connectivity issues with Kafka endpoint.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# ADLS configuration
STORAGE_ACCOUNT = "itcbdneadls"
CONTAINER = "eventhub-capture"
CAPTURE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/ehubnamespacemay2026/demo-transactions/*/*/*/*/*/*/*.avro"

print("✅ Configuration loaded")
print(f"   Reading from: {CAPTURE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Avro Files from ADLS

# COMMAND ----------

# Read Avro files
# Event Hubs Capture writes in Avro format with a specific schema
df_raw = spark.read.format("avro").load(CAPTURE_PATH)

print(f"✅ Loaded {df_raw.count()} records from ADLS")
df_raw.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse Event Hub Schema

# COMMAND ----------

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType, BooleanType

# Event Hub Capture Avro schema has: Body, SequenceNumber, Offset, EnqueuedTimeUtc, SystemProperties, Properties
# Our data is in the Body field (binary)

# Define our transaction schema
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

# Parse the Body field (convert from binary to string, then parse JSON)
df = (df_raw
    .select(
        col("Body").cast("string").alias("body_string"),
        col("SequenceNumber"),
        col("EnqueuedTimeUtc"),
        col("Offset")
    )
    .select(
        from_json(col("body_string"), transaction_schema).alias("transaction"),
        col("SequenceNumber"),
        col("EnqueuedTimeUtc"),
        col("Offset")
    )
    .select(
        "transaction.*",
        "SequenceNumber",
        "EnqueuedTimeUtc",
        "Offset"
    )
)

print("✅ Parsed transaction data")
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Data

# COMMAND ----------

print(f"Total transactions: {df.count()}")
display(df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Statistics

# COMMAND ----------

from pyspark.sql.functions import count, sum as _sum, avg, min as _min, max as _max

# Category statistics
category_stats = (df
    .groupBy("category")
    .agg(
        count("*").alias("transaction_count"),
        _sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount")
    )
    .orderBy("total_amount", ascending=False)
)

print("=== Transactions by Category ===")
display(category_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fraud Detection

# COMMAND ----------

fraud_df = df.filter(col("is_fraud") == True)
fraud_count = fraud_df.count()
total_count = df.count()

print(f"⚠️  Fraudulent Transactions: {fraud_count} ({fraud_count/total_count*100:.1f}%)")
display(fraud_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Cosmos DB

# COMMAND ----------

# Cosmos DB configuration
COSMOS_ENDPOINT = "https://YOUR-COSMOS-ACCOUNT.documents.azure.com:443/"
COSMOS_KEY = "YOUR-COSMOS-PRIMARY-KEY-HERE"
COSMOS_DATABASE = "TransactionsDB"
COSMOS_CONTAINER = "transactions"

cosmos_config = {
    "spark.cosmos.accountEndpoint": COSMOS_ENDPOINT,
    "spark.cosmos.accountKey": COSMOS_KEY,
    "spark.cosmos.database": COSMOS_DATABASE,
    "spark.cosmos.container": COSMOS_CONTAINER,
    "spark.cosmos.write.strategy": "ItemOverwrite"
}

# Select only transaction fields (no Event Hub metadata)
transactions_only = df.select(
    "transaction_id",
    "customer_id",
    "amount",
    "merchant",
    "category",
    "timestamp",
    "location",
    "is_fraud",
    "payment_method"
)

print("⚠️  Update Cosmos DB credentials above, then uncomment below to write:")
print("    transactions_only.write.format('cosmos.oltp').options(**cosmos_config).mode('append').save()")

# Uncomment when ready:
# transactions_only.write \
#     .format("cosmos.oltp") \
#     .options(**cosmos_config) \
#     .mode("append") \
#     .save()
#
# print(f"✅ Written {transactions_only.count()} records to Cosmos DB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 70)
print("TEST RESULTS - Event Hub Capture → ADLS → Databricks")
print("=" * 70)
print(f"✅ Total transactions read: {total_count}")
print(f"✅ Fraudulent transactions: {fraud_count}")
print(f"✅ Unique merchants: {df.select('merchant').distinct().count()}")
print(f"✅ Unique customers: {df.select('customer_id').distinct().count()}")
print(f"✅ Categories: {df.select('category').distinct().count()}")
print("=" * 70)
print("\n✅ SUCCESS - Event Hub → ADLS Capture → Databricks working!")
print("   This approach bypasses Kafka connectivity issues.")
