# Databricks notebook source
# MAGIC %md
# MAGIC # Event Hub → Cosmos DB Pipeline
# MAGIC
# MAGIC End-to-end streaming pipeline:
# MAGIC 1. Read from Event Hub (Kafka endpoint)
# MAGIC 2. Process transaction data
# MAGIC 3. Write to Cosmos DB

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Event Hub configuration
# TODO: Replace these with your actual values or use Databricks Secrets
EVENTHUB_NAMESPACE = "YOUR-EVENTHUB-NAMESPACE"  # e.g., "ehubnamespacemay2026"
EVENTHUB_NAME = "demo-transactions"
EVENTHUB_CONNECTION_STRING = "Endpoint=sb://YOUR-NAMESPACE.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=YOUR-KEY-HERE"

# Cosmos DB configuration
# TODO: Replace these with your actual values or use Databricks Secrets
COSMOS_ENDPOINT = "https://YOUR-COSMOS-ACCOUNT.documents.azure.com:443/"
COSMOS_KEY = "YOUR-COSMOS-PRIMARY-KEY-HERE"
COSMOS_DATABASE = "TransactionsDB"
COSMOS_CONTAINER = "transactions"

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = f"{EVENTHUB_NAMESPACE}.servicebus.windows.net:9093"
KAFKA_SASL_JAAS_CONFIG = f'org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{EVENTHUB_CONNECTION_STRING}";'

print("✅ Configuration loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read from Event Hub

# COMMAND ----------

from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, BooleanType

# Transaction schema
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

# Read from Event Hub
raw_stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", EVENTHUB_NAME)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", KAFKA_SASL_JAAS_CONFIG)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

# Parse JSON
transactions = (raw_stream
    .selectExpr("CAST(value AS STRING) as json_string")
    .select(from_json(col("json_string"), transaction_schema).alias("data"))
    .select("data.*")
    .withColumn("processed_at", current_timestamp())
)

print("✅ Stream configured")
transactions.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure Cosmos DB Connector

# COMMAND ----------

# Cosmos DB write configuration
cosmos_write_config = {
    "spark.cosmos.accountEndpoint": COSMOS_ENDPOINT,
    "spark.cosmos.accountKey": COSMOS_KEY,
    "spark.cosmos.database": COSMOS_DATABASE,
    "spark.cosmos.container": COSMOS_CONTAINER,
    "spark.cosmos.write.strategy": "ItemOverwrite",  # Upsert mode
    "spark.cosmos.write.bulk.enabled": "true"
}

print("✅ Cosmos DB config ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Stream to Cosmos DB

# COMMAND ----------

# Write to Cosmos DB
query = (transactions
    .writeStream
    .format("cosmos.oltp")
    .outputMode("append")
    .options(**cosmos_write_config)
    .option("checkpointLocation", "/tmp/eventhub_cosmos_checkpoint")
    .start()
)

print("✅ Streaming to Cosmos DB started")
print(f"   Query ID: {query.id}")
print(f"   Status: {query.status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitor Stream

# COMMAND ----------

# Check stream status
import time

for i in range(10):
    status = query.status
    progress = query.lastProgress

    print(f"\n--- Status Check {i+1} ---")
    print(f"Active: {query.isActive}")
    print(f"Status: {status}")

    if progress:
        print(f"Input Rows: {progress.get('numInputRows', 0)}")
        print(f"Processed Rows: {progress.get('processedRowsPerSecond', 0)}")

    time.sleep(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stop Stream (when done testing)

# COMMAND ----------

# Uncomment to stop the stream
# query.stop()
# print("✅ Stream stopped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Data in Cosmos DB

# COMMAND ----------

# Read back from Cosmos DB to verify
cosmos_read_config = {
    "spark.cosmos.accountEndpoint": COSMOS_ENDPOINT,
    "spark.cosmos.accountKey": COSMOS_KEY,
    "spark.cosmos.database": COSMOS_DATABASE,
    "spark.cosmos.container": COSMOS_CONTAINER,
    "spark.cosmos.read.inferSchema.enabled": "true"
}

# Read from Cosmos DB
cosmos_df = (spark.read
    .format("cosmos.oltp")
    .options(**cosmos_read_config)
    .load()
)

print(f"✅ Total records in Cosmos DB: {cosmos_df.count()}")
display(cosmos_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analytics Queries

# COMMAND ----------

# Total amount by category
from pyspark.sql.functions import sum, count, avg

category_stats = (cosmos_df
    .groupBy("category")
    .agg(
        count("*").alias("transaction_count"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount")
    )
    .orderBy("total_amount", ascending=False)
)

display(category_stats)

# COMMAND ----------

# Fraud detection
fraud_df = cosmos_df.filter(col("is_fraud") == True)

print(f"⚠️  Fraudulent transactions: {fraud_df.count()}")
display(fraud_df)

# COMMAND ----------

# Top merchants by transaction volume
top_merchants = (cosmos_df
    .groupBy("merchant")
    .agg(
        count("*").alias("txn_count"),
        sum("amount").alias("total_revenue")
    )
    .orderBy("total_revenue", ascending=False)
    .limit(10)
)

display(top_merchants)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ✅ Event Hub → Databricks streaming pipeline working
# MAGIC ✅ Data successfully written to Cosmos DB
# MAGIC ✅ Real-time analytics queries functional
# MAGIC
# MAGIC **Architecture Flow:**
# MAGIC ```
# MAGIC Python Producer → Event Hub → Databricks (Kafka) → Cosmos DB
# MAGIC ```
# MAGIC
# MAGIC **Next Steps:**
# MAGIC - Set up alerts for fraudulent transactions
# MAGIC - Add data quality checks
# MAGIC - Implement aggregation windows (tumbling/sliding)
# MAGIC - Scale up partition count for higher throughput
