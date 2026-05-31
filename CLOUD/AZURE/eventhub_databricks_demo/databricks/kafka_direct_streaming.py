# Databricks notebook source
# MAGIC %md
# MAGIC # Direct Event Hub Kafka Streaming

# COMMAND ----------

# Event Hub Configuration
# TODO: Replace with your Event Hub connection string
EVENTHUB_NAMESPACE = "YOUR-EVENTHUB-NAMESPACE"
EVENTHUB_NAME = "demo-transactions"
EVENTHUB_CONN_STRING = "Endpoint=sb://YOUR-NAMESPACE.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=YOUR-KEY-HERE"

KAFKA_BOOTSTRAP_SERVERS = f"{EVENTHUB_NAMESPACE}.servicebus.windows.net:9093"
KAFKA_SASL_CONFIG = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{EVENTHUB_CONN_STRING}";'

print("✅ Configuration loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch Read from Event Hub

# COMMAND ----------

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType, BooleanType

print("Reading from Event Hub via Kafka...")

df_kafka = (spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", EVENTHUB_NAME)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", KAFKA_SASL_CONFIG)
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)

count = df_kafka.count()
print(f"✅ Read {count} messages")
display(df_kafka.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse Transactions

# COMMAND ----------

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

df_parsed = (df_kafka
    .selectExpr("CAST(value AS STRING) as json_string")
    .select(from_json(col("json_string"), transaction_schema).alias("data"))
    .select("data.*")
)

print(f"✅ Parsed {df_parsed.count()} transactions")
display(df_parsed)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Statistics

# COMMAND ----------

from pyspark.sql.functions import count, sum as _sum

total = df_parsed.count()
fraud = df_parsed.filter(col("is_fraud") == True).count()

print(f"Total: {total}, Fraudulent: {fraud}")

category_stats = df_parsed.groupBy("category").agg(
    count("*").alias("count"),
    _sum("amount").alias("total_amount")
).orderBy("total_amount", ascending=False)

display(category_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Mode

# COMMAND ----------

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
    .selectExpr("CAST(value AS STRING) as json_string")
    .select(from_json(col("json_string"), transaction_schema).alias("data"))
    .select("data.*")
)

display(parsed_stream)
