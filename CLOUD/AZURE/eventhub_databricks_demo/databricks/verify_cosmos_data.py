# Databricks notebook source
# MAGIC %md
# MAGIC # Verify Cosmos DB Data
# MAGIC
# MAGIC Simple verification script to check data in Cosmos DB

# COMMAND ----------

# Cosmos DB configuration
# TODO: Replace these with your actual values or use Databricks Secrets
COSMOS_ENDPOINT = "https://YOUR-COSMOS-ACCOUNT.documents.azure.com:443/"
COSMOS_KEY = "YOUR-COSMOS-PRIMARY-KEY-HERE"
COSMOS_DATABASE = "TransactionsDB"
COSMOS_CONTAINER = "transactions"

cosmos_config = {
    "spark.cosmos.accountEndpoint": COSMOS_ENDPOINT,
    "spark.cosmos.accountKey": COSMOS_KEY,
    "spark.cosmos.database": COSMOS_DATABASE,
    "spark.cosmos.container": COSMOS_CONTAINER,
    "spark.cosmos.read.inferSchema.enabled": "true"
}

# COMMAND ----------

# Read from Cosmos DB
df = (spark.read
    .format("cosmos.oltp")
    .options(**cosmos_config)
    .load()
)

# COMMAND ----------

# Display count
total_records = df.count()
print(f"✅ Total records in Cosmos DB: {total_records}")

# COMMAND ----------

# Show sample data
display(df.limit(10))

# COMMAND ----------

# Show schema
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Stats

# COMMAND ----------

from pyspark.sql.functions import count, sum, avg, min, max

stats = df.select(
    count("*").alias("total_transactions"),
    sum("amount").alias("total_amount"),
    avg("amount").alias("avg_amount"),
    min("amount").alias("min_amount"),
    max("amount").alias("max_amount"),
    count("is_fraud").filter("is_fraud = true").alias("fraud_count")
)

display(stats)
