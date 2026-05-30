# Databricks notebook source
# MAGIC %md
# MAGIC # Read from Event Hub using Python SDK
# MAGIC
# MAGIC Alternative approach using azure-eventhub Python library

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install SDK

# COMMAND ----------

# MAGIC %pip install azure-eventhub==5.11.6

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

EVENTHUB_CONNECTION_STRING = "Endpoint=sb://YOUR-NAMESPACE.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=YOUR-KEY-HERE"
EVENTHUB_NAME = "demo-transactions"

print("✅ Configuration loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Messages from Event Hub

# COMMAND ----------

from azure.eventhub import EventHubConsumerClient
import json

messages = []

def on_event(partition_context, event):
    # Parse the event body
    body = event.body_as_str()
    data = json.loads(body)
    messages.append(data)
    partition_context.update_checkpoint(event)

# Create consumer client
consumer_client = EventHubConsumerClient.from_connection_string(
    conn_str=EVENTHUB_CONNECTION_STRING,
    consumer_group="$Default",
    eventhub_name=EVENTHUB_NAME,
)

try:
    print("📥 Reading messages from Event Hub...")

    # Receive messages (with timeout)
    with consumer_client:
        consumer_client.receive(
            on_event=on_event,
            starting_position="-1",  # Start from beginning
            max_wait_time=10  # Wait 10 seconds
        )

    print(f"✅ Read {len(messages)} messages")

except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert to DataFrame

# COMMAND ----------

if messages:
    # Create DataFrame from messages
    df = spark.createDataFrame(messages)

    print(f"✅ DataFrame created with {df.count()} records")
    df.printSchema()

    # Show sample data
    display(df.limit(10))
else:
    print("⚠️  No messages received")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Statistics

# COMMAND ----------

if messages:
    from pyspark.sql.functions import count, sum as _sum, avg

    # Category stats
    category_stats = (df
        .groupBy("category")
        .agg(
            count("*").alias("count"),
            _sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount")
        )
        .orderBy("count", ascending=False)
    )

    print("=== Transactions by Category ===")
    display(category_stats)

    # Fraud detection
    fraud_df = df.filter(df.is_fraud == True)
    fraud_count = fraud_df.count()

    print(f"\n⚠️  Fraudulent transactions: {fraud_count} ({fraud_count/df.count()*100:.1f}%)")
    display(fraud_df)

    # Summary
    print("\n" + "=" * 60)
    print("TEST RESULTS")
    print("=" * 60)
    print(f"✅ Total messages: {len(messages)}")
    print(f"✅ Fraudulent transactions: {fraud_count}")
    print(f"✅ Unique merchants: {df.select('merchant').distinct().count()}")
    print(f"✅ Unique customers: {df.select('customer_id').distinct().count()}")
    print("=" * 60)
    print("\n✅ TEST PASSED - Event Hub → Databricks integration working!")
