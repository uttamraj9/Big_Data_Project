# Troubleshooting Guide

## Issue: Kafka Connection Fails from Databricks

### Error
```
kafkashaded.org.apache.kafka.common.KafkaException
Failed to create new KafkaAdminClient
```

### Root Cause
The Databricks cluster may not have network connectivity to Event Hub's Kafka endpoint on port 9093.

### Solutions

#### Option 1: Use Azure Event Hubs SDK (Python)
Instead of Kafka connector, use the Python SDK directly:
- Install: `%pip install azure-eventhub`
- Read messages using `EventHubConsumerClient`
- Convert to Spark DataFrame

See: `databricks/read_eventhub_sdk.py`

#### Option 2: Check Network Connectivity
Ensure the Databricks cluster can reach Event Hub:
```python
# In Databricks notebook
import socket
try:
    socket.create_connection(("ehubnamespacemay2026.servicebus.windows.net", 9093), timeout=5)
    print("✅ Can reach Event Hub port 9093")
except:
    print("❌ Cannot reach Event Hub - check NSG/firewall rules")
```

#### Option 3: Check VNet Configuration
- If Databricks is in a VNet, ensure Service Endpoints are enabled for Event Hubs
- Check Network Security Group (NSG) rules
- Verify Event Hub firewall settings allow Databricks subnet

#### Option 4: Use Event Hubs Capture
Instead of streaming, use Event Hubs Capture to write to ADLS:
```bash
az eventhubs eventhub update \
  --resource-group Itc_Bigdata \
  --namespace-name ehubnamespacemay2026 \
  --name demo-transactions \
  --enable-capture true \
  --capture-interval 300 \
  --destination-name EventHubArchive.AzureBlockBlob \
  --storage-account itcbdneadls \
  --blob-container eventhub-capture
```

Then read from ADLS in Databricks.

---

## Issue: Python SDK Times Out

### Error
```
TimeoutError or ConnectionError when using EventHubConsumerClient
```

### Solutions

1. **Increase timeout**:
   ```python
   consumer_client.receive(
       on_event=on_event,
       starting_position="-1",
       max_wait_time=30  # Increase to 30 seconds
   )
   ```

2. **Check Connection String**:
   - Verify format: `Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=...`
   - Ensure no trailing spaces
   - Test connection string locally first

3. **Check Event Hub has messages**:
   ```bash
   # Check partition details
   az eventhubs eventhub partition show \
     --resource-group Itc_Bigdata \
     --namespace-name ehubnamespacemay2026 \
     --eventhub-name demo-transactions \
     --partition-id 0
   ```

---

## Issue: No Messages Received

### Possible Causes

1. **Messages already consumed**:
   - Event Hub consumer groups track offsets
   - Once messages are read and checkpointed, they won't be re-read
   - Solution: Use different consumer group or send fresh messages

2. **Wrong starting position**:
   ```python
   # Try different starting positions
   starting_position="-1"  # From beginning
   starting_position="@latest"  # From now
   starting_position={"offset": "0"}  # Specific offset
   ```

3. **Partition assignment**:
   ```python
   # Explicitly receive from all partitions
   consumer_client.receive_batch(
       partition_id="0",  # Try each partition
       max_batch_size=100,
       max_wait_time=10
   )
   ```

---

## Issue: Cosmos DB Write Fails

### Error
```
Cosmos DB connector not found
```

### Solution
Install Cosmos DB connector:
```python
%pip install azure-cosmos-spark
```

Or add Maven library to cluster:
```
com.azure.cosmos.spark:azure-cosmos-spark_3-4_2-12:4.19.0
```

### Alternative: Use Cosmos DB SDK
```python
%pip install azure-cosmos

from azure.cosmos import CosmosClient, PartitionKey

client = CosmosClient(COSMOS_ENDPOINT, COSMOS_KEY)
database = client.get_database_client("TransactionsDB")
container = database.get_container_client("transactions")

# Insert from DataFrame
for row in df.collect():
    item = row.asDict()
    container.upsert_item(item)
```

---

## Testing Checklist

- [ ] Producer sends data successfully
- [ ] Event Hub shows incoming messages in Azure Portal
- [ ] Databricks cluster is RUNNING
- [ ] Network connectivity test passes
- [ ] Python SDK can connect (test locally first)
- [ ] Messages are visible in Event Hub Data Explorer
- [ ] Consumer group offset is correct

---

## Quick Tests

### Test 1: Send Fresh Data
```bash
cd producer
source venv/bin/activate
python eventhub_producer.py
```

### Test 2: Verify Messages in Azure Portal
- Go to Azure Portal → Event Hubs → demo-transactions
- Click "Data Explorer"
- Should see message count > 0

### Test 3: Local SDK Test
```python
# Run locally (not in Databricks)
from azure.eventhub import EventHubConsumerClient

def on_event(partition_context, event):
    print(event.body_as_str())

client = EventHubConsumerClient.from_connection_string(
    conn_str="<connection_string>",
    consumer_group="$Default",
    eventhub_name="demo-transactions"
)

with client:
    client.receive(on_event=on_event, max_wait_time=5)
```

If this works locally but not in Databricks, it's a network issue.

---

## Recommended Workaround

For immediate testing without network issues:

1. **Use Producer to generate sample data** → **Save to CSV**
2. **Upload CSV to ADLS or DBFS**
3. **Read in Databricks with Spark**
4. **Test Cosmos DB write with this data**

This validates the full pipeline except Event Hub connectivity.

Example:
```python
# In Databricks
df = spark.read.json("/dbfs/sample_transactions.json")
display(df)

# Write to Cosmos DB
df.write \
  .format("cosmos.oltp") \
  .options(**cosmos_config) \
  .mode("append") \
  .save()
```

---

## Getting Help

- Azure Event Hubs docs: https://learn.microsoft.com/en-us/azure/event-hubs/
- Databricks Event Hubs: https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/streaming-from-eventhubs
- Cosmos DB Spark connector: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/quickstart-spark