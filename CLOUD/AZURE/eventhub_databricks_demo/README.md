# Event Hub → Databricks → Cosmos DB Streaming Pipeline

Complete real-time data streaming pipeline using Azure Event Hubs, Databricks, and Cosmos DB.

## 🏗️ Architecture

```
Producer (Python) → Event Hub → Databricks Spark Streaming (Kafka) → Cosmos DB
```

**Real-time streaming** - no intermediate storage, direct Kafka protocol connection.

---

## 📋 Prerequisites

- Azure subscription with:
  - Event Hubs namespace
  - Databricks workspace with cluster
  - Cosmos DB account (optional, for write step)
- Python 3.8+
- Azure CLI configured

---

## 🚀 Quick Start

### Step 1: Send Test Data to Event Hub

```bash
cd producer

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Send 100 sample transactions
python eventhub_producer.py
```

**Output:**
```
✅ SUCCESS: Sent 100 transactions to Event Hub
```

### Step 2: Read in Databricks (Real-Time)

1. Open Databricks workspace
2. Navigate to: `Workspace → Users → [your-email] → eventhub_demo`
3. Open: **`realtime_streaming`**
4. **Important:** Select **ADBcluster** (not Serverless!)
5. Run all cells

You'll see transactions streaming in real-time! 🔴 LIVE

---

## 📦 What's Included

### Producer (Local Python)

**Location:** `producer/eventhub_producer.py`

**What it does:**
- Generates realistic transaction data (credit card fraud detection use case)
- Sends JSON messages to Event Hub
- Configurable batch size (default: 100 transactions)

**Sample data:**
```json
{
  "transaction_id": "TXN-0000001234",
  "customer_id": "CUST-5678",
  "amount": 125.50,
  "merchant": "Amazon UK",
  "category": "Shopping",
  "timestamp": "2026-05-31T01:15:30.123Z",
  "location": "London, UK",
  "is_fraud": false,
  "payment_method": "credit_card"
}
```

### Databricks Notebooks

#### 1. `realtime_streaming` ⭐ **RECOMMENDED**
**Real-time Event Hub streaming**
- Connects to Event Hub via Kafka protocol
- Streams transactions in real-time
- Live statistics dashboard
- Shows new data as it arrives

#### 2. `kafka_direct_streaming`
**Batch + Streaming modes**
- Reads historical data (batch)
- Real-time streaming option
- Full transaction parsing and statistics

#### 3. `read_from_capture`
**ADLS Capture reader** (alternative approach)
- Reads from Event Hub Capture (ADLS)
- Good for batch processing
- Fallback if Kafka has issues

#### 4. `adls_to_cosmos_complete`
**Full pipeline with Cosmos DB**
- Reads from ADLS Capture
- Writes to Cosmos DB
- End-to-end batch pipeline

---

## 🧪 Testing Real-Time Streaming

### Test the Live Pipeline

1. **Open Databricks notebook:** `realtime_streaming`
2. **Select cluster:** ADBcluster
3. **Run cell 3** (the streaming cell with "🔴 LIVE")
4. **Keep it running**

5. **In a terminal, send new data:**
```bash
cd producer
source venv/bin/activate
python eventhub_producer.py
```

6. **Watch Databricks** - new transactions appear in real-time! ⚡

---

## 🔑 Configuration

### Event Hub Settings

**Namespace:** `ehubnamespacemay2026`  
**Event Hub:** `demo-transactions`  
**Partitions:** 2  
**Retention:** 7 days

### Databricks Cluster

**Name:** ADBcluster  
**Runtime:** 17.3.x-scala2.13  
**Mode:** Standard (not Serverless)

**Why not Serverless?**
- Serverless doesn't have ADLS access configured
- ADBcluster has the storage key pre-configured

### Cosmos DB (Optional)

**Account:** `itc-bd-cosmos-demo`  
**Database:** `TransactionsDB`  
**Container:** `transactions`  
**Partition Key:** `/transaction_id`

---

## 📝 How to Add New Test Data

### Option 1: Run Producer Again (Easiest)

```bash
cd producer
source venv/bin/activate
python eventhub_producer.py
```

Sends 100 new random transactions.

### Option 2: Modify Producer Code

Edit `producer/eventhub_producer.py`:

```python
# Change batch size
num_batches = 20  # Send 20 batches instead of 10
batch_size = 50   # 50 per batch = 1000 total

# Add custom transaction
custom_txn = {
    "transaction_id": "TXN-CUSTOM-001",
    "customer_id": "CUST-9999",
    "amount": 999.99,
    "merchant": "Custom Merchant",
    "category": "Test",
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "location": "Test Location",
    "is_fraud": True,
    "payment_method": "test"
}
```

### Option 3: Send Single Message via Azure CLI

```bash
az eventhubs eventhub send \
  --resource-group Itc_Bigdata \
  --namespace-name ehubnamespacemay2026 \
  --name demo-transactions \
  --body '{"transaction_id":"TXN-TEST-001","amount":100.00,"merchant":"Test"}'
```

---

## 🔧 Troubleshooting

### Issue: "CONFIG_NOT_AVAILABLE" Error

**Cause:** Using Serverless compute instead of ADBcluster

**Fix:** 
1. Click cluster dropdown (top right in notebook)
2. Select **ADBcluster**
3. Re-run notebook

### Issue: "Connection refused" to Event Hub

**Cause:** Kafka endpoint blocked or wrong credentials

**Check:**
```bash
# Verify Event Hub exists
az eventhubs eventhub show \
  --resource-group Itc_Bigdata \
  --namespace-name ehubnamespacemay2026 \
  --name demo-transactions
```

**Fix:** Check connection string in notebook matches your Event Hub

### Issue: No data in streaming notebook

**Cause:** Reading from "latest" offset with no new data

**Fix:** 
- Change `startingOffsets` to `"earliest"` to read historical data
- OR send new data using producer

### Issue: Producer fails with "externally-managed-environment"

**Fix:**
```bash
# Use virtual environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

---

## 📊 Monitoring

### Check Event Hub Messages

```bash
# View Event Hub details
az eventhubs eventhub show \
  --resource-group Itc_Bigdata \
  --namespace-name ehubnamespacemay2026 \
  --name demo-transactions \
  --query "{status:status, partitions:partitionCount, retention:messageRetentionInDays}"
```

### Check Databricks Stream Status

In notebook:
```python
# Get active streams
for stream in spark.streams.active:
    print(f"Stream: {stream.name}")
    print(f"Status: {stream.status}")
    print(f"Recent progress: {stream.recentProgress}")
```

### Check Cosmos DB Data

Azure Portal → Cosmos DB → Data Explorer → Run query:
```sql
SELECT COUNT(1) as total_records FROM c
SELECT * FROM c WHERE c.is_fraud = true
```

---

## 🏃 Running in Production

### 1. Deploy Producer

**Options:**
- Azure Functions (serverless)
- Azure Container Instances
- Azure Kubernetes Service
- Azure App Service

**Example Dockerfile:**
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY producer/requirements.txt .
RUN pip install -r requirements.txt
COPY producer/ .
CMD ["python", "eventhub_producer.py"]
```

### 2. Schedule Databricks Jobs

```bash
# Create scheduled job
databricks jobs create --json '{
  "name": "Process Event Hub Stream",
  "notebook_task": {
    "notebook_path": "/Users/[email]/eventhub_demo/kafka_direct_streaming"
  },
  "existing_cluster_id": "0417-133347-aqen2ief",
  "schedule": {
    "quartz_cron_expression": "0 0 * * * ?",
    "timezone_id": "UTC"
  }
}'
```

### 3. Use Databricks Secrets

Instead of hardcoded connection strings:

```python
# Store secret
databricks secrets put --scope azure --key eventhub-connection-string

# Use in notebook
EVENTHUB_CONN_STRING = dbutils.secrets.get(scope="azure", key="eventhub-connection-string")
```

---

## 📁 Project Structure

```
eventhub_databricks_demo/
├── README.md                           # This file
├── FINAL_SOLUTION.md                   # Quick reference
├── TROUBLESHOOTING.md                  # Common issues
├── SECRETS.md                          # How to get credentials
├── producer/
│   ├── eventhub_producer.py           # Python producer
│   ├── requirements.txt               # Dependencies
│   ├── .env                           # Local secrets (not in git)
│   └── venv/                          # Virtual environment
├── databricks/
│   ├── realtime_streaming.py          # ⭐ Real-time streaming
│   ├── kafka_direct_streaming.py      # Kafka batch + stream
│   ├── read_from_capture.py           # ADLS Capture reader
│   └── adls_to_cosmos_complete.py     # Full pipeline
├── sample_data/
│   └── sample_transactions.json       # Example data
└── upload_to_databricks.sh            # Upload script
```

---

## 🎯 Key Features

✅ **Real-time streaming** - sub-second latency  
✅ **Scalable** - Event Hubs can handle millions of events/sec  
✅ **Fault-tolerant** - Event Hubs retention + Spark checkpointing  
✅ **Production-ready** - Kafka protocol, proven architecture  
✅ **Easy testing** - Simple Python producer for demos  

---

## 📚 References

- [Azure Event Hubs Kafka](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-for-kafka-ecosystem-overview)
- [Databricks Structured Streaming](https://docs.databricks.com/structured-streaming/index.html)
- [Event Hubs Python SDK](https://learn.microsoft.com/en-us/python/api/overview/azure/eventhub-readme)
- [Cosmos DB Spark Connector](https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/quickstart-spark)

---

## 🤝 Contributing

To add new features:
1. Test locally with producer
2. Update Databricks notebook
3. Verify in both batch and streaming modes
4. Update this README

---

## 📄 License

Educational/Demo project for Azure streaming architectures.

---

## ✅ Current Status

- ✅ Producer working - 400+ transactions sent
- ✅ Event Hub receiving
- ✅ Kafka direct streaming working
- ✅ Real-time display in Databricks
- ✅ Cosmos DB write functional
- ✅ End-to-end pipeline tested

**Last Updated:** 2026-05-31
