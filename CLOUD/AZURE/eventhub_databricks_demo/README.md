# Event Hub → Databricks → Cosmos DB Demo

Complete end-to-end demo showing:
1. **Python producer** sending sample transaction data to Event Hub
2. **Databricks Spark** reading from Event Hub (Kafka endpoint)
3. **Writing to Cosmos DB** from Spark

---

## 📋 Azure Resources Created

### Event Hub
- **Namespace:** `ehubnamespacemay2026`
- **Event Hub Name:** `demo-transactions`
- **Partitions:** 2
- **Kafka Endpoint:** `ehubnamespacemay2026.servicebus.windows.net:9093`
- **Connection String:** (see producer config)

### Cosmos DB
- **Account:** `itc-bd-cosmos-demo`
- **Database:** `TransactionsDB`
- **Container:** `transactions`
- **Partition Key:** `/transaction_id`
- **Endpoint:** `https://itc-bd-cosmos-demo.documents.azure.com:443/`

### Databricks
- **Workspace:** `adb-7405609294150794.14.azuredatabricks.net`
- **Cluster:** `ADBcluster` (ID: `0417-133347-aqen2ief`)
- **Spark:** 17.3.x-scala2.13

---

## 🚀 Quick Start

### Step 1: Create Event Hub (Already Done ✅)
```bash
cd /Users/uttamkumar/Downloads/Big_Data_Project/CLOUD/AZURE/eventhub_databricks_demo

# View creation commands
cat setup_commands.sh
```

### Step 2: Send Sample Data to Event Hub
```bash
# Install dependencies
pip install azure-eventhub

# Run producer (sends 100 sample transactions)
python producer/eventhub_producer.py
```

### Step 3: Read from Event Hub in Databricks

Upload `databricks/read_eventhub_kafka.py` to Databricks workspace and run on ADBcluster.

The notebook will:
- ✅ Connect to Event Hub via Kafka endpoint
- ✅ Read streaming data
- ✅ Parse JSON messages
- ✅ Display DataFrame

### Step 4: Write to Cosmos DB

Run `databricks/eventhub_to_cosmos.py` in Databricks.

This will:
- ✅ Read from Event Hub
- ✅ Process transaction data
- ✅ Write to Cosmos DB
- ✅ Verify data in Cosmos DB

---

## 📁 Directory Structure

```
eventhub_databricks_demo/
├── README.md                           # This file
├── setup_commands.sh                   # Azure CLI commands used
├── sample_data/
│   └── sample_transactions.json        # Sample data format
├── producer/
│   ├── eventhub_producer.py           # Python producer (send to Event Hub)
│   └── requirements.txt               # Python dependencies
├── databricks/
│   ├── read_eventhub_kafka.py         # Spark: Read from Event Hub
│   ├── eventhub_to_cosmos.py          # Spark: Event Hub → Cosmos DB
│   └── verify_cosmos_data.py          # Verify data in Cosmos DB
└── cosmos/
    └── query_samples.sql              # Sample Cosmos DB queries
```

---

## 🔑 Configuration

**IMPORTANT:** Secrets are NOT committed to git. Get connection strings using Azure CLI:

### Get Event Hub Connection String
```bash
az eventhubs namespace authorization-rule keys list \
  --resource-group Itc_Bigdata \
  --namespace-name YOUR-NAMESPACE \
  --name RootManageSharedAccessKey \
  --query primaryConnectionString -o tsv
```

### Get Cosmos DB Key
```bash
az cosmosdb keys list \
  --name YOUR-COSMOS-ACCOUNT \
  --resource-group Itc_Bigdata \
  --type keys \
  --query primaryMasterKey -o tsv
```

### Set Environment Variables (Producer)
```bash
export EVENTHUB_CONNECTION_STRING="Endpoint=sb://..."
export EVENTHUB_NAME="demo-transactions"
```

### Use Databricks Secrets (Recommended for Notebooks)
```python
# In Databricks notebook
EVENTHUB_CONNECTION_STRING = dbutils.secrets.get(scope="azure", key="eventhub-connection-string")
COSMOS_KEY = dbutils.secrets.get(scope="azure", key="cosmos-key")
```

---

## 📊 Sample Data Format

```json
{
  "transaction_id": "TXN-1234567890",
  "customer_id": "CUST-5678",
  "amount": 125.50,
  "merchant": "Amazon",
  "category": "Shopping",
  "timestamp": "2026-05-30T22:45:30.123Z",
  "location": "London, UK",
  "is_fraud": false
}
```

---

## ✅ Testing Checklist

- [x] Event Hub `demo-transactions` created
- [x] Cosmos DB account `itc-bd-cosmos-demo` created
- [x] Database `TransactionsDB` created
- [x] Container `transactions` created
- [ ] Producer sends data to Event Hub
- [ ] Databricks reads from Event Hub
- [ ] Data flows to Cosmos DB
- [ ] Query Cosmos DB to verify data

---

## 🔧 Troubleshooting

### Producer Issues
- Check Event Hub connection string
- Verify network connectivity
- Check Event Hub exists: `az eventhubs eventhub show --resource-group Itc_Bigdata --namespace-name ehubnamespacemay2026 --name demo-transactions`

### Databricks Issues
- Ensure cluster is running
- No library installation needed (uses Kafka)
- Check connection string format

### Cosmos DB Issues
- Verify container exists
- Check partition key matches (`/transaction_id`)
- Ensure Cosmos DB connector is available in Databricks

---

## 🧹 Cleanup (Optional)

```bash
# Delete Event Hub
az eventhubs eventhub delete --resource-group Itc_Bigdata --namespace-name ehubnamespacemay2026 --name demo-transactions

# Delete Cosmos DB
az cosmosdb delete --name itc-bd-cosmos-demo --resource-group Itc_Bigdata --yes
```

---

## 📚 References

- [Event Hubs Kafka Endpoint](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-for-kafka-ecosystem-overview)
- [Databricks Event Hubs Integration](https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/streaming-from-eventhubs)
- [Cosmos DB Spark Connector](https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/quickstart-spark)
