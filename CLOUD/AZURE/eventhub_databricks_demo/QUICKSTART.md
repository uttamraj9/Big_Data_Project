# 🚀 Quick Start Guide

## ✅ Step 1: Send Data to Event Hub (COMPLETED)

```bash
cd /Users/uttamkumar/Downloads/Big_Data_Project/CLOUD/AZURE/eventhub_databricks_demo/producer

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install azure-eventhub

# Run producer
python eventhub_producer.py
```

**Status:** ✅ 100 transactions sent successfully!

---

## 📊 Step 2: Read from Event Hub in Databricks

### Option A: Via Databricks UI
1. Open Databricks workspace: https://adb-7405609294150794.14.azuredatabricks.net
2. Go to **Workspace** → **Shared**
3. Click **Import**
4. Upload `databricks/read_eventhub_kafka.py`
5. Attach to cluster `ADBcluster`
6. Run all cells

### Option B: Via Databricks CLI
```bash
# List clusters (should show ADBcluster)
az databricks cluster list --resource-group Itc_Bigdata --workspace-name itc-bd-ne-adb

# Start cluster if needed
az databricks cluster start --cluster-id 0417-133347-aqen2ief --resource-group Itc_Bigdata --workspace-name itc-bd-ne-adb
```

---

## 📝 Step 3: Verify Data Flow

In the Databricks notebook, you'll see:
- ✅ Real-time streaming data from Event Hub
- ✅ Parsed JSON transactions
- ✅ Category counts
- ✅ Fraud detection

Sample output:
```
transaction_id: TXN-0000000001
customer_id: CUST-5001
amount: 125.50
merchant: Amazon UK
category: Shopping
is_fraud: false
```

---

## 💾 Step 4: Write to Cosmos DB

1. Upload `databricks/eventhub_to_cosmos.py` to Databricks
2. Run all cells
3. Verify data is written to Cosmos DB

---

## 🔍 Step 5: Query Cosmos DB

### Via Azure Portal
1. Go to https://portal.azure.com
2. Navigate to **Cosmos DB** → `itc-bd-cosmos-demo`
3. Open **Data Explorer**
4. Select `TransactionsDB` → `transactions`
5. Click **New SQL Query**
6. Run queries from `cosmos/query_samples.sql`

Example query:
```sql
SELECT c.category, COUNT(1) as count
FROM c
GROUP BY c.category
```

### Via Databricks
Run `databricks/verify_cosmos_data.py` to see:
- Total record count
- Sample data
- Quick statistics

---

## 🎯 Expected Results

| Component | Status | Records |
|-----------|--------|---------|
| Event Hub | ✅ Sent | 100 transactions |
| Databricks | 🔄 Reading | Streaming |
| Cosmos DB | ⏳ Pending | Will match Event Hub |

---

## 🐛 Troubleshooting

### Producer fails to connect
```bash
# Verify Event Hub exists
az eventhubs eventhub show \
  --resource-group Itc_Bigdata \
  --namespace-name ehubnamespacemay2026 \
  --name demo-transactions
```

### Databricks can't read from Event Hub
- Check connection string is correct
- Verify cluster is running
- Check network connectivity

### Cosmos DB write fails
- Verify container exists
- Check partition key is `/transaction_id`
- Ensure Cosmos DB connector is available

---

## 📚 What's Included

```
eventhub_databricks_demo/
├── README.md              ← Architecture & full docs
├── QUICKSTART.md          ← This file
├── setup_commands.sh      ← Azure CLI commands
├── sample_data/
│   └── sample_transactions.json
├── producer/
│   ├── eventhub_producer.py    ✅ TESTED & WORKING
│   └── requirements.txt
├── databricks/
│   ├── read_eventhub_kafka.py       ⏳ Upload to Databricks
│   ├── eventhub_to_cosmos.py        ⏳ Upload to Databricks
│   └── verify_cosmos_data.py        ⏳ Upload to Databricks
└── cosmos/
    └── query_samples.sql
```

---

## 🔗 Quick Links

- **Event Hub Namespace:** `ehubnamespacemay2026`
- **Event Hub Name:** `demo-transactions`
- **Databricks:** https://adb-7405609294150794.14.azuredatabricks.net
- **Cosmos DB:** https://itc-bd-cosmos-demo.documents.azure.com:443/

---

## ✅ Success Criteria

- [x] Event Hub created
- [x] Cosmos DB created
- [x] Producer sends 100 transactions
- [ ] Databricks reads from Event Hub
- [ ] Data visible in Databricks
- [ ] Data written to Cosmos DB
- [ ] Can query Cosmos DB

**Next:** Upload notebooks to Databricks and complete steps 2-5!
