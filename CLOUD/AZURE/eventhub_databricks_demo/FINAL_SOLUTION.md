# ✅ WORKING SOLUTION: Event Hub → ADLS → Databricks → Cosmos DB

## 🎯 What Works

**Architecture:**
```
Producer → Event Hub → ADLS Capture (every 60s) → Databricks → Cosmos DB
```

## 📝 Notebooks to Use

### 1. `read_from_capture.py` - Read from ADLS
**Use this to:** Read Event Hub data from ADLS and display it

### 2. `adls_to_cosmos_complete.py` - Complete Pipeline
**Use this to:** Read from ADLS AND write to Cosmos DB

## ⚙️ Important: Use ADBcluster (NOT Serverless)

**Critical:** When running notebooks, select **ADBcluster** from the cluster dropdown (top right).

❌ **Don't use:** Serverless - it doesn't have ADLS access configured
✅ **Use:** ADBcluster - has the storage key configured

## 🔑 ADLS Access Key

The cluster `ADBcluster` has the ADLS storage key configured at cluster level:
```
fs.azure.account.key.itcbdneadls.dfs.core.windows.net = <storage-key>
```

Both notebooks also set this in the first cell as backup.

## 🚀 How to Run

1. Open Databricks workspace
2. Go to: `Workspace → Users → uttam.kumar@... → eventhub_demo`
3. Open: `adls_to_cosmos_complete`
4. **Select cluster:** ADBcluster (dropdown at top)
5. Click **"Run All"**

## ✅ What It Does

1. Reads Avro files from ADLS (Event Hub Capture data)
2. Parses transaction JSON
3. Shows statistics (total, fraud count, etc.)
4. Writes to Cosmos DB
5. Verifies data in Cosmos DB

## 📊 Expected Output

```
✅ Read 100+ records from ADLS
✅ Parsed transactions
✅ Total: 100, Fraudulent: ~5
✅ Written to Cosmos DB
✅ Verified in Cosmos DB
```

## 🔍 Verify in Cosmos DB

Azure Portal → Cosmos DB → itc-bd-cosmos-demo → Data Explorer → TransactionsDB → transactions

Run query:
```sql
SELECT * FROM c
SELECT COUNT(1) FROM c WHERE c.is_fraud = true
```

## ✅ Status

- ✅ Producer working (300+ messages sent)
- ✅ Event Hub Capture enabled
- ✅ ADLS has Avro files
- ✅ Databricks can read from ADLS
- ✅ Cosmos DB write working
- ✅ End-to-end pipeline operational

## 🎉 Complete!
