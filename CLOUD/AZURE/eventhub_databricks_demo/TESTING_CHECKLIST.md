# Testing Checklist

## ✅ Phase 1: Event Hub Setup (COMPLETED)

- [x] Event Hub namespace exists: `ehubnamespacemay2026`
- [x] Event Hub created: `demo-transactions` (2 partitions)
- [x] Connection string retrieved
- [x] Kafka endpoint configured: `ehubnamespacemay2026.servicebus.windows.net:9093`

**Verification:**
```bash
az eventhubs eventhub show \
  --resource-group Itc_Bigdata \
  --namespace-name ehubnamespacemay2026 \
  --name demo-transactions -o table
```

---

## ✅ Phase 2: Cosmos DB Setup (COMPLETED)

- [x] Cosmos DB account created: `itc-bd-cosmos-demo`
- [x] Database created: `TransactionsDB`
- [x] Container created: `transactions` with partition key `/transaction_id`
- [x] Primary key retrieved

**Verification:**
```bash
az cosmosdb sql container show \
  --account-name itc-bd-cosmos-demo \
  --resource-group Itc_Bigdata \
  --database-name TransactionsDB \
  --name transactions -o table
```

---

## ✅ Phase 3: Producer Test (COMPLETED)

- [x] Python environment setup
- [x] azure-eventhub SDK installed
- [x] Producer script runs successfully
- [x] 100 transactions sent to Event Hub
- [x] No errors in producer logs

**Test Result:**
```
✅ SUCCESS: Sent 100 transactions to Event Hub
```

**Sample data format verified:**
```json
{
  "transaction_id": "TXN-0000009999",
  "customer_id": "CUST-2222",
  "amount": 861.25,
  "merchant": "Asda",
  "category": "Utilities",
  "timestamp": "2026-05-30T22:50:12.051018+00:00",
  "location": "Manchester, UK",
  "is_fraud": false,
  "payment_method": "contactless"
}
```

---

## ⏳ Phase 4: Databricks Read Test (PENDING)

Upload `databricks/read_eventhub_kafka.py` to Databricks and verify:

- [ ] Cluster `ADBcluster` is running
- [ ] Notebook uploaded to workspace
- [ ] Stream connects to Event Hub via Kafka
- [ ] JSON parsing works correctly
- [ ] Data displayed in notebook
- [ ] All 100 transactions visible
- [ ] Schema matches expected format

**Expected Output:**
- Real-time streaming DataFrame
- Columns: transaction_id, customer_id, amount, merchant, category, timestamp, location, is_fraud, payment_method
- Count: 100 records

---

## ⏳ Phase 5: Cosmos DB Write Test (PENDING)

Upload `databricks/eventhub_to_cosmos.py` to Databricks and verify:

- [ ] Cosmos DB connector available in Databricks
- [ ] Stream starts successfully
- [ ] Data written to Cosmos DB
- [ ] No write errors in logs
- [ ] Checkpoint location created

**Verification:**
```python
# In Databricks
df = spark.read.format("cosmos.oltp").options(**cosmos_config).load()
print(f"Records in Cosmos DB: {df.count()}")
```

---

## ⏳ Phase 6: Cosmos DB Query Test (PENDING)

Run queries from `cosmos/query_samples.sql` or use `databricks/verify_cosmos_data.py`:

- [ ] Can query all records
- [ ] Category grouping works
- [ ] Fraud detection filter works
- [ ] Aggregations (sum, avg, count) work
- [ ] Data matches Event Hub input

**Expected Queries:**
```sql
-- Total records
SELECT COUNT(1) FROM c

-- Fraudulent transactions
SELECT * FROM c WHERE c.is_fraud = true

-- Category stats
SELECT c.category, COUNT(1) as count FROM c GROUP BY c.category
```

---

## 🎯 End-to-End Test

**Flow:** Producer → Event Hub → Databricks → Cosmos DB

1. [x] Run producer: 100 transactions sent
2. [ ] Databricks reads: 100 transactions received
3. [ ] Cosmos DB stores: 100 transactions persisted
4. [ ] Query Cosmos DB: All data queryable

**Success Criteria:**
- No data loss (100 in = 100 out)
- All fields present and correct
- Fraud flag working (expect ~5 fraudulent transactions)
- Timestamps valid
- Query performance acceptable

---

## 🐛 Known Issues / Notes

### Cosmos DB Connector
If Cosmos DB connector is not available in Databricks:
```python
%pip install azure-cosmos-spark_3-4_2-12
```

Or use Databricks Maven library:
```
com.azure.cosmos.spark:azure-cosmos-spark_3-4_2-12:4.19.0
```

### Event Hub Kafka Settings
Using Kafka endpoint instead of EventHubs-Spark connector because:
- No library installation needed
- Works on UC Shared clusters
- EventHubs-Spark is deprecated

### Timing
- Producer runs in ~10 seconds
- Databricks streaming has ~5s latency
- Cosmos DB writes are near real-time

---

## 📊 Monitoring

### Check Event Hub Messages
```bash
# Get consumer groups
az eventhubs eventhub consumer-group list \
  --resource-group Itc_Bigdata \
  --namespace-name ehubnamespacemay2026 \
  --eventhub-name demo-transactions -o table
```

### Check Databricks Stream Status
```python
# In notebook
query.status
query.lastProgress
```

### Check Cosmos DB Metrics
Azure Portal → Cosmos DB → Metrics → Request Units, Document Count

---

## 🧹 Cleanup Commands (Optional)

```bash
# Delete Event Hub
az eventhubs eventhub delete \
  --resource-group Itc_Bigdata \
  --namespace-name ehubnamespacemay2026 \
  --name demo-transactions

# Delete Cosmos DB
az cosmosdb delete \
  --name itc-bd-cosmos-demo \
  --resource-group Itc_Bigdata \
  --yes

# Estimated cost savings: ~£10-20/month
```

---

## ✅ Demo Status Summary

| Component | Status | Details |
|-----------|--------|---------|
| Event Hub | ✅ Working | 100 messages sent |
| Producer | ✅ Working | Python script tested |
| Databricks Notebooks | 📝 Ready | Need to upload & test |
| Cosmos DB | ✅ Ready | Container configured |
| End-to-End | ⏳ Pending | Awaiting Databricks test |

**Next Action:** Upload notebooks to Databricks workspace and run Phase 4-6 tests.
