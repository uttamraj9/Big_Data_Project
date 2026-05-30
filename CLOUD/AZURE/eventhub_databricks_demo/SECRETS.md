# 🔐 Secrets Management

This demo requires Azure connection strings and keys. **Secrets are NOT committed to git.**

---

## Getting Your Secrets

### 1. Event Hub Connection String

```bash
az eventhubs namespace authorization-rule keys list \
  --resource-group Itc_Bigdata \
  --namespace-name ehubnamespacemay2026 \
  --name RootManageSharedAccessKey \
  --query primaryConnectionString -o tsv
```

Format:
```
Endpoint=sb://YOUR-NAMESPACE.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=YOUR-KEY-HERE
```

### 2. Cosmos DB Connection Details

```bash
# Get endpoint
az cosmosdb show \
  --name itc-bd-cosmos-demo \
  --resource-group Itc_Bigdata \
  --query documentEndpoint -o tsv

# Get primary key
az cosmosdb keys list \
  --name itc-bd-cosmos-demo \
  --resource-group Itc_Bigdata \
  --type keys \
  --query primaryMasterKey -o tsv
```

---

## For Python Producer

### Create `.env` file

Create `producer/.env` with:

```bash
EVENTHUB_CONNECTION_STRING=Endpoint=sb://YOUR-NAMESPACE.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=YOUR-KEY-HERE
EVENTHUB_NAME=demo-transactions
```

### Run Producer

```bash
cd producer
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python eventhub_producer.py
```

---

## For Databricks Notebooks

### Option 1: Databricks Secrets (Recommended)

```bash
# Create secret scope
databricks secrets create-scope --scope azure

# Add secrets
databricks secrets put --scope azure --key eventhub-connection-string
databricks secrets put --scope azure --key cosmos-key
```

In notebooks:
```python
EVENTHUB_CONNECTION_STRING = dbutils.secrets.get(scope="azure", key="eventhub-connection-string")
COSMOS_KEY = dbutils.secrets.get(scope="azure", key="cosmos-key")
```

### Option 2: Direct Edit (Testing Only)

Edit the notebook files and replace placeholders:
- `YOUR-EVENTHUB-NAMESPACE` → `ehubnamespacemay2026`
- `YOUR-NAMESPACE.servicebus.windows.net` → `ehubnamespacemay2026.servicebus.windows.net`
- `YOUR-KEY-HERE` → Paste your actual key
- `YOUR-COSMOS-ACCOUNT` → `itc-bd-cosmos-demo`
- `YOUR-COSMOS-PRIMARY-KEY-HERE` → Paste your Cosmos DB key

⚠️ **NEVER commit notebooks with real keys back to git!**

---

## Security Best Practices

✅ **DO:**
- Use environment variables or secret managers
- Use Databricks Secrets for notebooks
- Rotate keys regularly
- Use managed identities where possible

❌ **DON'T:**
- Hardcode secrets in code
- Commit `.env` files to git
- Share connection strings in chat/email
- Use production keys for testing

---

## Files Excluded from Git

The following are in `.gitignore`:
- `producer/.env` (your local secrets)
- `producer/venv/` (Python virtual environment)
- Any `config.py` with secrets

Committed (safe):
- `.env.example` (template with placeholders)
- Code files with placeholder values

---

## Quick Secret Check

If you see these in your files, you're safe:
```
YOUR-KEY-HERE
YOUR-NAMESPACE
YOUR-COSMOS-ACCOUNT
```

If you see actual Azure keys (base64 strings), **DO NOT COMMIT!**
