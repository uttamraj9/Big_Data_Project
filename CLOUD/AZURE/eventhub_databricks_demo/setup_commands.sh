#!/bin/bash
# Commands used to create Azure resources for this demo

set -e

RESOURCE_GROUP="Itc_Bigdata"
LOCATION="northeurope"
EVENTHUB_NAMESPACE="ehubnamespacemay2026"
EVENTHUB_NAME="demo-transactions"
COSMOS_ACCOUNT="itc-bd-cosmos-demo"
COSMOS_DB="TransactionsDB"
COSMOS_CONTAINER="transactions"

echo "=== Azure Event Hub → Databricks → Cosmos DB Demo Setup ==="
echo ""

# 1. Create Event Hub
echo "1. Creating Event Hub..."
az eventhubs eventhub create \
  --resource-group $RESOURCE_GROUP \
  --namespace-name $EVENTHUB_NAMESPACE \
  --name $EVENTHUB_NAME \
  --partition-count 2 \
  -o table

echo ""
echo "✅ Event Hub created: $EVENTHUB_NAME"
echo ""

# 2. Get Event Hub connection string
echo "2. Getting Event Hub connection string..."
EVENTHUB_CONN_STRING=$(az eventhubs namespace authorization-rule keys list \
  --resource-group $RESOURCE_GROUP \
  --namespace-name $EVENTHUB_NAMESPACE \
  --name RootManageSharedAccessKey \
  --query primaryConnectionString -o tsv)

echo "Connection String: $EVENTHUB_CONN_STRING"
echo ""

# 3. Create Cosmos DB account
echo "3. Creating Cosmos DB account (this takes ~5 minutes)..."
az cosmosdb create \
  --name $COSMOS_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --locations regionName=$LOCATION failoverPriority=0 \
  --default-consistency-level Session \
  -o table

echo ""
echo "✅ Cosmos DB account created: $COSMOS_ACCOUNT"
echo ""

# 4. Create Cosmos DB database
echo "4. Creating Cosmos DB database..."
az cosmosdb sql database create \
  --account-name $COSMOS_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --name $COSMOS_DB \
  -o table

echo ""
echo "✅ Database created: $COSMOS_DB"
echo ""

# 5. Create Cosmos DB container
echo "5. Creating Cosmos DB container..."
az cosmosdb sql container create \
  --account-name $COSMOS_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --database-name $COSMOS_DB \
  --name $COSMOS_CONTAINER \
  --partition-key-path "/transaction_id" \
  --throughput 400 \
  -o table

echo ""
echo "✅ Container created: $COSMOS_CONTAINER"
echo ""

# 6. Get Cosmos DB keys
echo "6. Getting Cosmos DB keys..."
COSMOS_ENDPOINT="https://$COSMOS_ACCOUNT.documents.azure.com:443/"
COSMOS_KEY=$(az cosmosdb keys list \
  --name $COSMOS_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --type keys \
  --query primaryMasterKey -o tsv)

echo "Endpoint: $COSMOS_ENDPOINT"
echo "Primary Key: $COSMOS_KEY"
echo ""

# Summary
echo "=========================================="
echo "✅ SETUP COMPLETE"
echo "=========================================="
echo ""
echo "Event Hub Details:"
echo "  Namespace: $EVENTHUB_NAMESPACE"
echo "  Event Hub: $EVENTHUB_NAME"
echo "  Kafka Endpoint: $EVENTHUB_NAMESPACE.servicebus.windows.net:9093"
echo ""
echo "Cosmos DB Details:"
echo "  Account: $COSMOS_ACCOUNT"
echo "  Database: $COSMOS_DB"
echo "  Container: $COSMOS_CONTAINER"
echo "  Endpoint: $COSMOS_ENDPOINT"
echo ""
echo "Next Steps:"
echo "  1. Run: python producer/eventhub_producer.py"
echo "  2. Upload databricks/read_eventhub_kafka.py to Databricks"
echo "  3. Run databricks/eventhub_to_cosmos.py"
echo "=========================================="
