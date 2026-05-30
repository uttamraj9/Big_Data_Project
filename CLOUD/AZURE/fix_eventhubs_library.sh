#!/bin/bash
set -e

# Fix Databricks EventHubs library issue
# This script:
# 1. Adds the EventHubs Maven artifact to the UC metastore allowlist
# 2. Installs the library on the ADBcluster

WORKSPACE="adb-7405609294150794.14.azuredatabricks.net"
CLUSTER_ID="0417-133347-aqen2ief"
MAVEN_COORDINATES="com.microsoft.azure:azure-eventhubs-spark_2.13:2.3.22"

echo "=== Databricks EventHubs Library Fix ==="
echo "Workspace: $WORKSPACE"
echo "Cluster: $CLUSTER_ID"
echo "Maven Coordinates: $MAVEN_COORDINATES"
echo ""

# Get Azure AD token for Databricks
echo "Getting Azure AD token..."
TOKEN=$(az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --query accessToken -o tsv)

# Step 1: Add artifact to metastore allowlist via SQL
echo ""
echo "Step 1: Adding artifact to UC metastore allowlist..."
echo "Running SQL: ALTER METASTORE ADD ARTIFACT '$MAVEN_COORDINATES' MAVEN;"

# Create SQL statement
SQL_STATEMENT="ALTER METASTORE ADD ARTIFACT '$MAVEN_COORDINATES' MAVEN;"

# Execute via SQL Statement API
RESPONSE=$(curl -s -X POST "https://$WORKSPACE/api/2.0/sql/statements" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"statement\": \"$SQL_STATEMENT\",
    \"warehouse_id\": \"auto\",
    \"wait_timeout\": \"30s\"
  }")

# Check if successful
if echo "$RESPONSE" | grep -q '"status":"SUCCEEDED"'; then
  echo "✅ Artifact added to allowlist successfully"
elif echo "$RESPONSE" | grep -q "already exists"; then
  echo "✅ Artifact already in allowlist"
elif echo "$RESPONSE" | grep -q "error"; then
  echo "⚠️  SQL execution response:"
  echo "$RESPONSE" | python3 -c "import sys,json; print(json.dumps(json.load(sys.stdin), indent=2))"
  echo ""
  echo "Note: If you get a permission error, you need metastore admin access."
  echo "Alternative: Use Event Hubs Kafka endpoint (no library needed)"
else
  echo "Response:"
  echo "$RESPONSE" | python3 -c "import sys,json; print(json.dumps(json.load(sys.stdin), indent=2))"
fi

echo ""
echo "Step 2: Installing library on cluster $CLUSTER_ID..."

# Install library via REST API
INSTALL_RESPONSE=$(curl -s -X POST "https://$WORKSPACE/api/2.0/libraries/install" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"cluster_id\": \"$CLUSTER_ID\",
    \"libraries\": [
      {
        \"maven\": {
          \"coordinates\": \"$MAVEN_COORDINATES\"
        }
      }
    ]
  }")

if [ -z "$INSTALL_RESPONSE" ] || echo "$INSTALL_RESPONSE" | grep -q '{}'; then
  echo "✅ Library installation initiated"
else
  echo "Response:"
  echo "$INSTALL_RESPONSE" | python3 -c "import sys,json; print(json.dumps(json.load(sys.stdin), indent=2))" 2>/dev/null || echo "$INSTALL_RESPONSE"
fi

echo ""
echo "Step 3: Checking library status..."
sleep 3

STATUS_RESPONSE=$(curl -s "https://$WORKSPACE/api/2.0/libraries/cluster-status?cluster_id=$CLUSTER_ID" \
  -H "Authorization: Bearer $TOKEN")

echo "$STATUS_RESPONSE" | python3 -c "
import sys, json
data = json.load(sys.stdin)
libs = data.get('library_statuses', [])
if not libs:
    print('No libraries found')
else:
    for lib in libs:
        maven = lib.get('library', {}).get('maven', {})
        status = lib.get('status', 'UNKNOWN')
        if maven:
            coords = maven.get('coordinates', 'Unknown')
            print(f'Library: {coords}')
            print(f'Status: {status}')
            if lib.get('messages'):
                print(f'Messages: {lib.get(\"messages\")}')
"

echo ""
echo "=== Done ==="
echo ""
echo "Note: If the cluster is TERMINATED, start it first:"
echo "  az databricks cluster start --cluster-id $CLUSTER_ID --resource-group Itc_Bigdata --workspace-name itc-bd-ne-adb"
echo ""
echo "Alternative approach (recommended):"
echo "Use Event Hubs Kafka endpoint - no library installation needed:"
echo "  .format('kafka')"
echo "  .option('kafka.bootstrap.servers', '<namespace>.servicebus.windows.net:9093')"
echo "  .option('subscribe', '<eventhub-name>')"
