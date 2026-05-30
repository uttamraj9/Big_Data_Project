#!/bin/bash
# Upload notebooks to Databricks workspace

set -e

WORKSPACE_URL="https://adb-7405609294150794.14.azuredatabricks.net"
WORKSPACE_PATH="/Users/uttam.kumar@informationtechconsultants.com/eventhub_demo"
LOCAL_DIR="databricks"

echo "=== Upload Notebooks to Databricks ==="
echo "Workspace: $WORKSPACE_URL"
echo "Target Path: $WORKSPACE_PATH"
echo ""

# Get Azure AD token for Databricks
echo "Getting authentication token..."
TOKEN=$(az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --query accessToken -o tsv)

if [ -z "$TOKEN" ]; then
    echo "❌ Failed to get authentication token"
    exit 1
fi

echo "✅ Authenticated"
echo ""

# Create folder if it doesn't exist
echo "Creating workspace folder..."
curl -s -X POST "$WORKSPACE_URL/api/2.0/workspace/mkdirs" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"path\": \"$WORKSPACE_PATH\"}"

echo "✅ Folder created/verified"
echo ""

# Upload each notebook
echo "Uploading notebooks..."

for notebook in read_eventhub_kafka eventhub_to_cosmos verify_cosmos_data; do
    echo "  Uploading $notebook.py..."

    # Read file and base64 encode
    CONTENT=$(base64 -i "$LOCAL_DIR/${notebook}.py")

    # Import notebook
    RESPONSE=$(curl -s -X POST "$WORKSPACE_URL/api/2.0/workspace/import" \
      -H "Authorization: Bearer $TOKEN" \
      -H "Content-Type: application/json" \
      -d "{
        \"path\": \"$WORKSPACE_PATH/$notebook\",
        \"format\": \"SOURCE\",
        \"language\": \"PYTHON\",
        \"content\": \"$CONTENT\",
        \"overwrite\": true
      }")

    if echo "$RESPONSE" | grep -q "error"; then
        echo "    ⚠️  Error: $RESPONSE"
    else
        echo "    ✅ Uploaded: $notebook"
    fi
done

echo ""
echo "=== Upload Complete ==="
echo ""
echo "Open Databricks and navigate to:"
echo "  Workspace → Users → uttam.kumar@... → eventhub_demo"
echo ""
echo "Next steps:"
echo "  1. Open read_eventhub_kafka notebook"
echo "  2. Replace placeholder credentials (see SECRETS.md)"
echo "  3. Attach to cluster: ADBcluster"
echo "  4. Run all cells"
