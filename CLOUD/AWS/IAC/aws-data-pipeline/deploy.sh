#!/bin/bash
set -e

echo "======================================"
echo "  AWS Data Pipeline – DEPLOY"
echo "======================================"

cd "$(dirname "$0")"

# Init
terraform init

# Plan
terraform plan -out=tfplan

# Apply
terraform apply tfplan

echo ""
echo "======================================"
echo "  Deployment Complete!"
echo "======================================"
terraform output
