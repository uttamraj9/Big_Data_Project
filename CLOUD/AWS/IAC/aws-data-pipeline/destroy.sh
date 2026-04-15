#!/bin/bash
set -e

echo "======================================"
echo "  AWS Data Pipeline – DESTROY"
echo "  WARNING: This will delete EVERYTHING"
echo "======================================"

cd "$(dirname "$0")"

read -p "Are you sure? Type 'yes' to confirm: " confirm
if [ "$confirm" != "yes" ]; then
  echo "Aborted."
  exit 0
fi

# Empty S3 buckets first (Terraform can't delete non-empty buckets without force_destroy)
echo "Emptying S3 buckets..."
PROJECT="uttam-pipeline"
ENV="dev"

for bucket in \
  "${PROJECT}-bronze-${ENV}" \
  "${PROJECT}-silver-${ENV}" \
  "${PROJECT}-glue-scripts-${ENV}" \
  "${PROJECT}-athena-results-${ENV}"; do
  echo "  Emptying s3://${bucket}"
  aws s3 rm "s3://${bucket}" --recursive 2>/dev/null || true
done

# Destroy all Terraform resources
echo "Running terraform destroy..."
terraform destroy -auto-approve

echo ""
echo "======================================"
echo "  All resources destroyed."
echo "======================================"
