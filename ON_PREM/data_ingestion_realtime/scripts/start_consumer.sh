#!/bin/bash
# =============================================================================
# start_consumer.sh
# =============================================================================
# Starts the Kafka consumer that reads from cc_fraud_stream and writes each
# transaction to the HBase table cc_fraud_realtime.
#
# Run from the edge node (13.41.167.97) — HBase Thrift must be reachable.
#
# Usage
# -----
#   bash start_consumer.sh                    # uses config/config.properties
#   bash start_consumer.sh --batch-size 100   # override HBase batch size
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="${SCRIPT_DIR}/../src"
CONFIG="${SCRIPT_DIR}/../config/config.properties"

echo "============================================================"
echo " Starting Kafka Consumer → HBase Writer"
echo " Config : ${CONFIG}"
echo " Source : ${SRC_DIR}/kafka_consumer.py"
echo "============================================================"
echo ""

python3.12 "${SRC_DIR}/kafka_consumer.py" \
    --config "${CONFIG}" \
    "$@"
