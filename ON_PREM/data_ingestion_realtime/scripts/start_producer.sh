#!/bin/bash
# =============================================================================
# start_producer.sh
# =============================================================================
# Starts the Kafka producer that polls the FastAPI /data endpoint and
# publishes each transaction record as a JSON message to cc_fraud_stream.
#
# Run from the edge node (13.41.167.97) or any node with Python 3 + pip.
#
# Usage
# -----
#   bash start_producer.sh                    # uses config/config.properties
#   bash start_producer.sh --interval 10      # override poll interval
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="${SCRIPT_DIR}/../src"
CONFIG="${SCRIPT_DIR}/../config/config.properties"

echo "============================================================"
echo " Starting Kafka Producer"
echo " Config : ${CONFIG}"
echo " Source : ${SRC_DIR}/kafka_producer.py"
echo "============================================================"
echo ""

python3.12 "${SRC_DIR}/kafka_producer.py" \
    --config "${CONFIG}" \
    "$@"
