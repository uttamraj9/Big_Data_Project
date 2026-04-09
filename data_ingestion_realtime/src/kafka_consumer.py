"""
kafka_consumer.py
=================
Real-time Layer — Kafka Consumer → HBase Writer

Consumes JSON transaction messages from the Kafka topic ``cc_fraud_stream``
and writes each message to the HBase table ``cc_fraud_realtime``.

Flow
----
  Kafka topic: cc_fraud_stream
       |
       |  consume JSON messages
       v
  kafka_consumer.py
       |
       |  happybase Thrift client  (batched writes)
       v
  HBase table: cc_fraud_realtime  (row key = trans_num)

Usage
-----
  # Using config file (recommended)
  python3 kafka_consumer.py --config ../config/config.properties

  # Using flags
  python3 kafka_consumer.py \
      --brokers   172.31.3.85:9092 \
      --topic     cc_fraud_stream \
      --group-id  cc_fraud_consumer_group \
      --hbase-host ip-172-31-3-85.eu-west-2.compute.internal \
      --batch-size 50

Environment Variables (override config file)
---------------------------------------------
  KAFKA_BROKERS   Comma-separated broker list
  KAFKA_TOPIC     Source topic
  KAFKA_GROUP_ID  Consumer group ID
  HBASE_HOST      HBase Thrift server hostname
  HBASE_PORT      HBase Thrift server port  (default: 9090)
  CONSUMER_BATCH  Messages to buffer before flushing to HBase (default: 50)
"""

import argparse
import json
import logging
import os
import signal
import sys

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from hbase_client import HBaseClient

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Graceful shutdown flag
_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    log.info("Shutdown signal received (%s). Draining buffer…", signum)
    _shutdown = True


signal.signal(signal.SIGINT,  _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def load_config(path: str) -> dict:
    cfg = {}
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, val = line.partition("=")
                cfg[key.strip()] = val.strip()
    return cfg


def build_args(argv=None):
    p = argparse.ArgumentParser(description="Kafka consumer → HBase writer")
    p.add_argument("--config",     default=None, help="Path to config.properties")
    p.add_argument("--brokers",    default=None, help="Kafka broker list")
    p.add_argument("--topic",      default=None, help="Kafka topic")
    p.add_argument("--group-id",   default=None, help="Consumer group ID")
    p.add_argument("--hbase-host", default=None, help="HBase Thrift host")
    p.add_argument("--hbase-port", type=int, default=None, help="HBase Thrift port")
    p.add_argument("--batch-size", type=int, default=None, help="HBase batch write size")
    return p.parse_args(argv)


def resolve_config(args) -> dict:
    cfg = {}

    if args.config:
        cfg.update(load_config(args.config))

    if os.getenv("KAFKA_BROKERS"):   cfg["kafka.brokers"]    = os.getenv("KAFKA_BROKERS")
    if os.getenv("KAFKA_TOPIC"):     cfg["kafka.topic"]      = os.getenv("KAFKA_TOPIC")
    if os.getenv("KAFKA_GROUP_ID"):  cfg["kafka.group.id"]   = os.getenv("KAFKA_GROUP_ID")
    if os.getenv("HBASE_HOST"):      cfg["hbase.host"]       = os.getenv("HBASE_HOST")
    if os.getenv("HBASE_PORT"):      cfg["hbase.port"]       = os.getenv("HBASE_PORT")
    if os.getenv("CONSUMER_BATCH"):  cfg["consumer.batch"]   = os.getenv("CONSUMER_BATCH")

    if args.brokers:    cfg["kafka.brokers"]  = args.brokers
    if args.topic:      cfg["kafka.topic"]    = args.topic
    if args.group_id:   cfg["kafka.group.id"] = args.group_id
    if args.hbase_host: cfg["hbase.host"]     = args.hbase_host
    if args.hbase_port: cfg["hbase.port"]     = str(args.hbase_port)
    if args.batch_size: cfg["consumer.batch"] = str(args.batch_size)

    cfg.setdefault("kafka.brokers",  "172.31.3.85:9092")
    cfg.setdefault("kafka.topic",    "cc_fraud_stream")
    cfg.setdefault("kafka.group.id", "cc_fraud_consumer_group")
    cfg.setdefault("hbase.host",     "ip-172-31-3-85.eu-west-2.compute.internal")
    cfg.setdefault("hbase.port",     "9090")
    cfg.setdefault("consumer.batch", "50")

    return cfg


# ---------------------------------------------------------------------------
# Consumer
# ---------------------------------------------------------------------------

def create_consumer(brokers: str, topic: str, group_id: str) -> KafkaConsumer:
    """Create a KafkaConsumer that deserialises values from UTF-8 JSON."""
    return KafkaConsumer(
        topic,
        bootstrap_servers=brokers.split(","),
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        consumer_timeout_ms=10000,   # 10 s idle timeout before loop re-checks _shutdown
    )


def run_consumer(cfg: dict):
    """Main consumer loop — reads from Kafka and writes batches to HBase."""
    brokers     = cfg["kafka.brokers"]
    topic       = cfg["kafka.topic"]
    group_id    = cfg["kafka.group.id"]
    hbase_host  = cfg["hbase.host"]
    hbase_port  = int(cfg["hbase.port"])
    batch_size  = int(cfg["consumer.batch"])

    log.info("Consumer starting")
    log.info("  Kafka brokers: %s", brokers)
    log.info("  Topic        : %s", topic)
    log.info("  Group ID     : %s", group_id)
    log.info("  HBase host   : %s:%d", hbase_host, hbase_port)
    log.info("  Batch size   : %d", batch_size)

    with HBaseClient(host=hbase_host, port=hbase_port) as hbase:
        hbase.create_table_if_not_exists()

        consumer = create_consumer(brokers, topic, group_id)
        buffer   = []
        total    = 0

        try:
            while not _shutdown:
                for message in consumer:
                    if _shutdown:
                        break

                    row = message.value
                    buffer.append(row)

                    if len(buffer) >= batch_size:
                        hbase.put_batch(buffer)
                        total += len(buffer)
                        log.info("Flushed %d rows to HBase | partition=%d offset=%d | total=%d",
                                 len(buffer), message.partition, message.offset, total)
                        buffer = []

            # Flush remaining
            if buffer:
                hbase.put_batch(buffer)
                total += len(buffer)
                log.info("Final flush: %d rows | total=%d", len(buffer), total)

        except KafkaError as exc:
            log.error("Kafka error: %s", exc)
            sys.exit(1)
        finally:
            consumer.close()
            log.info("Consumer closed. Total rows written to HBase: %d", total)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    args = build_args()
    cfg  = resolve_config(args)
    run_consumer(cfg)
