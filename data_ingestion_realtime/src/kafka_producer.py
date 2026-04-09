"""
kafka_producer.py
=================
Real-time Layer — Kafka Producer

Polls the FastAPI streaming endpoint (GET /data) in a continuous loop,
converts each transaction row to a JSON Kafka message, and publishes it
to the configured Kafka topic.

Flow
----
  FastAPI /data  (PostgreSQL source)
       |
       |  HTTP GET  offset / limit pagination
       v
  kafka_producer.py
       |
       |  produce JSON messages
       v
  Kafka topic: cc_fraud_stream

Usage
-----
  # Using config file (recommended)
  python3 kafka_producer.py --config ../config/config.properties

  # Using env vars / flags
  python3 kafka_producer.py \
      --api-url  http://13.42.152.118:5310 \
      --topic    cc_fraud_stream \
      --brokers  172.31.3.85:9092 \
      --batch    100 \
      --interval 5

Environment Variables (override config file)
---------------------------------------------
  API_URL          FastAPI base URL  (default: http://13.42.152.118:5310)
  KAFKA_BROKERS    Comma-separated broker list
  KAFKA_TOPIC      Target topic name
  BATCH_SIZE       Records per API call  (default: 100)
  POLL_INTERVAL    Seconds between polls (default: 5)
"""

import argparse
import json
import logging
import os
import time

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def load_config(path: str) -> dict:
    """Parse a Java-style .properties file into a dict."""
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
    p = argparse.ArgumentParser(description="Kafka producer — streams FastAPI data to Kafka")
    p.add_argument("--config",   default=None,                       help="Path to config.properties")
    p.add_argument("--api-url",  default=None,                       help="FastAPI base URL")
    p.add_argument("--brokers",  default=None,                       help="Kafka broker list (comma-separated)")
    p.add_argument("--topic",    default=None,                       help="Kafka topic name")
    p.add_argument("--batch",    type=int, default=None,             help="Records per API call")
    p.add_argument("--interval", type=float, default=None,           help="Seconds between polls")
    return p.parse_args(argv)


def resolve_config(args) -> dict:
    """Merge config file → env vars → CLI flags (later wins)."""
    cfg = {}

    if args.config:
        cfg.update(load_config(args.config))

    # env vars
    if os.getenv("API_URL"):       cfg["api.url"]       = os.getenv("API_URL")
    if os.getenv("KAFKA_BROKERS"): cfg["kafka.brokers"] = os.getenv("KAFKA_BROKERS")
    if os.getenv("KAFKA_TOPIC"):   cfg["kafka.topic"]   = os.getenv("KAFKA_TOPIC")
    if os.getenv("BATCH_SIZE"):    cfg["batch.size"]    = os.getenv("BATCH_SIZE")
    if os.getenv("POLL_INTERVAL"): cfg["poll.interval"] = os.getenv("POLL_INTERVAL")

    # CLI flags
    if args.api_url:  cfg["api.url"]       = args.api_url
    if args.brokers:  cfg["kafka.brokers"] = args.brokers
    if args.topic:    cfg["kafka.topic"]   = args.topic
    if args.batch:    cfg["batch.size"]    = str(args.batch)
    if args.interval: cfg["poll.interval"] = str(args.interval)

    # defaults
    cfg.setdefault("api.url",       "http://13.42.152.118:5310")
    cfg.setdefault("kafka.brokers", "172.31.3.85:9092")
    cfg.setdefault("kafka.topic",   "cc_fraud_stream")
    cfg.setdefault("batch.size",    "100")
    cfg.setdefault("poll.interval", "5")

    return cfg


# ---------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------

def create_producer(brokers: str) -> KafkaProducer:
    """Create a KafkaProducer that serialises values as UTF-8 JSON."""
    return KafkaProducer(
        bootstrap_servers=brokers.split(","),
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        linger_ms=5,
    )


def fetch_batch(api_url: str, offset: int, limit: int) -> list:
    """Call GET /data with pagination and return a list of row dicts."""
    url = f"{api_url}/data"
    resp = requests.get(url, params={"offset": offset, "limit": limit}, timeout=10)
    resp.raise_for_status()
    return resp.json()


def run_producer(cfg: dict):
    """Main producer loop — polls API and publishes messages until interrupted."""
    api_url       = cfg["api.url"].rstrip("/")
    brokers       = cfg["kafka.brokers"]
    topic         = cfg["kafka.topic"]
    batch_size    = int(cfg["batch.size"])
    poll_interval = float(cfg["poll.interval"])

    log.info("Producer starting")
    log.info("  API URL      : %s", api_url)
    log.info("  Kafka brokers: %s", brokers)
    log.info("  Topic        : %s", topic)
    log.info("  Batch size   : %d", batch_size)
    log.info("  Poll interval: %.1f s", poll_interval)

    producer = create_producer(brokers)

    offset          = 0
    total_produced  = 0

    try:
        while True:
            try:
                rows = fetch_batch(api_url, offset, batch_size)
            except requests.RequestException as exc:
                log.warning("API call failed (offset=%d): %s — retrying in %ds",
                            offset, exc, poll_interval)
                time.sleep(poll_interval)
                continue

            if not rows:
                log.info("No new rows at offset=%d. Waiting %ds before next poll.",
                         offset, poll_interval)
                # Reset to 0 to re-stream from the beginning (circular simulation)
                offset = 0
                time.sleep(poll_interval)
                continue

            for row in rows:
                # Use Transaction_ID as the Kafka message key for partition locality
                key = str(row.get("Transaction_ID", row.get("trans_num", "")))
                future = producer.send(topic, key=key, value=row)
                try:
                    future.get(timeout=10)
                except KafkaError as exc:
                    log.error("Failed to send message (key=%s): %s", key, exc)

            total_produced += len(rows)
            offset         += len(rows)
            log.info("Produced %d messages | offset=%d | total=%d",
                     len(rows), offset, total_produced)

            time.sleep(poll_interval)

    except KeyboardInterrupt:
        log.info("Producer interrupted by user. Total messages produced: %d", total_produced)
    finally:
        producer.flush()
        producer.close()
        log.info("Producer closed.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    args = build_args()
    cfg  = resolve_config(args)
    run_producer(cfg)
