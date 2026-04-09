"""
conftest.py
===========
pytest fixtures for real-time layer unit tests.

Uses unittest.mock to avoid requiring live Kafka or HBase connections.
"""

import pytest
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Sample transaction rows (mirrors what the FastAPI /data endpoint returns)
# ---------------------------------------------------------------------------

SAMPLE_ROWS = [
    {
        "trans_num":   "t001",
        "Timestamp":   "2023-06-15 02:30:00",
        "cc_num":      "4111111111111111",
        "merchant":    "Amazon",
        "category":    "shopping_net",
        "amt":         250.0,
        "first":       "Alice",
        "last":        "Smith",
        "gender":      "F",
        "street":      "1 Main St",
        "city":        "London",
        "state":       "ENG",
        "zip":         "SW1A",
        "lat":         51.509,
        "long":        -0.118,
        "city_pop":    8900000,
        "job":         "Engineer",
        "dob":         "1985-03-22",
        "unix_time":   1686793800,
        "merch_lat":   51.600,
        "merch_long":  -0.200,
        "is_fraud":    0,
    },
    {
        "trans_num":   "t002",
        "Timestamp":   "2023-06-15 14:00:00",
        "cc_num":      "4111111111111111",
        "merchant":    "Tesco",
        "category":    "grocery_pos",
        "amt":         15.5,
        "first":       "Alice",
        "last":        "Smith",
        "gender":      "F",
        "street":      "1 Main St",
        "city":        "London",
        "state":       "ENG",
        "zip":         "SW1A",
        "lat":         51.509,
        "long":        -0.118,
        "city_pop":    8900000,
        "job":         "Engineer",
        "dob":         "1985-03-22",
        "unix_time":   1686837600,
        "merch_lat":   51.510,
        "merch_long":  -0.120,
        "is_fraud":    0,
    },
    {
        "trans_num":   "t003",
        "Timestamp":   "2023-06-15 03:00:00",
        "cc_num":      "5555555555554444",
        "merchant":    "Casino",
        "category":    "entertainment",
        "amt":         800.0,
        "first":       "Bob",
        "last":        "Jones",
        "gender":      "M",
        "street":      "5 Broadway",
        "city":        "New York",
        "state":       "NY",
        "zip":         "10001",
        "lat":         40.712,
        "long":        -74.006,
        "city_pop":    8336817,
        "job":         "Manager",
        "dob":         "1990-07-04",
        "unix_time":   1686795600,
        "merch_lat":   40.730,
        "merch_long":  -73.990,
        "is_fraud":    1,
    },
]


@pytest.fixture
def sample_rows():
    """Return a list of sample transaction dicts."""
    return SAMPLE_ROWS.copy()


@pytest.fixture
def mock_kafka_producer():
    """Return a MagicMock KafkaProducer."""
    with patch("kafka.KafkaProducer") as mock_cls:
        producer = MagicMock()
        mock_cls.return_value = producer
        # .send() returns a future-like mock
        future = MagicMock()
        future.get.return_value = MagicMock()
        producer.send.return_value = future
        yield producer


@pytest.fixture
def mock_kafka_consumer(sample_rows):
    """Return a MagicMock KafkaConsumer that yields sample_rows as messages."""
    messages = []
    for row in sample_rows:
        msg = MagicMock()
        msg.value     = row
        msg.key       = row["trans_num"]
        msg.partition = 0
        msg.offset    = sample_rows.index(row)
        messages.append(msg)

    with patch("kafka.KafkaConsumer") as mock_cls:
        consumer = MagicMock()
        consumer.__iter__ = MagicMock(return_value=iter(messages))
        mock_cls.return_value = consumer
        yield consumer


@pytest.fixture
def mock_hbase(sample_rows):
    """Return a MagicMock HBaseClient."""
    with patch("hbase_client.HBaseClient") as mock_cls:
        client = MagicMock()
        mock_cls.return_value.__enter__ = MagicMock(return_value=client)
        mock_cls.return_value.__exit__  = MagicMock(return_value=False)
        client.scan.return_value = sample_rows
        yield client
