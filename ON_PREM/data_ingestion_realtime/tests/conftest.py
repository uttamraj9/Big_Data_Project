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
        "Transaction_ID":              "TXN_001",
        "User_ID":                     "USER_1001",
        "Transaction_Amount":          250.0,
        "Transaction_Type":            "Online",
        "Timestamp":                   "2023-06-15 02:30:00",
        "Account_Balance":             15000.0,
        "Device_Type":                 "Mobile",
        "Location":                    "London",
        "Merchant_Category":           "Electronics",
        "IP_Address_Flag":             0,
        "Previous_Fraudulent_Activity": 0,
        "Daily_Transaction_Count":     3,
        "Avg_Transaction_Amount_7d":   120.0,
        "Failed_Transaction_Count_7d": 0,
        "Card_Type":                   "Visa",
        "Card_Age":                    48,
        "Transaction_Distance":        12.5,
        "Authentication_Method":       "OTP",
        "Risk_Score":                  0.15,
        "Is_Weekend":                  0,
        "Fraud_Label":                 0,
    },
    {
        "Transaction_ID":              "TXN_002",
        "User_ID":                     "USER_1001",
        "Transaction_Amount":          15.5,
        "Transaction_Type":            "POS",
        "Timestamp":                   "2023-06-15 14:00:00",
        "Account_Balance":             14750.0,
        "Device_Type":                 "Laptop",
        "Location":                    "London",
        "Merchant_Category":           "Groceries",
        "IP_Address_Flag":             0,
        "Previous_Fraudulent_Activity": 0,
        "Daily_Transaction_Count":     4,
        "Avg_Transaction_Amount_7d":   118.0,
        "Failed_Transaction_Count_7d": 0,
        "Card_Type":                   "Visa",
        "Card_Age":                    48,
        "Transaction_Distance":        1.2,
        "Authentication_Method":       "PIN",
        "Risk_Score":                  0.05,
        "Is_Weekend":                  0,
        "Fraud_Label":                 0,
    },
    {
        "Transaction_ID":              "TXN_003",
        "User_ID":                     "USER_2002",
        "Transaction_Amount":          800.0,
        "Transaction_Type":            "Online",
        "Timestamp":                   "2023-06-15 03:00:00",
        "Account_Balance":             2000.0,
        "Device_Type":                 "Mobile",
        "Location":                    "New York",
        "Merchant_Category":           "Travel",
        "IP_Address_Flag":             1,
        "Previous_Fraudulent_Activity": 1,
        "Daily_Transaction_Count":     12,
        "Avg_Transaction_Amount_7d":   450.0,
        "Failed_Transaction_Count_7d": 3,
        "Card_Type":                   "Mastercard",
        "Card_Age":                    12,
        "Transaction_Distance":        4500.0,
        "Authentication_Method":       "Password",
        "Risk_Score":                  0.92,
        "Is_Weekend":                  0,
        "Fraud_Label":                 1,
    },
]


@pytest.fixture
def sample_rows():
    """Return a list of sample transaction dicts."""
    return SAMPLE_ROWS.copy()


@pytest.fixture
def mock_kafka_producer():
    """Return a MagicMock KafkaProducer."""
    with patch("kafka_producer.KafkaProducer") as mock_cls:
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
        msg.key       = row["Transaction_ID"]
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
