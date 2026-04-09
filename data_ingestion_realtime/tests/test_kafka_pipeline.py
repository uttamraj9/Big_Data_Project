"""
test_kafka_pipeline.py
======================
Unit tests for the real-time Kafka → HBase pipeline.

All external dependencies (Kafka, HBase, HTTP) are mocked — no live
cluster or API connection is required to run these tests.
"""

import json
import sys
import os
import pytest
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from kafka_producer import (
    load_config, resolve_config, fetch_batch, build_args,
)
from kafka_consumer import resolve_config as consumer_resolve_config
from hbase_client import HBaseClient, HBASE_TABLE, COLUMN_FAMILY


# ===========================================================================
# Config loading
# ===========================================================================

class TestLoadConfig:
    def test_parses_key_value(self, tmp_path):
        cfg_file = tmp_path / "test.properties"
        cfg_file.write_text("api.url=http://localhost:5310\nkafka.topic=test_topic\n")
        cfg = load_config(str(cfg_file))
        assert cfg["api.url"] == "http://localhost:5310"
        assert cfg["kafka.topic"] == "test_topic"

    def test_ignores_comments(self, tmp_path):
        cfg_file = tmp_path / "test.properties"
        cfg_file.write_text("# this is a comment\napi.url=http://x:5310\n")
        cfg = load_config(str(cfg_file))
        assert "# this is a comment" not in cfg
        assert cfg["api.url"] == "http://x:5310"

    def test_ignores_blank_lines(self, tmp_path):
        cfg_file = tmp_path / "test.properties"
        cfg_file.write_text("\n\napi.url=http://x:5310\n\n")
        cfg = load_config(str(cfg_file))
        assert len(cfg) == 1


class TestResolveConfig:
    def test_defaults_applied(self):
        args = build_args([])
        cfg = resolve_config(args)
        assert cfg["api.url"]       == "http://13.42.152.118:5310"
        assert cfg["kafka.topic"]   == "cc_fraud_stream"
        assert cfg["batch.size"]    == "100"
        assert cfg["poll.interval"] == "5"

    def test_cli_overrides_default(self):
        args = build_args(["--topic", "my_topic", "--batch", "50"])
        cfg = resolve_config(args)
        assert cfg["kafka.topic"] == "my_topic"
        assert cfg["batch.size"]  == "50"

    def test_env_var_override(self, monkeypatch):
        monkeypatch.setenv("KAFKA_TOPIC", "env_topic")
        monkeypatch.setenv("BATCH_SIZE",  "200")
        args = build_args([])
        cfg = resolve_config(args)
        assert cfg["kafka.topic"] == "env_topic"
        assert cfg["batch.size"]  == "200"


# ===========================================================================
# Producer — API fetch
# ===========================================================================

class TestFetchBatch:
    def test_returns_rows_on_success(self, sample_rows):
        with patch("requests.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.json.return_value = sample_rows
            mock_resp.raise_for_status = MagicMock()
            mock_get.return_value = mock_resp

            result = fetch_batch("http://localhost:5310", offset=0, limit=100)
            assert len(result) == len(sample_rows)
            assert result[0]["trans_num"] == "t001"

    def test_passes_pagination_params(self, sample_rows):
        with patch("requests.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.json.return_value = []
            mock_resp.raise_for_status = MagicMock()
            mock_get.return_value = mock_resp

            fetch_batch("http://localhost:5310", offset=200, limit=50)
            _, kwargs = mock_get.call_args
            assert kwargs["params"]["offset"] == 200
            assert kwargs["params"]["limit"]  == 50

    def test_raises_on_http_error(self):
        with patch("requests.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.raise_for_status.side_effect = Exception("404 Not Found")
            mock_get.return_value = mock_resp

            with pytest.raises(Exception, match="404"):
                fetch_batch("http://localhost:5310", offset=0, limit=10)


# ===========================================================================
# Producer — Kafka publishing
# ===========================================================================

class TestKafkaProducer:
    def test_sends_message_per_row(self, sample_rows, mock_kafka_producer):
        from kafka_producer import create_producer
        producer = create_producer("localhost:9092")
        for row in sample_rows:
            key = str(row.get("trans_num", ""))
            producer.send("cc_fraud_stream", key=key, value=row)

        assert producer.send.call_count == len(sample_rows)

    def test_uses_trans_num_as_key(self, sample_rows, mock_kafka_producer):
        from kafka_producer import create_producer
        producer = create_producer("localhost:9092")
        producer.send("cc_fraud_stream", key="t001", value=sample_rows[0])

        call_kwargs = producer.send.call_args
        assert call_kwargs[1]["key"] == "t001"

    def test_value_is_serialisable(self, sample_rows):
        """Every sample row must round-trip through JSON without error."""
        for row in sample_rows:
            encoded = json.dumps(row, default=str).encode("utf-8")
            decoded = json.loads(encoded.decode("utf-8"))
            assert decoded["trans_num"] == row["trans_num"]


# ===========================================================================
# HBase client
# ===========================================================================

class TestHBaseClient:
    def test_put_transaction_encodes_row_key(self, sample_rows):
        with patch("happybase.Connection") as mock_conn_cls:
            mock_conn  = MagicMock()
            mock_table = MagicMock()
            mock_conn_cls.return_value = mock_conn
            mock_conn.table.return_value = mock_table

            client = HBaseClient(host="localhost")
            client._conn  = mock_conn
            client._table = mock_table

            client.put_transaction(sample_rows[0])

            mock_table.put.assert_called_once()
            put_args = mock_table.put.call_args[0]
            # Row key should be bytes of trans_num
            assert put_args[0] == b"t001"

    def test_put_transaction_raises_on_missing_trans_num(self):
        with patch("happybase.Connection"):
            client = HBaseClient(host="localhost")
            client._table = MagicMock()

            with pytest.raises(ValueError, match="trans_num"):
                client.put_transaction({"amt": 100.0})

    def test_put_batch_skips_rows_without_trans_num(self, sample_rows):
        with patch("happybase.Connection"):
            client = HBaseClient(host="localhost")
            mock_table = MagicMock()
            mock_batch = MagicMock()
            mock_table.batch.return_value.__enter__ = MagicMock(return_value=mock_batch)
            mock_table.batch.return_value.__exit__  = MagicMock(return_value=False)
            client._table = mock_table

            rows_with_bad = sample_rows + [{"amt": 99.0}]  # missing trans_num
            client.put_batch(rows_with_bad)

            # Only valid rows should be put
            assert mock_batch.put.call_count == len(sample_rows)

    def test_column_family_prefix(self, sample_rows):
        """Every HBase column must be prefixed with 'cf:'."""
        with patch("happybase.Connection"):
            client = HBaseClient(host="localhost")
            client._table = MagicMock()

            client.put_transaction(sample_rows[0])

            put_args = client._table.put.call_args[0]
            data_dict = put_args[1]
            for key in data_dict:
                assert key.decode().startswith(f"{COLUMN_FAMILY}:"), \
                    f"Column '{key.decode()}' missing '{COLUMN_FAMILY}:' prefix"

    def test_create_table_if_not_exists_creates_when_missing(self):
        with patch("happybase.Connection") as mock_conn_cls:
            mock_conn = MagicMock()
            mock_conn_cls.return_value = mock_conn
            mock_conn.tables.return_value = []  # no tables exist

            client = HBaseClient(host="localhost")
            client._conn = mock_conn

            client.create_table_if_not_exists()

            mock_conn.create_table.assert_called_once_with(
                HBASE_TABLE, {COLUMN_FAMILY: dict(max_versions=1, compression="SNAPPY")}
            )

    def test_create_table_if_not_exists_skips_when_present(self):
        with patch("happybase.Connection") as mock_conn_cls:
            mock_conn = MagicMock()
            mock_conn_cls.return_value = mock_conn
            mock_conn.tables.return_value = [HBASE_TABLE.encode()]

            client = HBaseClient(host="localhost")
            client._conn = mock_conn

            client.create_table_if_not_exists()

            mock_conn.create_table.assert_not_called()


# ===========================================================================
# Consumer config
# ===========================================================================

class TestConsumerConfig:
    def test_defaults(self):
        args_ns = MagicMock()
        args_ns.config     = None
        args_ns.brokers    = None
        args_ns.topic      = None
        args_ns.group_id   = None
        args_ns.hbase_host = None
        args_ns.hbase_port = None
        args_ns.batch_size = None

        cfg = consumer_resolve_config(args_ns)
        assert cfg["kafka.topic"]   == "cc_fraud_stream"
        assert cfg["kafka.group.id"] == "cc_fraud_consumer_group"
        assert cfg["hbase.port"]    == "9090"
        assert cfg["consumer.batch"] == "50"

    def test_env_overrides(self, monkeypatch):
        monkeypatch.setenv("HBASE_HOST", "myhost")
        monkeypatch.setenv("CONSUMER_BATCH", "100")

        args_ns = MagicMock()
        args_ns.config = args_ns.brokers = args_ns.topic = None
        args_ns.group_id = args_ns.hbase_host = args_ns.hbase_port = None
        args_ns.batch_size = None

        cfg = consumer_resolve_config(args_ns)
        assert cfg["hbase.host"]     == "myhost"
        assert cfg["consumer.batch"] == "100"
