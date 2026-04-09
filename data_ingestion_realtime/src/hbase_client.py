"""
hbase_client.py
===============
Real-time Layer — HBase connection utility

Wraps happybase to provide a context-manager for HBase connections and
a helper to write a single transaction row.

HBase table layout
------------------
  Table name  : cc_fraud_realtime
  Row key     : trans_num  (unique per transaction)
  Column family: cf

  Columns (cf:<field>)
  --------------------
    cf:cc_num        cf:merchant      cf:category
    cf:amt           cf:first         cf:last
    cf:gender        cf:street        cf:city
    cf:state         cf:zip           cf:lat
    cf:long          cf:city_pop      cf:job
    cf:dob           cf:unix_time     cf:merch_lat
    cf:merch_long    cf:is_fraud      cf:Timestamp

Usage
-----
  from hbase_client import HBaseClient

  with HBaseClient(host="ip-172-31-3-85.eu-west-2.compute.internal") as client:
      client.put_transaction(row)
"""

import logging
import happybase

log = logging.getLogger(__name__)

HBASE_TABLE  = "cc_fraud_realtime"
COLUMN_FAMILY = "cf"


class HBaseClient:
    """Context-manager wrapper around a happybase connection."""

    def __init__(self, host: str = "ip-172-31-3-85.eu-west-2.compute.internal",
                 port: int = 9090,
                 table: str = HBASE_TABLE):
        self.host  = host
        self.port  = port
        self.table_name = table
        self._conn  = None
        self._table = None

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def connect(self):
        """Open the Thrift connection and bind the table handle."""
        log.info("Connecting to HBase Thrift at %s:%d", self.host, self.port)
        self._conn  = happybase.Connection(host=self.host, port=self.port, autoconnect=True)
        self._table = self._conn.table(self.table_name)
        log.info("Connected to HBase table '%s'", self.table_name)

    def close(self):
        """Close the Thrift connection."""
        if self._conn:
            self._conn.close()
            log.info("HBase connection closed.")

    # ------------------------------------------------------------------
    # Table management
    # ------------------------------------------------------------------

    def create_table_if_not_exists(self):
        """Create the HBase table with column family 'cf' if it does not exist."""
        existing = [t.decode() for t in self._conn.tables()]
        if self.table_name not in existing:
            log.info("Creating HBase table '%s' with column family '%s'",
                     self.table_name, COLUMN_FAMILY)
            self._conn.create_table(
                self.table_name,
                {COLUMN_FAMILY: dict(max_versions=1, compression="SNAPPY")}
            )
            log.info("Table '%s' created.", self.table_name)
        else:
            log.debug("Table '%s' already exists.", self.table_name)

    # ------------------------------------------------------------------
    # Write helpers
    # ------------------------------------------------------------------

    def put_transaction(self, row: dict):
        """Write a single transaction dict to HBase.

        Parameters
        ----------
        row : dict
            A transaction record as returned by the FastAPI /data endpoint.
            The ``trans_num`` field is used as the HBase row key.

        Raises
        ------
        ValueError
            If ``trans_num`` is missing from the row.
        """
        row_key = str(row.get("trans_num", "")).strip()
        if not row_key:
            raise ValueError("Row is missing required field 'trans_num'")

        data = {
            f"{COLUMN_FAMILY}:{col}".encode(): str(val).encode()
            for col, val in row.items()
            if col != "trans_num" and val is not None
        }

        self._table.put(row_key.encode(), data)

    def put_batch(self, rows: list):
        """Write multiple transaction rows using a happybase Batch for efficiency.

        Parameters
        ----------
        rows : list[dict]
            List of transaction records.
        """
        with self._table.batch(batch_size=len(rows)) as batch:
            for row in rows:
                row_key = str(row.get("trans_num", "")).strip()
                if not row_key:
                    log.warning("Skipping row without trans_num: %s", row)
                    continue
                data = {
                    f"{COLUMN_FAMILY}:{col}".encode(): str(val).encode()
                    for col, val in row.items()
                    if col != "trans_num" and val is not None
                }
                batch.put(row_key.encode(), data)

    # ------------------------------------------------------------------
    # Read helpers (for verification)
    # ------------------------------------------------------------------

    def get_transaction(self, trans_num: str) -> dict:
        """Fetch a single transaction by trans_num."""
        raw = self._table.row(trans_num.encode())
        return {
            k.decode().split(":")[1]: v.decode()
            for k, v in raw.items()
        }

    def scan(self, limit: int = 10) -> list:
        """Return up to ``limit`` rows from the table (for smoke testing)."""
        results = []
        for key, data in self._table.scan(limit=limit):
            record = {"trans_num": key.decode()}
            record.update({
                k.decode().split(":")[1]: v.decode()
                for k, v in data.items()
            })
            results.append(record)
        return results
