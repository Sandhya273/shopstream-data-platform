"""Warehouse connections for the DQ / reconciliation jobs (DuckDB locally, Snowflake in AWS)."""
from __future__ import annotations

import os


class _SnowflakeAdapter:
    """Gives snowflake-connector the small duckdb-like surface the jobs use."""

    def __init__(self, con):
        self.con = con

    def execute(self, sql, params=None):
        cur = self.con.cursor()
        cur.execute(sql, params)
        return cur

    def executemany(self, sql, rows):
        cur = self.con.cursor()
        cur.executemany(sql.replace("?", "%s"), rows)
        return cur

    def close(self):
        self.con.close()


def connect(target: str, db_path: str = "warehouse/shopstream.duckdb"):
    """Returns (connection, raw_schema_prefix)."""
    if target == "snowflake":  # pragma: no cover - needs a Snowflake account
        import snowflake.connector
        from cryptography.hazmat.primitives import serialization

        with open(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"], "rb") as fh:
            pkey = serialization.load_pem_private_key(fh.read(), password=os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "").encode() or None)
        con = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"], user=os.getenv("SNOWFLAKE_USER", "SVC_DBT"),
            private_key=pkey.private_bytes(serialization.Encoding.DER, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()),
            role="TRANSFORMER_ROLE", warehouse="TRANSFORM_WH", database="ANALYTICS_DB",
            session_parameters={"QUERY_TAG": "dq_framework"},
        )
        return _SnowflakeAdapter(con), "raw_db.raw"
    import duckdb

    return duckdb.connect(db_path), "raw"
