"""
Snowflake batch loader (Lambda, invoked by Step Functions after extraction).

Runs COPY INTO for each DMS table + payments, using key-pair auth from Secrets
Manager, and returns per-file results so the state machine can fail on errors.
COPY's load metadata makes this safe to retry: already-loaded files are skipped.
"""
from __future__ import annotations

import json
import os

COPY_TEMPLATES = {
    "dms_orders":      ("@raw_db.raw.stg_dms/orders/", 11),
    "dms_order_items": ("@raw_db.raw.stg_dms/order_items/", 9),
    "dms_customers":   ("@raw_db.raw.stg_dms/customers/", 13),
    "dms_products":    ("@raw_db.raw.stg_dms/products/", 12),
}


def build_copy_sql(table: str, stage: str, n_cols: int) -> str:
    cols = ", ".join(f"${i}" for i in range(1, n_cols + 1))
    return (
        f"copy into raw_db.raw.{table} from (select {cols}, metadata$filename, metadata$file_row_number "
        f"from {stage}) pattern = '.*[.]csv' on_error = 'ABORT_STATEMENT'"
    )


PAYMENTS_COPY = (
    "copy into raw_db.raw.payments (raw, _source_file, _file_row_number) "
    "from (select parse_json($1:raw::varchar), metadata$filename, metadata$file_row_number from @raw_db.raw.stg_payments) "
    "pattern = '.*part-.*[.]parquet' on_error = 'ABORT_STATEMENT'"
)


def _connect():  # pragma: no cover - needs AWS + Snowflake
    import boto3
    import snowflake.connector
    from cryptography.hazmat.primitives import serialization

    secret = json.loads(boto3.client("secretsmanager").get_secret_value(SecretId=os.environ["SNOWFLAKE_SECRET"])["SecretString"])
    pkey = serialization.load_pem_private_key(secret["private_key"].encode(), password=(secret.get("passphrase") or "").encode() or None)
    der = pkey.private_bytes(serialization.Encoding.DER, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    return snowflake.connector.connect(
        account=secret["account"], user="SVC_LOADER", private_key=der,
        role="LOADER_ROLE", warehouse="LOAD_WH", database="RAW_DB", schema="RAW",
        session_parameters={"QUERY_TAG": "step_functions_loader"},
    )


def lambda_handler(event, context):  # pragma: no cover
    statements = {t: build_copy_sql(t, s, n) for t, (s, n) in COPY_TEMPLATES.items()}
    statements["payments"] = PAYMENTS_COPY
    results, errors = {}, 0
    with _connect() as con, con.cursor() as cur:
        for table, sql in statements.items():
            cur.execute(sql)
            rows = cur.fetchall()
            # COPY returns one row per file: file, status, rows_parsed, rows_loaded, ..., errors_seen, first_error
            files = [{"file": r[0], "status": r[1], "rows_loaded": r[3] if len(r) > 3 else 0} for r in rows if r[0] != "Copy executed with 0 files processed."]
            errors += sum(1 for f in files if f["status"] not in ("LOADED", "PARTIALLY_LOADED"))
            results[table] = {"files": len(files), "rows": sum(f["rows_loaded"] or 0 for f in files)}
    if errors:
        raise RuntimeError(f"COPY INTO reported {errors} failed files: {json.dumps(results)}")
    return {"status": "LOADED", "tables": results}
