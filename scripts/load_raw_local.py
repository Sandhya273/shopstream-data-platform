"""
Local stand-in for Snowflake COPY INTO / Snowpipe.

Loads files from the local lake (same layout as the S3 raw bucket) into a DuckDB
database whose RAW schema mirrors snowflake/migrations/V1.0.4__raw_tables.sql:

  raw.dms_customers / dms_products / dms_orders / dms_order_items   (CDC rows, all VARCHAR)
  raw.payments            (raw VARIANT-like JSON string)
  raw.clickstream_events  (raw VARIANT-like JSON string)
  raw.load_history        (one row per file, like COPY_HISTORY / Snowpipe load metadata)

Like Snowflake's 64-day load metadata, a file that is already in load_history is
skipped, so re-running the loader is idempotent.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

DMS_TABLES = ["customers", "products", "orders", "order_items"]


def init(con):
    con.execute("create schema if not exists raw")
    con.execute("""
        create table if not exists raw.load_history (
            file_name varchar primary key, target_table varchar, row_count bigint,
            status varchar, loaded_at timestamp)""")
    con.execute("""create table if not exists raw.payments (
            raw varchar, _source_file varchar, _file_row_number bigint, _loaded_at timestamp)""")
    con.execute("""create table if not exists raw.clickstream_events (
            raw varchar, _source_file varchar, _file_row_number bigint, _loaded_at timestamp)""")


def already_loaded(con, f: str) -> bool:
    return con.execute("select 1 from raw.load_history where file_name = ?", [f]).fetchone() is not None


def record(con, f, table, n):
    con.execute("insert into raw.load_history values (?,?,?,?,?)", [f, table, n, "LOADED", datetime.now(timezone.utc).replace(tzinfo=None)])


def load_dms(con, root: Path) -> dict:
    out = {}
    for t in DMS_TABLES:
        files = sorted((root / "dms" / t).rglob("*.csv"))
        loaded = 0
        for f in files:
            rel = f.relative_to(root).as_posix()
            if already_loaded(con, rel):
                continue
            # all_varchar=true mirrors landing CDC into string columns (typed later in dbt staging)
            rel_sql = f"read_csv('{f}', header=true, all_varchar=true)"
            exists = con.execute("select count(*) from information_schema.tables where table_schema='raw' and table_name=?", [f"dms_{t}"]).fetchone()[0]
            if not exists:
                con.execute(f"create table raw.dms_{t} as select *, ''::varchar as _source_file, 0::bigint as _file_row_number, now()::timestamp as _loaded_at from {rel_sql} limit 0")
            n = con.execute(f"""insert into raw.dms_{t}
                select *, '{rel}', row_number() over (), now()::timestamp from {rel_sql}""").fetchone()[0]
            record(con, rel, f"raw.dms_{t}", n)
            loaded += 1
        out[t] = loaded
    return out


def load_json_lines(con, root: Path, prefix: str, table: str, pattern: str) -> int:
    n_files = 0
    for f in sorted((root / prefix).rglob(pattern)):
        rel = f.relative_to(root).as_posix()
        if already_loaded(con, rel):
            continue
        # one JSON document per line -> one VARIANT-like row (like COPY INTO ... FILE_FORMAT=(TYPE=JSON))
        n = con.execute(f"""insert into raw.{table}
            select json::varchar, '{rel}', row_number() over (), now()::timestamp
            from read_ndjson_objects('{f}', compression='gzip')""").fetchone()[0]
        record(con, rel, f"raw.{table}", n)
        n_files += 1
    return n_files


def load_payments(con, root: Path) -> int:
    n_files = 0
    for f in sorted((root / "raw" / "payments").rglob("*.parquet")):
        rel = f.relative_to(root).as_posix()
        if already_loaded(con, rel):
            continue
        n = con.execute(f"""insert into raw.payments
            select raw, '{rel}', row_number() over (), now()::timestamp from read_parquet('{f}')""").fetchone()[0]
        record(con, rel, "raw.payments", n)
        n_files += 1
    return n_files


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lake-root", default="landing")
    ap.add_argument("--db", default="warehouse/shopstream.duckdb")
    a = ap.parse_args()
    root = Path(a.lake_root)
    Path(a.db).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(a.db)
    init(con)
    summary = {
        "dms_files": load_dms(con, root),
        "payment_files": load_payments(con, root),
        "clickstream_files": load_json_lines(con, root, "clickstream_validated", "clickstream_events", "*.json.gz"),
    }
    summary["raw_row_counts"] = {
        t: con.execute(f"select count(*) from raw.{t}").fetchone()[0]
        for (t,) in con.execute("select table_name from information_schema.tables where table_schema='raw' order by 1").fetchall()
    }
    con.close()
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    sys.exit(main())