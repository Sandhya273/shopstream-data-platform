"""
End-to-end reconciliation: proves no data was lost or duplicated between layers.

  SOURCE (files in the lake) -> RAW (warehouse load) -> STAGING (dedup) -> MARTS

Checks
  1. file completeness   : every landed file appears in raw.load_history
  2. row completeness    : rows per file in lake == rows loaded for that file
  3. clickstream         : Lambda audit 'valid' counts == raw rows
  4. payments            : extractor manifest record counts == raw.payments rows
  5. CDC -> mart         : distinct live orders in CDC == fct_orders rows
  6. financial           : sum(line net) + shipping in staging == sum(order_total) in mart
                           captured payments in staging == captured in mart

Writes audit.reconciliation and exits 1 on any mismatch.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from connections import connect  # noqa: E402

from ingestion.common.storage import get_storage  # noqa: E402


def count_rows(storage, key: str) -> int:
    data = storage.read_bytes(key)
    if key.endswith(".gz"):
        data = gzip.decompress(data)
    text = data.decode("utf-8")
    if key.endswith(".csv"):
        return sum(1 for _ in csv.reader(io.StringIO(text))) - 1
    return sum(1 for line in text.splitlines() if line.strip())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lake-root", default="landing")
    ap.add_argument("--target", choices=["local", "snowflake"], default="local")
    ap.add_argument("--db", default="warehouse/shopstream.duckdb")
    ap.add_argument("--report", default="warehouse/reconciliation_report.json")
    a = ap.parse_args()

    storage = get_storage(a.lake_root)        # STORAGE_BACKEND=s3 in AWS
    con, raw = connect(a.target, a.db)        # raw = 'raw' (duckdb) | 'raw_db.raw' (snowflake)
    checks = []

    def add(name, source, target, tolerance=0.0):
        ok = abs(source - target) <= tolerance
        checks.append({"check": name, "source": source, "target": target,
                       "difference": round(source - target, 2), "status": "PASS" if ok else "FAIL"})

    # Snowflake COPY_HISTORY may record the stage-relative or full s3:// path -> normalise to the
    # part after 'dms/' so both forms match the lake key.
    loaded = {}
    for name, n in con.execute(f"select file_name, row_count from {raw}.load_history").fetchall():
        loaded[name.split("dms/", 1)[-1]] = n

    # 1 + 2  DMS CSV files
    lake_files = [k for k in storage.list_keys("dms/") if k.endswith(".csv")]
    add("dms: files landed vs loaded", len(lake_files), sum(1 for k in lake_files if k.split("dms/", 1)[-1] in loaded))
    add("dms: rows in files vs rows loaded", sum(count_rows(storage, k) for k in lake_files),
        sum(loaded.get(k.split("dms/", 1)[-1], 0) for k in lake_files))

    # 3  clickstream: Lambda audit -> raw
    audits = [json.loads(storage.read_bytes(k)) for k in storage.list_keys("_audit/clickstream/")]
    add("clickstream: lambda valid events vs raw rows", sum(x["valid"] for x in audits),
        con.execute(f"select count(*) from {raw}.clickstream_events").fetchone()[0])
    add("clickstream: lines received = valid + quarantined + duplicates",
        sum(x["lines_in"] for x in audits), sum(x["valid"] + x["quarantined"] + x["duplicates_dropped"] for x in audits))

    # 4  payments manifests -> raw
    manifests = [json.loads(storage.read_bytes(k)) for k in storage.list_keys("raw/payments/") if k.endswith("_manifest.json")]
    add("payments: extractor records vs raw rows", sum(m["records"] for m in manifests),
        con.execute(f"select count(*) from {raw}.payments").fetchone()[0])

    # 5  CDC -> mart
    live = con.execute("select count(*) from staging.stg_shop__orders where not is_deleted").fetchone()[0]
    add("orders: live orders in CDC vs fct_orders", live, con.execute("select count(*) from marts.fct_orders").fetchone()[0])
    add("order lines: staging vs fct_order_items",
        con.execute("select count(*) from staging.stg_shop__order_items i join staging.stg_shop__orders o using(order_id) where not o.is_deleted").fetchone()[0],
        con.execute("select count(*) from marts.fct_order_items").fetchone()[0])

    # 6  financial totals
    stg_total = con.execute("""
        select sum(x.line_net + coalesce(o.shipping_fee, 0)) from staging.stg_shop__orders o
        join (select order_id, sum(net_amount) line_net from staging.stg_shop__order_items group by 1) x using (order_id)
        where not o.is_deleted""").fetchone()[0]
    add("revenue: staging order totals vs fct_orders.order_total (INR)", float(stg_total),
        float(con.execute("select sum(order_total) from marts.fct_orders").fetchone()[0]), tolerance=0.01)
    add("payments: captured INR staging vs fct_payments",
        float(con.execute("select sum(amount) from staging.stg_payments__payments where payment_status='captured'").fetchone()[0]),
        float(con.execute("select sum(amount) from marts.fct_payments where payment_status='captured'").fetchone()[0]),
        tolerance=0.01)

    run_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    con.execute("create schema if not exists audit")
    con.execute("""create table if not exists audit.reconciliation (
        run_at varchar, check_name varchar, source double, target double, difference double, status varchar)""")
    con.executemany("insert into audit.reconciliation values (?,?,?,?,?,?)",
                    [(run_at, c["check"], c["source"], c["target"], c["difference"], c["status"]) for c in checks])
    con.close()

    for c in checks:
        print(f"[{c['status']}] {c['check']:<62} source={c['source']:<16,.2f} target={c['target']:<16,.2f}")
    Path(a.report).write_text(json.dumps(checks, indent=2))
    failed = [c for c in checks if c["status"] == "FAIL"]
    print(f"\n{len(checks)} checks | {len(failed)} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
