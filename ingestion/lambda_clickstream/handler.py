"""
Clickstream validator Lambda.

Trigger : S3 ObjectCreated on s3://<raw>/clickstream/dt=YYYY-MM-DD/*.json.gz
          (Kinesis Data Firehose delivers gzipped JSON lines)
Output  : s3://<raw>/clickstream_validated/dt=.../<file>.json.gz   -> Snowpipe auto-ingest
          s3://<raw>/quarantine/clickstream/dt=.../<file>.json.gz   -> bad records + reason
          s3://<raw>/_audit/clickstream/<file>.json                 -> per-file counts (reconciliation)

Rules: valid JSON, required fields present, known event_type, ISO timestamp,
       dedupe on event_id within the file (at-least-once delivery from Firehose).
Cross-file duplicates are handled again in Snowflake / dbt (QUALIFY on event_id).
"""
from __future__ import annotations

import gzip
import json
import sys
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from ingestion.common.logging_utils import get_logger, log  # noqa: E402
from ingestion.common.storage import Storage, get_storage  # noqa: E402

logger = get_logger("clickstream_validator")

REQUIRED = ("event_id", "event_type", "event_ts", "session_id")
EVENT_TYPES = {"page_view", "product_view", "add_to_cart", "checkout_start", "purchase"}


def validate_line(line: str) -> tuple[dict | None, str | None]:
    try:
        ev = json.loads(line)
    except json.JSONDecodeError:
        return None, "malformed_json"
    if not isinstance(ev, dict):
        return None, "not_an_object"
    missing = [f for f in REQUIRED if not ev.get(f)]
    if missing:
        return None, f"missing_fields:{','.join(missing)}"
    if ev["event_type"] not in EVENT_TYPES:
        return None, f"unknown_event_type:{ev['event_type']}"
    try:
        datetime.fromisoformat(ev["event_ts"].replace("Z", "+00:00"))
    except ValueError:
        return None, "bad_timestamp"
    if ev["event_type"] == "purchase" and not (ev.get("properties") or {}).get("order_id"):
        return None, "purchase_without_order_id"
    return ev, None


def process_object(storage: Storage, key: str) -> dict:
    raw = storage.read_bytes(key)
    text = gzip.decompress(raw).decode("utf-8") if key.endswith(".gz") else raw.decode("utf-8")
    good, bad, seen, dups = [], [], set(), 0
    for n, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        ev, err = validate_line(line)
        if err:
            bad.append(json.dumps({"line_no": n, "reason": err, "payload": line[:2000]}))
            continue
        if ev["event_id"] in seen:
            dups += 1
            continue
        seen.add(ev["event_id"])
        good.append(json.dumps(ev, separators=(",", ":")))

    rel = key.split("clickstream/", 1)[1]
    out_key = f"clickstream_validated/{rel}"
    storage.write_bytes(out_key, gzip.compress(("\n".join(good) + "\n").encode()))
    if bad:
        storage.write_bytes(f"quarantine/clickstream/{rel}", gzip.compress(("\n".join(bad) + "\n").encode()))
    audit = {"source_key": key, "validated_key": out_key, "lines_in": len(good) + len(bad) + dups,
             "valid": len(good), "quarantined": len(bad), "duplicates_dropped": dups,
             "processed_at": datetime.now(timezone.utc).isoformat()}
    storage.write_bytes(f"_audit/clickstream/{Path(rel).name}.json", json.dumps(audit).encode())
    log(logger, "file processed", **audit)
    return audit


def lambda_handler(event, context):  # pragma: no cover - AWS entry point
    storage = get_storage()
    results = []
    for rec in event.get("Records", []):
        key = urllib.parse.unquote_plus(rec["s3"]["object"]["key"])
        results.append(process_object(storage, key))
    return {"processed": len(results), "results": results}


if __name__ == "__main__":
    root = sys.argv[1] if len(sys.argv) > 1 else "landing"
    st = get_storage(root)
    totals = {"files": 0, "valid": 0, "quarantined": 0, "duplicates_dropped": 0}
    for k in st.list_keys("clickstream/"):
        a = process_object(st, k)
        totals["files"] += 1
        for f in ("valid", "quarantined", "duplicates_dropped"):
            totals[f] += a[f]
    print(json.dumps(totals, indent=2))
