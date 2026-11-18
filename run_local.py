"""
Run the whole platform locally, end to end, with no cloud account:

  1. generate source data      (OLTP CDC files, payments API pages, clickstream)
  2. extract payments API      -> Parquet in the lake         (Lambda code, mock transport)
  3. validate clickstream      -> validated / quarantine       (Lambda code, local storage)
  4. load raw                  -> DuckDB RAW schema             (stand-in for COPY INTO / Snowpipe)
  5. dbt build                 -> staging / intermediate / marts + 80+ tests (same models as Snowflake)
  6. incremental re-run        -> proves MERGE models are idempotent
  7. data quality framework    -> audit.dq_results
  8. reconciliation            -> audit.reconciliation
  9. export BI dataset         -> bi/dashboard_data.json

    python run_local.py              # full run
    python run_local.py --skip-generate
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PY = sys.executable


def step(n: int, title: str, cmd: list[str], cwd: Path = ROOT, env: dict | None = None, allow_fail=False):
    print(f"\n=== [{n}] {title} ===", flush=True)
    t = time.perf_counter()
    r = subprocess.run(cmd, cwd=cwd, env={**os.environ, **(env or {})}, text=True, capture_output=True)
    out = (r.stdout or "") + (r.stderr or "")
    lines = [ln for ln in out.splitlines() if ln.strip() and "transient API error" not in ln
             and '"level": "INFO"' not in ln and "dbt v2" not in ln.lower()]
    print("\n".join(lines[-14:]))
    print(f"--> {'OK' if r.returncode == 0 else 'FAILED'} in {time.perf_counter() - t:.1f}s", flush=True)
    if r.returncode != 0 and not allow_fail:
        sys.exit(r.returncode)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-generate", action="store_true")
    ap.add_argument("--target", default="local", choices=["local", "ci"])
    a = ap.parse_args()
    dbt_env = {"DBT_PROFILES_DIR": str(ROOT / "dbt"), "DUCKDB_PATH": str(ROOT / "warehouse" / "shopstream.duckdb")}
    dbt = shutil.which("dbt") or "dbt"

    if not a.skip_generate:
        shutil.rmtree(ROOT / "warehouse", ignore_errors=True)
        step(1, "Generate source data", [PY, "data_generator/generate_data.py", "--out", "landing"])
    step(2, "Extract payments API -> lake (Parquet)", [PY, "ingestion/api_extractor/payments_extractor.py", "--lake-root", "landing"])
    step(3, "Validate clickstream (Lambda logic)", [PY, "ingestion/lambda_clickstream/handler.py", "landing"])
    step(4, "Load raw layer (COPY INTO stand-in)", [PY, "scripts/load_raw_local.py", "--lake-root", "landing"])
    step(5, "dbt seed + snapshot + build (models + tests)", [dbt, "build", "--target", a.target], cwd=ROOT / "dbt", env=dbt_env)
    step(6, "dbt incremental re-run (MERGE idempotency)", [dbt, "run", "--target", a.target, "--select", "config.materialized:incremental"], cwd=ROOT / "dbt", env=dbt_env)
    step(7, "Data quality framework", [PY, "data_quality/dq_framework.py"])
    step(8, "Reconciliation (source -> raw -> marts)", [PY, "data_quality/reconciliation.py"])
    step(9, "Export BI dataset", [PY, "bi/export_dashboard_data.py"])
    print("\nPipeline complete. Warehouse: warehouse/shopstream.duckdb | BI data: bi/dashboard_data.json")


if __name__ == "__main__":
    main()
