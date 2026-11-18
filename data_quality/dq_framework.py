"""
Config-driven data quality framework.

  python data_quality/dq_framework.py --rules data_quality/rules --db warehouse/shopstream.duckdb

* Rules are YAML (data_quality/rules/*.yml) -> no code change to add a check
* Each rule compiles to ONE SQL returning a single number (failures / ratio)
* Results are appended to audit.dq_results (run_id, rule, observed, status) for trend dashboards
* Exit code 1 if any CRITICAL rule fails -> Step Functions marks the run failed and alerts
* Warehouse-agnostic: pass a DB-API connection (duckdb locally, snowflake-connector in AWS)
"""
from __future__ import annotations

import argparse
import json
import sys
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent))
from connections import connect  # noqa: E402


@dataclass
class RuleResult:
    run_id: str
    rule: str
    table: str
    rule_type: str
    severity: str
    observed: float
    threshold: float
    status: str  # PASS / FAIL / ERROR
    detail: str
    checked_at: str


def compile_rule(r: dict) -> tuple[str, float]:
    """Return (sql producing one number, threshold). Rule passes if number <= threshold."""
    t, c, kind = r["table"], r.get("column"), r["type"]
    thr = float(r.get("threshold", 0))
    if kind == "not_null":
        return f"select count(*) from {t} where {c} is null", thr
    if kind == "unique":
        return f"select count(*) from (select {c} from {t} where {c} is not null group by 1 having count(*) > 1) d", thr
    if kind == "accepted_values":
        vals = ", ".join("'" + str(v).replace("'", "''") + "'" for v in r["values"])
        return f"select count(*) from {t} where {c} is not null and {c} not in ({vals})", thr
    if kind == "range":
        return f"select count(*) from {t} where {c} < {r['min']} or {c} > {r['max']}", thr
    if kind == "freshness":
        as_of = f"cast('{r['as_of']}' as timestamp)" if r.get("as_of") else "current_timestamp"
        # hours between newest record and as_of; passes when <= max_age_hours
        return (f"select (extract(epoch from {as_of}) - extract(epoch from max({c}))) / 3600.0 from {t}",
                float(r["max_age_hours"]))
    if kind == "row_count_min":
        return f"select case when count(*) >= {r['min_rows']} then 0 else 1 end from {t}", 0
    if kind == "custom_sql":
        return r["sql"], thr
    raise ValueError(f"unknown rule type {kind}")


def run_rules(con, rules: list[dict], run_id: str) -> list[RuleResult]:
    out = []
    for r in rules:
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        try:
            sql, thr = compile_rule(r)
            val = con.execute(sql).fetchone()[0]
            val = float(val or 0)
            status = "PASS" if val <= thr else "FAIL"
            out.append(RuleResult(run_id, r["name"], r["table"], r["type"], r.get("severity", "critical"),
                                  round(val, 6), thr, status, r.get("description", ""), now))
        except Exception as e:  # a broken rule must not hide the others
            out.append(RuleResult(run_id, r["name"], r["table"], r["type"], r.get("severity", "critical"),
                                  -1, 0, "ERROR", str(e)[:300], now))
    return out


def persist(con, results: list[RuleResult]):
    con.execute("create schema if not exists audit")
    con.execute("""create table if not exists audit.dq_results (
        run_id varchar, rule varchar, table_name varchar, rule_type varchar, severity varchar,
        observed double, threshold double, status varchar, detail varchar, checked_at varchar)""")
    con.executemany("insert into audit.dq_results values (?,?,?,?,?,?,?,?,?,?)",
                    [tuple(asdict(r).values()) for r in results])


def load_rules(path: Path) -> list[dict]:
    files = [path] if path.is_file() else sorted(path.glob("*.yml"))
    return [rule for f in files for rule in yaml.safe_load(f.read_text())["rules"]]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rules", default="data_quality/rules")
    ap.add_argument("--target", choices=["local", "snowflake"], default="local")
    ap.add_argument("--db", default="warehouse/shopstream.duckdb")
    ap.add_argument("--report", default="warehouse/dq_report.json")
    a = ap.parse_args()

    con, _ = connect(a.target, a.db)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:6]
    results = run_rules(con, load_rules(Path(a.rules)), run_id)
    persist(con, results)
    con.close()

    critical_fail = [r for r in results if r.status != "PASS" and r.severity == "critical"]
    for r in results:
        flag = {"PASS": "  ok ", "FAIL": " FAIL", "ERROR": "ERROR"}[r.status]
        print(f"[{flag}] {r.severity:<8} {r.rule:<38} observed={r.observed:<12g} threshold={r.threshold:g}")
    Path(a.report).write_text(json.dumps([asdict(r) for r in results], indent=2))
    print(f"\n{len(results)} rules | {sum(r.status == 'PASS' for r in results)} passed | "
          f"{len(critical_fail)} critical failures")
    return 1 if critical_fail else 0


if __name__ == "__main__":
    sys.exit(main())
