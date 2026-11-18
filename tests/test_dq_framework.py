import duckdb
from dq_framework import compile_rule, run_rules


def _db():
    con = duckdb.connect()
    con.execute("create schema marts")
    con.execute("create table marts.t as select * from (values (1, 'a', 10.0), (2, 'b', -5.0), (2, null, 3.0)) v(id, status, amount)")
    return con


def test_rule_compilation_and_results():
    con = _db()
    rules = [
        {"name": "uniq", "table": "marts.t", "type": "unique", "column": "id"},
        {"name": "nn", "table": "marts.t", "type": "not_null", "column": "status", "severity": "warning"},
        {"name": "vals", "table": "marts.t", "type": "accepted_values", "column": "status", "values": ["a", "b"]},
        {"name": "rng", "table": "marts.t", "type": "range", "column": "amount", "min": 0, "max": 100},
        {"name": "ratio", "table": "marts.t", "type": "custom_sql", "threshold": 0.5,
         "sql": "select avg(case when amount < 0 then 1.0 else 0 end) from marts.t"},
        {"name": "broken", "table": "marts.t", "type": "custom_sql", "sql": "select * from nope"},
    ]
    res = {r.rule: r for r in run_rules(con, rules, "test")}
    assert res["uniq"].status == "FAIL" and res["uniq"].observed == 1
    assert res["nn"].status == "FAIL" and res["nn"].severity == "warning"
    assert res["vals"].status == "PASS"
    assert res["rng"].status == "FAIL"
    assert res["ratio"].status == "PASS"           # 1/3 <= 0.5
    assert res["broken"].status == "ERROR"         # bad rule doesn't stop the run


def test_freshness_threshold_is_hours():
    sql, thr = compile_rule({"table": "t", "type": "freshness", "column": "ts", "max_age_hours": 6})
    assert thr == 6.0 and "max(ts)" in sql
