"""
Exports the gold marts into one JSON document that feeds the BI dashboard.
(Power BI connects to the same MARTS tables directly - see docs/powerbi_guide.md.)
"""
from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import duckdb

QUERIES = {
    "kpis": """
        with cur as (select * from marts.fct_orders where order_date between date '2025-11-01' and date '2026-10-31'),
             prev as (select * from marts.fct_orders where order_date between date '2024-11-01' and date '2025-10-31')
        select
          (select sum(recognised_revenue) from cur)                                     as net_revenue,
          (select sum(recognised_revenue) from prev)                                    as net_revenue_prev,
          (select count(*) from cur where not is_cancelled_or_returned)                 as orders,
          (select count(*) from prev where not is_cancelled_or_returned)                as orders_prev,
          (select sum(recognised_revenue) / count(*) from cur where not is_cancelled_or_returned) as aov,
          (select sum(recognised_revenue) / count(*) from prev where not is_cancelled_or_returned) as aov_prev,
          (select sum(gross_margin) / sum(recognised_revenue) from cur)                 as margin_pct,
          (select sum(gross_margin) / sum(recognised_revenue) from prev)                as margin_pct_prev,
          (select count(distinct customer_id) from cur where not is_cancelled_or_returned) as active_customers,
          (select count(distinct customer_id) from prev where not is_cancelled_or_returned) as active_customers_prev,
          (select avg(case when order_status = 'returned' then 1.0 else 0 end) from cur) as return_rate,
          (select avg(case when order_status = 'returned' then 1.0 else 0 end) from prev) as return_rate_prev
    """,
    "monthly": """
        select strftime(month_start, '%Y-%m') as month, sum(orders - cancelled_orders - returned_orders) as orders,
               sum(net_revenue) as net_revenue, sum(gross_margin) as gross_margin,
               sum(new_customer_orders) as new_customer_orders, sum(discounts) as discounts
        from marts.mart_daily_sales group by 1 order by 1
    """,
    "monthly_channel": """
        select strftime(month_start, '%Y-%m') as month, channel, sum(net_revenue) as net_revenue
        from marts.mart_daily_sales group by 1, 2 order by 1, 2
    """,
    "daily_last_90": """
        select order_date, sum(orders) as orders, sum(net_revenue) as net_revenue
        from marts.mart_daily_sales where order_date > date '2026-10-31' - 90 group by 1 order by 1
    """,
    "category": """
        select category, sum(net_revenue) as net_revenue, sum(gross_margin) as gross_margin,
               sum(gross_margin) / nullif(sum(net_revenue), 0) as margin_pct,
               sum(units_sold) as units, sum(return_rate * units_sold) / nullif(sum(units_sold), 0) as return_rate
        from marts.mart_product_performance group by 1 order by 2 desc
    """,
    "top_products": """
        select product_name, category, units_sold, net_revenue, gross_margin, return_rate
        from marts.mart_product_performance order by net_revenue desc limit 10
    """,
    "funnel": """
        select sum(sessions) as sessions, sum(product_view_sessions) as product_view,
               sum(add_to_cart_sessions) as add_to_cart, sum(checkout_sessions) as checkout,
               sum(purchase_sessions) as purchase
        from marts.mart_funnel_daily
    """,
    "funnel_by_device": """
        select device_type, sum(sessions) as sessions, sum(purchase_sessions) as purchases,
               sum(purchase_sessions) * 1.0 / sum(sessions) as conversion_rate
        from marts.mart_funnel_daily group by 1 order by 2 desc
    """,
    "traffic_sources": """
        select utm_source, sum(sessions) as sessions, sum(purchase_sessions) as purchases,
               sum(purchase_sessions) * 1.0 / sum(sessions) as conversion_rate, sum(purchase_revenue) as revenue
        from marts.mart_funnel_daily group by 1 order by revenue desc
    """,
    "rfm": """
        select rfm_segment, count(*) as customers, sum(monetary) as revenue,
               avg(recency_days) as avg_recency_days, avg(frequency) as avg_orders
        from marts.mart_customer_rfm group by 1 order by revenue desc
    """,
    "cohorts": """
        select strftime(cohort_month, '%Y-%m') as cohort, cohort_size, months_since_first, retention_rate
        from marts.mart_cohort_retention
        where cohort_month between date '2025-04-01' and date '2026-03-01' and months_since_first <= 6
        order by 1, 3
    """,
    "cities": """
        select c.city, count(*) as orders, sum(f.recognised_revenue) as net_revenue
        from marts.fct_orders f join marts.dim_customer c using (customer_sk)
        group by 1 order by 3 desc
    """,
    "payment_methods": """
        select payment_method, count(*) as payments, sum(amount) as amount
        from marts.fct_payments where payment_status = 'captured' group by 1 order by 3 desc
    """,
    "recon_status": """
        select recon_status, count(*) as orders, sum(abs(capture_variance)) as abs_variance
        from marts.mart_payment_reconciliation group by 1 order by 2 desc
    """,
    "recon_exceptions": """
        select order_id, order_date, order_status, payment_method, order_total, captured_amount, capture_variance, recon_status
        from marts.mart_payment_reconciliation
        where recon_status in ('amount_mismatch', 'missing_payment')
        order by order_date desc limit 25
    """,
    "dq_results": """
        select rule, table_name, rule_type, severity, observed, threshold, status
        from audit.dq_results where run_id = (select max(run_id) from audit.dq_results) order by severity, rule
    """,
    "reconciliation": """
        select check_name, source, target, difference, status
        from audit.reconciliation where run_at = (select max(run_at) from audit.reconciliation)
    """,
    "layer_counts": """
        select 'raw.dms_orders (CDC rows)' as object, count(*) as row_count from raw.dms_orders
        union all select 'staging.stg_shop__orders', count(*) from staging.stg_shop__orders
        union all select 'marts.fct_orders', count(*) from marts.fct_orders
        union all select 'raw.clickstream_events', count(*) from raw.clickstream_events
        union all select 'marts.fct_sessions', count(*) from marts.fct_sessions
        union all select 'raw.payments', count(*) from raw.payments
        union all select 'marts.fct_payments', count(*) from marts.fct_payments
        union all select 'marts.dim_customer (SCD2 versions)', count(*) from marts.dim_customer
    """,
}


def clean(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    if isinstance(v, float):
        return round(v, 6)
    return v


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="warehouse/shopstream.duckdb")
    ap.add_argument("--out", default="bi/dashboard_data.json")
    a = ap.parse_args()
    con = duckdb.connect(a.db, read_only=True)
    data = {}
    for name, sql in QUERIES.items():
        cur = con.execute(sql)
        cols = [d[0] for d in cur.description]
        data[name] = [{c: clean(v) for c, v in zip(cols, row)} for row in cur.fetchall()]
    data["kpis"] = data["kpis"][0]
    data["funnel"] = data["funnel"][0]
    data["generated_at"] = datetime.now().isoformat(timespec="seconds")
    data["period"] = {"current": "Nov 2025 - Oct 2026", "previous": "Nov 2024 - Oct 2025"}
    Path(a.out).write_text(json.dumps(data, indent=1))
    print(f"wrote {a.out} ({', '.join(f'{k}:{len(v) if isinstance(v, list) else 1}' for k, v in data.items())})")


if __name__ == "__main__":
    main()
