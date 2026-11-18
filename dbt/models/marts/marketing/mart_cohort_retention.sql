{# Monthly acquisition cohorts: % of customers ordering again N months after their first order #}
with orders as (
    select customer_id, cast({{ dbt.date_trunc('month', 'order_date') }} as date) as order_month
    from {{ ref('fct_orders') }}
    where not is_cancelled_or_returned
    group by 1, 2
),
cohorts as (
    select customer_id, min(order_month) as cohort_month from orders group by 1
),
activity as (
    select
        c.cohort_month,
        {{ dbt.datediff('c.cohort_month', 'o.order_month', 'month') }} as months_since_first,
        count(distinct o.customer_id) as active_customers
    from orders o join cohorts c using (customer_id)
    group by 1, 2
),
sizes as (
    select cohort_month, count(*) as cohort_size from cohorts group by 1
)
select
    a.cohort_month,
    s.cohort_size,
    a.months_since_first,
    a.active_customers,
    round(a.active_customers * 1.0 / s.cohort_size, 4) as retention_rate
from activity a join sizes s using (cohort_month)
