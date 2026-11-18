{# RFM segmentation as of var('reporting_as_of'). Only non-cancelled/returned orders count. #}
with as_of as (
    select cast('{{ var("reporting_as_of") }}' as date) as d
),
base as (
    select
        f.customer_id,
        max(f.order_date)                  as last_order_date,
        min(f.order_date)                  as first_order_date,
        count(*)                           as frequency,
        sum(f.recognised_revenue)          as monetary
    from {{ ref('fct_orders') }} f
    where not f.is_cancelled_or_returned
    group by 1
),
scored as (
    select
        b.*,
        {{ dbt.datediff('b.last_order_date', 'a.d', 'day') }}  as recency_days,
        ntile(5) over (order by {{ dbt.datediff('b.last_order_date', 'a.d', 'day') }} desc) as r_score,
        ntile(5) over (order by b.frequency)                  as f_score,
        ntile(5) over (order by b.monetary)                   as m_score
    from base b cross join as_of a
)
select
    s.customer_id,
    c.customer_sk,
    c.city,
    c.segment                                 as customer_segment,
    s.first_order_date,
    s.last_order_date,
    s.recency_days,
    s.frequency,
    s.monetary,
    round(s.monetary / s.frequency, 2)        as avg_order_value,
    s.r_score, s.f_score, s.m_score,
    cast(s.r_score as {{ dbt.type_string() }}) || cast(s.f_score as {{ dbt.type_string() }}) || cast(s.m_score as {{ dbt.type_string() }}) as rfm_code,
    case
        when s.r_score >= 4 and s.f_score >= 4                    then 'Champions'
        when s.r_score >= 3 and s.f_score >= 3                    then 'Loyal'
        when s.r_score >= 4 and s.f_score <= 2                    then 'New / Promising'
        when s.r_score = 3 and s.f_score <= 2                     then 'Needs Attention'
        when s.r_score <= 2 and s.f_score >= 3                    then 'At Risk'
        else 'Hibernating'
    end                                       as rfm_segment
from scored s
left join {{ ref('dim_customer') }} c on c.customer_id = s.customer_id and c.is_current
