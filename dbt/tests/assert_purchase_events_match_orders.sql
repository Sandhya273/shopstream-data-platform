-- Every web/app purchase event must point to a real order (clickstream <-> OLTP integration check)
{{ config(severity = 'warn', warn_if = '>10') }}
select e.event_id, e.order_id
from {{ ref('stg_clickstream__events') }} e
left join {{ ref('stg_shop__orders') }} o using (order_id)
where e.event_type = 'purchase' and o.order_id is null
