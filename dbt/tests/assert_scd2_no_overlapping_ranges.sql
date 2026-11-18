-- SCD2 integrity: a customer's versions must not overlap or leave gaps
select customer_id, valid_from, valid_to, next_from
from (
    select customer_id, valid_from, valid_to,
           lead(valid_from) over (partition by customer_id order by valid_from) as next_from
    from {{ ref('dim_customer') }}
) t
where next_from is not null and next_from <> valid_to
