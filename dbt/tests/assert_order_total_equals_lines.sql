-- Fact-to-fact consistency: order header totals equal the sum of their lines
select f.order_id, f.net_item_amount, sum(i.net_amount) as line_total
from {{ ref('fct_orders') }} f
join {{ ref('fct_order_items') }} i using (order_id)
group by 1, 2
having abs(f.net_item_amount - sum(i.net_amount)) > 0.01
