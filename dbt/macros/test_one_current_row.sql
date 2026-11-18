{# Every customer must have exactly one is_current version #}
{% test dbt_utils_free_one_current_row_per_customer(model) %}
select customer_id, sum(case when is_current then 1 else 0 end) as current_rows
from {{ model }}
group by 1
having sum(case when is_current then 1 else 0 end) <> 1
{% endtest %}
