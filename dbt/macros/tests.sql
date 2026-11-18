{# Generic tests used in schema.yml files #}

{% test non_negative(model, column_name) %}
select * from {{ model }} where {{ column_name }} < 0
{% endtest %}

{% test valid_email(model, column_name) %}
select * from {{ model }}
where {{ column_name }} is not null
  and not {{ regex_like(column_name, '^[^@ ]+@[^@ ]+[.][^@ ]+$') }}
{% endtest %}

{# Row-count reconciliation between two relations, with tolerance #}
{% test row_count_matches(model, compare_model, tolerance_pct=0) %}
with a as (select count(*) as n from {{ model }}),
     b as (select count(*) as n from {{ compare_model }})
select a.n as model_rows, b.n as compare_rows
from a cross join b
where abs(a.n - b.n) > b.n * {{ tolerance_pct }} / 100.0
{% endtest %}

