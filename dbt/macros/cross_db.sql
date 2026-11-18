{#
  Cross-database helpers so the SAME dbt models run on Snowflake (prod) and
  DuckDB (local dev / CI). Each macro uses adapter.dispatch, so Snowflake gets
  native VARIANT path syntax and LATERAL FLATTEN.
#}

{# ---- read a scalar from a semi-structured column, returned as varchar ---- #}
{% macro json_str(col, path) -%}
  {{ return(adapter.dispatch('json_str')(col, path)) }}
{%- endmacro %}

{% macro snowflake__json_str(col, path) -%}
  {{ col }}:{{ path }}::varchar
{%- endmacro %}

{% macro default__json_str(col, path) -%}
  json_extract_string({{ col }}, '$.{{ path }}')
{%- endmacro %}


{# ---- typed read: json_get(raw, 'amount', 'decimal(12,2)') ---- #}
{% macro json_get(col, path, dtype) -%}
  {{ dbt.safe_cast(json_str(col, path), dtype) }}
{%- endmacro %}


{# ---- ISO-8601 string (2026-08-01T10:15:00.123Z / +05:30) -> timestamp (wall clock) ---- #}
{% macro iso_to_ts(expr) -%}
  {{ dbt.safe_cast("replace(substr(" ~ expr ~ ", 1, 19), 'T', ' ')", dbt.type_timestamp()) }}
{%- endmacro %}


{# ---- explode a JSON array into rows: use inside FROM after the parent relation ----
     select ... from parent p {{ flatten_json_array('p.raw', 'properties.items', 'it') }}
     then read elements with json_str('it.value', 'product_id')                       #}
{% macro flatten_json_array(col, path, alias) -%}
  {{ return(adapter.dispatch('flatten_json_array')(col, path, alias)) }}
{%- endmacro %}

{% macro snowflake__flatten_json_array(col, path, alias) -%}
  , lateral flatten(input => {{ col }}:{{ path }}) as {{ alias }}
{%- endmacro %}

{% macro default__flatten_json_array(col, path, alias) -%}
  , unnest(cast(json_extract({{ col }}, '$.{{ path }}') as json[])) as {{ alias }}(value)
{%- endmacro %}


{# ---- incremental strategy per platform ---- #}
{% macro incremental_strategy() -%}
  {{ return('merge' if target.type == 'snowflake' else 'delete+insert') }}
{%- endmacro %}


{# ---- surrogate key (md5 of concatenated, null-safe columns) ---- #}
{% macro surrogate_key(cols) -%}
  md5(cast(
    {%- for c in cols %}
      coalesce(cast({{ c }} as {{ dbt.type_string() }}), '_null_'){% if not loop.last %} || '|' || {% endif %}
    {%- endfor %}
  as {{ dbt.type_string() }}))
{%- endmacro %}


{# ---- regex match (Snowflake REGEXP_LIKE matches the whole string) ---- #}
{% macro regex_like(col, pattern) -%}
  {{ return(adapter.dispatch('regex_like')(col, pattern)) }}
{%- endmacro %}

{% macro snowflake__regex_like(col, pattern) -%}
  regexp_like({{ col }}, '{{ pattern }}')
{%- endmacro %}

{% macro default__regex_like(col, pattern) -%}
  regexp_full_match({{ col }}, '{{ pattern }}')
{%- endmacro %}
