{#
  prod / local : models land in the schema named in config (STAGING, MARTS, ...)
  dev          : every developer gets isolated schemas, e.g. DBT_SANDHYA_MARTS
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if custom_schema_name is none -%}
    {{ target.schema }}
  {%- elif target.name in ('prod', 'local', 'ci') -%}
    {{ custom_schema_name | trim }}
  {%- else -%}
    {{ target.schema }}_{{ custom_schema_name | trim }}
  {%- endif -%}
{%- endmacro %}
