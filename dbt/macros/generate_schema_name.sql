{#
    dbt's default generate_schema_name macro prefixes any custom schema with
    the profile's target schema -- e.g. `+schema: marts` in dbt_project.yml
    becomes the literal schema `public_marts`, not `marts`. That surprises
    almost everyone the first time. This override makes the custom schema
    literal, so `staging` and `marts` are exactly what gets created, matching
    what sources.yml and downstream code (e.g. forecasting/train_predict.py)
    expect.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}