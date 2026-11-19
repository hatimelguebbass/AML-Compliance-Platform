{% macro lineage_columns() -%}
    cast({{ dbt.string_literal(var('pipeline_run') or invocation_id) }} as varchar) as _pipeline_run_id,
    cast(current_timestamp at time zone 'UTC' as timestamp(6)) as _loaded_at
{%- endmacro %}
