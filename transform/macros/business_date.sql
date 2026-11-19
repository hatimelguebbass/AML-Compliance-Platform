{#- Dated models load one business date per pipeline run and replace it when the date is
    reloaded. The delete is on the partition column, so Iceberg drops whole files. -#}

{% macro business_date() -%}
    {%- set value = var('business_date') -%}
    {%- if value is not none and not modules.re.fullmatch('[0-9]{4}-[0-9]{2}-[0-9]{2}', value | string) -%}
        {{ exceptions.raise_compiler_error("business_date must be YYYY-MM-DD, got " ~ value) }}
    {%- endif -%}
    {{ return(value) }}
{%- endmacro %}

{% macro business_date_filter(column) -%}
    {%- if business_date() is none -%}
        true
    {%- else -%}
        {{ column }} = date '{{ business_date() }}'
    {%- endif -%}
{%- endmacro %}

{% macro replace_business_date(column='business_date') -%}
    {%- if is_incremental() -%}
        delete from {{ this }} where {{ business_date_filter(column) }}
    {%- endif -%}
{%- endmacro %}

{#- The loaded date and the days before it that rules looking back over history need. -#}
{% macro business_date_window(column, days_before) -%}
    {%- if business_date() is none -%}
        true
    {%- else -%}
        {{ column }} between date '{{ business_date() }}' - interval '{{ days_before }}' day and date '{{ business_date() }}'
    {%- endif -%}
{%- endmacro %}
