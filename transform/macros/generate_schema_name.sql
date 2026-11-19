{#- Layers are namespaces shared by every environment, so a custom schema is used as is
    instead of being prefixed with the target schema. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ (custom_schema_name or target.schema) | trim }}
{%- endmacro %}
