{#- Records the outcome of every node of a dbt invocation in audit.dbt_node_run, on the
    staging branch: published with the data it built, so main holds which run built and
    tested each date, with timings, row counts and test failures. A run that fails stays in
    the orchestrator's history only, since its branch is discarded (ADR 0010). -#}
{% macro record_run_results(results) %}
    {%- if not execute or flags.WHICH not in ('run', 'build', 'test', 'seed') or results | length == 0 -%}
        {{ return('') }}
    {%- endif -%}
    {%- set relation = run_results_relation() -%}
    {%- set business_date = business_date() -%}
    {%- set selection = invocation_args_dict.get('selector') or (invocation_args_dict.get('select') or []) | join(' ') -%}
    {%- set rows = [] -%}
    {%- for result in results -%}
        {%- set rows_affected = (result.adapter_response or {}).get('rows_affected') -%}
        {%- do rows.append(
            "(" ~ [
                quoted(var('pipeline_run') or invocation_id),
                quoted(invocation_id),
                quoted(flags.WHICH),
                quoted(selection) if selection else 'cast(null as varchar)',
                ("date '" ~ business_date ~ "'") if business_date else 'cast(null as date)',
                quoted(result.node.unique_id),
                quoted(result.node.resource_type),
                quoted(result.status),
                result.failures if result.failures is not none else 'cast(null as bigint)',
                rows_affected if rows_affected is number else 'cast(null as bigint)',
                result.execution_time,
                "timestamp '" ~ run_started_at.strftime('%Y-%m-%d %H:%M:%S.%f') ~ "'",
                "cast(current_timestamp at time zone 'UTC' as timestamp(6))",
            ] | join(', ') ~ ")"
        ) -%}
    {%- endfor -%}
    {%- call statement('insert_run_results') -%}
        insert into {{ relation }} values {{ rows | join(', ') }}
    {%- endcall -%}
    {{ return('') }}
{% endmacro %}

{#- Created at the start of every invocation, so models reading it never miss it. -#}
{% macro create_run_results_table() %}
    {%- if execute and flags.WHICH in ('run', 'build', 'test', 'seed') -%}
        {%- call statement('create_run_results') -%}
            create table if not exists {{ run_results_relation() }} (
                pipeline_run_id varchar,
                invocation_id varchar,
                command varchar,
                selection varchar,
                business_date date,
                node_id varchar,
                resource_type varchar,
                status varchar,
                failures bigint,
                rows_affected bigint,
                execution_seconds double,
                run_started_at timestamp(6),
                recorded_at timestamp(6)
            )
            with (partitioning = ARRAY['day(run_started_at)'])
        {%- endcall -%}
    {%- endif -%}
    {{ return('') }}
{% endmacro %}

{% macro run_results_relation() -%}
    {{ target.database }}.{{ var('audit_schema') }}.dbt_node_run
{%- endmacro %}

{% macro quoted(value) -%}
    '{{ value | string | replace("'", "''") }}'
{%- endmacro %}
