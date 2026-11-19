{#- Record counts of `relation` per business date: one `total` row for every date of
    `dates`, zero included, then one row per bin of each dimension, a dimension being a name
    and the SQL expression that bins a record. -#}
{% macro profile(relation, dataset, dimensions, dates) -%}
    select dates.business_date, '{{ dataset }}' as dataset, 'total' as dimension, 'all' as bin,
        count(records.business_date) as record_count
    from {{ dates }} as dates
    left join {{ relation }} as records on records.business_date = dates.business_date
    group by dates.business_date
    {%- for name, expression in dimensions.items() %}
    union all
    select business_date, '{{ dataset }}', '{{ name }}', cast({{ expression }} as varchar), count(*)
    from {{ relation }}
    group by business_date, cast({{ expression }} as varchar)
    {%- endfor %}
{%- endmacro %}


{#- Saturday and Sunday are the weekend in Morocco. Bank holidays are not modelled: an
    expected low day is acknowledged in the acknowledged_anomaly seed. -#}
{% macro day_kind(business_date) -%}
    if(day_of_week({{ business_date }}) in (6, 7), 'weekend', 'weekday')
{%- endmacro %}


{#- Order-of-magnitude band of an amount, from "<10" to ">=1M". -#}
{% macro amount_band(amount) -%}
    case
        when {{ amount }} is null then null
        when {{ amount }} < 10 then '<10'
        when {{ amount }} < 100 then '10-100'
        when {{ amount }} < 1000 then '100-1K'
        when {{ amount }} < 10000 then '1K-10K'
        when {{ amount }} < 100000 then '10K-100K'
        when {{ amount }} < 1000000 then '100K-1M'
        else '>=1M'
    end
{%- endmacro %}


{#- Business dates a check compares the loaded date with: for each dataset, the
    `baseline_days` loaded dates of the same kind before it that nobody acknowledged as
    anomalous. Payments clear on working days, so weekend volumes are less than half of a
    weekday's: weekends are compared with weekends. A known outage left in the baseline
    would make the next days look normal. -#}
{% macro baseline_dates(profile_relation) -%}
    {%- set settings = var('observability') -%}
    with loaded as (
        select distinct profile.dataset, profile.business_date
        from {{ profile_relation }} as profile
        where profile.dimension = 'total'
            and not exists (
                select 1
                from {{ ref('acknowledged_anomaly') }} as acknowledged
                where acknowledged.dataset = profile.dataset
                    and profile.business_date between acknowledged.from_date and acknowledged.to_date
            )
    ),

    checked as (
        select distinct dataset, business_date
        from {{ profile_relation }}
        where dimension = 'total' and {{ business_date_filter('business_date') }}
    )

    select dataset, business_date, baseline_date
    from (
        select
            checked.dataset,
            checked.business_date,
            loaded.business_date as baseline_date,
            row_number() over (
                partition by checked.dataset, checked.business_date order by loaded.business_date desc
            ) as recency
        from checked
        inner join loaded
            on loaded.dataset = checked.dataset
            and loaded.business_date < checked.business_date
            and {{ day_kind('loaded.business_date') }} = {{ day_kind('checked.business_date') }}
    )
    where recency <= {{ settings.baseline_days }}
{%- endmacro %}



{#- One row comparing the core banking rows of `entity` with the current versions of its
    silver history. `columns` maps each compared silver column to the expression deriving
    it from the source row. Source rows modified after the capture watermark, less
    `grace_minutes` for transactions committed late, are in flight: left out on both sides. -#}
{% macro reconcile_current_state(entity, key, source_relation, silver_relation, change_log, columns, grace_minutes) -%}
    select * from (
        with watermark as (
            select
                max(_source_committed_at) as captured_until,
                max(_source_committed_at) - interval '{{ grace_minutes }}' minute as cutoff
            from {{ change_log }}
        ),

        source_rows as (
            select
                {{ key }},
                {%- for column, expression in columns.items() %}
                {{ expression }} as {{ column }},
                {%- endfor %}
                cast(updated_at at time zone 'UTC' as timestamp(6)) > (select cutoff from watermark) as is_in_flight
            from {{ source_relation }}
        ),

        lakehouse_rows as (
            select {{ key }}, {{ columns.keys() | join(', ') }}
            from {{ silver_relation }}
            where is_current
        ),

        compared as (
            select
                source_rows.{{ key }} is not null as in_source,
                lakehouse_rows.{{ key }} is not null as in_lakehouse,
                coalesce(source_rows.is_in_flight, false) as is_in_flight,
                (
                    {%- for column in columns.keys() %}
                    {{ 'or ' if not loop.first }}cast(source_rows.{{ column }} as varchar) is distinct from cast(lakehouse_rows.{{ column }} as varchar)
                    {%- endfor %}
                ) as differs
            from source_rows
            full outer join lakehouse_rows on lakehouse_rows.{{ key }} = source_rows.{{ key }}
        )

        select
            cast('{{ entity }}' as varchar) as entity,
            (select captured_until from watermark) as captured_until,
            count_if(in_source and not is_in_flight) as source_rows,
            count_if(in_lakehouse) as lakehouse_rows,
            count_if(in_source and not is_in_flight and not in_lakehouse) as missing_in_lakehouse,
            count_if(in_lakehouse and not in_source) as unexpected_in_lakehouse,
            count_if(in_source and in_lakehouse and not is_in_flight and differs) as mismatched,
            count_if(is_in_flight) as in_flight
        from compared
    )
{%- endmacro %}
