{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        properties={'partitioning': "ARRAY['business_date']"},
        pre_hook="{{ replace_business_date() }}",
    )
}}

{#- Record counts and control totals of each dated feed, from the landed file to silver.
    Bronze ingestion reconciles each file with what it wrote (ADR 0005); this follows the
    records on, so a filter or join that drops or repeats rows between layers cannot pass.
    A payments date is the partition its latest accepted file replaced; card files only
    ever add the records not present yet, so a date sums every accepted file. -#}

with payment_files as (
    select business_date, records_written
    from (
        select
            business_date,
            records_written,
            row_number() over (partition by business_date order by processed_at desc) as recency
        from {{ source('audit', 'ingestion_file') }}
        where target_table like '%.payment_transfer' and status = 'ACCEPTED'
            and {{ business_date_filter('business_date') }}
    )
    where recency = 1
),

card_files as (
    select business_date, sum(records_written) as records_written
    from {{ source('audit', 'ingestion_file') }}
    where target_table like '%.card_transaction' and status = 'ACCEPTED'
        and {{ business_date_filter('business_date') }}
    group by business_date
),

bronze_payments as (
    select _business_date as business_date, count(*) as record_count, sum(amount_paid) as amount
    from {{ source('bronze', 'payment_transfer') }}
    where {{ business_date_filter('_business_date') }}
    group by _business_date
),

silver_payments as (
    select business_date, count(*) as record_count, sum(amount_paid) as amount
    from {{ ref('payment_transfer') }}
    where {{ business_date_filter('business_date') }}
    group by business_date
),

bronze_cards as (
    select _business_date as business_date, count(*) as record_count, sum(amount_minor) as amount
    from {{ source('bronze', 'card_transaction') }}
    where {{ business_date_filter('_business_date') }}
    group by _business_date
),

silver_cards as (
    -- Back to minor units, the unit of the processor's control totals.
    select
        cards.business_date,
        count(*) as record_count,
        sum(cards.amount * power(10, currencies.minor_unit)) as amount
    from {{ ref('card_transaction') }} as cards
    left join {{ ref('iso_currency') }} as currencies on currencies.alpha_code = cards.currency
    where {{ business_date_filter('cards.business_date') }}
    group by cards.business_date
),

feeds as (
    select
        'payments' as feed,
        coalesce(files.business_date, bronze.business_date, silver.business_date) as business_date,
        files.records_written as landed_records,
        bronze.record_count as bronze_records,
        silver.record_count as silver_records,
        cast(bronze.amount as decimal(38, 6)) as bronze_amount,
        cast(silver.amount as decimal(38, 6)) as silver_amount
    from payment_files as files
    full outer join bronze_payments as bronze on bronze.business_date = files.business_date
    full outer join silver_payments as silver
        on silver.business_date = coalesce(files.business_date, bronze.business_date)
    union all
    select
        'cards',
        coalesce(files.business_date, bronze.business_date, silver.business_date),
        files.records_written,
        bronze.record_count,
        silver.record_count,
        cast(bronze.amount as decimal(38, 6)),
        cast(silver.amount as decimal(38, 6))
    from card_files as files
    full outer join bronze_cards as bronze on bronze.business_date = files.business_date
    full outer join silver_cards as silver
        on silver.business_date = coalesce(files.business_date, bronze.business_date)
)

select
    business_date,
    cast(feed as varchar) as feed,
    coalesce(landed_records, 0) as landed_records,
    coalesce(bronze_records, 0) as bronze_records,
    coalesce(silver_records, 0) as silver_records,
    coalesce(bronze_amount, 0) as bronze_amount,
    coalesce(silver_amount, 0) as silver_amount,
    coalesce(landed_records, 0) = coalesce(bronze_records, 0)
        and coalesce(bronze_records, 0) = coalesce(silver_records, 0)
        and coalesce(bronze_amount, 0) = coalesce(silver_amount, 0) as is_reconciled,
    {{ lineage_columns() }}
from feeds
