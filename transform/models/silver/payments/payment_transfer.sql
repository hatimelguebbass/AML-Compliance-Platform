{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        properties={'partitioning': "ARRAY['business_date']"},
        pre_hook="{{ replace_business_date() }}",
    )
}}

{#- Transfers of the payments hub with both accounts resolved to the core banking. Bank ids
    are normalized here, where every consumer needs the same key (ADR 0004). -#}

with transfers as (
    select *
    from {{ source('bronze', 'payment_transfer') }}
    where {{ business_date_filter('_business_date') }}
),

accounts as (
    select account_id, customer_id, bank_id, account_number
    from {{ ref('account_history') }}
    where is_current
)

select
    transfers.transfer_id,
    transfers._business_date as business_date,
    transfers.booked_at,
    {{ normalize_bank_id('transfers.originator_bank_id') }} as originator_bank_id,
    transfers.originator_account,
    originator.account_id as originator_account_id,
    originator.customer_id as originator_customer_id,
    {{ normalize_bank_id('transfers.beneficiary_bank_id') }} as beneficiary_bank_id,
    transfers.beneficiary_account,
    beneficiary.account_id as beneficiary_account_id,
    beneficiary.customer_id as beneficiary_customer_id,
    transfers.amount_paid,
    transfers.payment_currency,
    transfers.amount_received,
    transfers.receiving_currency,
    transfers.payment_format,
    transfers.payment_currency <> transfers.receiving_currency as is_cross_currency,
    originator.customer_id = beneficiary.customer_id as is_same_customer,
    transfers._source_file,
    {{ lineage_columns() }}
from transfers
left join accounts as originator
    on originator.bank_id = {{ normalize_bank_id('transfers.originator_bank_id') }}
    and originator.account_number = transfers.originator_account
left join accounts as beneficiary
    on beneficiary.bank_id = {{ normalize_bank_id('transfers.beneficiary_bank_id') }}
    and beneficiary.account_number = transfers.beneficiary_account
