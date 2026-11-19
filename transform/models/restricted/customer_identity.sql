{#- Direct identifiers of current customers. Readable by compliance only (ADR 0008): sanctions
    screening and investigations need names, analytics do not. -#}

with current_customers as (
    {{ current_rows(source('bronze', 'corebanking_customer'), 'customer_id') }}
),

named as (
    select
        *,
        coalesce(legal_name, concat_ws(' ', first_name, last_name)) as full_name
    from current_customers
)

select
    customer_id,
    customer_type,
    first_name,
    last_name,
    legal_name,
    full_name,
    {{ name_key('full_name') }} as full_name_key,
    birth_date,
    id_document_type,
    id_document_number,
    {{ document_key('id_document_number') }} as id_document_key,
    tax_id,
    nationality,
    address_line,
    city,
    {{ name_key('city') }} as city_key,
    postal_code,
    phone,
    email,
    {{ lineage_columns() }}
from named
