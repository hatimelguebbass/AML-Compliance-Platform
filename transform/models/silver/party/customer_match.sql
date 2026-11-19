{#- Pairs of customer records that belong to the same party, with every rule that links
    them. Deterministic rules, each one explainable to an investigator and measured against
    the ground truth with `make evaluate-parties` (ADR 0008). -#}

with identity as (
    select
        customer_id,
        customer_type,
        full_name_key,
        birth_date,
        -- Day and month swapped at data entry; only a valid date when the day could be a month.
        case
            when day(birth_date) <= 12 and day(birth_date) <> month(birth_date)
                then date_trunc('year', birth_date) + (day(birth_date) - 1) * interval '1' month + (month(birth_date) - 1) * interval '1' day
        end as birth_date_swapped,
        id_document_type,
        id_document_key,
        tax_id,
        city_key
    from {{ ref('customer_identity') }}
),

-- A shared personal document alone is not proof: numbers get keyed in wrong or reused
-- within a family. A similar name or the same birth date must corroborate it.
same_document as (
    select a.customer_id, b.customer_id as matched_customer_id, 'SAME_ID_DOCUMENT_CORROBORATED' as match_rule
    from identity as a
    inner join identity as b
        on b.id_document_type = a.id_document_type
        and b.id_document_key = a.id_document_key
        and b.customer_id > a.customer_id
    -- Trade register (RC) numbers are only unique within one commercial court; the tax id identifies an organization.
    where a.id_document_type in ('CIN', 'PASSPORT')
        and (
            {{ name_similarity('a.full_name_key', 'b.full_name_key') }} >= {{ var('name_similarity_threshold') }}
            or b.birth_date = a.birth_date
        )
),

same_tax_id as (
    select a.customer_id, b.customer_id as matched_customer_id, 'SAME_TAX_ID' as match_rule
    from identity as a
    inner join identity as b
        on b.tax_id = a.tax_id
        and b.customer_id > a.customer_id
),

-- Blocking on birth date and city keeps the comparison to a handful of records per block.
person_candidates as (
    select a.customer_id, b.customer_id as matched_customer_id, a.full_name_key, b.full_name_key as matched_name_key
    from identity as a
    inner join identity as b
        on b.birth_date = a.birth_date
        and b.city_key = a.city_key
        and b.customer_id > a.customer_id
    where a.customer_type = 'PERSON' and b.customer_type = 'PERSON'

    union all

    select
        least(a.customer_id, b.customer_id),
        greatest(a.customer_id, b.customer_id),
        if(a.customer_id < b.customer_id, a.full_name_key, b.full_name_key),
        if(a.customer_id < b.customer_id, b.full_name_key, a.full_name_key)
    from identity as a
    inner join identity as b
        on b.birth_date = a.birth_date_swapped
        and b.city_key = a.city_key
        -- The swap is symmetric: keep each pair once.
        and a.birth_date < b.birth_date
    where a.customer_type = 'PERSON' and b.customer_type = 'PERSON'
),

similar_person as (
    select customer_id, matched_customer_id, 'SIMILAR_NAME_SAME_BIRTH_DATE_AND_CITY' as match_rule
    from person_candidates
    where {{ name_similarity('full_name_key', 'matched_name_key') }} >= {{ var('name_similarity_threshold') }}
),

matches as (
    select * from same_document
    union all
    select * from same_tax_id
    union all
    select * from similar_person
)

select
    customer_id,
    matched_customer_id,
    array_sort(array_distinct(array_agg(match_rule))) as match_rules,
    {{ lineage_columns() }}
from matches
group by customer_id, matched_customer_id
