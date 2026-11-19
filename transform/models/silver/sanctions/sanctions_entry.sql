{#- Entries of the latest snapshot of each sanctions list, UN and OFAC in one shape. UN
    listing dates keep their calendar date: the xs:date offset some carry must not shift
    them to the day before (ADR 0005). OFAC does not publish a listing date. -#}

with un_individual as (
    select *
    from {{ source('bronze', 'sanctions_un_individual') }}
    where _retrieved_at = (select max(_retrieved_at) from {{ source('bronze', 'sanctions_un_individual') }})
),

un_entity as (
    select *
    from {{ source('bronze', 'sanctions_un_entity') }}
    where _retrieved_at = (select max(_retrieved_at) from {{ source('bronze', 'sanctions_un_entity') }})
),

ofac as (
    select *
    from {{ source('bronze', 'sanctions_ofac_sdn_entry') }}
    where _retrieved_at = (select max(_retrieved_at) from {{ source('bronze', 'sanctions_ofac_sdn_entry') }})
),

entries as (
    select
        'UN' as list_name,
        reference_number as entry_id,
        'INDIVIDUAL' as entry_type,
        concat_ws(' ', first_name, second_name, third_name, fourth_name) as primary_name,
        array[un_list_type] as programs,
        date(substr(listed_on, 1, 10)) as listed_on,
        nationality."VALUE" as nationalities,
        _list_published as list_published,
        _retrieved_at as retrieved_at
    from un_individual

    union all

    select
        'UN',
        reference_number,
        'ENTITY',
        first_name,
        array[un_list_type],
        date(substr(listed_on, 1, 10)),
        cast(array[] as array(varchar)),
        _list_published,
        _retrieved_at
    from un_entity

    union all

    select
        'OFAC',
        cast(uid as varchar),
        upper(sdntype),
        concat_ws(' ', firstname, lastname),
        programlist.program,
        cast(null as date),
        transform(nationalitylist.nationality, item -> item.country),
        _list_published,
        _retrieved_at
    from ofac
)

select
    list_name,
    entry_id,
    entry_type,
    primary_name,
    coalesce(programs, cast(array[] as array(varchar))) as programs,
    listed_on,
    coalesce(nationalities, cast(array[] as array(varchar))) as nationalities,
    list_published,
    retrieved_at,
    {{ lineage_columns() }}
from entries
