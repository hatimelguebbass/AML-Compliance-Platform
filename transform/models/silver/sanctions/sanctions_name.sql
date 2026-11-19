{#- Every name a sanctioned party is listed under, primary and aliases, with the key
    customer names are screened against. Alias quality is kept: a weak alias alone is not
    enough to block a payment. -#}

with entries as (
    select list_name, entry_id, entry_type, primary_name
    from {{ ref('sanctions_entry') }}
),

un_aliases as (
    select 'UN' as list_name, listed.reference_number as entry_id, alias.alias_name as name, alias.quality
    from {{ source('bronze', 'sanctions_un_individual') }} as listed
    cross join unnest(listed.individual_alias) as alias (quality, alias_name)
    where listed._retrieved_at = (select max(_retrieved_at) from {{ source('bronze', 'sanctions_un_individual') }})

    union all

    select 'UN', listed.reference_number, alias.alias_name, alias.quality
    from {{ source('bronze', 'sanctions_un_entity') }} as listed
    cross join unnest(listed.entity_alias) as alias (quality, alias_name)
    where listed._retrieved_at = (select max(_retrieved_at) from {{ source('bronze', 'sanctions_un_entity') }})
),

ofac_aliases as (
    select
        'OFAC' as list_name,
        cast(listed.uid as varchar) as entry_id,
        concat_ws(' ', alias.first_name, alias.last_name) as name,
        alias.category as quality
    from {{ source('bronze', 'sanctions_ofac_sdn_entry') }} as listed
    cross join unnest(listed.akalist.aka) as alias (uid, alias_type, category, last_name, first_name)
    where listed._retrieved_at = (select max(_retrieved_at) from {{ source('bronze', 'sanctions_ofac_sdn_entry') }})
),

names as (
    select list_name, entry_id, primary_name as name, 'PRIMARY' as name_type, cast(null as varchar) as alias_quality
    from entries

    union all

    select list_name, entry_id, name, 'ALIAS', upper(nullif(quality, ''))
    from un_aliases

    union all

    select list_name, entry_id, name, 'ALIAS', upper(nullif(quality, ''))
    from ofac_aliases
)

select distinct
    names.list_name,
    names.entry_id,
    entries.entry_type,
    names.name,
    names.name_type,
    names.alias_quality,
    {{ name_key('names.name') }} as name_key,
    {{ lineage_columns() }}
from names
inner join entries
    on entries.list_name = names.list_name
    and entries.entry_id = names.entry_id
where nullif(trim(names.name), '') is not null
