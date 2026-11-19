{#- Current customers whose name matches a name on the UN or OFAC list, for compliance to
    confirm or discard. Candidates share two name tokens or the whole compacted name, which
    keeps the comparison to a few candidates per customer instead of every listed name.
    Weak aliases are too generic to stand alone: only an exact match on them is a hit. -#}

{%- set min_coverage = var('sanctions_min_token_coverage') -%}
{%- set min_similarity = var('sanctions_name_similarity_threshold') %}

with customers as (
    select
        customer_id,
        case customer_type when 'PERSON' then 'INDIVIDUAL' else 'ENTITY' end as entry_type,
        full_name_key as name_key,
        {{ name_tokens('full_name') }} as tokens
    from {{ ref('customer_identity') }}
),

listed as (
    select
        list_name,
        entry_id,
        entry_type,
        name,
        name_type,
        coalesce(alias_quality in ('WEAK', 'LOW'), false) as is_weak_alias,
        name_key,
        {{ name_tokens('name') }} as tokens
    from {{ ref('sanctions_name') }}
    where entry_type in ('INDIVIDUAL', 'ENTITY')
),

customer_token_pairs as (
    select customers.customer_id, customers.entry_type, first.token || ' ' || second.token as token_pair
    from customers
    cross join unnest(customers.tokens) with ordinality as first (token, position)
    cross join unnest(customers.tokens) with ordinality as second (token, position)
    where first.position < second.position
),

listed_token_pairs as (
    select listed.list_name, listed.entry_id, listed.name, listed.entry_type, first.token || ' ' || second.token as token_pair
    from listed
    cross join unnest(listed.tokens) with ordinality as first (token, position)
    cross join unnest(listed.tokens) with ordinality as second (token, position)
    where first.position < second.position
),

candidates as (
    select distinct customer_pairs.customer_id, listed_pairs.list_name, listed_pairs.entry_id, listed_pairs.name
    from customer_token_pairs as customer_pairs
    inner join listed_token_pairs as listed_pairs
        on listed_pairs.token_pair = customer_pairs.token_pair
        and listed_pairs.entry_type = customer_pairs.entry_type

    union

    select customers.customer_id, listed.list_name, listed.entry_id, listed.name
    from customers
    inner join listed
        on listed.name_key = customers.name_key
        and listed.entry_type = customers.entry_type
),

compared as (
    select
        candidates.customer_id,
        listed.list_name,
        listed.entry_id,
        listed.entry_type,
        listed.name as listed_name,
        listed.name_type,
        listed.is_weak_alias,
        customers.name_key = listed.name_key as is_exact,
        {{ name_similarity('customers.name_key', 'listed.name_key') }} as similarity,
        cardinality(array_intersect(customers.tokens, listed.tokens)) as shared_tokens,
        least(cardinality(customers.tokens), cardinality(listed.tokens)) as shorter_tokens,
        greatest(cardinality(customers.tokens), cardinality(listed.tokens)) as longer_tokens
    from candidates
    inner join customers
        on customers.customer_id = candidates.customer_id
    inner join listed
        on listed.list_name = candidates.list_name
        and listed.entry_id = candidates.entry_id
        and listed.name = candidates.name
),

matched as (
    select
        *,
        case
            when is_exact then 'EXACT'
            when is_weak_alias then null
            when shared_tokens = shorter_tokens
                and shared_tokens >= 2
                and cast(shared_tokens as double) / longer_tokens >= {{ min_coverage }} then 'TOKENS'
            when similarity >= {{ min_similarity }} then 'FUZZY'
        end as match_type
    from compared
),

{#- One hit per customer and listed entry: the best of the entry's names. -#}
best as (
    select
        *,
        row_number() over (
            partition by customer_id, list_name, entry_id
            order by is_exact desc, similarity desc, listed_name
        ) as rank
    from matched
    where match_type is not null
)

select
    lower(to_hex(md5(to_utf8(cast(best.customer_id as varchar) || '|' || best.list_name || '|' || best.entry_id)))) as hit_id,
    best.customer_id,
    members.party_id,
    best.list_name,
    best.entry_id,
    best.entry_type,
    best.listed_name,
    best.name_type,
    best.match_type,
    round(best.similarity, 4) as name_similarity,
    {{ lineage_columns() }}
from best
left join {{ ref('party_member') }} as members
    on members.customer_id = best.customer_id
where best.rank = 1
