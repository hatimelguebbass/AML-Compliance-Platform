{#- Party of every current customer record: the connected components of the match graph,
    found by label propagation. A party is identified by its lowest customer_id, the
    surviving record. The rounds are bounded; a test fails if they did not converge. -#}

with edges as (
    select customer_id as node, matched_customer_id as neighbour from {{ ref('customer_match') }}
    union all
    select matched_customer_id, customer_id from {{ ref('customer_match') }}
),

round_0 as (
    select customer_id, customer_id as party_id from {{ ref('customer_identity') }}
)

{%- for round in range(1, var('party_resolution_rounds') + 1) %},

round_{{ round }} as (
    select
        node.customer_id,
        least(node.party_id, coalesce(min(neighbour.party_id), node.party_id)) as party_id
    from round_{{ round - 1 }} as node
    left join edges on edges.node = node.customer_id
    left join round_{{ round - 1 }} as neighbour on neighbour.customer_id = edges.neighbour
    group by node.customer_id, node.party_id
)
{%- endfor %}

select
    customer_id,
    party_id,
    count(*) over (partition by party_id) as party_size,
    {{ lineage_columns() }}
from round_{{ var('party_resolution_rounds') }}
