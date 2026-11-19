-- Both records of every match must end in the same party; if not, the label propagation
-- needs more rounds (var party_resolution_rounds).
select
    match.customer_id,
    match.matched_customer_id,
    record.party_id,
    matched_record.party_id as matched_party_id
from {{ ref('customer_match') }} as match
inner join {{ ref('party_member') }} as record on record.customer_id = match.customer_id
inner join {{ ref('party_member') }} as matched_record on matched_record.customer_id = match.matched_customer_id
where record.party_id <> matched_record.party_id
