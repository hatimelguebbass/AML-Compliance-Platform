{#- Current state of the core banking rows in silver against the core banking database
    itself, column by column, read through the read-only corebanking catalog (ADR 0010).

    The lakehouse is as current as the last captured change: source rows modified after
    that watermark, less a grace period for transactions still committing, are in flight
    and left out on both sides. A row deleted in that window still shows as unexpected. -#}

{%- set grace = var('observability').reconciliation_grace_minutes %}
{%- set entities = [
    {
        'entity': 'customer', 'key': 'customer_id', 'silver': ref('customer_history'),
        'change_log': source('bronze', 'corebanking_customer'),
        'columns': {
            'customer_type': 'customer_type', 'gender': 'gender', 'birth_year': 'year(birth_date)',
            'legal_form': 'legal_form', 'incorporation_year': 'year(incorporation_date)',
            'nationality': 'nationality', 'residence_country': 'residence_country', 'city': 'city',
            'occupation': 'occupation', 'risk_rating': 'risk_rating', 'is_pep': 'is_pep',
            'customer_since': 'customer_since',
        },
    },
    {
        'entity': 'account', 'key': 'account_id', 'silver': ref('account_history'),
        'change_log': source('bronze', 'corebanking_account'),
        'columns': {
            'customer_id': 'customer_id', 'bank_id': normalize_bank_id('bank_id'),
            'account_number': 'account_number', 'account_type': 'account_type', 'currency': 'currency',
            'status': 'status', 'opened_on': 'opened_on', 'closed_on': 'closed_on',
        },
    },
    {
        'entity': 'card', 'key': 'card_id', 'silver': ref('card_history'),
        'change_log': source('bronze', 'corebanking_card'),
        'columns': {
            'account_id': 'account_id', 'card_token': 'card_token', 'scheme': 'scheme', 'product': 'product',
            'status': 'status', 'issued_on': 'issued_on', 'expires_on': 'expires_on',
        },
    },
] -%}

{%- for e in entities %}
{%- if not loop.first %}
union all
{% endif %}
{{ reconcile_current_state(e.entity, e.key, source('corebanking', e.entity), e.silver, e.change_log, e.columns, grace) }}
{%- endfor %}
