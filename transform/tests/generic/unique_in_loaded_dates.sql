{#- Uniqueness over the business dates the run loaded: other dates were tested when they
    were loaded, and a full-table scan of every date does not fit the query memory. -#}
{% test unique_in_loaded_dates(model, column_name) %}

select {{ column_name }}, count(*) as occurrences
from {{ model }}
where {{ business_date_filter('business_date') }}
group by {{ column_name }}
having count(*) > 1

{% endtest %}
