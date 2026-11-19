{#- Matching keys. Each strips what data entry varies and nothing that tells two parties apart. -#}

{#- The payments hub writes the same bank as 010, 0010 or 10. -#}
{% macro normalize_bank_id(column) -%}
    regexp_replace({{ column }}, '^0+(?=.)', '')
{%- endmacro %}

{#- Upper case, accents dropped, everything but letters removed: "El Amrani", "ELAMRANI" and "él-amrani" agree. -#}
{% macro name_key(expression) -%}
    nullif(regexp_replace(upper(normalize({{ expression }}, NFD)), '[^A-Z]', ''), '')
{%- endmacro %}

{#- Identity document numbers keyed in as "AB 123456", "ab123456" or "AB-123456". -#}
{% macro document_key(column) -%}
    nullif(upper(regexp_replace({{ column }}, '[^0-9A-Za-z]', '')), '')
{%- endmacro %}

{% macro name_similarity(left, right) -%}
    1e0 - cast(levenshtein_distance({{ left }}, {{ right }}) as double) / greatest(length({{ left }}), length({{ right }}))
{%- endmacro %}

{#- Distinct name tokens, sorted, with initials dropped: "Ali  al-KARAKI" -> [AL, ALI, KARAKI]. -#}
{% macro name_tokens(expression) -%}
    array_sort(array_distinct(filter(
        regexp_extract_all(upper(normalize({{ expression }}, NFD)), '[A-Z]+'),
        token -> length(token) > 1
    )))
{%- endmacro %}
