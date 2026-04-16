{% macro normalize_list_string(column) %}
    -- 1. Remove colchetes: [ ou ]
    -- 2. Remove aspas simples: '
    -- 3. O resultado final será apenas
    regexp_replace(
        regex_replace({{ column }}, '[\[\]''*.]', '', 'g'),
        ',\s*', ',', 'g'
    )
{% endmacro %}