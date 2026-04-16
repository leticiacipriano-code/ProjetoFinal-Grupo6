{% macro convert_price_to_numeric(column) %}
    regexp_replace({{column}}, '[^0-9\.]', '', 'g')::numeric(10,2),
{% endmacro %}