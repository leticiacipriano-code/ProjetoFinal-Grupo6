with source AS (
    SELECT * FROM {{ ref('sales_data_enrichment') }}
)

SELECT
    Sales_person As sales_person,
    Country AS country,
    Product AS product,
    Date as date,
    EXTRACT(year FROM Date::date) AS sale_year,
    amount AS price,
    Boxes_shipped AS boxes_shipped
FROM source
