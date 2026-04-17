
-- Enriched Sales


with source AS (
    SELECT * FROM {{ ref('sales_data_enrichment') }}
),

sales_rich AS (
    SELECT
        Sales_person As sales_person,
        Country AS country,
        Product AS product,
        date::text,
        amount AS price,
        Boxes_shipped AS boxes_shipped
    FROM source
),

sales_base AS (
    SELECT * FROM {{ ref('stg_sales_data') }}
)

SELECT * FROM sales_base

UNION ALL

SELECT 
    sales_person,
    country,
    product,
    date::text,
    price,
    boxes_shipped
FROM sales_rich



