with cosmetics_enriched AS (
    SELECT * FROM {{ ref('int_cosmetics_enrichment')}}
),

skincare_enrichment AS (
    SELECT * FROM {{ ref('int_skincare_enrichment') }}
)

SELECT
    product_name,
    brand,
    product_type,
    ingredients_list,
    price,
    rank::text,
    dry::text,
    oily::text,
    sensitive::text,
    source
FROM cosmetics_enriched

UNION ALL

SELECT
    product_name,
    brand,
    product_type,
    ingredients_list,
    price,
    rank::text,
    dry::text,
    oily::text,
    sensitive::text,
    source
FROM skincare_enrichment