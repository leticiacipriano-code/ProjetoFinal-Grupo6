-- Cosmetics from Base:

with cos_base AS (
    SELECT * FROM {{ ref('stg_cosmetics_products') }}
),

cosmetics_base AS (
    SELECT
        name AS product_name,
        brand,
        label AS product_type,
        ingredients_list,
        price,
        rank,
        dry,
        oily,
        sensitive,
        'cosmetics' AS source
    FROM cos_base
),


-- Cosmetics enrichment

sanitized AS (
    SELECT
        Label AS label,
        Brand AS brand,
        Name AS name,
        Price AS price,
        Rank AS rank,
        {{ normalize_list_string('ingredients') }} AS ingredients_list,
        Combination AS combination,
        Dry AS dry,
        Normal AS normal,
        Oily AS oily,
        Sensitive AS sensitive
    FROM {{ ref('cosmetics_enrichment') }}
),

cosmetics_rich AS (
    SELECT *
    FROM sanitized
    WHERE ingredients_list ~ '^[^,]+,.*'
)


SELECT * FROM  cosmetics_base

UNION ALL

SELECT
    name AS product_name,
    brand,
    label AS product_type,
    ingredients_list,
    price,
    rank,
    dry,
    oily,
    sensitive,
    'cosmetics' AS source
FROM cosmetics_rich



