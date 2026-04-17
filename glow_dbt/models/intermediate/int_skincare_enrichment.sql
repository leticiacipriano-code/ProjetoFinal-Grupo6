with skc_base AS (
    SELECT * FROM {{ ref('stg_skincare_products') }} 
),

-- Skincare Base

skincare_base AS (
    SELECT 
        product_name,
        product_type,
        ingredients_skc_list AS ingredients_list,
        price_numeric AS price,
        null AS rank,
        null AS dry,
        null AS oily,
        null AS sensitive,
        'skincare' AS source
    FROM skc_base
),

mapping AS (
    SELECT * 
    FROM {{ref('brand_mapping')}}
),

brand_mapped AS (
    SELECT
        skc.*,
        coalesce(m.brand, 'unknown') as brand
    FROM skincare_base AS skc
    LEFT JOIN mapping AS m
    ON
        lower(skc.product_name) LIKE '%' || m.pattern || '%'

),

-- Skincare Enrichment

sanitized_skc_rich AS (
    SELECT
        product_name,
        brand,
        product_type,
        {{ normalize_list_string('clean_ingreds') }} AS ingredients_list,
        price,
        product_url
    FROM {{ ref('skincare_products_enrichment') }}
),

final_sanitized AS (
    SELECT *
    FROM sanitized_skc_rich
    WHERE ingredients_list ~ '^[^,]+,.*'
)

-- Union

SELECT
    product_name,
    brand,
    product_type,
    ingredients_list,
    price,
    rank,
    dry,
    oily,
    sensitive,
    source
FROM brand_mapped

UNION ALL

SELECT
    product_name,
    brand,
    product_type,
    ingredients_list,
    price,
    null AS rank,
    null AS dry,
    null AS oily,
    null AS sensitive,
    'skincare' AS source
FROM final_sanitized
