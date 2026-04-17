
SELECT 
    product_name,
    brand,
    product_type_mapped AS product_type,
    ingredients_skc_list AS ingredients,
    price_numeric AS price,
    null AS rank,
    null AS dry,
    null AS oily,
    null AS sensitive,
    'skincare' AS source
FROM {{ ref('stg_skincare_products') }}

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
FROM {{ ref('stg_cosmetics_products') }}


