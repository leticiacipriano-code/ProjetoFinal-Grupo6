with source_data AS (
    SELECT * FROM {{ source('staging_raw_sources', 'skincare_products') }}
),

sanitized_skc AS (
    SELECT
        product_name,
        product_type,
        {{ normalize_list_string('clean_ingreds') }} AS ingredients_skc_list,
        {{ convert_price_to_numeric('price') }} AS price_numeric,
        product_url
    FROM source_data
),

final_skc AS (
    SELECT *
    FROM 
        sanitized_skc
    WHERE 
        ingredients_skc_list ~ '^[^,]+,.*'
),

mapping AS (
    SELECT * 
    FROM {{ref('brand_mapping')}}
),

brand_mapped AS (
    SELECT
        skc.*,
        coalesce(m.brand, 'unknown') as brand
    FROM sanitized_skc AS skc
    LEFT JOIN mapping AS m
    ON
        lower(skc.product_name) LIKE '%' || m.pattern || '%'

),

type_mapping AS (
    SELECT *
    FROM {{ ref('product_type_mapping') }}
)

SELECT
    bmap.*,
    coalesce(tmap.standardized, lower(bmap.product_type)) AS product_type_mapped
FROM
    brand_mapped AS bmap
LEFT JOIN
    type_mapping AS tmap
ON
    lower(bmap.product_type) = tmap.raw_value
