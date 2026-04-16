with source_data AS (
    SELECT * FROM {{ source('staging_raw_sources', 'skincare_products') }}
),

sanitized_skc AS (
    SELECT
        product_name,
        product_type,
        {{ normalize_list_string('clean_ingreds') }} AS ingredients_skc_list,
        {{ convert_price_to_numeric('price') }} AS price_in_pounds
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

)

SELECT
    skc.*,
    coalesce(m.brand, 'unknown') as brand
FROM sanitized_skc AS skc
LEFT JOIN mapping AS m
ON
    lower(skc.product_name) LIKE '%' || m.pattern || '%'
