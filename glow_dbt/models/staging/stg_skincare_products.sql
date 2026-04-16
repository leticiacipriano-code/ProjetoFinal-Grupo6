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
) 

SELECT * FROM final_skc
