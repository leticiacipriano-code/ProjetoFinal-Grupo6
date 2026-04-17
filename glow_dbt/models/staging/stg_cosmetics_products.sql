with source_data AS (
    SELECT * FROM {{ source('staging_raw_sources', 'cosmetics_products') }}
),

sanitized AS (
    SELECT
        label,
        brand,
        name,
        price,
        rank,
        {{ normalize_list_string('ingredients') }} AS ingredients_list,
        combination,
        dry,
        normal,
        oily,
        sensitive
    FROM source_data
),

final AS (
    SELECT *
    FROM sanitized
    WHERE ingredients_list ~ '^[^,]+,.*'
)

SELECT * FROM final