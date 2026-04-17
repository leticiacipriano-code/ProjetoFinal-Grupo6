with source AS (
    SELECT * FROM {{ ref('skincare_products_enrichment') }}
),

sanitized_skc_rich AS (
    SELECT
        product_name,
        brand,
        product_type,
        {{ normalize_list_string('clean_ingreds') }} AS ingredients_skc_list,
        price,
        product_url
    FROM source
),

final AS (
    SELECT *
    FROM sanitized_skc_rich
    WHERE ingredients_skc_list ~ '^[^,]+,.*'
),

mapping AS (
    SELECT * 
    FROM {{ref('skincare_label_mapping')}}
)

SELECT
    f.*,
    coalesce(lower(map.Standard), lower(f.product_type)) AS label_mapped
FROM
    final AS f
LEFT JOIN
    mapping AS map
ON
    lower(f.product_type) = lower(map.Raw)