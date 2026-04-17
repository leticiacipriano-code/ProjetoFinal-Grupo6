with source AS (
    SELECT * FROM {{ ref('cosmetics_enrichment') }}
),

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
    FROM source
),

final AS (
    SELECT *
    FROM sanitized
    WHERE ingredients_list ~ '^[^,]+,.*'
),

mapping AS (
    SELECT * 
    FROM {{ref('cosmetics_label_mapping')}}
)

SELECT
    f.*,
    coalesce(lower(map.standard), lower(f.label)) AS label_mapped
FROM
    final AS f
LEFT JOIN
    mapping AS map
ON
    lower(f.label) = lower(map.raw)

